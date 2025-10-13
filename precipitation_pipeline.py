#!/usr/bin/env python3
"""
Precipitation indices pipeline for xclim-timber.
Efficiently processes precipitation-based climate indices using Zarr streaming.
Calculates 13 precipitation indices including extremes, consecutive events, intensity, and enhanced analysis (Phase 6).
"""

import logging
import sys
from pathlib import Path
from typing import Dict
import threading

import xarray as xr
import xclim.indicators.atmos as atmos

from core import BasePipeline, PipelineConfig, BaselineLoader, PipelineCLI, SpatialTilingMixin

logger = logging.getLogger(__name__)


class PrecipitationPipeline(BasePipeline, SpatialTilingMixin):
    """
    Memory-efficient precipitation indices pipeline using Zarr streaming.
    Processes 13 precipitation indices without loading full dataset into memory.

    Indices:
    - Basic (6): prcptot, rx1day, rx5day, sdii, cdd, cwd
    - Extreme (2): r95p, r99p (percentile-based using 1981-2000 baseline)
    - Threshold (2): r10mm, r20mm (fixed thresholds)
    - Enhanced Phase 6 (3): dry_days, wetdays, wetdays_prop
    """

    def __init__(self, n_tiles: int = 4, **kwargs):
        """
        Initialize the pipeline with parallel spatial tiling.

        Args:
            n_tiles: Number of spatial tiles (2, 4, or 8, default: 4 for quadrants)
            **kwargs: Additional arguments passed to BasePipeline (chunk_years, enable_dashboard)
        """
        # Initialize BasePipeline
        BasePipeline.__init__(
            self,
            zarr_paths={'precipitation': PipelineConfig.PRECIP_ZARR},
            chunk_config=PipelineConfig.DEFAULT_CHUNKS,
            **kwargs
        )

        # Initialize SpatialTilingMixin
        SpatialTilingMixin.__init__(self, n_tiles=n_tiles)

        # Load baseline percentiles for extreme indices
        self.baseline_loader = BaselineLoader()
        self.baselines = self.baseline_loader.get_precipitation_baselines()

        # Thread lock for baseline access (fixes data race in parallel tile processing)
        self.baseline_lock = threading.Lock()

    def _preprocess_datasets(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """
        Preprocess precipitation datasets (rename variables, fix units).

        Args:
            datasets: Dictionary with 'precipitation' dataset

        Returns:
            Preprocessed datasets dictionary
        """
        precip_ds = datasets['precipitation']

        # Rename precipitation variable for xclim compatibility
        precip_ds = self._rename_variables(precip_ds, PipelineConfig.PRECIP_RENAME_MAP)

        # Fix units for precipitation variable
        precip_ds = self._fix_units(precip_ds, PipelineConfig.PRECIP_UNIT_FIXES)

        # Add CF standard name
        if 'pr' in precip_ds:
            precip_ds['pr'].attrs['standard_name'] = PipelineConfig.CF_STANDARD_NAMES.get(
                'pr', 'precipitation_flux'
            )

        datasets['precipitation'] = precip_ds
        return datasets

    def calculate_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Calculate all precipitation indices for a single spatial region.

        Combines four calculation methods:
        - Basic precipitation indices (6 indices)
        - Extreme percentile-based indices (2 indices)
        - Threshold indices (2 indices)
        - Enhanced precipitation indices (3 indices)

        Args:
            datasets: Dictionary with 'precipitation' dataset

        Returns:
            Dictionary of calculated indices
        """
        ds = datasets['precipitation']

        # Calculate all four types of indices
        basic_indices = self.calculate_precipitation_indices(ds)
        extreme_indices = self.calculate_extreme_indices(ds, self.baselines)
        threshold_indices = self.calculate_threshold_indices(ds)
        enhanced_indices = self.calculate_enhanced_precipitation_indices(ds)

        # Combine all indices
        all_indices = {**basic_indices, **extreme_indices, **threshold_indices, **enhanced_indices}
        return all_indices

    def calculate_precipitation_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate precipitation-based climate indices.

        Args:
            ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated indices
        """
        indices = {}

        if 'pr' in ds:
            logger.info("  - Calculating total precipitation...")
            indices['prcptot'] = atmos.wet_precip_accumulation(
                ds.pr, thresh='1 mm/day', freq='YS'
            )
            logger.info("  - Calculating max 1-day precipitation...")
            indices['rx1day'] = atmos.max_1day_precipitation_amount(ds.pr, freq='YS')
            logger.info("  - Calculating max 5-day precipitation...")
            indices['rx5day'] = atmos.max_n_day_precipitation_amount(
                ds.pr, window=5, freq='YS'
            )
            logger.info("  - Calculating consecutive dry days...")
            indices['cdd'] = atmos.maximum_consecutive_dry_days(
                ds.pr, thresh='1 mm/day', freq='YS'
            )
            logger.info("  - Calculating consecutive wet days...")
            indices['cwd'] = atmos.maximum_consecutive_wet_days(
                ds.pr, thresh='1 mm/day', freq='YS'
            )
            logger.info("  - Calculating daily precipitation intensity...")
            indices['sdii'] = atmos.daily_pr_intensity(
                ds.pr, thresh='1 mm/day', freq='YS'
            )

        return indices

    def calculate_extreme_indices(self, ds: xr.Dataset, baseline_percentiles: dict) -> dict:
        """
        Calculate percentile-based extreme precipitation indices.

        Uses pre-calculated baseline percentiles (1981-2000) on wet days.

        Args:
            ds: Dataset with precipitation variable (pr)
            baseline_percentiles: Dictionary of baseline percentile thresholds

        Returns:
            Dictionary of calculated extreme indices
        """
        indices = {}

        if 'pr' not in ds:
            logger.warning("No precipitation variable found for extreme indices")
            return indices

        if not baseline_percentiles:
            logger.warning("No baseline percentiles loaded, skipping extreme indices")
            return indices

        # r95p: Very wet days (days above 95th percentile of wet days)
        if 'pr95p_threshold' in baseline_percentiles:
            logger.info("  - Calculating r95p (very wet days)...")
            indices['r95p'] = atmos.days_over_precip_doy_thresh(
                pr=ds.pr,
                pr_per=baseline_percentiles['pr95p_threshold'],
                thresh='1 mm/day',
                freq='YS'
            )

        # r99p: Extremely wet days (days above 99th percentile of wet days)
        if 'pr99p_threshold' in baseline_percentiles:
            logger.info("  - Calculating r99p (extremely wet days)...")
            indices['r99p'] = atmos.days_over_precip_doy_thresh(
                pr=ds.pr,
                pr_per=baseline_percentiles['pr99p_threshold'],
                thresh='1 mm/day',
                freq='YS'
            )

        return indices

    def calculate_threshold_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate fixed-threshold precipitation indices.

        Args:
            ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated threshold indices
        """
        indices = {}

        if 'pr' not in ds:
            logger.warning("No precipitation variable found for threshold indices")
            return indices

        # r10mm: Heavy precipitation days (>= 10mm)
        logger.info("  - Calculating r10mm (heavy precipitation days)...")
        indices['r10mm'] = atmos.wetdays(
            pr=ds.pr,
            thresh='10 mm/day',
            freq='YS'
        )

        # r20mm: Very heavy precipitation days (>= 20mm)
        logger.info("  - Calculating r20mm (very heavy precipitation days)...")
        indices['r20mm'] = atmos.wetdays(
            pr=ds.pr,
            thresh='20 mm/day',
            freq='YS'
        )

        return indices

    def calculate_enhanced_precipitation_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate enhanced precipitation analysis indices (Phase 6).

        Adds 3 new distinct indices complementing the existing 10:
        - dry_days: Total number of dry days (< 1mm)
        - wetdays: Total number of wet days (>= 1mm)
        - wetdays_prop: Proportion of days that are wet

        Note: Spell frequency indices would require custom temporal logic
        beyond standard xclim functions and are deferred to future phases.

        Args:
            ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated enhanced precipitation indices
        """
        indices = {}

        if 'pr' not in ds:
            logger.warning("No precipitation variable found for enhanced precipitation indices")
            return indices

        # dry_days: Total number of dry days (pr < 1mm)
        logger.info("  - Calculating dry_days (total dry days < 1mm)...")
        indices['dry_days'] = atmos.dry_days(
            pr=ds.pr,
            thresh='1 mm/day',
            freq='YS'
        )

        # wetdays: Total number of wet days (pr >= 1mm)
        logger.info("  - Calculating wetdays (total wet days >= 1mm)...")
        indices['wetdays'] = atmos.wetdays(
            pr=ds.pr,
            thresh='1 mm/day',
            freq='YS'
        )

        # wetdays_prop: Proportion of wet days
        logger.info("  - Calculating wetdays_prop (proportion of wet days)...")
        indices['wetdays_prop'] = atmos.wetdays_prop(
            pr=ds.pr,
            thresh='1 mm/day',
            freq='YS'
        )

        return indices

    def _process_single_tile(
        self,
        ds: xr.Dataset,
        lat_slice: slice,
        lon_slice: slice,
        tile_name: str
    ) -> Dict[str, xr.DataArray]:
        """
        Process a single spatial tile (precipitation-specific override).

        This override handles baseline percentiles subsetting, which is specific
        to precipitation pipeline's extreme indices.

        Args:
            ds: Full dataset
            lat_slice: Latitude slice for this tile
            lon_slice: Longitude slice for this tile
            tile_name: Name of this tile (for logging)

        Returns:
            Dictionary of calculated indices for this tile
        """
        logger.info(f"  Processing tile: {tile_name}")

        # Select spatial subset
        tile_ds = ds.isel(lat=lat_slice, lon=lon_slice)

        # Subset baseline percentiles to match tile
        # Use lock to prevent concurrent access to shared baseline data
        with self.baseline_lock:
            tile_baselines = {
                key: baseline.isel(lat=lat_slice, lon=lon_slice)
                for key, baseline in self.baselines.items()
            }

        # Calculate indices for this tile
        basic_indices = self.calculate_precipitation_indices(tile_ds)
        extreme_indices = self.calculate_extreme_indices(tile_ds, tile_baselines)
        threshold_indices = self.calculate_threshold_indices(tile_ds)
        enhanced_indices = self.calculate_enhanced_precipitation_indices(tile_ds)

        all_indices = {**basic_indices, **extreme_indices, **threshold_indices, **enhanced_indices}
        return all_indices

    def _calculate_all_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Override to implement spatial tiling for precipitation indices.

        Uses the SpatialTilingMixin to process the dataset in parallel spatial tiles
        for better memory efficiency and performance, then merges the results.

        Args:
            datasets: Dictionary with 'precipitation' dataset

        Returns:
            Dictionary mapping index name to calculated DataArray
        """
        ds = datasets['precipitation']

        # Define expected dimensions for validation
        # PRISM CONUS dimensions: 621 lat × 1405 lon
        # Time dimension after annual aggregation (freq='YS'):
        #   - Single year: time=1
        #   - Multiple years: time=num_years
        num_years = len(ds.time.groupby('time.year').groups)
        expected_dims = {
            'time': num_years,  # Number of years (aggregated to annual)
            'lat': 621,
            'lon': 1405
        }

        # Create temporary directory for tiles
        output_dir = Path('./outputs')
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use the mixin's spatial tiling functionality
        all_indices = self.process_with_spatial_tiling(
            ds=ds,
            output_dir=output_dir,
            expected_dims=expected_dims
        )

        return all_indices

    def _add_global_metadata(
        self,
        result_ds: xr.Dataset,
        start_year: int,
        end_year: int,
        pipeline_name: str,
        indices_count: int,
        additional_attrs: dict = None
    ) -> xr.Dataset:
        """
        Override to add precipitation-specific metadata.

        Args:
            result_ds: Result dataset
            start_year: Start year
            end_year: End year
            pipeline_name: Name of the pipeline
            indices_count: Number of indices calculated
            additional_attrs: Additional attributes to add

        Returns:
            Dataset with global metadata
        """
        # Call base implementation
        result_ds = super()._add_global_metadata(
            result_ds, start_year, end_year, pipeline_name, indices_count, additional_attrs
        )

        # Add precipitation-specific metadata
        result_ds.attrs['phase'] = 'Phase 6: Enhanced Precipitation Analysis (+3 indices, total 13)'
        result_ds.attrs['baseline_period'] = PipelineConfig.BASELINE_PERIOD
        result_ds.attrs['processing'] = f'Parallel processing of {self.n_tiles} spatial tiles'
        result_ds.attrs['note'] = 'Processed with parallel spatial tiling for optimal memory and performance. Extreme indices (r95p, r99p) use wet-day percentiles from baseline period. Phase 6 adds dry_days, wetdays, wetdays_prop.'

        return result_ds


def main():
    """Main entry point with command-line interface."""
    indices_list = """
  Basic (6): prcptot, rx1day, rx5day, sdii, cdd, cwd
  Extreme (2): r95p, r99p (percentile-based using 1981-2000 baseline)
  Threshold (2): r10mm, r20mm (fixed thresholds)
  Enhanced Phase 6 (3): dry_days, wetdays, wetdays_prop
"""

    examples = """
  # Process with 2 tiles (east/west split)
  python precipitation_pipeline.py --n-tiles 2

  # Process with 4 tiles (quadrants, default)
  python precipitation_pipeline.py --n-tiles 4

  # Process single year
  python precipitation_pipeline.py --start-year 2023 --end-year 2023
"""

    parser = PipelineCLI.create_parser(
        "Precipitation Indices",
        "Calculate 13 precipitation-based climate indices (Phase 6)",
        indices_list,
        examples
    )

    parser.add_argument(
        '--n-tiles',
        type=int,
        default=4,
        choices=[2, 4, 8],
        help='Number of spatial tiles: 2 (east/west), 4 (quadrants), or 8 (octants) (default: 4)'
    )

    args = parser.parse_args()

    # Handle common setup (logging, warnings)
    PipelineCLI.handle_common_setup(args)

    # Validate year range
    PipelineCLI.validate_years(args.start_year, args.end_year)

    # Create and run pipeline
    pipeline = PrecipitationPipeline(
        n_tiles=args.n_tiles,
        chunk_years=args.chunk_years,
        enable_dashboard=args.dashboard
    )

    try:
        output_files = pipeline.run(
            start_year=args.start_year,
            end_year=args.end_year,
            output_dir=args.output_dir
        )

        if output_files:
            print(f"\n✓ Successfully generated {len(output_files)} output files:")
            for f in output_files:
                print(f"  - {f}")
        else:
            print("\n✗ No output files generated")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

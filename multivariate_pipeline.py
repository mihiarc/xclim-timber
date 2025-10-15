#!/usr/bin/env python3
"""
Multivariate climate indices pipeline for xclim-timber.
Efficiently processes compound climate extreme indices using Zarr streaming.
Calculates 4 multivariate indices combining temperature and precipitation data.
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


class MultivariatePipeline(BasePipeline, SpatialTilingMixin):
    """
    Memory-efficient multivariate climate indices pipeline using Zarr streaming.
    Processes 4 compound extreme indices combining temperature and precipitation
    without loading full dataset into memory.

    Indices:
    - Cold and dry days (compound drought)
    - Cold and wet days (flooding risk)
    - Warm and dry days (drought/fire risk)
    - Warm and wet days (compound extremes)
    """

    def __init__(self, n_tiles: int = 4, **kwargs):
        """
        Initialize the pipeline with parallel spatial tiling.

        Args:
            n_tiles: Number of spatial tiles (2, 4, or 8, default: 4 for quadrants)
            **kwargs: Additional arguments passed to BasePipeline (chunk_years, enable_dashboard)
        """
        # Initialize BasePipeline with BOTH temperature and precipitation Zarr stores
        BasePipeline.__init__(
            self,
            zarr_paths={
                'temperature': PipelineConfig.TEMP_ZARR,
                'precipitation': PipelineConfig.PRECIP_ZARR
            },
            chunk_config=PipelineConfig.DEFAULT_CHUNKS,
            **kwargs
        )

        # Initialize SpatialTilingMixin
        SpatialTilingMixin.__init__(self, n_tiles=n_tiles)

        # Load baseline percentiles for multivariate indices
        self.baseline_loader = BaselineLoader()
        self.baselines = self._load_multivariate_baselines()

        # Thread lock for baseline access (fixes data race in parallel tile processing)
        self.baseline_lock = threading.Lock()

    def _load_multivariate_baselines(self) -> Dict[str, xr.DataArray]:
        """
        Load multivariate baseline percentiles (temperature + precipitation).

        Returns:
            Dictionary with tas_25p, tas_75p, pr_25p, pr_75p thresholds
        """
        # Load multivariate-specific baselines directly
        # These are stored separately in the baseline file (tas_25p, tas_75p, pr_25p, pr_75p)
        multivariate_baselines = self.baseline_loader.get_multivariate_baselines()

        # Validate we have all required percentiles
        required = ['tas_25p_threshold', 'tas_75p_threshold', 'pr_25p_threshold', 'pr_75p_threshold']
        missing = [p for p in required if p not in multivariate_baselines]

        if missing:
            logger.warning(f"Missing multivariate percentiles: {missing}")
            logger.warning("Some indices may be skipped")

        logger.info(f"  Loaded {len(multivariate_baselines)} multivariate baseline percentile thresholds")
        return multivariate_baselines

    def _preprocess_datasets(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """
        Preprocess temperature and precipitation datasets.

        Args:
            datasets: Dictionary with 'temperature' and 'precipitation' datasets

        Returns:
            Preprocessed datasets dictionary with merged 'combined' dataset
        """
        temp_ds = datasets['temperature']
        precip_ds = datasets['precipitation']

        # Select only needed variables (memory efficiency)
        if 'tmean' in temp_ds:
            temp_ds = temp_ds[['tmean']]
        if 'ppt' in precip_ds:
            precip_ds = precip_ds[['ppt']]

        # Rename variables for xclim compatibility
        temp_ds = self._rename_variables(temp_ds, {'tmean': 'tas'})
        precip_ds = self._rename_variables(precip_ds, PipelineConfig.PRECIP_RENAME_MAP)

        # Fix units
        temp_ds = self._fix_units(temp_ds, {'tas': 'degC'})
        precip_ds = self._fix_units(precip_ds, PipelineConfig.PRECIP_UNIT_FIXES)

        # Add CF standard names
        if 'tas' in temp_ds:
            temp_ds['tas'].attrs['standard_name'] = 'air_temperature'
        if 'pr' in precip_ds:
            precip_ds['pr'].attrs['standard_name'] = 'precipitation_flux'

        # Validate coordinate alignment
        logger.info("Validating coordinate alignment...")
        self._validate_coordinates(temp_ds, precip_ds, ['time', 'lat', 'lon'])

        # Merge temperature and precipitation into single dataset
        logger.info("Merging temperature and precipitation datasets...")
        combined_ds = xr.merge([temp_ds, precip_ds])

        # Return combined dataset
        return {'combined': combined_ds}

    def _validate_coordinates(self, ds1: xr.Dataset, ds2: xr.Dataset, coord_names: list):
        """
        Validate that two datasets have matching coordinates.

        Args:
            ds1: First dataset
            ds2: Second dataset
            coord_names: List of coordinate names to validate

        Raises:
            ValueError: If coordinates don't match
        """
        import numpy as np

        for coord in coord_names:
            if coord not in ds1.coords:
                raise ValueError(f"Coordinate '{coord}' missing in first dataset")
            if coord not in ds2.coords:
                raise ValueError(f"Coordinate '{coord}' missing in second dataset")

            # Check shape
            if ds1[coord].shape != ds2[coord].shape:
                raise ValueError(
                    f"Coordinate '{coord}' shape mismatch: "
                    f"{ds1[coord].shape} vs {ds2[coord].shape}"
                )

            # Check values match (with floating point tolerance for spatial coords)
            if coord in ['lat', 'lon']:
                if not np.allclose(ds1[coord].values, ds2[coord].values, rtol=1e-6):
                    max_diff = float(np.max(np.abs(ds1[coord].values - ds2[coord].values)))
                    raise ValueError(
                        f"Coordinate '{coord}' values mismatch. Max difference: {max_diff}"
                    )
            else:  # time coordinate - must match exactly
                if not ds1[coord].equals(ds2[coord]):
                    raise ValueError(f"Time coordinates don't match between datasets")

    def calculate_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Calculate all multivariate compound extreme indices.

        Args:
            datasets: Dictionary with 'combined' dataset (merged temperature + precipitation)

        Returns:
            Dictionary of calculated indices
        """
        ds = datasets['combined']

        # Calculate multivariate indices
        indices = self.calculate_multivariate_indices(ds)
        return indices

    def calculate_multivariate_indices(self, ds: xr.Dataset, baselines: Dict[str, xr.DataArray] = None) -> dict:
        """
        Calculate multivariate compound climate extreme indices.

        Args:
            ds: Dataset with temperature and precipitation variables (tas, pr)
            baselines: Optional baseline percentiles dict. If None, uses self.baselines

        Returns:
            Dictionary of calculated indices
        """
        indices = {}

        # All four indices require both temperature and precipitation
        if 'tas' not in ds or 'pr' not in ds:
            logger.warning("Missing required variables (tas or pr) for multivariate indices")
            return indices

        # Use provided baselines or fall back to instance baselines
        if baselines is None:
            baselines = self.baselines

        # Validate we have all required baseline percentiles
        required = ['tas_25p_threshold', 'tas_75p_threshold', 'pr_25p_threshold', 'pr_75p_threshold']
        missing = [p for p in required if p not in baselines]
        if missing:
            logger.warning(f"Missing baseline percentiles: {missing}. Skipping multivariate indices.")
            return indices

        # Use pre-calculated baseline percentiles
        tas_25 = baselines['tas_25p_threshold']
        tas_75 = baselines['tas_75p_threshold']
        pr_25 = baselines['pr_25p_threshold']
        pr_75 = baselines['pr_75p_threshold']

        logger.info("  Calculating 4 compound extreme indices using baseline percentiles...")

        # 1. Cold and Dry Days (compound drought)
        try:
            logger.info("  - Calculating cold_and_dry_days...")
            cold_dry = atmos.cold_and_dry_days(
                tas=ds.tas,
                pr=ds.pr,
                tas_per=tas_25,  # Cold threshold (25th percentile by day-of-year)
                pr_per=pr_25,    # Dry threshold (25th percentile of wet days by day-of-year)
                freq='YS'
            )
            # Drop quantile coordinate if present
            if 'quantile' in cold_dry.coords:
                cold_dry = cold_dry.drop_vars('quantile')
            indices['cold_and_dry_days'] = cold_dry
        except Exception as e:
            logger.error(f"Failed to calculate cold_and_dry_days: {e}")

        # 2. Cold and Wet Days (flooding risk)
        try:
            logger.info("  - Calculating cold_and_wet_days...")
            # Manual calculation: compare each day to its day-of-year percentile
            tas_25_bcast = tas_25.sel(dayofyear=ds.time.dt.dayofyear).drop_vars('dayofyear')
            pr_75_bcast = pr_75.sel(dayofyear=ds.time.dt.dayofyear).drop_vars('dayofyear')
            cold_days = ds.tas < tas_25_bcast
            wet_days = ds.pr > pr_75_bcast
            cold_wet = (cold_days & wet_days).resample(time='YS').sum()
            cold_wet.attrs['units'] = 'days'
            cold_wet.attrs['long_name'] = 'Cold and wet days'
            cold_wet.attrs['description'] = 'Days with temperature below 25th percentile and precipitation above 75th percentile'
            # Drop quantile coordinate if present
            if 'quantile' in cold_wet.coords:
                cold_wet = cold_wet.drop_vars('quantile')
            indices['cold_and_wet_days'] = cold_wet
        except Exception as e:
            logger.error(f"Failed to calculate cold_and_wet_days: {e}")

        # 3. Warm and Dry Days (drought/fire risk)
        try:
            logger.info("  - Calculating warm_and_dry_days...")
            warm_dry = atmos.warm_and_dry_days(
                tas=ds.tas,
                pr=ds.pr,
                tas_per=tas_75,  # Warm threshold (75th percentile by day-of-year)
                pr_per=pr_25,    # Dry threshold (25th percentile of wet days by day-of-year)
                freq='YS'
            )
            # Drop quantile coordinate if present
            if 'quantile' in warm_dry.coords:
                warm_dry = warm_dry.drop_vars('quantile')
            indices['warm_and_dry_days'] = warm_dry
        except Exception as e:
            logger.error(f"Failed to calculate warm_and_dry_days: {e}")

        # 4. Warm and Wet Days (compound extremes)
        try:
            logger.info("  - Calculating warm_and_wet_days...")
            warm_wet = atmos.warm_and_wet_days(
                tas=ds.tas,
                pr=ds.pr,
                tas_per=tas_75,  # Warm threshold (75th percentile by day-of-year)
                pr_per=pr_75,    # Wet threshold (75th percentile of wet days by day-of-year)
                freq='YS'
            )
            # Drop quantile coordinate if present
            if 'quantile' in warm_wet.coords:
                warm_wet = warm_wet.drop_vars('quantile')
            indices['warm_and_wet_days'] = warm_wet
        except Exception as e:
            logger.error(f"Failed to calculate warm_and_wet_days: {e}")

        return indices

    def _process_single_tile(
        self,
        ds: xr.Dataset,
        lat_slice: slice,
        lon_slice: slice,
        tile_name: str
    ) -> Dict[str, xr.DataArray]:
        """
        Process a single spatial tile (multivariate-specific override).

        This override handles baseline percentiles subsetting for both
        temperature and precipitation thresholds.

        Args:
            ds: Full combined dataset (temperature + precipitation merged)
            lat_slice: Latitude slice for this tile
            lon_slice: Longitude slice for this tile
            tile_name: Name of this tile (for logging)

        Returns:
            Dictionary of calculated indices for this tile
        """
        logger.info(f"  Processing tile: {tile_name}")

        # Select spatial subset
        tile_ds = ds.isel(lat=lat_slice, lon=lon_slice)

        # Subset baseline percentiles to match tile (thread-safe)
        with self.baseline_lock:
            tile_baselines = {}
            for key, baseline in self.baselines.items():
                # Slice baseline spatially to match tile dimensions
                # Note: Coordinates already match perfectly, no reindexing needed
                tile_baselines[key] = baseline.isel(lat=lat_slice, lon=lon_slice)

        # CRITICAL FIX for Issue #85: Pass baselines as parameter instead of modifying instance attribute
        # Modifying self.baselines causes race conditions in parallel processing where threads
        # overwrite each other's baseline arrays, resulting in coordinate mismatches
        tile_indices = self.calculate_multivariate_indices(tile_ds, baselines=tile_baselines)

        return tile_indices

    def _calculate_all_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Override to implement spatial tiling for multivariate indices.

        Uses the SpatialTilingMixin to process the merged dataset in parallel
        spatial tiles for better memory efficiency and performance.

        Args:
            datasets: Dictionary with 'combined' dataset (merged temp + precip)

        Returns:
            Dictionary mapping index name to calculated DataArray
        """
        ds = datasets['combined']

        # Define expected dimensions for validation
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
        Override to add multivariate-specific metadata.

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

        # Add multivariate-specific metadata
        result_ds.attrs['description'] = 'Compound climate extreme indices combining temperature and precipitation'
        result_ds.attrs['baseline_period'] = PipelineConfig.BASELINE_PERIOD
        result_ds.attrs['processing'] = f'Parallel processing of {self.n_tiles} spatial tiles'
        result_ds.attrs['note'] = (
            'Multivariate compound extremes using day-of-year percentile thresholds from 1981-2000 baseline. '
            'Cold/warm thresholds: 25th/75th percentile of temperature. '
            'Dry/wet thresholds: 25th/75th percentile of precipitation (wet days only). '
            'Processed with parallel spatial tiling for optimal memory and performance.'
        )

        return result_ds


def main():
    """Main entry point with command-line interface."""
    indices_list = """
  Compound Extreme Indices (4 total):
  - Cold and dry days: Temperature < 25th percentile AND precipitation < 25th percentile
  - Cold and wet days: Temperature < 25th percentile AND precipitation > 75th percentile
  - Warm and dry days: Temperature > 75th percentile AND precipitation < 25th percentile
  - Warm and wet days: Temperature > 75th percentile AND precipitation > 75th percentile
"""

    examples = """
  # Process with 2 tiles (east/west split)
  python multivariate_pipeline.py --n-tiles 2

  # Process with 4 tiles (quadrants, default)
  python multivariate_pipeline.py --n-tiles 4

  # Process single year
  python multivariate_pipeline.py --start-year 2023 --end-year 2023
"""

    parser = PipelineCLI.create_parser(
        "Multivariate Climate Indices",
        "Calculate 4 compound extreme indices combining temperature and precipitation",
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
    pipeline = MultivariatePipeline(
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

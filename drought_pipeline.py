#!/usr/bin/env python3
"""
Drought indices pipeline for xclim-timber.
Efficiently processes drought and water deficit indices using Zarr streaming.
Calculates 12 drought indices (Phase 10).
"""

import logging
import sys
from pathlib import Path
from typing import Dict
import threading

import xarray as xr
import dask
import xclim.indicators.atmos as atmos

from core import BasePipeline, PipelineConfig, BaselineLoader, PipelineCLI, SpatialTilingMixin

logger = logging.getLogger(__name__)


class DroughtPipeline(BasePipeline, SpatialTilingMixin):
    """
    Memory-efficient drought indices pipeline using Zarr streaming.
    Processes 12 drought indices without loading full dataset into memory.

    Indices:
    - SPI (5 windows): Standardized Precipitation Index for multi-scale drought monitoring
    - Dry Spell (4 indices): Maximum consecutive dry days, dry spell frequency,
                             dry spell total length, total dry days count
    - Precipitation Intensity (3 indices): Daily intensity, maximum 7-day intensity,
                                          heavy precipitation fraction
    """

    def __init__(self, n_tiles: int = 4, **kwargs):
        """
        Initialize the pipeline with parallel spatial tiling.

        Args:
            n_tiles: Number of spatial tiles (1, 2, 4, or 8, default: 4 for quadrants)
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

        # Load baseline percentiles
        self.baseline_loader = BaselineLoader()
        self.baselines = self.baseline_loader.get_precipitation_baselines()

        # Thread lock for baseline access (fixes data race in parallel tile processing)
        self.baseline_lock = threading.Lock()

        # SPI calibration period configuration
        self.spi_cal_start = '1981-01-01'
        self.spi_cal_end = '2010-12-31'

        # Target years for SPI filtering (set during _calculate_all_indices)
        self.target_start_year = None
        self.target_end_year = None

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
        Calculate all drought indices for a single spatial region.

        Combines three calculation methods:
        - SPI indices (5 windows)
        - Dry spell indices (4 indices)
        - Precipitation intensity indices (3 indices)

        Args:
            datasets: Dictionary with 'precipitation' dataset

        Returns:
            Dictionary of calculated indices
        """
        ds = datasets['precipitation']

        # For SPI calculation, need full calibration period (1981-2010) plus target years
        # Note: This is handled in _calculate_all_indices() for the full dataset
        # For tiles, we assume the data already includes calibration period

        # Calculate all three types of indices
        spi_indices = self.calculate_spi_indices(ds)
        dry_spell_indices = self.calculate_dry_spell_indices(ds)
        intensity_indices = self.calculate_precip_intensity_indices(ds)

        # Combine all indices
        all_indices = {**spi_indices, **dry_spell_indices, **intensity_indices}
        return all_indices

    def calculate_spi_indices(self, precip_ds: xr.Dataset) -> dict:
        """
        Calculate Standardized Precipitation Index (SPI) at multiple time windows.

        Implements McKee et al. (1993) standard methodology using gamma distribution fitting.

        Args:
            precip_ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated SPI indices (5 windows)
        """
        indices = {}

        if 'pr' not in precip_ds:
            logger.warning("Precipitation variable 'pr' not found, skipping SPI calculation")
            return indices

        # Define SPI windows (in months)
        spi_windows = {
            1: 'spi_1month',
            3: 'spi_3month',
            6: 'spi_6month',
            12: 'spi_12month',
            24: 'spi_24month'
        }

        for window, var_name in spi_windows.items():
            try:
                logger.info(f"  - Calculating {var_name} (SPI-{window})...")

                # Calculate SPI using gamma distribution (McKee et al. 1993)
                spi = atmos.standardized_precipitation_index(
                    pr=precip_ds.pr,
                    freq='MS',              # Monthly frequency (required for SPI)
                    window=window,          # N-month window
                    dist='gamma',           # Gamma distribution (standard)
                    method='ML',            # Maximum likelihood fitting
                    cal_start=self.spi_cal_start,    # 30-year calibration period
                    cal_end=self.spi_cal_end
                )

                # Compute immediately to avoid task graph accumulation
                # Use synchronous scheduler to avoid threading conflicts with parallel tiles
                logger.info(f"  - Computing {var_name}...")
                with dask.config.set(scheduler='synchronous'):
                    spi = spi.compute()

                indices[var_name] = spi

                # Enhance metadata for CF-compliance
                indices[var_name].attrs['units'] = '1'  # Dimensionless
                indices[var_name].attrs['long_name'] = f'{window}-Month Standardized Precipitation Index'
                indices[var_name].attrs['description'] = f'Standardized precipitation index over {window}-month window using gamma distribution (McKee et al. 1993)'
                indices[var_name].attrs['calibration_period'] = f'{self.spi_cal_start} to {self.spi_cal_end}'
                indices[var_name].attrs['distribution'] = 'gamma'
                indices[var_name].attrs['method'] = 'ML'
                indices[var_name].attrs['interpretation'] = 'SPI < -2.0: Extreme drought, -1.5 to -1.0: Moderate drought, -1.0 to 1.0: Near normal, > 2.0: Extremely wet'

            except Exception as e:
                logger.error(f"Failed to calculate {var_name}: {e}")
                import traceback
                logger.error(traceback.format_exc())

        return indices

    def calculate_dry_spell_indices(self, precip_ds: xr.Dataset) -> dict:
        """
        Calculate dry spell and consecutive dry days indices.

        Args:
            precip_ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated dry spell indices (4 indices)
        """
        indices = {}

        if 'pr' not in precip_ds:
            logger.warning("Precipitation variable 'pr' not found, skipping dry spell indices")
            return indices

        # 1. Maximum Consecutive Dry Days (ETCCDI standard)
        try:
            logger.info("  - Calculating maximum consecutive dry days (CDD)...")
            indices['cdd'] = atmos.maximum_consecutive_dry_days(
                pr=precip_ds.pr,
                thresh='1.0 mm/day',
                freq='YS'
            )
        except Exception as e:
            logger.error(f"Failed to calculate cdd: {e}")

        # 2. Dry Spell Frequency (manual implementation)
        try:
            logger.info("  - Calculating dry spell frequency...")
            dry_threshold = 1.0  # mm
            min_spell_length = 3

            is_dry = precip_ds.pr < dry_threshold

            def count_dry_spells(dry_mask):
                """Count number of dry spell events (3+ consecutive dry days)."""
                dry_array = dry_mask.values
                spell_count = 0
                current_spell_length = 0

                for is_dry_day in dry_array:
                    if is_dry_day:
                        current_spell_length += 1
                    else:
                        if current_spell_length >= min_spell_length:
                            spell_count += 1
                        current_spell_length = 0

                if current_spell_length >= min_spell_length:
                    spell_count += 1

                return spell_count

            dry_spell_freq = is_dry.resample(time='YS').apply(count_dry_spells)

            indices['dry_spell_frequency'] = dry_spell_freq
            indices['dry_spell_frequency'].attrs.update({
                'units': '1',
                'long_name': 'Dry Spell Frequency',
                'description': f'Number of dry spell events (≥{min_spell_length} consecutive days with precipitation < {dry_threshold} mm)',
                'standard_name': 'number_of_dry_spell_events',
                'cell_methods': 'time: sum',
                'threshold': f'{dry_threshold} mm',
                'min_spell_length': f'{min_spell_length} days'
            })
        except Exception as e:
            logger.error(f"Failed to calculate dry_spell_frequency: {e}")

        # 3. Dry Spell Total Length (manual implementation)
        try:
            logger.info("  - Calculating dry spell total length...")
            dry_threshold = 1.0  # mm
            min_spell_length = 3

            is_dry = precip_ds.pr < dry_threshold

            def total_dry_spell_days(dry_mask):
                """Calculate total days in dry spells (3+ consecutive dry days)."""
                dry_array = dry_mask.values
                total_days = 0
                current_spell_length = 0

                for is_dry_day in dry_array:
                    if is_dry_day:
                        current_spell_length += 1
                    else:
                        if current_spell_length >= min_spell_length:
                            total_days += current_spell_length
                        current_spell_length = 0

                if current_spell_length >= min_spell_length:
                    total_days += current_spell_length

                return total_days

            dry_spell_total = is_dry.resample(time='YS').apply(total_dry_spell_days)

            indices['dry_spell_total_length'] = dry_spell_total
            indices['dry_spell_total_length'].attrs.update({
                'units': 'days',
                'long_name': 'Dry Spell Total Length',
                'description': f'Total number of days in dry spells (≥{min_spell_length} consecutive days with precipitation < {dry_threshold} mm)',
                'standard_name': 'dry_spell_total_days',
                'cell_methods': 'time: sum',
                'threshold': f'{dry_threshold} mm',
                'min_spell_length': f'{min_spell_length} days'
            })
        except Exception as e:
            logger.error(f"Failed to calculate dry_spell_total_length: {e}")

        # 4. Dry Days (simple count)
        try:
            logger.info("  - Calculating dry days...")
            indices['dry_days'] = atmos.dry_days(
                pr=precip_ds.pr,
                thresh='1.0 mm d-1',
                freq='YS'
            )
        except Exception as e:
            logger.error(f"Failed to calculate dry_days: {e}")

        return indices

    def calculate_precip_intensity_indices(self, precip_ds: xr.Dataset) -> dict:
        """
        Calculate precipitation intensity and distribution indices.

        Args:
            precip_ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated precipitation intensity indices (3 indices)
        """
        indices = {}

        if 'pr' not in precip_ds:
            logger.warning("Precipitation variable 'pr' not found, skipping intensity indices")
            return indices

        # 1. Simple Daily Intensity Index (ETCCDI standard)
        try:
            logger.info("  - Calculating simple daily intensity index (SDII)...")
            indices['sdii'] = atmos.daily_pr_intensity(
                pr=precip_ds.pr,
                thresh='1.0 mm d-1',
                freq='YS'
            )
        except Exception as e:
            logger.error(f"Failed to calculate sdii: {e}")

        # 2. Maximum 7-Day Precipitation (manual implementation)
        try:
            logger.info("  - Calculating maximum 7-day precipitation intensity...")
            window_size = 7
            pr_7day_rolling = precip_ds.pr.rolling(time=window_size, min_periods=window_size).sum()
            max_7day = pr_7day_rolling.resample(time='YS').max()

            indices['max_7day_pr_intensity'] = max_7day
            indices['max_7day_pr_intensity'].attrs.update({
                'units': 'mm',
                'long_name': 'Maximum 7-Day Precipitation Intensity',
                'description': f'Maximum {window_size}-day rolling sum of precipitation',
                'standard_name': 'maximum_7day_precipitation_amount',
                'cell_methods': 'time: maximum',
                'window': f'{window_size} days'
            })
        except Exception as e:
            logger.error(f"Failed to calculate max_7day_pr_intensity: {e}")

        # 3. Fraction of Heavy Precipitation (requires baseline percentiles)
        if 'pr_75p_threshold' in self.baselines:
            try:
                logger.info("  - Calculating fraction of heavy precipitation...")
                indices['fraction_heavy_precip'] = atmos.fraction_over_precip_thresh(
                    pr=precip_ds.pr,
                    pr_per=self.baselines['pr_75p_threshold'],
                    freq='YS'
                )
                indices['fraction_heavy_precip'].attrs['long_name'] = 'Fraction of Heavy Precipitation'
                indices['fraction_heavy_precip'].attrs['description'] = 'Fraction of annual precipitation from heavy events (>75th percentile)'
                indices['fraction_heavy_precip'].attrs['baseline_period'] = '1981-2000'
            except Exception as e:
                logger.error(f"Failed to calculate fraction_heavy_precip: {e}")
        else:
            logger.warning("Skipping fraction_heavy_precip (baseline pr_75p_threshold not available)")

        return indices

    def _process_single_tile(
        self,
        ds: xr.Dataset,
        lat_slice: slice,
        lon_slice: slice,
        tile_name: str
    ) -> Dict[str, xr.DataArray]:
        """
        Process a single spatial tile (drought-specific override).

        This override handles:
        1. Baseline percentiles subsetting
        2. SPI calibration period handling
        3. Filtering results to target years

        Args:
            ds: Full dataset (includes calibration period 1981-2010 + target years)
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
            tile_baselines_temp = {}
            for key, baseline in self.baselines.items():
                # Slice spatial dimensions first
                tile_baseline = baseline.isel(lat=lat_slice, lon=lon_slice)

                # Rechunk to match tile data structure (prevents implicit dask rechunk operations)
                # This eliminates 2-3x hidden memory overhead during percentile-based index calculations
                if hasattr(tile_ds, 'chunks') and hasattr(tile_baseline, 'chunk'):
                    chunk_dict = {
                        'lat': tile_ds.chunks.get('lat', (-1,))[0] if 'lat' in tile_ds.chunks else -1,
                        'lon': tile_ds.chunks.get('lon', (-1,))[0] if 'lon' in tile_ds.chunks else -1,
                        'dayofyear': -1  # Keep temporal dimension together for efficiency
                    }
                    tile_baseline = tile_baseline.chunk(chunk_dict)

                tile_baselines_temp[key] = tile_baseline

        # Temporarily replace baselines with tile-specific versions
        original_baselines = self.baselines
        self.baselines = tile_baselines_temp

        try:
            # Calculate SPI indices (uses full calibration period)
            # Note: SPI indices are now computed immediately inside calculate_spi_indices()
            spi_indices = self.calculate_spi_indices(tile_ds)

            # Filter SPI results to target years (if we loaded extended calibration period)
            if self.target_start_year and self.target_end_year:
                # Check if we need to filter (extended dataset starts before target)
                time_vals = tile_ds.time.values
                if len(time_vals) > 0:
                    data_start_year = int(str(time_vals[0])[:4])
                    if data_start_year < self.target_start_year:
                        logger.info(f"    Filtering SPI to {self.target_start_year}-{self.target_end_year}...")
                        for key in spi_indices.keys():
                            # SPI has monthly frequency, filter carefully
                            try:
                                spi_indices[key] = spi_indices[key].sel(
                                    time=slice(f'{self.target_start_year}-01-01', f'{self.target_end_year}-12-31')
                                )
                            except Exception as e:
                                logger.warning(f"Could not filter {key}: {e}")

            # Calculate other indices (dry spell, intensity) - these use target years only
            dry_spell_indices = self.calculate_dry_spell_indices(tile_ds)
            intensity_indices = self.calculate_precip_intensity_indices(tile_ds)

            all_indices = {**spi_indices, **dry_spell_indices, **intensity_indices}

        finally:
            # Restore original baselines
            self.baselines = original_baselines

        return all_indices

    def _calculate_all_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Override to implement spatial tiling with SPI calibration period handling.

        SPI requires full calibration period (1981-2010) plus target years.
        We load the extended period, calculate indices, then filter to target years.

        Args:
            datasets: Dictionary with 'precipitation' dataset

        Returns:
            Dictionary mapping index name to calculated DataArray
        """
        ds = datasets['precipitation']

        # Extract target years from the dataset
        time_vals = ds.time.values
        target_start_year = int(str(time_vals[0])[:4])
        target_end_year = int(str(time_vals[-1])[:4])

        # Store target years for filtering in _process_single_tile
        self.target_start_year = target_start_year
        self.target_end_year = target_end_year

        # For SPI, need to load full calibration period (1981-2010) plus target years
        spi_start_year = min(target_start_year, 1981)

        # Reload with extended period for SPI calibration
        logger.info(f"Loading {spi_start_year}-{target_end_year} for SPI calibration...")
        full_zarr = xr.open_zarr(
            PipelineConfig.PRECIP_ZARR,
            chunks=PipelineConfig.DEFAULT_CHUNKS
        )
        ds_extended = full_zarr.sel(time=slice(f'{spi_start_year}-01-01', f'{target_end_year}-12-31'))

        # Preprocess extended dataset
        datasets_extended = {'precipitation': ds_extended}
        datasets_extended = self._preprocess_datasets(datasets_extended)
        ds_extended = datasets_extended['precipitation']

        # Define expected dimensions for validation
        # Note: SPI indices have monthly frequency, others have annual
        # We'll validate based on annual indices
        num_years = target_end_year - target_start_year + 1
        expected_dims = {
            'time': num_years,  # Annual indices
            'lat': 621,
            'lon': 1405
        }

        # Create temporary directory for tiles
        output_dir = Path('./outputs')
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use the mixin's spatial tiling functionality with extended dataset
        all_indices = self.process_with_spatial_tiling(
            ds=ds_extended,
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
        Override to add drought-specific metadata.

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

        # Add drought-specific metadata
        result_ds.attrs['phase'] = 'Phase 10 Final: Drought Indices (+12 indices, total 80/80 complete)'
        result_ds.attrs['spi_calibration_period'] = f'{self.spi_cal_start} to {self.spi_cal_end}'
        result_ds.attrs['spi_distribution'] = 'gamma (McKee et al. 1993)'
        result_ds.attrs['spi_method'] = 'ML (Maximum Likelihood)'
        result_ds.attrs['processing'] = f'Parallel processing of {self.n_tiles} spatial tiles'
        result_ds.attrs['note'] = f'Comprehensive drought monitoring indices (12 total). SPI uses 30-year calibration ({self.spi_cal_start} to {self.spi_cal_end}). Dry spell threshold: 1mm, min length: 3 days. Includes manual implementations for dry_spell_frequency, dry_spell_total_length, and max_7day_pr_intensity. Baseline percentiles: 1981-2000. Processed with parallel spatial tiling.'

        return result_ds


def main():
    """Main entry point with command-line interface."""
    indices_list = """
  SPI (5): Standardized Precipitation Index at 1, 3, 6, 12, 24-month windows
  Dry Spell (4): Maximum consecutive dry days (CDD), dry spell frequency,
                 dry spell total length, total dry days count
  Intensity (3): SDII (simple daily intensity), maximum 7-day precipitation,
                 fraction of heavy precipitation
"""

    examples = """
  # Process with 1 tile (no parallelism, recommended for drought to avoid threading deadlock)
  python drought_pipeline.py --n-tiles 1

  # Process with 2 tiles (east/west split)
  python drought_pipeline.py --n-tiles 2

  # Process single year
  python drought_pipeline.py --start-year 2023 --end-year 2023
"""

    parser = PipelineCLI.create_parser(
        "Drought Indices",
        "Calculate 12 drought and water deficit indices (Phase 10 Final - completes 80/80)",
        indices_list,
        examples
    )

    parser.add_argument(
        '--n-tiles',
        type=int,
        default=4,
        choices=[1, 2, 4, 8],
        help='Number of spatial tiles: 1 (no tiling), 2 (east/west), 4 (quadrants), or 8 (octants) (default: 4)'
    )

    args = parser.parse_args()

    # Handle common setup (logging, warnings)
    PipelineCLI.handle_common_setup(args)

    # Validate year range
    PipelineCLI.validate_years(args.start_year, args.end_year)

    # Create and run pipeline
    pipeline = DroughtPipeline(
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

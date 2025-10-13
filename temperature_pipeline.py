#!/usr/bin/env python3
"""
Temperature indices pipeline for xclim-timber.
Efficiently processes temperature-based climate indices using Zarr streaming.
Calculates 35 temperature indices (19 basic + 6 extreme percentile-based + 8 Phase 7 + 2 Phase 9).
"""

import logging
import sys
from pathlib import Path
from typing import Dict
import threading

import xarray as xr
import xclim.indicators.atmos as atmos
import xclim.indices as xi

from core import BasePipeline, PipelineConfig, BaselineLoader, PipelineCLI, SpatialTilingMixin

logger = logging.getLogger(__name__)


class TemperaturePipeline(BasePipeline, SpatialTilingMixin):
    """
    Memory-efficient temperature indices pipeline using Zarr streaming.
    Processes 35 temperature indices without loading full dataset into memory.

    Indices:
    - Basic (19): Core temperature statistics, thresholds, degree days, frost season
    - Extreme (6): Percentile-based warm/cool days/nights, spell duration
    - Advanced Phase 7 (8): Spell frequency, growing season timing, variability
    - Advanced Phase 9 (2): Temperature seasonality, heat wave index
    """

    # Count indices that need timedelta encoding fix
    # These indices measure "number of days" and must use units='1' (dimensionless)
    # to prevent xarray from interpreting units='days' as CF timedelta during NetCDF write
    COUNT_INDICES = [
        'summer_days', 'hot_days', 'ice_days', 'frost_days',
        'tropical_nights', 'consecutive_frost_days',
        'frost_season_length', 'frost_free_season_length',
        'tx90p', 'tx10p', 'tn90p', 'tn10p',
        'warm_spell_duration_index', 'cold_spell_duration_index',
        'heat_wave_index'
    ]

    def __init__(self, n_tiles: int = 4, **kwargs):
        """
        Initialize the pipeline with parallel spatial tiling.

        Args:
            n_tiles: Number of spatial tiles (2 or 4, default: 4 for quadrants)
            **kwargs: Additional arguments passed to BasePipeline (chunk_years, enable_dashboard)
        """
        # Initialize BasePipeline
        BasePipeline.__init__(
            self,
            zarr_paths={'temperature': PipelineConfig.TEMP_ZARR},
            chunk_config=PipelineConfig.DEFAULT_CHUNKS,
            **kwargs
        )

        # Initialize SpatialTilingMixin
        SpatialTilingMixin.__init__(self, n_tiles=n_tiles)

        # Load baseline percentiles for extreme indices
        self.baseline_loader = BaselineLoader()
        self.baselines = self.baseline_loader.get_temperature_baselines()

        # Thread lock for baseline access (fixes data race in parallel tile processing)
        self.baseline_lock = threading.Lock()

    def _preprocess_datasets(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """
        Preprocess temperature datasets (rename variables, fix units).

        Args:
            datasets: Dictionary with 'temperature' dataset

        Returns:
            Preprocessed datasets dictionary
        """
        temp_ds = datasets['temperature']

        # Rename temperature variables for xclim compatibility
        temp_ds = self._rename_variables(temp_ds, PipelineConfig.TEMP_RENAME_MAP)

        # Fix units for temperature variables
        temp_ds = self._fix_units(temp_ds, PipelineConfig.TEMP_UNIT_FIXES)

        # Add CF standard names
        for var_name in ['tas', 'tasmax', 'tasmin']:
            if var_name in temp_ds:
                temp_ds[var_name].attrs['standard_name'] = PipelineConfig.CF_STANDARD_NAMES.get(
                    var_name, 'air_temperature'
                )

        datasets['temperature'] = temp_ds
        return datasets

    def calculate_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Calculate all temperature indices for a single spatial region.

        Combines three calculation methods:
        - Basic temperature indices (19 indices)
        - Extreme percentile-based indices (6 indices)
        - Advanced temperature indices (10 indices)

        Args:
            datasets: Dictionary with 'temperature' dataset

        Returns:
            Dictionary of calculated indices
        """
        ds = datasets['temperature']

        # Calculate all three types of indices
        basic_indices = self.calculate_temperature_indices(ds)
        extreme_indices = self.calculate_extreme_indices(ds, self.baselines)
        advanced_indices = self.calculate_advanced_temperature_indices(ds)

        # Combine all indices
        all_indices = {**basic_indices, **extreme_indices, **advanced_indices}
        return all_indices

    def calculate_temperature_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate temperature-based climate indices.

        Args:
            ds: Dataset with temperature variables (tas, tasmax, tasmin)

        Returns:
            Dictionary of calculated indices
        """
        indices = {}

        # Basic temperature statistics
        if 'tas' in ds:
            logger.info("  - Calculating annual mean temperature...")
            indices['tg_mean'] = atmos.tg_mean(ds.tas, freq='YS')

        if 'tasmax' in ds:
            logger.info("  - Calculating annual maximum temperature...")
            indices['tx_max'] = atmos.tx_max(ds.tasmax, freq='YS')
            logger.info("  - Calculating summer days (>25°C)...")
            indices['summer_days'] = atmos.tx_days_above(ds.tasmax, thresh='25 degC', freq='YS')
            logger.info("  - Calculating hot days (>30°C)...")
            indices['hot_days'] = atmos.tx_days_above(ds.tasmax, thresh='30 degC', freq='YS')
            logger.info("  - Calculating ice days (<0°C)...")
            indices['ice_days'] = atmos.ice_days(ds.tasmax, freq='YS')

        if 'tasmin' in ds:
            logger.info("  - Calculating annual minimum temperature...")
            indices['tn_min'] = atmos.tn_min(ds.tasmin, freq='YS')
            logger.info("  - Calculating frost days...")
            indices['frost_days'] = atmos.frost_days(ds.tasmin, freq='YS')
            logger.info("  - Calculating tropical nights (>20°C)...")
            indices['tropical_nights'] = atmos.tropical_nights(ds.tasmin, freq='YS')
            logger.info("  - Calculating consecutive frost days...")
            indices['consecutive_frost_days'] = atmos.consecutive_frost_days(
                ds.tasmin, freq='YS'
            )

        if 'tas' in ds:
            logger.info("  - Calculating growing degree days...")
            indices['growing_degree_days'] = atmos.growing_degree_days(
                ds.tas, thresh='10 degC', freq='YS'
            )
            logger.info("  - Calculating heating degree days...")
            indices['heating_degree_days'] = atmos.heating_degree_days(
                ds.tas, thresh='17 degC', freq='YS'
            )
            logger.info("  - Calculating cooling degree days...")
            indices['cooling_degree_days'] = atmos.cooling_degree_days(
                ds.tas, thresh='18 degC', freq='YS'
            )
            logger.info("  - Calculating freezing degree days...")
            indices['freezing_degree_days'] = atmos.freezing_degree_days(
                ds.tas, freq='YS'
            )

        # Temperature range indices (require both tasmax and tasmin)
        if 'tasmax' in ds and 'tasmin' in ds:
            logger.info("  - Calculating daily temperature range...")
            indices['daily_temperature_range'] = atmos.daily_temperature_range(
                ds.tasmin, ds.tasmax, freq='YS'
            )
            logger.info("  - Calculating extreme temperature range...")
            indices['extreme_temperature_range'] = atmos.extreme_temperature_range(
                ds.tasmin, ds.tasmax, freq='YS'
            )

        # Frost season indices (require tasmin)
        if 'tasmin' in ds:
            logger.info("  - Calculating frost season length...")
            indices['frost_season_length'] = atmos.frost_season_length(
                ds.tasmin, freq='YS'
            )
            logger.info("  - Calculating frost-free season start...")
            indices['frost_free_season_start'] = atmos.frost_free_season_start(
                ds.tasmin, freq='YS'
            )
            logger.info("  - Calculating frost-free season end...")
            indices['frost_free_season_end'] = atmos.frost_free_season_end(
                ds.tasmin, freq='YS'
            )
            logger.info("  - Calculating frost-free season length...")
            indices['frost_free_season_length'] = atmos.frost_free_season_length(
                ds.tasmin, freq='YS'
            )

        return indices

    def calculate_extreme_indices(self, ds: xr.Dataset, baseline_percentiles: dict) -> dict:
        """
        Calculate percentile-based extreme temperature indices using pre-calculated baseline.

        Args:
            ds: Dataset with temperature variables (tasmax, tasmin)
            baseline_percentiles: Dictionary of baseline percentile thresholds

        Returns:
            Dictionary of calculated extreme indices
        """
        indices = {}

        # Warm/cool day indices (based on tasmax)
        if 'tasmax' in ds:
            logger.info("  - Calculating warm days (tx90p)...")
            indices['tx90p'] = atmos.tx90p(
                tasmax=ds.tasmax,
                tasmax_per=baseline_percentiles['tx90p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating cool days (tx10p)...")
            indices['tx10p'] = atmos.tx10p(
                tasmax=ds.tasmax,
                tasmax_per=baseline_percentiles['tx10p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating warm spell duration (WSDI)...")
            indices['warm_spell_duration_index'] = atmos.warm_spell_duration_index(
                tasmax=ds.tasmax,
                tasmax_per=baseline_percentiles['tx90p_threshold'],
                window=6,
                freq='YS'
            )

        # Warm/cool night indices (based on tasmin)
        if 'tasmin' in ds:
            logger.info("  - Calculating warm nights (tn90p)...")
            indices['tn90p'] = atmos.tn90p(
                tasmin=ds.tasmin,
                tasmin_per=baseline_percentiles['tn90p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating cool nights (tn10p)...")
            indices['tn10p'] = atmos.tn10p(
                tasmin=ds.tasmin,
                tasmin_per=baseline_percentiles['tn10p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating cold spell duration (CSDI)...")
            indices['cold_spell_duration_index'] = atmos.cold_spell_duration_index(
                tasmin=ds.tasmin,
                tasmin_per=baseline_percentiles['tn10p_threshold'],
                window=6,
                freq='YS'
            )

        return indices

    def calculate_advanced_temperature_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate advanced temperature extreme indices (Phase 7 & 9).

        Adds 10 new indices focused on:
        - Spell frequency (counting discrete events)
        - Seasonal timing (growing season, last frost)
        - Temperature variability

        All indices use fixed thresholds (no baseline percentiles required).

        Args:
            ds: Dataset with temperature variables (tas, tasmax, tasmin)

        Returns:
            Dictionary of calculated advanced temperature indices
        """
        indices = {}

        # Growing season timing indices (ETCCDI standard)
        if 'tas' in ds:
            try:
                logger.info("  - Calculating growing season start...")
                indices['growing_season_start'] = atmos.growing_season_start(
                    tas=ds.tas,
                    thresh='5 degC',
                    window=5,
                    freq='YS'
                )
                indices['growing_season_start'].attrs['units'] = 'day_of_year'
            except Exception as e:
                logger.error(f"Failed to calculate growing_season_start: {e}")

            try:
                logger.info("  - Calculating growing season end...")
                indices['growing_season_end'] = atmos.growing_season_end(
                    tas=ds.tas,
                    thresh='5 degC',
                    window=5,
                    freq='YS'
                )
                indices['growing_season_end'].attrs['units'] = 'day_of_year'
            except Exception as e:
                logger.error(f"Failed to calculate growing_season_end: {e}")

        # Spell frequency indices (event counting)
        if 'tas' in ds:
            try:
                logger.info("  - Calculating cold spell frequency...")
                indices['cold_spell_frequency'] = atmos.cold_spell_frequency(
                    tas=ds.tas,
                    thresh='-10 degC',
                    window=5,
                    freq='YS'
                )
                indices['cold_spell_frequency'].attrs['units'] = '1'
            except Exception as e:
                logger.error(f"Failed to calculate cold_spell_frequency: {e}")

        if 'tasmax' in ds:
            try:
                logger.info("  - Calculating hot spell frequency...")
                indices['hot_spell_frequency'] = atmos.hot_spell_frequency(
                    tasmax=ds.tasmax,
                    thresh='30 degC',
                    window=3,
                    freq='YS'
                )
                indices['hot_spell_frequency'].attrs['units'] = '1'
            except Exception as e:
                logger.error(f"Failed to calculate hot_spell_frequency: {e}")

        if 'tasmin' in ds and 'tasmax' in ds:
            try:
                logger.info("  - Calculating heat wave frequency...")
                indices['heat_wave_frequency'] = atmos.heat_wave_frequency(
                    tasmin=ds.tasmin,
                    tasmax=ds.tasmax,
                    thresh_tasmin='22 degC',
                    thresh_tasmax='30 degC',
                    window=3,
                    freq='YS'
                )
                indices['heat_wave_frequency'].attrs['units'] = '1'
            except Exception as e:
                logger.error(f"Failed to calculate heat_wave_frequency: {e}")

            try:
                logger.info("  - Calculating freeze-thaw spell frequency...")
                indices['freezethaw_spell_frequency'] = atmos.freezethaw_spell_frequency(
                    tasmin=ds.tasmin,
                    tasmax=ds.tasmax,
                    freq='YS'
                )
                indices['freezethaw_spell_frequency'].attrs['units'] = '1'
            except Exception as e:
                logger.error(f"Failed to calculate freezethaw_spell_frequency: {e}")

        # Seasonal timing - last spring frost
        if 'tasmin' in ds:
            try:
                logger.info("  - Calculating last spring frost...")
                indices['last_spring_frost'] = atmos.last_spring_frost(
                    tasmin=ds.tasmin,
                    thresh='0 degC',
                    freq='YS'
                )
                indices['last_spring_frost'].attrs['units'] = 'day_of_year'
            except Exception as e:
                logger.error(f"Failed to calculate last_spring_frost: {e}")

        # Temperature variability index
        if 'tasmin' in ds and 'tasmax' in ds:
            try:
                logger.info("  - Calculating daily temperature range variability...")
                indices['daily_temperature_range_variability'] = atmos.daily_temperature_range_variability(
                    tasmin=ds.tasmin,
                    tasmax=ds.tasmax,
                    freq='YS'
                )
            except Exception as e:
                logger.error(f"Failed to calculate daily_temperature_range_variability: {e}")

        # Phase 9: Temperature Variability Indices
        if 'tas' in ds:
            try:
                logger.info("  - Calculating temperature seasonality (Phase 9)...")
                indices['temperature_seasonality'] = xi.temperature_seasonality(
                    tas=ds.tas,
                    freq='YS'
                )
                indices['temperature_seasonality'].attrs['units'] = '%'
                indices['temperature_seasonality'].attrs['long_name'] = 'Temperature Seasonality (Coefficient of Variation)'
                indices['temperature_seasonality'].attrs['description'] = 'Annual temperature coefficient of variation (standard deviation as percentage of mean)'
            except Exception as e:
                logger.error(f"Failed to calculate temperature_seasonality: {e}")

        if 'tasmax' in ds:
            try:
                logger.info("  - Calculating heat wave index (total heat wave days, Phase 9)...")
                indices['heat_wave_index'] = atmos.heat_wave_index(
                    tasmax=ds.tasmax,
                    thresh='25 degC',
                    window=5,
                    freq='YS'
                )
                indices['heat_wave_index'].attrs['long_name'] = 'Heat Wave Index (Total Heat Wave Days)'
                indices['heat_wave_index'].attrs['description'] = 'Total days that are part of a heat wave (5+ consecutive days with tasmax > 25°C)'
            except Exception as e:
                logger.error(f"Failed to calculate heat_wave_index: {e}")

        return indices

    def fix_count_indices(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Fix count indices to prevent xarray timedelta encoding.

        xarray interprets units='days' as a CF timedelta unit and converts
        float64 → timedelta64[ns] during NetCDF write, resulting in NaT values.

        Solution: Change units to '1' (dimensionless) to prevent timedelta encoding.

        Args:
            ds: Dataset with calculated indices

        Returns:
            Dataset with fixed count indices metadata

        Raises:
            Exception: If fix fails for any index (re-raised with context)
        """
        try:
            for idx_name in self.COUNT_INDICES:
                if idx_name in ds.data_vars:
                    original_units = ds[idx_name].attrs.get('units', 'days')

                    # Only fix if units='days' or 'day' (the problematic cases)
                    if original_units in ['days', 'day']:
                        ds[idx_name].attrs['units'] = '1'
                        ds[idx_name].attrs['comment'] = f'Count of days (dimensionless to avoid CF timedelta encoding). Original units: {original_units}'

                        logger.info(f"Fixed {idx_name}: units='{original_units}' → units='1' (dimensionless)")

            return ds

        except Exception as e:
            logger.error(f"Failed to fix count indices: {e}")
            raise

    def _process_single_tile(
        self,
        ds: xr.Dataset,
        lat_slice: slice,
        lon_slice: slice,
        tile_name: str
    ) -> Dict[str, xr.DataArray]:
        """
        Process a single spatial tile (temperature-specific override).

        This override handles baseline percentiles subsetting, which is specific
        to temperature pipeline's extreme indices.

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
        basic_indices = self.calculate_temperature_indices(tile_ds)
        extreme_indices = self.calculate_extreme_indices(tile_ds, tile_baselines)
        advanced_indices = self.calculate_advanced_temperature_indices(tile_ds)

        all_indices = {**basic_indices, **extreme_indices, **advanced_indices}
        return all_indices

    def _calculate_all_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Override to implement spatial tiling for temperature indices.

        Uses the SpatialTilingMixin to process the dataset in parallel spatial tiles
        for better memory efficiency and performance, then merges the results.

        Args:
            datasets: Dictionary with 'temperature' dataset

        Returns:
            Dictionary mapping index name to calculated DataArray
        """
        ds = datasets['temperature']

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
        Override to add temperature-specific metadata.

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

        # Add temperature-specific metadata
        result_ds.attrs['phase'] = 'Phase 9: Temperature Variability (+2 indices, total 35)'
        result_ds.attrs['baseline_period'] = PipelineConfig.BASELINE_PERIOD
        result_ds.attrs['processing'] = f'Parallel processing of {self.n_tiles} spatial tiles'
        result_ds.attrs['note'] = 'Processed with parallel spatial tiling for optimal memory and performance. Extreme indices use baseline percentiles. Count indices stored as dimensionless (units=1) to prevent CF timedelta encoding.'

        return result_ds


def main():
    """Main entry point with command-line interface."""
    indices_list = """
  Basic (19): Temperature statistics, thresholds, degree days, frost season
  Extreme (6): Percentile-based warm/cool days/nights, spell duration (uses 1981-2000 baseline)
  Advanced Phase 7 (8): Spell frequency, growing season timing, variability
  Advanced Phase 9 (2): Temperature seasonality, heat wave index
"""

    examples = """
  # Process with 2 tiles (east/west split)
  python temperature_pipeline.py --n-tiles 2

  # Process with 4 tiles (quadrants, default)
  python temperature_pipeline.py --n-tiles 4
"""

    parser = PipelineCLI.create_parser(
        "Temperature Indices",
        "Calculate 35 temperature-based climate indices (Phase 9)",
        indices_list,
        examples
    )

    parser.add_argument(
        '--n-tiles',
        type=int,
        default=4,
        choices=[2, 4],
        help='Number of spatial tiles: 2 (east/west) or 4 (quadrants) (default: 4)'
    )

    args = parser.parse_args()

    # Handle common setup (logging, warnings)
    PipelineCLI.handle_common_setup(args)

    # Validate year range
    PipelineCLI.validate_years(args.start_year, args.end_year)

    # Create and run pipeline
    pipeline = TemperaturePipeline(
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

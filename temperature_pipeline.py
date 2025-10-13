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
from concurrent.futures import ThreadPoolExecutor, as_completed

import xarray as xr
import xclim.indicators.atmos as atmos
import xclim.indices as xi
import dask

from core import BasePipeline, PipelineConfig, BaselineLoader, PipelineCLI

logger = logging.getLogger(__name__)

# Thread lock for NetCDF file writing (HDF5/NetCDF4 is not fully thread-safe)
netcdf_write_lock = threading.Lock()


class TemperaturePipeline(BasePipeline):
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
        super().__init__(
            zarr_paths={'temperature': PipelineConfig.TEMP_ZARR},
            chunk_config=PipelineConfig.DEFAULT_CHUNKS,
            **kwargs
        )
        self.n_tiles = n_tiles

        # Load baseline percentiles for extreme indices
        self.baseline_loader = BaselineLoader()
        self.baselines = self.baseline_loader.get_temperature_baselines()

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

    def _get_spatial_tiles(self, ds: xr.Dataset) -> list:
        """
        Calculate spatial tile boundaries.

        Args:
            ds: Dataset to tile

        Returns:
            List of tuples (lat_slice, lon_slice, tile_name)
        """
        lat_vals = ds.lat.values
        lon_vals = ds.lon.values

        lat_mid = len(lat_vals) // 2
        lon_mid = len(lon_vals) // 2

        if self.n_tiles == 2:
            # Split east-west
            tiles = [
                (slice(None), slice(0, lon_mid), "west"),
                (slice(None), slice(lon_mid, None), "east")
            ]
        elif self.n_tiles == 4:
            # Split into quadrants (NW, NE, SW, SE)
            tiles = [
                (slice(0, lat_mid), slice(0, lon_mid), "northwest"),
                (slice(0, lat_mid), slice(lon_mid, None), "northeast"),
                (slice(lat_mid, None), slice(0, lon_mid), "southwest"),
                (slice(lat_mid, None), slice(lon_mid, None), "southeast")
            ]
        else:
            raise ValueError(f"n_tiles must be 2 or 4, got {self.n_tiles}")

        return tiles

    def _process_spatial_tile(
        self,
        ds: xr.Dataset,
        lat_slice: slice,
        lon_slice: slice,
        tile_name: str,
        baseline_percentiles: dict
    ) -> dict:
        """
        Process a single spatial tile.

        Args:
            ds: Full dataset
            lat_slice: Latitude slice for this tile
            lon_slice: Longitude slice for this tile
            tile_name: Name of this tile (for logging)
            baseline_percentiles: Baseline percentile thresholds for extreme indices

        Returns:
            Dictionary of calculated indices for this tile
        """
        logger.info(f"  Processing tile: {tile_name}")

        # Select spatial subset
        tile_ds = ds.isel(lat=lat_slice, lon=lon_slice)

        # Subset baseline percentiles to match tile (thread-safe - no shared state mutation)
        tile_baselines = {
            key: baseline.isel(lat=lat_slice, lon=lon_slice)
            for key, baseline in baseline_percentiles.items()
        }

        # Calculate indices for this tile (passing baselines as parameter)
        basic_indices = self.calculate_temperature_indices(tile_ds)
        extreme_indices = self.calculate_extreme_indices(tile_ds, tile_baselines)
        advanced_indices = self.calculate_advanced_temperature_indices(tile_ds)

        all_indices = {**basic_indices, **extreme_indices, **advanced_indices}
        return all_indices

    def _calculate_all_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Override to implement spatial tiling for temperature indices.

        This method processes the dataset in parallel spatial tiles for better
        memory efficiency and performance, then merges the results.

        Args:
            datasets: Dictionary with 'temperature' dataset

        Returns:
            Dictionary mapping index name to calculated DataArray
        """
        combined_ds = datasets['temperature']

        # Process tiles in parallel
        logger.info(f"Processing with parallel spatial tiling ({self.n_tiles} tiles)")
        tiles = self._get_spatial_tiles(combined_ds)

        def process_and_save_tile(tile_info, output_dir):
            """Process and save a single tile (thread-safe)."""
            lat_slice, lon_slice, tile_name = tile_info
            tile_indices = self._process_spatial_tile(
                combined_ds, lat_slice, lon_slice, tile_name, self.baselines
            )

            # Save tile immediately (with lock to ensure thread-safe NetCDF writes)
            tile_ds = xr.Dataset(tile_indices)

            # Fix count indices to prevent timedelta encoding (CRITICAL FIX)
            tile_ds = self.fix_count_indices(tile_ds)

            tile_file = output_dir / f'temperature_indices_tile_{tile_name}.nc'
            logger.info(f"  Saving tile {tile_name} to {tile_file}...")

            # Use lock to prevent concurrent NetCDF writes (HDF5 library limitation)
            with netcdf_write_lock:
                with dask.config.set(scheduler='threads'):
                    encoding = {}
                    for var_name in tile_ds.data_vars:
                        encoding[var_name] = {
                            'zlib': True,
                            'complevel': 4
                        }
                    tile_ds.to_netcdf(tile_file, engine='netcdf4', encoding=encoding)

            del tile_indices, tile_ds  # Free memory
            return tile_file

        # Create temporary directory for tiles
        output_dir = Path('./outputs')
        output_dir.mkdir(parents=True, exist_ok=True)

        # Process all tiles in parallel using ThreadPoolExecutor
        tile_files_dict = {}
        tile_files_lock = threading.Lock()

        def process_and_save_tile_wrapper(tile_info):
            """Wrapper to store result in dict with proper order."""
            tile_file = process_and_save_tile(tile_info, output_dir)
            tile_name = tile_info[2]
            # Thread-safe dict update
            with tile_files_lock:
                tile_files_dict[tile_name] = tile_file
            return tile_file

        with ThreadPoolExecutor(max_workers=self.n_tiles) as executor:
            future_to_tile = {executor.submit(process_and_save_tile_wrapper, tile): tile for tile in tiles}
            for future in as_completed(future_to_tile):
                tile_info = future_to_tile[future]
                tile_name = tile_info[2]
                try:
                    future.result()
                    logger.info(f"  ✓ Tile {tile_name} completed successfully")
                except Exception as e:
                    logger.error(f"  ✗ Tile {tile_name} failed: {e}")
                    raise

        # Verify we have the expected number of tiles
        if len(tile_files_dict) != self.n_tiles:
            raise ValueError(f"Expected {self.n_tiles} tile files, but got {len(tile_files_dict)}")

        # Build tile_files list in correct order for concatenation
        if self.n_tiles == 4:
            tile_files = [
                tile_files_dict['northwest'],
                tile_files_dict['northeast'],
                tile_files_dict['southwest'],
                tile_files_dict['southeast']
            ]
        elif self.n_tiles == 2:
            tile_files = [
                tile_files_dict['west'],
                tile_files_dict['east']
            ]

        # Merge tile files lazily using xarray
        logger.info("Merging tile files...")
        tile_datasets = [xr.open_dataset(f, chunks='auto') for f in tile_files]

        # Concatenate lazily (doesn't load data into memory)
        if self.n_tiles == 4:
            # NW + NE = North, SW + SE = South
            north = xr.concat([tile_datasets[0], tile_datasets[1]], dim='lon')
            south = xr.concat([tile_datasets[2], tile_datasets[3]], dim='lon')
            merged_ds = xr.concat([north, south], dim='lat')
        elif self.n_tiles == 2:
            # West + East
            merged_ds = xr.concat([tile_datasets[0], tile_datasets[1]], dim='lon')

        # Validate dimensions after merge
        expected_dims = {'time': 1, 'lat': 621, 'lon': 1405}
        actual_dims = dict(merged_ds.dims)
        if actual_dims != expected_dims:
            raise ValueError(
                f"Dimension mismatch after tile merge: {actual_dims} != {expected_dims}. "
                f"This indicates a tile concatenation bug."
            )

        # Fix count indices to prevent timedelta encoding
        merged_ds = self.fix_count_indices(merged_ds)

        # Extract indices as dictionary
        all_indices = {var: merged_ds[var] for var in merged_ds.data_vars}

        # Clean up tile files and datasets
        for tile_ds in tile_datasets:
            try:
                tile_ds.close()
            except Exception as e:
                logger.warning(f"Failed to close tile dataset: {e}")

        for tile_file in tile_files:
            try:
                tile_file.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete tile file {tile_file}: {e}")

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

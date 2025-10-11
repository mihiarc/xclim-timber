#!/usr/bin/env python3
"""
Temperature indices pipeline for xclim-timber.
Efficiently processes temperature-based climate indices using Zarr streaming.
Calculates 35 temperature indices (19 basic + 6 extreme percentile-based + 8 Phase 7 + 2 Phase 9).
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional
import warnings
import xarray as xr
import xclim.indicators.atmos as atmos
import xclim.indices as xi  # For temperature_seasonality (Phase 9)
from dask.distributed import Client
import dask
import psutil
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Suppress common warnings that don't affect functionality
warnings.filterwarnings('ignore', category=UserWarning, message='.*cell_methods.*')
warnings.filterwarnings('ignore', category=UserWarning, message='.*specified chunks.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*All-NaN slice.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*divide.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*invalid value.*')
warnings.filterwarnings('ignore', category=FutureWarning, message='.*return type of.*Dataset.dims.*')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Thread lock for NetCDF file writing (HDF5/NetCDF4 is not fully thread-safe)
netcdf_write_lock = threading.Lock()


class TemperaturePipeline:
    """
    Memory-efficient temperature indices pipeline using Zarr streaming.
    Processes 35 temperature indices without loading full dataset into memory.

    Indices:
    - Basic (19): Core temperature statistics, thresholds, degree days, frost season
    - Extreme (6): Percentile-based warm/cool days/nights, spell duration
    - Advanced Phase 7 (8): Spell frequency, growing season timing, variability
    - Advanced Phase 9 (2): Temperature seasonality, heat wave index
    """

    def __init__(self, chunk_years: int = 1, enable_dashboard: bool = False, use_spatial_tiling: bool = False, n_tiles: int = 4, parallel_tiles: bool = False):
        """
        Initialize the pipeline.

        Args:
            chunk_years: Number of years to process in each temporal chunk (default: 1 for memory efficiency)
            enable_dashboard: Whether to enable Dask dashboard
            use_spatial_tiling: Whether to use spatial tiling to reduce memory (default: False)
            n_tiles: Number of spatial tiles (2 or 4, default: 4 for quadrants)
            parallel_tiles: Whether to process tiles in parallel (default: False, requires use_spatial_tiling=True)
        """
        self.chunk_years = chunk_years
        self.enable_dashboard = enable_dashboard
        self.use_spatial_tiling = use_spatial_tiling
        self.n_tiles = n_tiles
        self.parallel_tiles = parallel_tiles
        self.client = None

        # Zarr store path for temperature data
        self.zarr_store = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'

        # Baseline percentiles path
        self.baseline_file = Path('data/baselines/baseline_percentiles_1981_2000.nc')

        # Load baseline percentiles for extreme indices
        self.baseline_percentiles = self._load_baseline_percentiles()

        # Memory-optimized chunk configuration (smaller spatial chunks)
        self.chunk_config = {
            'time': 365,   # One year of daily data
            'lat': 103,    # 621 / 103 = 6 chunks (smaller for less memory)
            'lon': 201     # 1405 / 201 = 7 chunks (smaller for less memory)
        }

    def setup_dask_client(self):
        """Initialize Dask client with memory limits."""
        # Use threaded scheduler instead of distributed for lower memory overhead
        logger.info("Using Dask threaded scheduler (no distributed client for memory efficiency)")

    def close(self):
        """Clean up resources."""
        if self.client:
            self.client.close()
            self.client = None

    def _load_baseline_percentiles(self):
        """
        Load pre-calculated baseline percentiles for extreme indices.

        Returns:
            dict: Dictionary of baseline percentile DataArrays

        Raises:
            FileNotFoundError: If baseline file doesn't exist with helpful message
        """
        if not self.baseline_file.exists():
            error_msg = f"""
ERROR: Baseline percentiles file not found at {self.baseline_file}

Please generate baseline percentiles first:
  python calculate_baseline_percentiles.py

This is a one-time operation that takes ~15 minutes.
See docs/BASELINE_DOCUMENTATION.md for more information.
            """
            raise FileNotFoundError(error_msg)

        logger.info(f"Loading baseline percentiles from {self.baseline_file}")
        ds = xr.open_dataset(self.baseline_file)

        percentiles = {
            'tx90p_threshold': ds['tx90p_threshold'],
            'tx10p_threshold': ds['tx10p_threshold'],
            'tn90p_threshold': ds['tn90p_threshold'],
            'tn10p_threshold': ds['tn10p_threshold']
        }

        logger.info(f"  Loaded {len(percentiles)} baseline percentile thresholds")
        return percentiles

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
        tile_name: str
    ) -> dict:
        """
        Process a single spatial tile.

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
        tile_baselines = {}
        for key, baseline in self.baseline_percentiles.items():
            tile_baselines[key] = baseline.isel(lat=lat_slice, lon=lon_slice)

        # Temporarily replace baselines with tile-specific ones
        original_baselines = self.baseline_percentiles
        self.baseline_percentiles = tile_baselines

        try:
            # Calculate indices for this tile
            basic_indices = self.calculate_temperature_indices(tile_ds)
            extreme_indices = self.calculate_extreme_indices(tile_ds)
            advanced_indices = self.calculate_advanced_temperature_indices(tile_ds)

            all_indices = {**basic_indices, **extreme_indices, **advanced_indices}
            return all_indices
        finally:
            # Restore original baselines
            self.baseline_percentiles = original_baselines

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

    def calculate_extreme_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate percentile-based extreme temperature indices using pre-calculated baseline.

        Args:
            ds: Dataset with temperature variables (tasmax, tasmin)

        Returns:
            Dictionary of calculated extreme indices
        """
        indices = {}

        # Warm/cool day indices (based on tasmax)
        if 'tasmax' in ds:
            logger.info("  - Calculating warm days (tx90p)...")
            indices['tx90p'] = atmos.tx90p(
                tasmax=ds.tasmax,
                tasmax_per=self.baseline_percentiles['tx90p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating cool days (tx10p)...")
            indices['tx10p'] = atmos.tx10p(
                tasmax=ds.tasmax,
                tasmax_per=self.baseline_percentiles['tx10p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating warm spell duration (WSDI)...")
            indices['warm_spell_duration_index'] = atmos.warm_spell_duration_index(
                tasmax=ds.tasmax,
                tasmax_per=self.baseline_percentiles['tx90p_threshold'],
                window=6,
                freq='YS'
            )

        # Warm/cool night indices (based on tasmin)
        if 'tasmin' in ds:
            logger.info("  - Calculating warm nights (tn90p)...")
            indices['tn90p'] = atmos.tn90p(
                tasmin=ds.tasmin,
                tasmin_per=self.baseline_percentiles['tn90p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating cool nights (tn10p)...")
            indices['tn10p'] = atmos.tn10p(
                tasmin=ds.tasmin,
                tasmin_per=self.baseline_percentiles['tn10p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating cold spell duration (CSDI)...")
            indices['cold_spell_duration_index'] = atmos.cold_spell_duration_index(
                tasmin=ds.tasmin,
                tasmin_per=self.baseline_percentiles['tn10p_threshold'],
                window=6,
                freq='YS'
            )

        return indices

    def calculate_advanced_temperature_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate advanced temperature extreme indices (Phase 7).

        Adds 8 new indices focused on:
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
                # Fix units metadata for CF-compliance
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
                # Fix units metadata for CF-compliance
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
                # Fix units metadata for CF-compliance (dimensionless)
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
                # Fix units metadata for CF-compliance (dimensionless)
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
                # Fix units metadata for CF-compliance (dimensionless)
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
                # Fix units metadata for CF-compliance (dimensionless, was "N/A")
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
                # Fix units metadata for CF-compliance
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
                # Units should already be correct (K or degC) from xclim
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
                # Fix units metadata for CF-compliance
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
                # Update long_name to distinguish from heat_wave_frequency
                indices['heat_wave_index'].attrs['long_name'] = 'Heat Wave Index (Total Heat Wave Days)'
                indices['heat_wave_index'].attrs['description'] = 'Total days that are part of a heat wave (5+ consecutive days with tasmax > 25°C)'
            except Exception as e:
                logger.error(f"Failed to calculate heat_wave_index: {e}")

        return indices


    def process_time_chunk(
        self,
        start_year: int,
        end_year: int,
        output_dir: Path
    ) -> Path:
        """
        Process a single time chunk.

        Args:
            start_year: Start year for this chunk
            end_year: End year for this chunk
            output_dir: Output directory

        Returns:
            Path to output file
        """
        logger.info(f"\nProcessing chunk: {start_year}-{end_year}")

        # Track memory
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"Initial memory: {initial_memory:.1f} MB")

        # Load temperature data
        logger.info("Loading temperature data...")
        ds = xr.open_zarr(self.zarr_store, chunks=self.chunk_config)

        # Select time range
        combined_ds = ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

        # Rename temperature variables for xclim compatibility
        rename_map = {
            'tmean': 'tas',
            'tmax': 'tasmax',
            'tmin': 'tasmin'
        }

        for old_name, new_name in rename_map.items():
            if old_name in combined_ds:
                combined_ds = combined_ds.rename({old_name: new_name})
                logger.debug(f"Renamed {old_name} to {new_name}")

        # Fix units for temperature variables
        unit_fixes = {
            'tas': 'degC',
            'tasmax': 'degC',
            'tasmin': 'degC'
        }

        for var_name, unit in unit_fixes.items():
            if var_name in combined_ds:
                combined_ds[var_name].attrs['units'] = unit
                combined_ds[var_name].attrs['standard_name'] = self._get_standard_name(var_name)

        # Calculate indices with optional spatial tiling
        if self.use_spatial_tiling:
            tiles = self._get_spatial_tiles(combined_ds)
            tile_files = []

            if self.parallel_tiles:
                # Process tiles in parallel
                logger.info(f"Processing with spatial tiling ({self.n_tiles} tiles IN PARALLEL)")

                def process_and_save_tile(tile_info):
                    lat_slice, lon_slice, tile_name = tile_info
                    tile_indices = self._process_spatial_tile(combined_ds, lat_slice, lon_slice, tile_name)

                    # Save tile immediately (with lock to ensure thread-safe NetCDF writes)
                    tile_ds = xr.Dataset(tile_indices)
                    tile_file = output_dir / f'temperature_indices_{start_year}_{end_year}_tile_{tile_name}.nc'
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

                # Process all tiles in parallel using ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=self.n_tiles) as executor:
                    future_to_tile = {executor.submit(process_and_save_tile, tile): tile for tile in tiles}
                    for future in as_completed(future_to_tile):
                        tile_file = future.result()
                        tile_files.append(tile_file)

            else:
                # Process tiles sequentially (original behavior)
                logger.info(f"Processing with spatial tiling ({self.n_tiles} tiles sequentially)")

                for lat_slice, lon_slice, tile_name in tiles:
                    tile_indices = self._process_spatial_tile(combined_ds, lat_slice, lon_slice, tile_name)

                    # Save tile immediately
                    tile_ds = xr.Dataset(tile_indices)
                    tile_file = output_dir / f'temperature_indices_{start_year}_{end_year}_tile_{tile_name}.nc'
                    logger.info(f"  Saving tile {tile_name} to {tile_file}...")

                    with dask.config.set(scheduler='threads'):
                        encoding = {}
                        for var_name in tile_ds.data_vars:
                            encoding[var_name] = {
                                'zlib': True,
                                'complevel': 4
                            }
                        tile_ds.to_netcdf(tile_file, engine='netcdf4', encoding=encoding)

                    tile_files.append(tile_file)
                    del tile_indices, tile_ds  # Free memory

            # Merge tile files lazily using xarray
            logger.info("Merging tile files...")
            output_file = output_dir / f'temperature_indices_{start_year}_{end_year}.nc'

            # Open tiles with chunking (lazy loading)
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

            # Add metadata
            merged_ds.attrs['creation_date'] = datetime.now().isoformat()
            merged_ds.attrs['software'] = 'xclim-timber temperature pipeline v4.1 (Spatial Tiling)'
            merged_ds.attrs['time_range'] = f'{start_year}-{end_year}'
            merged_ds.attrs['indices_count'] = len(merged_ds.data_vars)
            merged_ds.attrs['phase'] = 'Phase 9: Temperature Variability (+2 indices, total 35)'
            merged_ds.attrs['baseline_period'] = '1981-2000'
            merged_ds.attrs['spatial_tiling'] = f'{self.n_tiles} tiles merged'
            merged_ds.attrs['note'] = 'Processed with spatial tiling for memory efficiency. Extreme indices use baseline percentiles.'

            # Save merged dataset (compute in chunks to avoid OOM)
            logger.info(f"Saving merged dataset to {output_file}...")
            with dask.config.set(scheduler='threads'):
                encoding = {}
                for var_name in merged_ds.data_vars:
                    encoding[var_name] = {
                        'zlib': True,
                        'complevel': 4,
                        'chunksizes': (1, 69, 281)
                    }

                # Use delayed writing to avoid loading all data at once
                merged_ds.to_netcdf(
                    output_file,
                    engine='netcdf4',
                    encoding=encoding,
                    compute=True  # Let dask handle the computation in chunks
                )

            # Clean up
            for ds in tile_datasets:
                ds.close()
            for tile_file in tile_files:
                tile_file.unlink()

            logger.info(f"Merged tiles into {output_file}")

            # Report file size
            file_size_mb = output_file.stat().st_size / (1024 * 1024)
            logger.info(f"Output file size: {file_size_mb:.2f} MB")

            # Track final memory
            final_memory = process.memory_info().rss / 1024 / 1024
            logger.info(f"Final memory: {final_memory:.1f} MB (increase: {final_memory - initial_memory:.1f} MB)")

            return output_file

        else:
            # Calculate basic temperature indices
            logger.info("Calculating basic temperature indices...")
            basic_indices = self.calculate_temperature_indices(combined_ds)
            logger.info(f"  Calculated {len(basic_indices)} basic indices")

            # Calculate extreme temperature indices
            logger.info("Calculating extreme temperature indices...")
            extreme_indices = self.calculate_extreme_indices(combined_ds)
            logger.info(f"  Calculated {len(extreme_indices)} extreme indices")

            # Calculate Phase 7 & Phase 9 advanced temperature indices
            logger.info("Calculating advanced temperature indices (Phase 7 & 9)...")
            advanced_indices = self.calculate_advanced_temperature_indices(combined_ds)
            logger.info(f"  Calculated {len(advanced_indices)} advanced indices")

            # Merge all indices
            all_indices = {**basic_indices, **extreme_indices, **advanced_indices}
            logger.info(f"  Total: {len(all_indices)} temperature indices")
            logger.info(f"    Basic: {len(basic_indices)}, Extreme: {len(extreme_indices)}, Advanced (Phase 7+9): {len(advanced_indices)}")

        if not all_indices:
            logger.warning("No indices calculated")
            return None

        # Combine indices into dataset
        logger.info(f"Combining {len(all_indices)} indices into dataset...")
        result_ds = xr.Dataset(all_indices)

        # Add metadata
        result_ds.attrs['creation_date'] = datetime.now().isoformat()
        result_ds.attrs['software'] = 'xclim-timber temperature pipeline v4.0 (Phase 9)'
        result_ds.attrs['time_range'] = f'{start_year}-{end_year}'
        result_ds.attrs['indices_count'] = len(all_indices)
        result_ds.attrs['phase'] = 'Phase 9: Temperature Variability (+2 indices, total 35)'
        result_ds.attrs['baseline_period'] = '1981-2000'
        result_ds.attrs['note'] = 'Extreme indices (tx90p, tx10p, tn90p, tn10p, WSDI, CSDI) use baseline percentiles. Phase 7 adds spell frequency, growing season timing. Phase 9 adds temperature seasonality and heat wave index.'

        # Save output
        output_file = output_dir / f'temperature_indices_{start_year}_{end_year}.nc'
        logger.info(f"Saving to {output_file}...")

        with dask.config.set(scheduler='threads'):
            encoding = {}
            for var_name in result_ds.data_vars:
                encoding[var_name] = {
                    'zlib': True,
                    'complevel': 4,
                    'chunksizes': (1, 69, 281)  # Aligned chunks for storage
                }

            result_ds.to_netcdf(
                output_file,
                engine='netcdf4',
                encoding=encoding
            )

        # Report memory usage
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"Final memory: {final_memory:.1f} MB (increase: {final_memory - initial_memory:.1f} MB)")

        # Report file size
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        logger.info(f"Output file size: {file_size_mb:.2f} MB")

        return output_file

    def _get_standard_name(self, var_name: str) -> str:
        """Get CF-compliant standard name for temperature variable."""
        standard_names = {
            'tas': 'air_temperature',
            'tasmax': 'air_temperature',
            'tasmin': 'air_temperature'
        }
        return standard_names.get(var_name, 'air_temperature')

    def run(
        self,
        start_year: int,
        end_year: int,
        output_dir: str = './outputs'
    ) -> List[Path]:
        """
        Run the pipeline for specified years.

        Args:
            start_year: Start year
            end_year: End year
            output_dir: Output directory path

        Returns:
            List of output file paths
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info("=" * 60)
        logger.info("TEMPERATURE INDICES PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Period: {start_year}-{end_year}")
        logger.info(f"Output: {output_path}")
        logger.info(f"Chunk size: {self.chunk_years} years")

        # Setup Dask
        self.setup_dask_client()

        output_files = []

        try:
            # Process in temporal chunks
            current_year = start_year
            while current_year <= end_year:
                chunk_end = min(current_year + self.chunk_years - 1, end_year)

                output_file = self.process_time_chunk(
                    current_year,
                    chunk_end,
                    output_path
                )

                if output_file:
                    output_files.append(output_file)

                current_year = chunk_end + 1

            logger.info("=" * 60)
            logger.info(f"✓ Pipeline complete! Generated {len(output_files)} files")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.close()

        return output_files


def main():
    """Main entry point with command-line interface."""
    parser = argparse.ArgumentParser(
        description="Temperature Indices Pipeline: Calculate 35 temperature-based climate indices (Phase 9)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Indices calculated:
  Basic (19): Temperature statistics, thresholds, degree days, frost season
  Extreme (6): Percentile-based warm/cool days/nights, spell duration (uses 1981-2000 baseline)
  Advanced Phase 7 (8): Spell frequency, growing season timing, variability
  Advanced Phase 9 (2): Temperature seasonality, heat wave index

Examples:
  # Process default period (1981-2024)
  python temperature_pipeline.py

  # Process single year
  python temperature_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python temperature_pipeline.py --output-dir ./results
        """
    )

    parser.add_argument(
        '--start-year',
        type=int,
        default=1981,
        help='Start year for processing (default: 1981)'
    )

    parser.add_argument(
        '--end-year',
        type=int,
        default=2024,
        help='End year for processing (default: 2024)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='./outputs',
        help='Output directory for results (default: ./outputs)'
    )

    parser.add_argument(
        '--chunk-years',
        type=int,
        default=1,
        help='Number of years to process per chunk (default: 1 for memory efficiency)'
    )

    parser.add_argument(
        '--dashboard',
        action='store_true',
        help='Enable Dask dashboard on port 8787'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--show-warnings',
        action='store_true',
        help='Show all warnings (default: suppressed)'
    )

    parser.add_argument(
        '--spatial-tiling',
        action='store_true',
        help='Enable spatial tiling to reduce memory usage (~4x less RAM)'
    )

    parser.add_argument(
        '--n-tiles',
        type=int,
        default=4,
        choices=[2, 4],
        help='Number of spatial tiles: 2 (east/west) or 4 (quadrants) (default: 4)'
    )

    parser.add_argument(
        '--parallel-tiles',
        action='store_true',
        help='Process tiles in parallel for ~3-4x speedup (requires --spatial-tiling)'
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Re-enable warnings if requested
    if args.show_warnings:
        warnings.resetwarnings()
        logger.info("Warnings enabled")

    # Create and run pipeline
    pipeline = TemperaturePipeline(
        chunk_years=args.chunk_years,
        enable_dashboard=args.dashboard,
        use_spatial_tiling=args.spatial_tiling,
        n_tiles=args.n_tiles,
        parallel_tiles=args.parallel_tiles
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
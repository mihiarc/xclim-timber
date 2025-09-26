#!/usr/bin/env python3
"""
Simple test runner for a single temperature index with proper Zarr streaming.
This minimal implementation tests memory-efficient processing.
"""

import xarray as xr
import xclim.indicators.atmos as atmos
from dask.distributed import Client
import logging
import sys
from pathlib import Path
import dask
import psutil
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_multiple_indices_streaming(start_year: int = 2023, end_year: int = 2023):
    """
    Process multiple temperature indices with proper Zarr streaming.

    This function demonstrates:
    1. Proper chunked loading from Zarr
    2. Lazy computation with Dask for multiple indices
    3. Minimal memory footprint with multiple calculations
    """

    logger.info(f"Starting multiple indices calculation for {start_year}-{end_year}")

    # Path to PRISM temperature Zarr store
    zarr_path = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'

    # Step 1: Open Zarr with explicit small chunks
    logger.info("Opening Zarr store with explicit chunking...")

    # Use chunks that evenly divide dimensions for better performance
    # lat=621 divides evenly by 69 (9 chunks), lon=1405 divides evenly by 281 (5 chunks)
    # This creates 45 spatial chunks total, which is manageable
    chunk_config = {
        'time': 365,  # One year of daily data
        'lat': 69,    # 621 / 69 = 9 even chunks
        'lon': 281    # 1405 / 281 = 5 even chunks
    }

    try:
        ds = xr.open_zarr(zarr_path, chunks=chunk_config)
        logger.info(f"Opened dataset with dimensions: {dict(ds.dims)}")
        logger.info(f"Available variables: {list(ds.data_vars)}")

        # Check actual chunk sizes
        if 'tmean' in ds:
            logger.info(f"tmean chunk sizes: {ds.tmean.chunks}")
        elif 'tmax' in ds:
            logger.info(f"Using tmax, chunk sizes: {ds.tmax.chunks}")

    except Exception as e:
        logger.error(f"Failed to open Zarr store: {e}")
        return

    # Step 2: Select time range (still lazy)
    logger.info(f"Selecting time range {start_year}-{end_year} (lazy operation)...")
    ds_subset = ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))
    logger.info(f"Subset dimensions: time={len(ds_subset.time)}, lat={len(ds_subset.lat)}, lon={len(ds_subset.lon)}")

    # Step 3: Rename variables for xclim compatibility
    logger.info("Renaming variables for xclim...")
    rename_map = {
        'tmean': 'tas',
        'tmax': 'tasmax',
        'tmin': 'tasmin'
    }

    for old_name, new_name in rename_map.items():
        if old_name in ds_subset:
            ds_subset = ds_subset.rename({old_name: new_name})
            logger.info(f"Renamed {old_name} to {new_name}")

    # Step 4: Set proper attributes for all temperature variables
    for var_name in ['tas', 'tasmax', 'tasmin']:
        if var_name in ds_subset:
            # Fix the units from 'degrees_celsius' to 'degC' which pint understands
            ds_subset[var_name].attrs['units'] = 'degC'
            ds_subset[var_name].attrs['standard_name'] = 'air_temperature'
            logger.info(f"Set units for {var_name}: degC")

    # Determine which variables we have
    available_vars = [v for v in ['tas', 'tasmax', 'tasmin'] if v in ds_subset]
    logger.info(f"Available variables for processing: {available_vars}")

    if not available_vars:
        logger.error("No suitable temperature variables found")
        return

    # Memory monitoring
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    logger.info(f"Initial memory usage: {initial_memory:.1f} MB")

    # Step 5: Initialize minimal Dask client
    logger.info("Setting up Dask client...")
    client = Client(
        n_workers=2,
        threads_per_worker=1,
        memory_limit='2GB',  # Strict memory limit per worker
        dashboard_address=None  # No dashboard to reduce overhead
    )
    logger.info(f"Dask client ready: {client}")

    try:
        # Step 6: Calculate multiple indices
        logger.info("Calculating multiple temperature indices...")

        indices = {}

        # Calculate annual mean temperature
        if 'tas' in ds_subset:
            logger.info("  - Calculating annual mean temperature (tg_mean)...")
            indices['tg_mean'] = atmos.tg_mean(ds_subset.tas, freq='YS')
            logger.info(f"    Shape: {indices['tg_mean'].shape}, Chunks: {indices['tg_mean'].chunks}")

        # Calculate annual maximum temperature
        if 'tasmax' in ds_subset:
            logger.info("  - Calculating annual maximum temperature (tx_max)...")
            indices['tx_max'] = atmos.tx_max(ds_subset.tasmax, freq='YS')
            logger.info(f"    Shape: {indices['tx_max'].shape}, Chunks: {indices['tx_max'].chunks}")

        # Calculate annual minimum temperature
        if 'tasmin' in ds_subset:
            logger.info("  - Calculating annual minimum temperature (tn_min)...")
            indices['tn_min'] = atmos.tn_min(ds_subset.tasmin, freq='YS')
            logger.info(f"    Shape: {indices['tn_min'].shape}, Chunks: {indices['tn_min'].chunks}")

        # Calculate frost days
        if 'tasmin' in ds_subset:
            logger.info("  - Calculating frost days...")
            indices['frost_days'] = atmos.frost_days(ds_subset.tasmin, freq='YS')
            logger.info(f"    Shape: {indices['frost_days'].shape}, Chunks: {indices['frost_days'].chunks}")

        # Calculate summer days
        if 'tasmax' in ds_subset:
            logger.info("  - Calculating summer days...")
            indices['summer_days'] = atmos.tx_days_above(ds_subset.tasmax, thresh='25 degC', freq='YS')
            logger.info(f"    Shape: {indices['summer_days'].shape}, Chunks: {indices['summer_days'].chunks}")

        logger.info(f"Prepared {len(indices)} indices for calculation")

        # Check memory before combining
        current_memory = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"Memory after index preparation: {current_memory:.1f} MB (increase: {current_memory - initial_memory:.1f} MB)")

        # Step 7: Combine indices into a dataset and save
        output_file = Path('outputs') / f'multiple_indices_{start_year}_{end_year}.nc'
        output_file.parent.mkdir(exist_ok=True)

        logger.info(f"Combining {len(indices)} indices into dataset...")

        # Combine into dataset
        result_ds = xr.Dataset(indices)

        logger.info(f"Saving to {output_file} with streaming computation...")

        # Use to_netcdf with compute=True (default) to trigger streaming computation
        # The computation happens chunk by chunk as data is written
        with dask.config.set(scheduler='threads'):
            # Create encoding for each variable
            encoding = {}
            for var_name in result_ds.data_vars:
                encoding[var_name] = {
                    'zlib': True,
                    'complevel': 4,
                    'chunksizes': (1, 50, 50)  # Store in small chunks
                }

            result_ds.to_netcdf(
                output_file,
                engine='netcdf4',
                encoding=encoding
            )

        logger.info(f"✓ Successfully saved to {output_file}")

        # Check file size
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        logger.info(f"Output file size: {file_size_mb:.2f} MB")

        # Final memory check
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"Final memory usage: {final_memory:.1f} MB")
        logger.info(f"Memory increase: {final_memory - initial_memory:.1f} MB")

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise
    finally:
        logger.info("Closing Dask client...")
        client.close()

    logger.info("✓ Processing complete!")


if __name__ == "__main__":
    # Process single year by default for testing
    if len(sys.argv) > 2:
        start = int(sys.argv[1])
        end = int(sys.argv[2])
    else:
        start = 2023
        end = 2023

    process_multiple_indices_streaming(start, end)
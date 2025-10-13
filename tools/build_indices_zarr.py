#!/usr/bin/env python3
"""
Build Zarr store from annual climate indices NetCDF files.
Optimized for fast time series extraction at point locations.

Chunking Strategy:
- Large in time dimension (all years together)
- Moderate spatial chunks for efficient point queries
- Target: ~150-500MB per chunk

Usage:
    python build_indices_zarr.py --input-pattern "outputs/production_v2/temperature/*.nc" \
                                   --output-zarr "outputs/zarr_stores/temperature_indices.zarr" \
                                   --pipeline temperature
"""

import argparse
import logging
from pathlib import Path
import xarray as xr
import dask
from typing import Union
import shutil

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def build_zarr_store(
    nc_pattern: str,
    output_zarr: Union[str, Path],
    pipeline_name: str = "climate",
    chunk_config: dict = None,
    overwrite: bool = False
) -> Path:
    """
    Build Zarr store from multiple annual NetCDF files.

    Args:
        nc_pattern: Glob pattern for NetCDF files (e.g., "outputs/*.nc")
        output_zarr: Path for output Zarr store
        pipeline_name: Name of pipeline (for metadata)
        chunk_config: Custom chunk configuration (optional)
        overwrite: If True, remove existing Zarr store first

    Returns:
        Path to created Zarr store
    """
    output_zarr = Path(output_zarr)

    # Check if zarr store exists
    if output_zarr.exists():
        if overwrite:
            logger.warning(f"Removing existing Zarr store: {output_zarr}")
            shutil.rmtree(output_zarr)
        else:
            raise ValueError(f"Zarr store already exists: {output_zarr}. Use --overwrite to replace.")

    # Create output directory
    output_zarr.parent.mkdir(parents=True, exist_ok=True)

    # Find all NetCDF files
    nc_files = sorted(Path().glob(nc_pattern))
    logger.info(f"Found {len(nc_files)} NetCDF files to process")

    if not nc_files:
        raise ValueError(f"No files found matching pattern: {nc_pattern}")

    # Log file range
    logger.info(f"First file: {nc_files[0].name}")
    logger.info(f"Last file:  {nc_files[-1].name}")

    # Open all files with dask (lazy loading)
    logger.info("Opening NetCDF files (lazy mode)...")
    datasets = []
    for nc_file in nc_files:
        try:
            # Open with decode_timedelta=False to avoid type issues
            ds = xr.open_dataset(nc_file, chunks='auto', decode_timedelta=False)
            datasets.append(ds)
        except Exception as e:
            logger.error(f"Failed to open {nc_file}: {e}")
            raise

    # Concatenate along time dimension
    logger.info("Concatenating datasets along time dimension...")
    combined = xr.concat(datasets, dim='time', combine_attrs='override')

    # Get dimensions
    n_time = len(combined.time)
    n_lat = len(combined.lat)
    n_lon = len(combined.lon)
    n_vars = len(combined.data_vars)

    logger.info(f"Combined dataset dimensions:")
    logger.info(f"  time: {n_time} years")
    logger.info(f"  lat:  {n_lat} points")
    logger.info(f"  lon:  {n_lon} points")
    logger.info(f"  variables: {n_vars} climate indices")

    # Calculate optimal chunks for time series extraction
    # Strategy: Keep entire time series together, moderate spatial chunks
    if chunk_config is None:
        # For time series extraction: large time chunks, smaller spatial chunks
        chunk_config = {
            'time': n_time,        # All years together (optimal for time series)
            'lat': min(n_lat, 103),  # ~6 chunks for lat (same as pipeline)
            'lon': min(n_lon, 201)   # ~7 chunks for lon (same as pipeline)
        }
    else:
        # Use provided config, but fill in defaults if missing
        if 'time' not in chunk_config:
            chunk_config['time'] = n_time
        if 'lat' not in chunk_config:
            chunk_config['lat'] = min(n_lat, 103)
        if 'lon' not in chunk_config:
            chunk_config['lon'] = min(n_lon, 201)

    logger.info(f"Chunking configuration:")
    logger.info(f"  time: {chunk_config['time']} (all years together)")
    logger.info(f"  lat:  {chunk_config['lat']} ({n_lat // chunk_config['lat']} chunks)")
    logger.info(f"  lon:  {chunk_config['lon']} ({n_lon // chunk_config['lon']} chunks)")

    # Rechunk for optimal time series access
    logger.info("Rechunking dataset for time series extraction...")
    combined = combined.chunk(chunk_config)

    # Add metadata
    combined.attrs['created'] = str(Path(__file__).name)
    combined.attrs['pipeline'] = pipeline_name
    combined.attrs['time_range'] = f"{n_time} years"
    combined.attrs['n_indices'] = n_vars
    combined.attrs['chunk_strategy'] = 'optimized_for_time_series_extraction'
    combined.attrs['note'] = 'Chunked with entire time series together for fast point extraction'

    # Create encoding for all variables
    logger.info("Configuring compression settings...")
    encoding = {}
    for var_name in combined.data_vars:
        encoding[var_name] = {
            'compressor': None,  # Let Zarr use default compressor
            'chunks': (chunk_config['time'], chunk_config['lat'], chunk_config['lon'])
        }

    # Coordinate encoding
    encoding['time'] = {'chunks': (chunk_config['time'],)}
    encoding['lat'] = {'chunks': (chunk_config['lat'],)}
    encoding['lon'] = {'chunks': (chunk_config['lon'],)}

    # Write to Zarr store
    logger.info(f"Writing to Zarr store: {output_zarr}")
    logger.info("This may take several minutes...")

    with dask.config.set(scheduler='threads'):
        combined.to_zarr(
            output_zarr,
            mode='w',
            encoding=encoding,
            consolidated=True,  # Create consolidated metadata for faster opening
            compute=True
        )

    # Report final size
    zarr_size_mb = sum(f.stat().st_size for f in output_zarr.rglob('*') if f.is_file()) / (1024 * 1024)
    logger.info(f"✓ Zarr store created: {zarr_size_mb:.1f} MB")

    # Close datasets
    for ds in datasets:
        ds.close()

    return output_zarr


def append_year_to_zarr(
    nc_file: Union[str, Path],
    zarr_store: Union[str, Path]
) -> None:
    """
    Append a new year to existing Zarr store.

    Args:
        nc_file: Path to NetCDF file with new year
        zarr_store: Path to existing Zarr store
    """
    zarr_store = Path(zarr_store)

    if not zarr_store.exists():
        raise ValueError(f"Zarr store not found: {zarr_store}")

    logger.info(f"Appending {nc_file} to {zarr_store}")

    # Open new data
    new_ds = xr.open_dataset(nc_file, decode_timedelta=False)

    # Open existing zarr store to get chunk config
    existing = xr.open_zarr(zarr_store)
    chunk_config = {
        'time': existing.chunks['time'][0] if hasattr(existing.chunks['time'], '__getitem__') else existing.chunks['time'],
        'lat': existing.chunks['lat'][0] if hasattr(existing.chunks['lat'], '__getitem__') else existing.chunks['lat'],
        'lon': existing.chunks['lon'][0] if hasattr(existing.chunks['lon'], '__getitem__') else existing.chunks['lon']
    }
    existing.close()

    # Rechunk new data to match
    new_ds = new_ds.chunk(chunk_config)

    # Append to store
    with dask.config.set(scheduler='threads'):
        new_ds.to_zarr(
            zarr_store,
            mode='a',
            append_dim='time',
            consolidated=True
        )

    logger.info("✓ Year appended successfully")
    new_ds.close()


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Build Zarr store from climate indices NetCDF files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build temperature indices Zarr store
  python build_indices_zarr.py \\
      --input-pattern "outputs/production_v2/temperature/*.nc" \\
      --output-zarr "outputs/zarr_stores/temperature_indices.zarr" \\
      --pipeline temperature

  # Append new year to existing store
  python build_indices_zarr.py \\
      --append outputs/production_v2/temperature/temperature_indices_2025_2025.nc \\
      --zarr-store outputs/zarr_stores/temperature_indices.zarr

  # Overwrite existing store
  python build_indices_zarr.py \\
      --input-pattern "outputs/production_v2/temperature/*.nc" \\
      --output-zarr "outputs/zarr_stores/temperature_indices.zarr" \\
      --overwrite
        """
    )

    parser.add_argument(
        '--input-pattern',
        help='Glob pattern for input NetCDF files (e.g., "outputs/*.nc")'
    )

    parser.add_argument(
        '--output-zarr',
        help='Output path for Zarr store (e.g., "outputs/indices.zarr")'
    )

    parser.add_argument(
        '--pipeline',
        default='climate',
        help='Pipeline name for metadata (default: climate)'
    )

    parser.add_argument(
        '--chunk-time',
        type=int,
        help='Custom time chunk size (default: all years)'
    )

    parser.add_argument(
        '--chunk-lat',
        type=int,
        default=103,
        help='Latitude chunk size (default: 103)'
    )

    parser.add_argument(
        '--chunk-lon',
        type=int,
        default=201,
        help='Longitude chunk size (default: 201)'
    )

    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Overwrite existing Zarr store'
    )

    parser.add_argument(
        '--append',
        help='NetCDF file to append to existing Zarr store (requires --zarr-store)'
    )

    parser.add_argument(
        '--zarr-store',
        help='Existing Zarr store (for append mode)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Append mode
    if args.append:
        if not args.zarr_store:
            parser.error("--zarr-store required for append mode")
        append_year_to_zarr(args.append, args.zarr_store)
        return 0

    # Build mode
    if not args.input_pattern or not args.output_zarr:
        parser.error("--input-pattern and --output-zarr required for build mode")

    # Custom chunk config if specified
    chunk_config = None
    if args.chunk_time or args.chunk_lat or args.chunk_lon:
        chunk_config = {}
        if args.chunk_time:
            chunk_config['time'] = args.chunk_time
        if args.chunk_lat:
            chunk_config['lat'] = args.chunk_lat
        if args.chunk_lon:
            chunk_config['lon'] = args.chunk_lon

    try:
        build_zarr_store(
            args.input_pattern,
            args.output_zarr,
            args.pipeline,
            chunk_config,
            args.overwrite
        )
        logger.info("\n✓ Zarr store creation complete!")
        return 0
    except Exception as e:
        logger.error(f"\n✗ Failed to create Zarr store: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())

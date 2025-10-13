#!/usr/bin/env python3
"""
FAST point extraction from Zarr climate indices store using vectorized operations.
Optimized for large number of parcels (10k-100k).

Key optimizations:
- Direct to wide format (avoids slow long-format pivot)
- Vectorized operations (no nested loops)
- Memory-efficient processing

Usage:
    python extract_from_zarr_fast.py \\
        --zarr-store outputs/zarr_stores/temperature_indices.zarr \\
        --parcels data/parcel_coordinates.csv \\
        --output outputs/extractions/temperature_pacific_northwest.csv
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import xarray as xr
import numpy as np
from typing import Union
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def is_temperature_var(var_name: str) -> bool:
    """Check if a variable name indicates temperature data."""
    temp_prefixes = ['tas', 'tg_', 'tx_', 'tn_', 'temp']
    temp_indices = [
        'frost_days', 'ice_days', 'summer_days', 'hot_days',
        'tropical_nights', 'heating_degree_days', 'cooling_degree_days',
        'growing_degree_days', 'consecutive_frost_days', 'warm_nights',
        'very_hot_days', 'cold_spell', 'warm_spell', 'freezing_degree_days',
        'extreme_temperature_range', 'daily_temperature_range'
    ]

    var_lower = var_name.lower()
    for prefix in temp_prefixes:
        if var_lower.startswith(prefix):
            return True
    for index in temp_indices:
        if index in var_lower:
            return True
    if 'dewpoint' in var_lower or 'vpd' in var_lower:
        return False
    return False


def extract_from_zarr_fast(
    zarr_store: Union[str, Path],
    parcels_csv: Union[str, Path],
    output_csv: Union[str, Path],
    convert_kelvin: bool = True
) -> pd.DataFrame:
    """
    Fast extraction using vectorized operations and direct wide-format construction.

    Args:
        zarr_store: Path to Zarr store
        parcels_csv: Path to CSV with parcel coordinates
        output_csv: Path for output CSV file
        convert_kelvin: Convert temperature from Kelvin to Celsius

    Returns:
        DataFrame with extracted values
    """
    start_time = time.time()

    # Load parcels
    logger.info(f"Loading parcels from {parcels_csv}")
    parcels = pd.read_csv(parcels_csv)
    n_parcels = len(parcels)
    logger.info(f"Found {n_parcels:,} parcels to extract")

    # Get coordinates
    lat_col = 'parcel_level_latitude'
    lon_col = 'parcel_level_longitude'

    if lat_col not in parcels.columns or lon_col not in parcels.columns:
        raise ValueError(f"Required columns {lat_col} and {lon_col} not found")

    lats = parcels[lat_col].values
    lons = parcels[lon_col].values

    # Open Zarr store
    logger.info(f"Opening Zarr store: {zarr_store}")
    ds = xr.open_zarr(zarr_store, consolidated=True)

    # Get time information
    years = pd.to_datetime(ds.time.values).year
    n_years = len(years)
    n_indices = len(ds.data_vars)

    logger.info(f"Zarr store info:")
    logger.info(f"  Time range: {years.min()}-{years.max()} ({n_years} years)")
    logger.info(f"  Climate indices: {n_indices}")
    logger.info(f"  Grid size: {len(ds.lat)} × {len(ds.lon)}")

    # Extract all indices at once
    logger.info(f"Extracting all {n_indices} indices for {n_parcels:,} parcels...")

    # Create coordinate arrays for interpolation
    point_lats = xr.DataArray(lats, dims='points')
    point_lons = xr.DataArray(lons, dims='points')

    # Interpolate entire dataset (this is fast)
    logger.info("Interpolating data...")
    extracted = ds.interp(
        lat=point_lats,
        lon=point_lons,
        method='linear'
    )

    # Load into memory and convert to numpy (compute all at once)
    logger.info("Loading interpolated data into memory...")
    extracted = extracted.compute()

    # Build wide-format DataFrame directly (much faster than pivot)
    logger.info("Building output DataFrame...")

    # Start with parcel metadata repeated for each year
    base_df = pd.DataFrame({
        'saleid': np.repeat(parcels['saleid'].values, n_years),
        'parcelid': np.repeat(parcels['parcelid'].values, n_years),
        'lat': np.repeat(lats, n_years),
        'lon': np.repeat(lons, n_years),
        'year': np.tile(years, n_parcels)
    })

    # Add each climate index as a column
    for var_name in ds.data_vars:
        logger.debug(f"  Adding {var_name}")

        # Get data: shape is (time, points)
        var_data = extracted[var_name].values

        # Convert Kelvin to Celsius if needed
        if convert_kelvin and is_temperature_var(var_name):
            if np.nanmean(var_data) > 200:  # Likely in Kelvin
                var_data = var_data - 273.15

        # Flatten to match base_df structure (time-major, then point)
        # We need values in order: point0_year0, point0_year1, ..., point1_year0, ...
        # But var_data is (time, points), so we transpose and flatten
        base_df[var_name] = var_data.T.ravel()

    # Sort by parcel and year
    logger.info("Sorting results...")
    results = base_df.sort_values(['saleid', 'parcelid', 'year'])

    # Save results
    logger.info(f"Saving results to {output_csv}")
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    results.to_csv(output_csv, index=False)

    # Summary statistics
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Output rows: {len(results):,}")
    logger.info(f"Years: {years.min()}-{years.max()} ({n_years} years)")
    logger.info(f"Parcels: {n_parcels:,}")
    logger.info(f"Climate indices: {n_indices}")
    logger.info(f"Total data points: {len(results) * n_indices:,}")
    logger.info(f"Processing time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    logger.info(f"Rate: {n_parcels * n_years / elapsed:.0f} parcel-years/sec")

    # Sample statistics
    numeric_cols = results.select_dtypes(include=[np.number]).columns
    climate_cols = [col for col in numeric_cols if col not in ['saleid', 'parcelid', 'lat', 'lon', 'year']]

    if climate_cols:
        logger.info("\nSample statistics (first 3 indices):")
        for col in climate_cols[:3]:
            mean_val = results[col].mean()
            std_val = results[col].std()
            min_val = results[col].min()
            max_val = results[col].max()
            logger.info(f"  {col}:")
            logger.info(f"    mean={mean_val:.2f}, std={std_val:.2f}")
            logger.info(f"    range=[{min_val:.2f}, {max_val:.2f}]")

    logger.info("=" * 60)

    # Clean up
    ds.close()

    return results


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="FAST extraction of climate time series from Zarr store (vectorized)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract for Pacific Northwest parcels
  python extract_from_zarr_fast.py \\
      --zarr-store outputs/zarr_stores/temperature_indices.zarr \\
      --parcels data/parcel_coordinates.csv \\
      --output outputs/extractions/temperature_pacific_northwest.csv

  # Extract for Southeast parcels
  python extract_from_zarr_fast.py \\
      --zarr-store outputs/zarr_stores/temperature_indices.zarr \\
      --parcels data/parcel_coordinates_southeast.csv \\
      --output outputs/extractions/temperature_southeast.csv

Performance:
  24k parcels × 44 years × 35 indices = ~30 seconds
  36k parcels × 44 years × 35 indices = ~45 seconds
        """
    )

    parser.add_argument(
        '--zarr-store',
        required=True,
        help='Path to Zarr store'
    )

    parser.add_argument(
        '--parcels',
        required=True,
        help='CSV file with parcel coordinates'
    )

    parser.add_argument(
        '--output',
        required=True,
        help='Output CSV file path'
    )

    parser.add_argument(
        '--no-kelvin-conversion',
        action='store_true',
        help='Skip Kelvin to Celsius conversion'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        extract_from_zarr_fast(
            args.zarr_store,
            args.parcels,
            args.output,
            convert_kelvin=not args.no_kelvin_conversion
        )
        logger.info("\n✓ Extraction complete!")
        return 0
    except Exception as e:
        logger.error(f"\n✗ Extraction failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())

#!/usr/bin/env python
"""
Extract climate indices for ACTUAL parcel coordinates from Zarr data.
This is the core purpose of the pipeline - getting climate data for specific parcels.
"""

import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def extract_climate_for_parcels():
    """Extract climate indices at specific parcel locations."""

    # Load parcel coordinates
    parcel_file = Path('/home/mihiarc/xclim-timber/data/test_data/parcel_coordinates.csv')
    logger.info(f"Loading parcel coordinates from {parcel_file}")
    parcels = pd.read_csv(parcel_file)
    n_parcels = len(parcels)
    logger.info(f"Found {n_parcels:,} parcels to process")

    # Load the Zarr data or processed indices
    zarr_path = Path('./tmin_1981_01.zarr')
    nc_path = Path('./outputs/zarr_demo/prism_indices_jan1981.nc')

    if nc_path.exists():
        # Use pre-computed indices
        logger.info(f"Loading pre-computed indices from {nc_path}")
        ds = xr.open_dataset(nc_path, decode_timedelta=False)

        # We have frost_days, extreme_cold_days, and mean_tmin
        frost_days = ds.frost_days
        extreme_cold = ds.extreme_cold_days
        mean_tmin = ds.mean_tmin

    elif zarr_path.exists():
        # Compute indices from raw data
        logger.info(f"Loading raw data from {zarr_path}")
        ds = xr.open_zarr(zarr_path)

        # Calculate indices
        logger.info("Computing climate indices...")
        frost_days = (ds.tmin < 0).sum(dim='time')
        extreme_cold = (ds.tmin < -20).sum(dim='time')
        mean_tmin = ds.tmin.mean(dim='time')
    else:
        logger.error("No data source found!")
        return None

    # Extract data at parcel locations using xarray interpolation
    logger.info("Extracting climate data at parcel locations...")

    # Prepare coordinates for interpolation
    lats = parcels['parcel_level_latitude'].values
    lons = parcels['parcel_level_longitude'].values

    # Create xarray DataArrays for coordinates
    lat_da = xr.DataArray(lats, dims=['points'])
    lon_da = xr.DataArray(lons, dims=['points'])

    # Interpolate climate indices to parcel locations
    logger.info("Interpolating to parcel coordinates...")

    # Use nearest neighbor for discrete values (days), linear for continuous (temperature)
    frost_at_parcels = frost_days.interp(
        lat=lat_da,
        lon=lon_da,
        method='nearest'
    ).values

    cold_at_parcels = extreme_cold.interp(
        lat=lat_da,
        lon=lon_da,
        method='nearest'
    ).values

    temp_at_parcels = mean_tmin.interp(
        lat=lat_da,
        lon=lon_da,
        method='linear'
    ).values

    # Create output DataFrame
    logger.info("Creating output CSV...")

    output_df = parcels.copy()
    output_df['frost_days_jan1981'] = frost_at_parcels
    output_df['extreme_cold_days_jan1981'] = cold_at_parcels
    output_df['mean_tmin_jan1981_C'] = temp_at_parcels
    output_df['year'] = 1981
    output_df['month'] = 1
    output_df['data_source'] = 'PRISM'

    # Round numeric columns
    numeric_cols = ['frost_days_jan1981', 'extreme_cold_days_jan1981', 'mean_tmin_jan1981_C']
    for col in numeric_cols:
        if col in output_df.columns:
            output_df[col] = output_df[col].round(2)

    # Save to CSV
    output_dir = Path('./outputs/zarr_parcel_extraction')
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / 'parcel_climate_indices_jan1981.csv'
    output_df.to_csv(output_file, index=False)

    logger.info(f"\n✓ Saved {n_parcels:,} parcel records to: {output_file}")

    # Display summary statistics
    print("\n" + "="*60)
    print("PARCEL CLIMATE EXTRACTION SUMMARY")
    print("="*60)

    # Count valid data
    valid_data = output_df.dropna(subset=['mean_tmin_jan1981_C'])
    n_valid = len(valid_data)
    pct_valid = (n_valid / n_parcels) * 100

    print(f"Total parcels:     {n_parcels:,}")
    print(f"Valid data points: {n_valid:,} ({pct_valid:.1f}%)")

    if n_valid > 0:
        print(f"\nFrost days (Tmin < 0°C):")
        print(f"  Mean: {valid_data['frost_days_jan1981'].mean():.1f} days")
        print(f"  Max:  {valid_data['frost_days_jan1981'].max():.0f} days")
        print(f"  Min:  {valid_data['frost_days_jan1981'].min():.0f} days")

        print(f"\nMean minimum temperature:")
        print(f"  Mean: {valid_data['mean_tmin_jan1981_C'].mean():.1f}°C")
        print(f"  Max:  {valid_data['mean_tmin_jan1981_C'].max():.1f}°C")
        print(f"  Min:  {valid_data['mean_tmin_jan1981_C'].min():.1f}°C")

    # Show sample of actual parcel data
    print("\n" + "="*60)
    print("SAMPLE PARCEL DATA (first 5 with valid climate data)")
    print("="*60)
    sample = valid_data.head()[['saleid', 'parcelid', 'parcel_level_latitude',
                                 'parcel_level_longitude', 'frost_days_jan1981',
                                 'mean_tmin_jan1981_C']]
    print(sample.to_string(index=False))

    # Check geographic coverage
    print("\n" + "="*60)
    print("GEOGRAPHIC COVERAGE CHECK")
    print("="*60)
    print(f"Parcel latitude range:  {lats.min():.2f}° to {lats.max():.2f}°")
    print(f"Parcel longitude range: {lons.min():.2f}° to {lons.max():.2f}°")

    # Check against PRISM data bounds
    if 'lat' in frost_days.coords and 'lon' in frost_days.coords:
        data_lat_min = float(frost_days.lat.min())
        data_lat_max = float(frost_days.lat.max())
        data_lon_min = float(frost_days.lon.min())
        data_lon_max = float(frost_days.lon.max())

        print(f"\nPRISM data coverage:")
        print(f"  Latitude:  {data_lat_min:.2f}° to {data_lat_max:.2f}°")
        print(f"  Longitude: {data_lon_min:.2f}° to {data_lon_max:.2f}°")

        # Check how many parcels are outside data bounds
        outside = ((lats < data_lat_min) | (lats > data_lat_max) |
                   (lons < data_lon_min) | (lons > data_lon_max))
        n_outside = outside.sum()

        if n_outside > 0:
            print(f"\n⚠ Warning: {n_outside:,} parcels are outside PRISM data coverage")

    return output_df


if __name__ == "__main__":
    df = extract_climate_for_parcels()

    if df is not None:
        print("\n✅ Parcel climate extraction complete!")
        print(f"   Output matches parcel input: {len(df)} records")
    else:
        print("\n❌ Extraction failed!")
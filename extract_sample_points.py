#!/usr/bin/env python
"""
Extract sample points from PRISM Zarr indices results to CSV.
Creates a clean CSV output from the processed climate indices.
"""

import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path


def main():
    """Extract sample points to CSV from the NetCDF results."""

    # Path to the processed indices
    nc_path = Path('./outputs/zarr_demo/prism_indices_jan1981.nc')

    if not nc_path.exists():
        # Try loading directly from the Zarr store and computing indices
        print("NetCDF results not found. Loading Zarr data directly...")

        zarr_path = Path('./tmin_1981_01.zarr')
        if not zarr_path.exists():
            print(f"Error: Neither {nc_path} nor {zarr_path} found")
            return

        # Load Zarr data
        ds = xr.open_zarr(zarr_path)

        # Calculate frost days directly
        frost_days = (ds.tmin < 0).sum(dim='time')
        extreme_cold_days = (ds.tmin < -20).sum(dim='time')
        mean_tmin = ds.tmin.mean(dim='time')

        # Sample every 50th point for manageable CSV
        lat_idx = slice(None, None, 50)
        lon_idx = slice(None, None, 50)

        # Extract sampled data
        frost_sample = frost_days.isel(lat=lat_idx, lon=lon_idx)
        cold_sample = extreme_cold_days.isel(lat=lat_idx, lon=lon_idx)
        temp_sample = mean_tmin.isel(lat=lat_idx, lon=lon_idx)

    else:
        # Load from processed NetCDF
        print(f"Loading processed indices from {nc_path}...")
        ds = xr.open_dataset(nc_path, decode_timedelta=False)

        # Sample every 50th point
        lat_idx = slice(None, None, 50)
        lon_idx = slice(None, None, 50)

        frost_sample = ds.frost_days.isel(lat=lat_idx, lon=lon_idx)
        cold_sample = ds.extreme_cold_days.isel(lat=lat_idx, lon=lon_idx)
        temp_sample = ds.mean_tmin.isel(lat=lat_idx, lon=lon_idx)

    # Convert to DataFrame
    print("Converting to CSV format...")

    # Stack the data to create (lat, lon) pairs
    frost_stacked = frost_sample.stack(points=['lat', 'lon'])
    cold_stacked = cold_sample.stack(points=['lat', 'lon'])
    temp_stacked = temp_sample.stack(points=['lat', 'lon'])

    # Create DataFrame
    df = pd.DataFrame({
        'lat': frost_stacked.lat.values,
        'lon': frost_stacked.lon.values,
        'frost_days_jan1981': frost_stacked.values,
        'extreme_cold_days_jan1981': cold_stacked.values,
        'mean_tmin_jan1981_C': temp_stacked.values,
        'year': 1981,
        'month': 1,
        'data_source': 'PRISM',
        'processed_with': 'xclim-timber-zarr'
    })

    # Remove rows with all NaN values (ocean/missing data)
    df = df.dropna(subset=['frost_days_jan1981', 'mean_tmin_jan1981_C'], how='all')

    # Round numeric columns
    numeric_cols = ['lat', 'lon', 'frost_days_jan1981', 'extreme_cold_days_jan1981', 'mean_tmin_jan1981_C']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].round(2)

    # Save to CSV
    output_dir = Path('./outputs/zarr_csv')
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / 'prism_indices_jan1981_sampled.csv'
    df.to_csv(output_file, index=False)

    print(f"\n✓ CSV saved to: {output_file}")
    print(f"  Total points: {len(df)}")
    print(f"  Columns: {', '.join(df.columns)}")

    # Display summary statistics
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    print(f"Frost days (Tmin < 0°C):")
    print(f"  Mean: {df['frost_days_jan1981'].mean():.1f} days")
    print(f"  Max:  {df['frost_days_jan1981'].max():.0f} days")
    print(f"  Min:  {df['frost_days_jan1981'].min():.0f} days")

    print(f"\nMean minimum temperature:")
    print(f"  Mean: {df['mean_tmin_jan1981_C'].mean():.1f}°C")
    print(f"  Max:  {df['mean_tmin_jan1981_C'].max():.1f}°C")
    print(f"  Min:  {df['mean_tmin_jan1981_C'].min():.1f}°C")

    print(f"\nSpatial coverage:")
    print(f"  Latitude range:  {df['lat'].min():.1f}° to {df['lat'].max():.1f}°")
    print(f"  Longitude range: {df['lon'].min():.1f}° to {df['lon'].max():.1f}°")

    # Show first few rows
    print("\n" + "="*60)
    print("SAMPLE DATA (first 5 rows)")
    print("="*60)
    print(df.head().to_string(index=False))

    return df


if __name__ == "__main__":
    df = main()
    print("\n✅ CSV extraction complete!")
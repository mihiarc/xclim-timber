#!/usr/bin/env python
"""
Extract climate indices from Zarr processing results to CSV format.
Creates a summary CSV with spatial statistics from the PRISM January 1981 data.
"""

import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path


def extract_indices_to_csv():
    """Extract the computed indices to a CSV summary."""

    # Load the NetCDF results from our Zarr processing
    nc_path = Path('./outputs/zarr_demo/prism_indices_jan1981.nc')

    if not nc_path.exists():
        print(f"No results found at {nc_path}")
        return

    print(f"Loading indices from {nc_path}...")
    ds = xr.open_dataset(nc_path, decode_timedelta=False)

    # Create spatial statistics summary
    print("Computing spatial statistics...")

    # Sample points across the domain (every 50th point to keep CSV manageable)
    lat_sample = ds.lat[::50]
    lon_sample = ds.lon[::50]

    # Create a DataFrame for sampled points
    data = []

    for lat in lat_sample.values:
        for lon in lon_sample.values:
            point_data = {
                'lat': float(lat),
                'lon': float(lon),
                'frost_days': float(ds.frost_days.sel(lat=lat, lon=lon, method='nearest').values),
                'extreme_cold_days': float(ds.extreme_cold_days.sel(lat=lat, lon=lon, method='nearest').values),
                'mean_tmin_jan1981': float(ds.mean_tmin.sel(lat=lat, lon=lon, method='nearest').values)
            }
            data.append(point_data)

    df_points = pd.DataFrame(data)

    # Filter out ocean/missing data points
    df_points = df_points[df_points['frost_days'] >= 0]

    # Save sampled points
    csv_points_path = Path('./outputs/zarr_demo/prism_indices_jan1981_points.csv')
    df_points.to_csv(csv_points_path, index=False, float_format='%.2f')
    print(f"✓ Saved point data to {csv_points_path}")
    print(f"  {len(df_points)} locations sampled")

    # Create regional summary statistics
    print("\nComputing regional statistics...")

    # Define latitude bands for regional analysis
    lat_bands = [
        ('Arctic', 60, 90),
        ('Northern', 45, 60),
        ('Mid-latitude', 30, 45),
        ('Subtropical', 20, 30),
        ('Tropical', -20, 20)
    ]

    regional_stats = []

    for region_name, lat_min, lat_max in lat_bands:
        # Select data for this latitude band
        region_mask = (ds.lat >= lat_min) & (ds.lat <= lat_max)
        if not region_mask.any():
            continue

        # Compute statistics, handling NaN values
        frost_mean = ds.frost_days.where(region_mask).mean().values
        frost_max = ds.frost_days.where(region_mask).max().values
        cold_mean = ds.extreme_cold_days.where(region_mask).mean().values
        temp_mean = ds.mean_tmin.where(region_mask).mean().values
        temp_min = ds.mean_tmin.where(region_mask).min().values
        temp_max = ds.mean_tmin.where(region_mask).max().values

        region_data = {
            'region': region_name,
            'lat_range': f"{lat_min}°N to {lat_max}°N",
            'mean_frost_days': float(frost_mean) if not np.isnan(frost_mean) else 0.0,
            'max_frost_days': float(frost_max) if not np.isnan(frost_max) else 0.0,
            'mean_extreme_cold_days': float(cold_mean) if not np.isnan(cold_mean) else 0.0,
            'mean_temperature': float(temp_mean) if not np.isnan(temp_mean) else np.nan,
            'min_temperature': float(temp_min) if not np.isnan(temp_min) else np.nan,
            'max_temperature': float(temp_max) if not np.isnan(temp_max) else np.nan
        }
        regional_stats.append(region_data)

    df_regional = pd.DataFrame(regional_stats)

    # Save regional summary
    csv_regional_path = Path('./outputs/zarr_demo/prism_indices_jan1981_regional.csv')
    df_regional.to_csv(csv_regional_path, index=False, float_format='%.2f')
    print(f"✓ Saved regional summary to {csv_regional_path}")

    # Create overall summary statistics
    summary_stats = {
        'metric': ['mean', 'std', 'min', 'max', 'median'],
        'frost_days': [
            float(ds.frost_days.mean().values),
            float(ds.frost_days.std().values),
            float(ds.frost_days.min().values),
            float(ds.frost_days.max().values),
            float(ds.frost_days.median().values)
        ],
        'extreme_cold_days': [
            float(ds.extreme_cold_days.mean().values),
            float(ds.extreme_cold_days.std().values),
            float(ds.extreme_cold_days.min().values),
            float(ds.extreme_cold_days.max().values),
            float(ds.extreme_cold_days.median().values)
        ],
        'mean_tmin': [
            float(ds.mean_tmin.mean().values),
            float(ds.mean_tmin.std().values),
            float(ds.mean_tmin.min().values),
            float(ds.mean_tmin.max().values),
            float(ds.mean_tmin.median().values)
        ]
    }

    df_summary = pd.DataFrame(summary_stats)

    # Save summary statistics
    csv_summary_path = Path('./outputs/zarr_demo/prism_indices_jan1981_summary.csv')
    df_summary.to_csv(csv_summary_path, index=False, float_format='%.2f')
    print(f"✓ Saved summary statistics to {csv_summary_path}")

    # Display preview
    print("\n" + "="*60)
    print("SUMMARY STATISTICS (January 1981)")
    print("="*60)
    print(df_summary.to_string(index=False))

    print("\n" + "="*60)
    print("REGIONAL ANALYSIS")
    print("="*60)
    print(df_regional[['region', 'mean_frost_days', 'mean_temperature']].to_string(index=False))

    print("\n" + "="*60)
    print("SAMPLE POINTS (first 5)")
    print("="*60)
    print(df_points.head().to_string(index=False))

    return df_points, df_regional, df_summary


if __name__ == "__main__":
    extract_indices_to_csv()
    print("\n✓ CSV extraction complete!")
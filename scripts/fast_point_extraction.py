#!/usr/bin/env python
"""
Fast climate indices calculation for parcel locations.
Optimized for extracting annual statistics at thousands of points.
"""

import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import xarray as xr
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def load_parcels(csv_path: str) -> pd.DataFrame:
    """Load parcel coordinates from CSV."""
    logger.info(f"Loading parcel coordinates from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} parcel locations")
    return df


def extract_at_points(ds: xr.Dataset, parcels: pd.DataFrame, variable: str = 'tas') -> np.ndarray:
    """
    Extract data at parcel locations using nearest neighbor.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Climate dataset
    parcels : pd.DataFrame
        DataFrame with parcel coordinates
    variable : str
        Variable to extract
    
    Returns:
    --------
    np.ndarray
        Array of shape (n_parcels, n_times) with extracted data
    """
    logger.info(f"Extracting {variable} at {len(parcels)} locations")
    
    # Get dataset coordinates
    ds_lats = ds.lat.values
    ds_lons = ds.lon.values
    
    # Pre-allocate output array
    n_times = len(ds.time)
    n_parcels = len(parcels)
    extracted_data = np.zeros((n_parcels, n_times))
    
    # Convert parcel longitudes to match dataset (0-360 if needed)
    parcel_lons = parcels['parcel_level_longitude'].values.copy()
    if ds_lons.min() >= 0 and parcel_lons.min() < 0:
        # Dataset uses 0-360, parcels use -180 to 180
        parcel_lons[parcel_lons < 0] += 360
    
    # Extract data for each parcel (vectorized where possible)
    for i, (lat, lon) in enumerate(zip(parcels['parcel_level_latitude'].values, parcel_lons)):
        # Find nearest grid point
        lat_idx = np.argmin(np.abs(ds_lats - lat))
        lon_idx = np.argmin(np.abs(ds_lons - lon))
        
        # Extract time series
        extracted_data[i, :] = ds[variable].isel(lat=lat_idx, lon=lon_idx).values
    
    return extracted_data


def calculate_annual_indices(data: np.ndarray, parcels: pd.DataFrame, year: int = None) -> pd.DataFrame:
    """
    Calculate annual climate indices from daily data.
    
    Parameters:
    -----------
    data : np.ndarray
        Array of shape (n_parcels, n_times) with temperature data in Kelvin
    parcels : pd.DataFrame
        DataFrame with parcel information
    year : int
        Year label
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with calculated indices
    """
    logger.info("Calculating annual climate indices")
    
    # Convert from Kelvin to Celsius if needed
    if data.mean() > 200:
        logger.info("Converting from Kelvin to Celsius")
        data = data - 273.15
    
    # Calculate indices for each parcel
    results = []
    
    for i in range(len(parcels)):
        parcel_data = data[i, :]
        
        # Skip if all NaN
        if np.all(np.isnan(parcel_data)):
            continue
        
        result = {
            'saleid': parcels.iloc[i]['saleid'],
            'parcelid': parcels.iloc[i]['parcelid'],
            'lat': parcels.iloc[i]['parcel_level_latitude'],
            'lon': parcels.iloc[i]['parcel_level_longitude'],
        }
        
        if year:
            result['year'] = year
        
        # Annual statistics
        result['annual_mean'] = np.nanmean(parcel_data)
        result['annual_min'] = np.nanmin(parcel_data)
        result['annual_max'] = np.nanmax(parcel_data)
        result['annual_std'] = np.nanstd(parcel_data)
        
        # Temperature indices
        result['frost_days'] = np.sum(parcel_data < 0)
        result['ice_days'] = np.sum(parcel_data < -10)
        result['summer_days'] = np.sum(parcel_data > 25)
        result['hot_days'] = np.sum(parcel_data > 30)
        result['tropical_nights'] = np.sum(parcel_data > 20)
        
        # Growing degree days (base 10°C)
        gdd = parcel_data - 10
        gdd[gdd < 0] = 0
        result['growing_degree_days'] = np.nansum(gdd)
        
        # Heating/Cooling degree days
        hdd = 18 - parcel_data
        hdd[hdd < 0] = 0
        result['heating_degree_days'] = np.nansum(hdd)
        
        cdd = parcel_data - 18
        cdd[cdd < 0] = 0
        result['cooling_degree_days'] = np.nansum(cdd)
        
        results.append(result)
    
    return pd.DataFrame(results)


def process_file(nc_file: str, parcels_csv: str, output_csv: str, year: int = None):
    """
    Process a single NetCDF file and extract indices at parcel locations.
    
    Parameters:
    -----------
    nc_file : str
        Path to NetCDF file
    parcels_csv : str
        Path to parcels CSV file
    output_csv : str
        Path for output CSV
    year : int
        Year label
    """
    logger.info(f"Processing {nc_file}")
    
    # Load parcels
    parcels = load_parcels(parcels_csv)
    
    # Load climate data
    logger.info("Loading climate data")
    ds = xr.open_dataset(nc_file)
    
    # Find temperature variable
    temp_vars = ['tas', 'temperature', 'temp', 'tmean']
    variable = None
    for var in temp_vars:
        if var in ds.data_vars:
            variable = var
            break
    if variable is None:
        variable = list(ds.data_vars)[0]
    
    logger.info(f"Using variable: {variable}")
    
    # Extract data at points
    data = extract_at_points(ds, parcels, variable)
    
    # Calculate indices
    df_indices = calculate_annual_indices(data, parcels, year)
    
    # Save results
    df_indices.to_csv(output_csv, index=False)
    logger.info(f"Results saved to {output_csv}")
    
    # Print summary
    logger.info("\n=== Summary Statistics ===")
    logger.info(f"Parcels processed: {len(df_indices)}")
    logger.info(f"Annual mean temperature: {df_indices['annual_mean'].mean():.2f} ± {df_indices['annual_mean'].std():.2f} °C")
    logger.info(f"Frost days: {df_indices['frost_days'].mean():.1f} ± {df_indices['frost_days'].std():.1f} days")
    logger.info(f"Summer days: {df_indices['summer_days'].mean():.1f} ± {df_indices['summer_days'].std():.1f} days")
    logger.info(f"Growing degree days: {df_indices['growing_degree_days'].mean():.0f} ± {df_indices['growing_degree_days'].std():.0f}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fast extraction of climate indices at parcel locations')
    parser.add_argument('--parcels', default='parcel_coordinates.csv',
                       help='CSV file with parcel coordinates')
    parser.add_argument('--input', '-i', required=True,
                       help='Input NetCDF file')
    parser.add_argument('--output', '-o', default='parcel_indices.csv',
                       help='Output CSV file')
    parser.add_argument('--year', type=int,
                       help='Year label for the data')
    
    args = parser.parse_args()
    
    process_file(args.input, args.parcels, args.output, args.year)
    
    logger.info("Processing complete!")


if __name__ == "__main__":
    main()
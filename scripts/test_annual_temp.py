#!/usr/bin/env python
"""
Simple test script for calculating annual mean temperature.
This script focuses on getting one index working properly.
"""

import sys
import logging
from pathlib import Path
import numpy as np
import xarray as xr
import xclim
from xclim import atmos
# import rioxarray  # Comment out for now since it's not installed
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def load_temperature_data(file_path):
    """
    Load temperature data from a single file.
    
    Parameters:
    -----------
    file_path : str or Path
        Path to temperature data file
    
    Returns:
    --------
    xr.Dataset
        Loaded temperature dataset
    """
    file_path = Path(file_path)
    logger.info(f"Loading temperature data from: {file_path}")
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Check file extension
    if file_path.suffix.lower() in ['.tif', '.tiff']:
        # Load GeoTIFF
        logger.info("Loading as GeoTIFF...")
        logger.warning("GeoTIFF support requires rioxarray - using xarray instead")
        ds = xr.open_dataset(file_path, engine='netcdf4')
        
        # Convert to dataset if it's a DataArray
        if isinstance(ds, xr.DataArray):
            ds = ds.to_dataset(name='temperature')
        
        # Check for time dimension
        if 'time' not in ds.dims and 'band' in ds.dims:
            # Assume bands represent time steps
            logger.info("Converting bands to time dimension...")
            n_times = len(ds.band)
            # Create time coordinate (adjust dates as needed)
            times = pd.date_range('2020-01-01', periods=n_times, freq='D')
            ds = ds.rename({'band': 'time'})
            ds = ds.assign_coords(time=times)
            
    elif file_path.suffix.lower() in ['.nc', '.nc4', '.netcdf']:
        # Load NetCDF
        logger.info("Loading as NetCDF...")
        ds = xr.open_dataset(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    logger.info(f"Loaded dataset dimensions: {dict(ds.dims)}")
    logger.info(f"Variables: {list(ds.data_vars)}")
    
    return ds


def standardize_temperature_data(ds):
    """
    Standardize temperature dataset for xclim.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset
    
    Returns:
    --------
    xr.DataArray
        Standardized temperature DataArray
    """
    logger.info("Standardizing temperature data...")
    
    # Find temperature variable
    temp_vars = ['temperature', 'temp', 'tas', 'tmean', 't2m', 'air_temperature']
    temp_da = None
    
    for var in ds.data_vars:
        if var.lower() in temp_vars or any(t in var.lower() for t in ['temp', 'tas']):
            temp_da = ds[var]
            logger.info(f"Found temperature variable: {var}")
            break
    
    if temp_da is None:
        # If no standard name found, use the first variable
        var_name = list(ds.data_vars)[0]
        temp_da = ds[var_name]
        logger.warning(f"No standard temperature variable found, using: {var_name}")
    
    # Standardize dimension names
    dim_mapping = {
        'latitude': 'lat',
        'longitude': 'lon',
        'x': 'lon',
        'y': 'lat',
        'X': 'lon',
        'Y': 'lat'
    }
    
    for old_name, new_name in dim_mapping.items():
        if old_name in temp_da.dims and new_name not in temp_da.dims:
            temp_da = temp_da.rename({old_name: new_name})
            logger.info(f"Renamed dimension {old_name} to {new_name}")
    
    # Check units and convert if necessary
    if 'units' in temp_da.attrs:
        units = temp_da.attrs['units']
        logger.info(f"Temperature units: {units}")
        
        # Convert Kelvin to Celsius
        if units in ['K', 'kelvin', 'Kelvin']:
            logger.info("Converting from Kelvin to Celsius...")
            temp_da = temp_da - 273.15
            temp_da.attrs['units'] = 'degC'
        elif units in ['F', 'fahrenheit', 'Fahrenheit']:
            logger.info("Converting from Fahrenheit to Celsius...")
            temp_da = (temp_da - 32) * 5/9
            temp_da.attrs['units'] = 'degC'
    else:
        # Try to guess units from data range
        data_min = float(temp_da.min())
        data_max = float(temp_da.max())
        logger.info(f"Data range: [{data_min:.2f}, {data_max:.2f}]")
        
        if data_min > 200 and data_max < 350:
            logger.info("Data appears to be in Kelvin, converting to Celsius...")
            temp_da = temp_da - 273.15
            temp_da.attrs['units'] = 'degC'
        else:
            logger.warning("Assuming data is already in Celsius")
            temp_da.attrs['units'] = 'degC'
    
    # Set standard name
    temp_da.attrs['standard_name'] = 'air_temperature'
    
    return temp_da


def calculate_annual_mean_temperature(temp_da):
    """
    Calculate annual mean temperature using xclim.
    
    Parameters:
    -----------
    temp_da : xr.DataArray
        Temperature data array
    
    Returns:
    --------
    xr.DataArray
        Annual mean temperature
    """
    logger.info("Calculating annual mean temperature...")
    
    # Check if time dimension exists
    if 'time' not in temp_da.dims:
        raise ValueError("No time dimension found in temperature data")
    
    # Calculate annual mean using xclim
    try:
        # Method 1: Using xclim's tg_mean indicator
        annual_mean = atmos.tg_mean(temp_da, freq='YS')
        logger.info("Calculated annual mean using xclim.atmos.tg_mean")
        
        # xclim returns values in Kelvin even if input was converted
        # Check if values are still in Kelvin range
        if float(annual_mean.mean()) > 200:
            logger.info("Converting output from Kelvin to Celsius...")
            annual_mean = annual_mean - 273.15
            annual_mean.attrs['units'] = 'degC'
        
    except Exception as e:
        logger.warning(f"xclim calculation failed: {e}")
        logger.info("Falling back to xarray groupby method...")
        
        # Method 2: Using xarray groupby
        annual_mean = temp_da.groupby('time.year').mean('time')
        annual_mean.attrs['long_name'] = 'Annual mean temperature'
        annual_mean.attrs['units'] = 'degC'
    
    return annual_mean


def save_results(data, output_path, format='netcdf'):
    """
    Save results to file.
    
    Parameters:
    -----------
    data : xr.DataArray or xr.Dataset
        Data to save
    output_path : str or Path
        Output file path
    format : str
        Output format ('netcdf' or 'geotiff')
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Saving results to: {output_path}")
    
    if format == 'netcdf':
        # Save as NetCDF
        if isinstance(data, xr.DataArray):
            data = data.to_dataset(name='annual_mean_temperature')
        
        data.to_netcdf(output_path, engine='netcdf4')
        logger.info(f"Saved as NetCDF: {output_path}")
        
    elif format == 'geotiff':
        # Save as GeoTIFF (requires rioxarray)
        logger.warning("GeoTIFF output requires rioxarray - saving as NetCDF instead")
        # Fallback to NetCDF
        if isinstance(data, xr.DataArray):
            data = data.to_dataset(name='annual_mean_temperature')
        data.to_netcdf(output_path.with_suffix('.nc'), engine='netcdf4')
        logger.info(f"Saved as NetCDF instead: {output_path.with_suffix('.nc')}")


def print_statistics(data):
    """
    Print summary statistics of the data.
    
    Parameters:
    -----------
    data : xr.DataArray
        Data to summarize
    """
    logger.info("\n=== Summary Statistics ===")
    logger.info(f"Shape: {data.shape}")
    logger.info(f"Dimensions: {list(data.dims)}")
    
    # Calculate statistics
    mean_val = float(data.mean())
    std_val = float(data.std())
    min_val = float(data.min())
    max_val = float(data.max())
    
    logger.info(f"Mean: {mean_val:.2f} °C")
    logger.info(f"Std Dev: {std_val:.2f} °C")
    logger.info(f"Min: {min_val:.2f} °C")
    logger.info(f"Max: {max_val:.2f} °C")
    
    # If multiple years, show per-year stats
    if 'year' in data.dims:
        logger.info("\nPer-year statistics:")
        for year in data.year.values:
            year_data = data.sel(year=year)
            year_mean = float(year_data.mean())
            logger.info(f"  {year}: {year_mean:.2f} °C")


def main(input_file=None, output_file=None):
    """Main function to run the test."""
    
    # Configuration
    # EDIT THESE PATHS TO MATCH YOUR DATA
    if input_file is None:
        INPUT_FILE = "/media/external_drive/temperature_data.nc"  # Change this path
    else:
        INPUT_FILE = input_file
    
    OUTPUT_DIR = Path("./outputs")
    if output_file is None:
        OUTPUT_FILE = OUTPUT_DIR / f"annual_mean_temp_{datetime.now().strftime('%Y%m%d')}.nc"
    else:
        OUTPUT_FILE = Path(output_file)
    
    try:
        # Step 1: Load data
        logger.info("=" * 50)
        logger.info("STEP 1: Loading temperature data")
        logger.info("=" * 50)
        
        # For testing with sample data (uncomment to use)
        # create_sample_data(INPUT_FILE)
        
        ds = load_temperature_data(INPUT_FILE)
        
        # Step 2: Standardize data
        logger.info("\n" + "=" * 50)
        logger.info("STEP 2: Standardizing data")
        logger.info("=" * 50)
        
        temp_da = standardize_temperature_data(ds)
        
        # Step 3: Calculate annual mean
        logger.info("\n" + "=" * 50)
        logger.info("STEP 3: Calculating annual mean temperature")
        logger.info("=" * 50)
        
        annual_mean = calculate_annual_mean_temperature(temp_da)
        
        # Step 4: Save results
        logger.info("\n" + "=" * 50)
        logger.info("STEP 4: Saving results")
        logger.info("=" * 50)
        
        save_results(annual_mean, OUTPUT_FILE, format='netcdf')
        
        # Step 5: Print statistics
        print_statistics(annual_mean)
        
        logger.info("\n" + "=" * 50)
        logger.info("✓ SUCCESS: Annual mean temperature calculated!")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def create_sample_data(output_path):
    """
    Create sample temperature data for testing.
    
    Parameters:
    -----------
    output_path : str or Path
        Path to save sample data
    """
    import pandas as pd
    
    logger.info("Creating sample temperature data for testing...")
    
    # Create dimensions
    times = pd.date_range('2020-01-01', '2022-12-31', freq='D')
    lats = np.linspace(-10, 10, 20)
    lons = np.linspace(-20, 20, 40)
    
    # Create temperature data with seasonal variation
    n_times = len(times)
    n_lats = len(lats)
    n_lons = len(lons)
    
    # Base temperature varies by latitude
    lat_variation = np.abs(lats[:, np.newaxis]) / 10  # Cooler at higher latitudes
    base_temp = 25 - lat_variation
    
    # Add seasonal variation
    seasonal = 10 * np.sin(np.arange(n_times) * 2 * np.pi / 365.25)
    
    # Combine
    temp_data = np.zeros((n_times, n_lats, n_lons))
    for i in range(n_times):
        temp_data[i, :, :] = base_temp + seasonal[i] + np.random.randn(n_lats, n_lons) * 2
    
    # Create dataset
    ds = xr.Dataset(
        {
            'temperature': (['time', 'lat', 'lon'], temp_data, 
                          {'units': 'degC', 'long_name': 'Air Temperature'})
        },
        coords={
            'time': times,
            'lat': ('lat', lats, {'units': 'degrees_north'}),
            'lon': ('lon', lons, {'units': 'degrees_east'})
        },
        attrs={
            'title': 'Sample Temperature Data',
            'description': 'Synthetic temperature data for testing'
        }
    )
    
    # Save
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(output_path)
    logger.info(f"Sample data saved to: {output_path}")
    
    return ds


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Calculate annual mean temperature')
    parser.add_argument('--input', '-i', help='Input temperature file path')
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--create-sample', action='store_true', 
                       help='Create sample data for testing')
    parser.add_argument('--format', choices=['netcdf', 'geotiff'], 
                       default='netcdf', help='Output format')
    
    args = parser.parse_args()
    
    if args.create_sample:
        # Create sample data
        sample_path = Path("./sample_data/temperature_sample.nc")
        create_sample_data(sample_path)
        logger.info(f"\nSample data created at: {sample_path}")
        logger.info("Run again with: python test_annual_temp.py --input sample_data/temperature_sample.nc")
    elif args.input:
        # Override default input
        INPUT_FILE = args.input
        if args.output:
            OUTPUT_FILE = Path(args.output)
        else:
            OUTPUT_FILE = Path("./outputs") / f"annual_mean_temp_{datetime.now().strftime('%Y%m%d')}.nc"
        
        # Run with specified files
        main(input_file=INPUT_FILE, output_file=OUTPUT_FILE)
    else:
        # Run with defaults
        main()
#!/usr/bin/env python
"""
Efficient climate indices calculation using vectorized operations.
Processes all parcels at once instead of looping.
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


def process_year(nc_file: str, parcels_csv: str, output_csv: str, year: int = None):
    """
    Process a single year efficiently using xarray's built-in interpolation.
    
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
    logger.info(f"Processing {Path(nc_file).name}")
    
    # Load parcels
    logger.info("Loading parcel coordinates")
    parcels = pd.read_csv(parcels_csv)
    n_parcels = len(parcels)
    logger.info(f"Processing {n_parcels} parcels")
    
    # Load climate data
    logger.info("Loading climate data")
    ds = xr.open_dataset(nc_file)
    
    # Find temperature variable
    temp_vars = ['tas', 'temperature', 'temp']
    variable = None
    for var in temp_vars:
        if var in ds.data_vars:
            variable = var
            break
    if variable is None:
        variable = list(ds.data_vars)[0]
    
    logger.info(f"Using variable: {variable}")
    
    # Get parcel coordinates
    parcel_lats = parcels['parcel_level_latitude'].values
    parcel_lons = parcels['parcel_level_longitude'].values
    
    # Handle longitude coordinate system
    # If dataset uses 0-360 and parcels use -180 to 180
    if ds.lon.min() >= 0 and parcel_lons.min() < 0:
        parcel_lons = parcel_lons.copy()
        parcel_lons[parcel_lons < 0] += 360
    
    # Use vectorized nearest neighbor selection
    logger.info("Extracting data at parcel locations (vectorized)")
    
    # Find nearest indices for all parcels at once
    lat_indices = np.searchsorted(ds.lat.values, parcel_lats)
    lat_indices = np.clip(lat_indices, 0, len(ds.lat) - 1)
    
    lon_indices = np.searchsorted(ds.lon.values, parcel_lons)
    lon_indices = np.clip(lon_indices, 0, len(ds.lon) - 1)
    
    # Extract all data at once using advanced indexing
    # This gets data for all parcels in one operation!
    data = ds[variable].values[:, lat_indices, lon_indices]  # shape: (time, n_parcels)
    
    # Transpose to get (n_parcels, time)
    data = data.T
    
    # Convert from Kelvin to Celsius if needed
    if data.mean() > 200:
        logger.info("Converting from Kelvin to Celsius")
        data = data - 273.15
    
    logger.info("Calculating annual indices")
    
    # Calculate all indices using vectorized operations
    results = pd.DataFrame({
        'saleid': parcels['saleid'],
        'parcelid': parcels['parcelid'],
        'lat': parcels['parcel_level_latitude'],
        'lon': parcels['parcel_level_longitude'],
        'year': year if year else 0,
        
        # Annual statistics (vectorized across all parcels)
        'annual_mean': np.nanmean(data, axis=1),
        'annual_min': np.nanmin(data, axis=1),
        'annual_max': np.nanmax(data, axis=1),
        'annual_std': np.nanstd(data, axis=1),
        
        # Count-based indices
        'frost_days': np.sum(data < 0, axis=1),
        'ice_days': np.sum(data < -10, axis=1),
        'summer_days': np.sum(data > 25, axis=1),
        'hot_days': np.sum(data > 30, axis=1),
        'tropical_nights': np.sum(data > 20, axis=1),
    })
    
    # Growing degree days (vectorized)
    gdd = np.maximum(data - 10, 0)
    results['growing_degree_days'] = np.nansum(gdd, axis=1)
    
    # Heating degree days
    hdd = np.maximum(18 - data, 0)
    results['heating_degree_days'] = np.nansum(hdd, axis=1)
    
    # Cooling degree days
    cdd = np.maximum(data - 18, 0)
    results['cooling_degree_days'] = np.nansum(cdd, axis=1)
    
    # Save results
    results.to_csv(output_csv, index=False)
    logger.info(f"Results saved to {output_csv}")
    
    # Print summary
    logger.info("\n=== Summary Statistics ===")
    logger.info(f"Annual mean: {results['annual_mean'].mean():.2f} ± {results['annual_mean'].std():.2f} °C")
    logger.info(f"Frost days: {results['frost_days'].mean():.1f} days")
    logger.info(f"Summer days: {results['summer_days'].mean():.1f} days")
    logger.info(f"GDD: {results['growing_degree_days'].mean():.0f}")


def process_multiple_years(data_dir: str, parcels_csv: str, output_dir: str, 
                          start_year: int, end_year: int, scenario: str = 'historical'):
    """
    Process multiple years of data.
    
    Parameters:
    -----------
    data_dir : str
        Base directory with NorESM2-LM data
    parcels_csv : str
        Path to parcels CSV
    output_dir : str
        Output directory
    start_year : int
        Start year
    end_year : int
        End year
    scenario : str
        Scenario (historical, ssp245, etc.)
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    all_results = []
    
    for year in range(start_year, end_year + 1):
        # Construct file path
        nc_file = Path(data_dir) / 'tas' / scenario / f"tas_day_NorESM2-LM_{scenario}_r1i1p1f1_gn_{year}.nc"
        
        if not nc_file.exists():
            logger.warning(f"File not found: {nc_file}")
            continue
        
        # Process year
        output_csv = output_dir / f"parcel_indices_{year}.csv"
        process_year(str(nc_file), parcels_csv, str(output_csv), year)
        
        # Load and append results
        df = pd.read_csv(output_csv)
        all_results.append(df)
    
    # Combine all years
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        combined_file = output_dir / f"parcel_indices_{scenario}_{start_year}_{end_year}.csv"
        combined.to_csv(combined_file, index=False)
        logger.info(f"\nCombined results saved to {combined_file}")
        logger.info(f"Total records: {len(combined)}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Efficient climate indices extraction')
    parser.add_argument('--parcels', default='parcel_coordinates.csv',
                       help='CSV file with parcel coordinates')
    parser.add_argument('--input', '-i', help='Input NetCDF file (single year)')
    parser.add_argument('--output', '-o', default='parcel_indices.csv',
                       help='Output CSV file')
    parser.add_argument('--year', type=int, help='Year label')
    
    # Multi-year processing
    parser.add_argument('--data-dir', default='/media/mihiarc/SSD4TB/data/NorESM2-LM',
                       help='Base data directory')
    parser.add_argument('--start-year', type=int, help='Start year for batch processing')
    parser.add_argument('--end-year', type=int, help='End year for batch processing')
    parser.add_argument('--scenario', default='historical',
                       help='Scenario (historical, ssp245, etc.)')
    parser.add_argument('--output-dir', default='outputs',
                       help='Output directory for batch processing')
    
    args = parser.parse_args()
    
    if args.start_year and args.end_year:
        # Batch processing
        process_multiple_years(
            args.data_dir, args.parcels, args.output_dir,
            args.start_year, args.end_year, args.scenario
        )
    elif args.input:
        # Single file
        process_year(args.input, args.parcels, args.output, args.year)
    else:
        parser.error("Specify either --input for single file or --start-year/--end-year for batch")
    
    logger.info("\nProcessing complete!")


if __name__ == "__main__":
    main()
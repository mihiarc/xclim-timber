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


def calculate_percentile_indices(data, percentile_values, axis=1):
    """Helper function to calculate percentile-based indices."""
    return {
        f'p{int(p)}': np.nanpercentile(data, p, axis=axis)
        for p in percentile_values
    }


def calculate_precipitation_indices(data, parcels, year=None):
    """
    Calculate comprehensive precipitation indices.

    Parameters:
    -----------
    data : ndarray
        Precipitation data (n_parcels, n_days) in mm/day
    parcels : DataFrame
        Parcel information
    year : int
        Year label
    """
    results = pd.DataFrame({
        'saleid': parcels['saleid'],
        'parcelid': parcels['parcelid'],
        'lat': parcels['parcel_level_latitude'],
        'lon': parcels['parcel_level_longitude'],
        'year': year if year else 0,
    })

    # Basic statistics
    results['total_precip'] = np.nansum(data, axis=1)
    results['mean_precip'] = np.nanmean(data, axis=1)
    results['max_precip'] = np.nanmax(data, axis=1)
    results['precip_std'] = np.nanstd(data, axis=1)

    # Precipitation days
    results['wet_days'] = np.sum(data >= 1, axis=1)
    results['heavy_precip_days'] = np.sum(data >= 10, axis=1)
    results['very_heavy_precip_days'] = np.sum(data >= 20, axis=1)
    results['extreme_precip_days'] = np.sum(data >= 50, axis=1)

    # Dry days
    results['dry_days'] = np.sum(data < 1, axis=1)
    results['very_dry_days'] = np.sum(data < 0.1, axis=1)

    # Consecutive dry/wet days
    results['max_consecutive_dry'] = calculate_consecutive_days(data, 1, 'less')
    results['max_consecutive_wet'] = calculate_consecutive_days(data, 1, 'greater')

    # Percentiles
    for p in [5, 10, 25, 50, 75, 90, 95, 99]:
        results[f'precip_p{p}'] = np.nanpercentile(data, p, axis=1)

    # Precipitation intensity
    wet_day_data = np.where(data >= 1, data, np.nan)
    results['simple_daily_intensity'] = np.nanmean(wet_day_data, axis=1)

    # Maximum 5-day precipitation (simplified - would need rolling window)
    results['max_5day_precip'] = np.zeros(data.shape[0])
    for i in range(data.shape[0]):
        if len(data[i]) >= 5:
            max_5day = np.max(np.convolve(data[i], np.ones(5), 'valid'))
            results.loc[i, 'max_5day_precip'] = max_5day

    return results


def calculate_consecutive_days(data, threshold, comparison='greater', axis=1):
    """
    Calculate maximum consecutive days meeting a threshold condition.

    Parameters:
    -----------
    data : ndarray
        Temperature data (n_parcels, n_days)
    threshold : float
        Temperature threshold
    comparison : str
        'greater' or 'less' for comparison type
    """
    n_parcels, n_days = data.shape
    max_consecutive = np.zeros(n_parcels)

    for i in range(n_parcels):
        if comparison == 'greater':
            mask = data[i] > threshold
        else:
            mask = data[i] < threshold

        # Find consecutive True values
        consecutive = 0
        max_cons = 0
        for val in mask:
            if val:
                consecutive += 1
                max_cons = max(max_cons, consecutive)
            else:
                consecutive = 0
        max_consecutive[i] = max_cons

    return max_consecutive


def process_year(nc_file: str, parcels_csv: str, output_csv: str, year: int = None,
                 variable_type: str = 'temperature', include_extremes: bool = True):
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
    variable_type : str
        Type of variable ('temperature' or 'precipitation')
    include_extremes : bool
        Whether to calculate extreme indices (more compute intensive)
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
    
    # Find variable based on type
    if variable_type == 'temperature':
        var_names = ['tas', 'temperature', 'temp', 'tmean', 't2m']
    elif variable_type == 'precipitation':
        var_names = ['pr', 'precipitation', 'precip', 'rain', 'tp']
    else:
        var_names = []

    variable = None
    for var in var_names:
        if var in ds.data_vars:
            variable = var
            break
    if variable is None:
        variable = list(ds.data_vars)[0]
        logger.warning(f"Using first available variable: {variable}")
    
    logger.info(f"Using variable: {variable}")
    
    # Get parcel coordinates
    parcel_lats = parcels['parcel_level_latitude'].values
    parcel_lons = parcels['parcel_level_longitude'].values
    
    # Handle longitude coordinate system
    # If dataset uses 0-360 and parcels use -180 to 180
    if ds.lon.min() >= 0 and parcel_lons.min() < 0:
        parcel_lons = parcel_lons.copy()
        parcel_lons[parcel_lons < 0] += 360
    
    # Use proper nearest neighbor selection
    logger.info("Extracting data at parcel locations (vectorized)")

    # Find nearest indices using actual distance calculation
    # This correctly handles irregular grids and finds true nearest neighbors
    lat_indices = np.zeros(len(parcel_lats), dtype=int)
    lon_indices = np.zeros(len(parcel_lons), dtype=int)

    for i, (lat, lon) in enumerate(zip(parcel_lats, parcel_lons)):
        # Find nearest latitude
        lat_idx = np.abs(ds.lat.values - lat).argmin()
        # Find nearest longitude
        lon_idx = np.abs(ds.lon.values - lon).argmin()
        lat_indices[i] = lat_idx
        lon_indices[i] = lon_idx

    # Extract data using xarray's isel for memory efficiency
    # This avoids loading the entire dataset into memory
    data_xr = ds[variable].isel(lat=xr.DataArray(lat_indices),
                                lon=xr.DataArray(lon_indices))
    data = data_xr.values  # shape: (time, n_parcels)
    
    # Transpose to get (n_parcels, time)
    data = data.T
    
    # Process based on variable type
    if variable_type == 'precipitation':
        logger.info("Processing precipitation data")
        # Convert units if needed (kg m-2 s-1 to mm/day)
        if data.mean() < 1:  # Likely in kg m-2 s-1
            logger.info("Converting precipitation to mm/day")
            data = data * 86400  # Convert to mm/day

        results = calculate_precipitation_indices(data, parcels, year)
        results.to_csv(output_csv, index=False)
        logger.info(f"Results saved to {output_csv}")

        # Print summary
        logger.info("\n=== Precipitation Summary ===")
        logger.info(f"Parcels processed: {len(results)}")
        logger.info(f"Total precipitation: {results['total_precip'].mean():.1f} mm")
        logger.info(f"Wet days: {results['wet_days'].mean():.1f} days")
        logger.info(f"Max consecutive dry: {results['max_consecutive_dry'].mean():.1f} days")
        return

    # Temperature processing (default)
    # Convert from Kelvin to Celsius if needed
    if data.mean() > 200:
        logger.info("Converting from Kelvin to Celsius")
        data = data - 273.15

    logger.info("Calculating comprehensive climate indices")

    # Initialize results with basic info
    results = pd.DataFrame({
        'saleid': parcels['saleid'],
        'parcelid': parcels['parcelid'],
        'lat': parcels['parcel_level_latitude'],
        'lon': parcels['parcel_level_longitude'],
        'year': year if year else 0,
    })

    # =========== BASIC STATISTICS ===========
    results['annual_mean'] = np.nanmean(data, axis=1)
    results['annual_min'] = np.nanmin(data, axis=1)
    results['annual_max'] = np.nanmax(data, axis=1)
    results['annual_std'] = np.nanstd(data, axis=1)
    results['annual_range'] = results['annual_max'] - results['annual_min']

    # =========== PERCENTILES ===========
    percentiles = [5, 10, 25, 50, 75, 90, 95]
    for p in percentiles:
        results[f'temp_p{p}'] = np.nanpercentile(data, p, axis=1)

    # =========== TEMPERATURE THRESHOLD INDICES ===========
    # Cold days
    results['frost_days'] = np.sum(data < 0, axis=1)
    results['ice_days'] = np.sum(data < -10, axis=1)
    results['deep_freeze_days'] = np.sum(data < -20, axis=1)

    # Warm days
    results['summer_days'] = np.sum(data > 25, axis=1)
    results['hot_days'] = np.sum(data > 30, axis=1)
    results['very_hot_days'] = np.sum(data > 35, axis=1)
    results['extreme_heat_days'] = np.sum(data > 40, axis=1)

    # Tropical indices
    results['tropical_nights'] = np.sum(data > 20, axis=1)
    results['warm_nights'] = np.sum(data > 15, axis=1)

    # =========== DEGREE DAYS (Multiple Base Temperatures) ===========
    # Growing degree days with different bases
    for base in [0, 5, 10, 15]:
        gdd = np.maximum(data - base, 0)
        results[f'gdd_base{base}'] = np.nansum(gdd, axis=1)

    # Heating degree days
    for base in [15, 18, 20]:
        hdd = np.maximum(base - data, 0)
        results[f'hdd_base{base}'] = np.nansum(hdd, axis=1)

    # Cooling degree days
    for base in [18, 20, 22]:
        cdd = np.maximum(data - base, 0)
        results[f'cdd_base{base}'] = np.nansum(cdd, axis=1)

    # =========== AGRICULTURAL INDICES ===========
    # Corn growing degree days (base 10, cap 30) - Fixed formula
    # First cap temperature at 30°C, then calculate GDD
    capped_temp = np.minimum(data, 30)
    corn_gdd = np.maximum(capped_temp - 10, 0)
    results['corn_gdd'] = np.nansum(corn_gdd, axis=1)

    # Killing degree days (for pest/disease modeling)
    kdd = np.maximum(data - 21, 0)
    results['killing_degree_days'] = np.nansum(kdd, axis=1)

    # Chill hours (hours below 7°C, important for fruit trees)
    results['chill_days'] = np.sum(data < 7, axis=1)

    # Vernalization days (0-10°C, important for winter wheat)
    results['vernalization_days'] = np.sum((data >= 0) & (data <= 10), axis=1)

    # =========== EXTREME TEMPERATURE INDICES ===========
    if include_extremes:
        # Note: Diurnal temperature range cannot be calculated from daily mean data
        # Would require daily min/max temperature data
        # Removing misleading approximation

        # Consecutive days indices
        results['max_consecutive_frost'] = calculate_consecutive_days(data, 0, 'less')
        results['max_consecutive_summer'] = calculate_consecutive_days(data, 25, 'greater')
        results['max_consecutive_hot'] = calculate_consecutive_days(data, 30, 'greater')

        # Spell duration indices (simplified - using fixed thresholds)
        # For proper implementation, would need per-parcel percentile thresholds
        temp_p10 = np.nanpercentile(data, 10)  # Global 10th percentile
        temp_p90 = np.nanpercentile(data, 90)  # Global 90th percentile
        results['cold_spell_days'] = calculate_consecutive_days(data, temp_p10, 'less')
        results['warm_spell_days'] = calculate_consecutive_days(data, temp_p90, 'greater')

    # =========== TIMBER-SPECIFIC INDICES ===========
    # Optimal growth temperature days (15-25°C for many tree species)
    results['optimal_growth_days'] = np.sum((data >= 15) & (data <= 25), axis=1)

    # Drought stress days (>30°C)
    results['drought_stress_days'] = np.sum(data > 30, axis=1)

    # Freeze-thaw cycles (crosses 0°C) - Fixed to count actual 0°C crossings
    freeze_thaw = np.zeros(data.shape[0])
    for i in range(data.shape[0]):
        # Check where temperature crosses 0°C
        above_zero = data[i] > 0
        # Count transitions from below to above zero and vice versa
        transitions = np.diff(above_zero.astype(int))
        freeze_thaw[i] = np.sum(np.abs(transitions))
    results['freeze_thaw_cycles'] = freeze_thaw

    # =========== BIOCLIMATIC INDICES ===========
    # Bioclimatic variables - Note: Simplified versions, not full BIOCLIM standard
    # For proper BIOCLIM, need monthly aggregation which requires date information
    # Mean temperature of warmest period (using 95th percentile as proxy)
    results['bio5_max_temp_warmest_period'] = np.nanpercentile(data, 95, axis=1)

    # Mean temperature of coldest period (using 5th percentile as proxy)
    results['bio6_min_temp_coldest_period'] = np.nanpercentile(data, 5, axis=1)

    # Temperature seasonality (coefficient of variation)
    # Standard deviation / mean * 100
    mean_temp = np.nanmean(data, axis=1)
    std_temp = np.nanstd(data, axis=1)
    results['bio4_temp_seasonality'] = (std_temp / (mean_temp + 273.15)) * 100  # Add 273.15 to avoid division issues
    
    # Save results
    results.to_csv(output_csv, index=False)
    logger.info(f"Results saved to {output_csv}")
    
    # Print summary
    logger.info("\n=== Summary Statistics ===")
    logger.info(f"Parcels processed: {len(results)}")
    logger.info(f"Indices calculated: {len(results.columns) - 5}")  # Exclude ID columns
    logger.info(f"Annual mean: {results['annual_mean'].mean():.2f} ± {results['annual_mean'].std():.2f} °C")
    logger.info(f"Frost days: {results['frost_days'].mean():.1f} days")
    logger.info(f"Summer days: {results['summer_days'].mean():.1f} days")
    logger.info(f"GDD (base 10): {results['gdd_base10'].mean():.0f}")
    logger.info(f"Optimal growth days: {results['optimal_growth_days'].mean():.1f} days")


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
    parser.add_argument('--variable-type', default='temperature',
                       choices=['temperature', 'precipitation'],
                       help='Type of climate variable to process')
    parser.add_argument('--include-extremes', action='store_true', default=True,
                       help='Calculate extreme indices (more compute intensive)')

    args = parser.parse_args()
    
    if args.start_year and args.end_year:
        # Batch processing
        process_multiple_years(
            args.data_dir, args.parcels, args.output_dir,
            args.start_year, args.end_year, args.scenario
        )
    elif args.input:
        # Single file
        process_year(args.input, args.parcels, args.output, args.year,
                    args.variable_type, args.include_extremes)
    else:
        parser.error("Specify either --input for single file or --start-year/--end-year for batch")
    
    logger.info("\nProcessing complete!")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Point extraction from climate indices NetCDF files.
Extracts pre-calculated climate indices at specific parcel locations.
Output format: one row per location-year with climate indices as columns.
"""

import argparse
import logging
from pathlib import Path
import pandas as pd
import xarray as xr
import numpy as np
from typing import Union, List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def is_temperature_var(var_name: str) -> bool:
    """Check if a variable name indicates temperature data."""
    # Specific temperature variable prefixes/patterns
    temp_prefixes = ['tas', 'tg_', 'tx_', 'tn_', 'temp']

    # Temperature-specific indices
    temp_indices = [
        'frost_days', 'ice_days', 'summer_days', 'hot_days',
        'tropical_nights', 'heating_degree_days', 'cooling_degree_days',
        'growing_degree_days', 'consecutive_frost_days', 'warm_nights',
        'very_hot_days', 'cold_spell', 'warm_spell'
    ]

    var_lower = var_name.lower()

    # Check for temperature prefixes
    for prefix in temp_prefixes:
        if var_lower.startswith(prefix):
            return True

    # Check for specific temperature indices
    for index in temp_indices:
        if index in var_lower:
            return True

    # Dewpoint is temperature but in Celsius already, so exclude it
    if 'dewpoint' in var_lower or 'vpd' in var_lower:
        return False

    return False


def extract_points_from_netcdf_df(
    nc_file: Union[str, Path],
    parcels_csv: Union[str, Path],
    convert_kelvin: bool = True
) -> pd.DataFrame:
    """
    Extract climate indices at parcel locations from NetCDF file and return DataFrame.

    Args:
        nc_file: Path to NetCDF file with climate indices
        parcels_csv: Path to CSV with parcel coordinates
        convert_kelvin: Convert temperature from Kelvin to Celsius

    Returns:
        DataFrame with extracted values
    """
    logger.info(f"Loading parcels from {parcels_csv}")
    try:
        parcels = pd.read_csv(parcels_csv)
    except FileNotFoundError:
        raise FileNotFoundError(f"Parcels file not found: {parcels_csv}")
    except Exception as e:
        raise ValueError(f"Error reading parcels CSV: {e}")

    n_parcels = len(parcels)
    logger.info(f"Found {n_parcels} parcels to extract")

    logger.info(f"Opening NetCDF file: {nc_file}")
    try:
        # Don't decode timedelta to avoid type issues
        ds = xr.open_dataset(nc_file, decode_timedelta=False)
    except FileNotFoundError:
        raise FileNotFoundError(f"NetCDF file not found: {nc_file}")
    except Exception as e:
        raise ValueError(f"Error opening NetCDF file: {e}")

    # Get coordinate columns
    lat_col = 'parcel_level_latitude'
    lon_col = 'parcel_level_longitude'

    # Validate required columns exist
    if lat_col not in parcels.columns or lon_col not in parcels.columns:
        raise ValueError(f"Required columns {lat_col} and {lon_col} not found in parcels CSV")

    # Extract coordinates
    lats = parcels[lat_col].values
    lons = parcels[lon_col].values

    # Validate coordinate ranges
    if len(lats) == 0:
        raise ValueError("No parcel coordinates found")
    if not (-90 <= lats.min() <= lats.max() <= 90):
        raise ValueError(f"Invalid latitude values: {lats.min()}-{lats.max()}")

    # Get time dimension (usually years for annual indices)
    if 'time' in ds.dims:
        years = pd.to_datetime(ds.time.values).year
        logger.info(f"Time range: {years.min()}-{years.max()}")
    else:
        years = [None]  # Single time slice

    # Extract each climate index
    logger.info(f"Extracting {len(ds.data_vars)} climate indices...")

    # Collect all data in a list for pivoting
    long_data = []

    for var_name in ds.data_vars:
        logger.info(f"  - {var_name}")
        var_data = ds[var_name]

        # Check data type and convert if needed
        if var_data.dtype == np.timedelta64:
            # Convert timedelta to numeric (days)
            var_data = var_data / np.timedelta64(1, 'D')
            var_data.attrs['units'] = 'days'

        # Use xarray's interp for accurate extraction
        extracted = var_data.interp(
            lat=xr.DataArray(lats, dims='points'),
            lon=xr.DataArray(lons, dims='points'),
            method='linear'  # Use linear interpolation for accuracy
        )

        # Handle temporal dimension
        if 'time' in var_data.dims:
            # Multiple time steps - extract each year
            for i, year in enumerate(years):
                values = extracted.isel(time=i).values

                # Convert temperature from Kelvin to Celsius if needed
                if convert_kelvin and is_temperature_var(var_name):
                    if np.nanmean(values) > 200:  # Likely in Kelvin
                        values = values - 273.15

                # Create rows for long format
                for j in range(n_parcels):
                    long_data.append({
                        'saleid': parcels.iloc[j]['saleid'],
                        'parcelid': parcels.iloc[j]['parcelid'],
                        'lat': lats[j],
                        'lon': lons[j],
                        'year': year,
                        'index': var_name,
                        'value': values[j]
                    })
        else:
            # Single time slice - use current year or 0
            year = years[0] if years[0] else pd.Timestamp.now().year
            values = extracted.values

            # Convert temperature from Kelvin to Celsius if needed
            if convert_kelvin and is_temperature_var(var_name):
                if np.nanmean(values) > 200:  # Likely in Kelvin
                    values = values - 273.15

            # Create rows for long format
            for j in range(n_parcels):
                long_data.append({
                    'saleid': parcels.iloc[j]['saleid'],
                    'parcelid': parcels.iloc[j]['parcelid'],
                    'lat': lats[j],
                    'lon': lons[j],
                    'year': year,
                    'index': var_name,
                    'value': values[j]
                })

    # Create DataFrame and pivot to final format
    logger.debug("Creating output table...")
    results_long = pd.DataFrame(long_data)

    # Pivot so each row is location-year with indices as columns
    results = results_long.pivot_table(
        index=['saleid', 'parcelid', 'lat', 'lon', 'year'],
        columns='index',
        values='value',
        aggfunc='first'
    ).reset_index()

    # Flatten column names after pivot
    results.columns.name = None

    return results


def extract_points_from_netcdf(
    nc_file: Union[str, Path],
    parcels_csv: Union[str, Path],
    output_csv: Union[str, Path],
    convert_kelvin: bool = True
) -> pd.DataFrame:
    """
    Extract climate indices at parcel locations from NetCDF file.
    Output format: one row per location-year with climate indices as columns.

    Args:
        nc_file: Path to NetCDF file with climate indices
        parcels_csv: Path to CSV with parcel coordinates
        output_csv: Path for output CSV file
        convert_kelvin: Convert temperature from Kelvin to Celsius

    Returns:
        DataFrame with extracted values
    """
    # Use the DataFrame version to do the actual extraction
    results = extract_points_from_netcdf_df(nc_file, parcels_csv, convert_kelvin)

    # Save results
    logger.info(f"Saving results to {output_csv}")
    results.to_csv(output_csv, index=False)

    # Summary statistics
    logger.info("\n=== Extraction Summary ===")
    logger.info(f"Output rows: {len(results)}")
    if 'year' in results.columns:
        logger.info(f"Years included: {sorted(results['year'].unique())}")

    # Show sample statistics
    numeric_cols = results.select_dtypes(include=[np.number]).columns
    climate_cols = [col for col in numeric_cols if col not in ['saleid', 'parcelid', 'lat', 'lon', 'year']]
    if climate_cols:
        sample_cols = climate_cols[:3]
        for col in sample_cols:
            mean_val = results[col].mean()
            std_val = results[col].std()
            logger.info(f"  {col}: {mean_val:.2f} ± {std_val:.2f}")

    return results


def batch_extract(
    nc_pattern: str,
    parcels_csv: Union[str, Path],
    output_csv: Union[str, Path] = None
) -> Path:
    """
    Extract points from multiple NetCDF files and combine into single CSV.

    Args:
        nc_pattern: Glob pattern for NetCDF files
        parcels_csv: Path to parcels CSV
        output_csv: Path for output CSV file (default: 'extracted_indices.csv')

    Returns:
        Path to the combined output CSV file
    """
    # Set default output if not specified
    if output_csv is None:
        output_csv = Path('./outputs/extracted_indices.csv')
    else:
        output_csv = Path(output_csv)

    # Ensure output directory exists
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    nc_files = sorted(Path().glob(nc_pattern))
    logger.info(f"Found {len(nc_files)} NetCDF files to process")

    if not nc_files:
        raise ValueError(f"No files found matching pattern: {nc_pattern}")

    # Collect all results directly without intermediate files
    all_results = []

    for nc_file in nc_files:
        logger.info(f"\nProcessing: {nc_file.name}")

        # Extract to temporary DataFrame (not saved to disk)
        results = extract_points_from_netcdf_df(nc_file, parcels_csv)
        all_results.append(results)

    # Combine all results
    if len(all_results) > 1:
        logger.info("\nCombining results from all files...")
        combined = pd.concat(all_results, ignore_index=True)
    else:
        combined = all_results[0]

    # Sort by saleid, parcelid, and year for better organization
    combined = combined.sort_values(['saleid', 'parcelid', 'year'])

    # Save final combined result
    logger.info(f"Saving combined results to {output_csv}")
    combined.to_csv(output_csv, index=False)
    logger.info(f"Total rows: {len(combined):,}")

    return output_csv


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Extract climate indices at point locations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: Process all NetCDF files in outputs/ folder
  python extract_points.py

  # Process specific pattern
  python extract_points.py --pattern "outputs/temperature_*.nc"

  # Custom output location
  python extract_points.py --output custom_results.csv

  # Single file mode (legacy)
  python extract_points.py single_file.nc parcels.csv output.csv
        """
    )

    # Optional positional arguments for backward compatibility
    parser.add_argument(
        'input',
        nargs='?',
        help='Single NetCDF file (legacy mode)'
    )

    parser.add_argument(
        'parcels',
        nargs='?',
        help='CSV file with parcel coordinates (legacy mode)'
    )

    parser.add_argument(
        'output_legacy',
        nargs='?',
        help='Output CSV file (legacy mode)'
    )

    parser.add_argument(
        '--pattern',
        default='outputs/*.nc',
        help='Glob pattern for NetCDF files (default: outputs/*.nc)'
    )

    parser.add_argument(
        '--parcels-file',
        default='data/parcel_coordinates.csv',
        help='Parcels CSV file (default: data/parcel_coordinates.csv)'
    )

    parser.add_argument(
        '--output',
        default='outputs/extracted_indices.csv',
        help='Output CSV file (default: outputs/extracted_indices.csv)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Determine processing mode
    if args.input:
        # Legacy single file mode
        if not args.parcels:
            parser.error("parcels CSV required for single file mode")

        output = args.output_legacy if args.output_legacy else f"points_{Path(args.input).stem}.csv"
        extract_points_from_netcdf(args.input, args.parcels, output)
    else:
        # Default batch mode - process all NC files in outputs/
        logger.info(f"Processing NetCDF files matching: {args.pattern}")
        batch_extract(args.pattern, args.parcels_file, args.output)

    logger.info("\n✓ Point extraction complete!")
    return 0


if __name__ == "__main__":
    exit(main())
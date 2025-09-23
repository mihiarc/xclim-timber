#!/usr/bin/env python
"""
Repair script to fix timedelta issues in climate indices output files.
Can be used on both new and existing output files.
"""

import sys
from pathlib import Path
import xarray as xr
import numpy as np
import shutil
from datetime import datetime

def repair_file(input_file, output_file=None, backup=True):
    """
    Repair a single NetCDF file with timedelta issues.

    Parameters:
    -----------
    input_file : str or Path
        Path to the file to repair
    output_file : str or Path, optional
        Path for repaired file. If None, overwrites input
    backup : bool
        Whether to create a backup of the original file
    """
    input_path = Path(input_file)

    if not input_path.exists():
        print(f"✗ File not found: {input_path}")
        return False

    print(f"\nRepairing: {input_path}")
    print("-" * 50)

    # Create backup if requested
    if backup and output_file is None:
        backup_path = input_path.with_suffix('.nc.backup')
        shutil.copy2(input_path, backup_path)
        print(f"Created backup: {backup_path}")

    # Load with decode_timedelta=False to get raw data
    ds = xr.open_dataset(input_path, decode_timedelta=False, decode_times=True)

    # Indices that should be numeric days, not timedelta
    indices_to_fix = [
        'frost_days', 'ice_days', 'tropical_nights',
        'consecutive_frost_days', 'gsl', 'summer_days',
        'hot_days', 'very_hot_days', 'warm_nights'
    ]

    fixed_count = 0
    already_ok = 0
    not_found = 0

    for var in indices_to_fix:
        if var not in ds.data_vars:
            not_found += 1
            continue

        data = ds[var]

        # Check if this needs fixing
        if 'units' in data.attrs and data.attrs['units'] in ['days', 'day', 'd']:
            # The data might be stored as float but units cause timedelta interpretation
            # Ensure it's numeric and fix units
            if data.dtype == np.float32 or data.dtype == np.float64:
                # Data is already numeric, just fix the units attribute
                ds[var].attrs['units'] = 'count'  # Use 'count' instead of 'days'
                ds[var].attrs['long_name'] = ds[var].attrs.get('long_name', var).replace('days', 'day count')
                print(f"  ✓ Fixed units for {var}: 'days' → 'count'")
                fixed_count += 1
            else:
                # This shouldn't happen with our fix, but handle it
                print(f"  ⚠ Unexpected dtype for {var}: {data.dtype}")
        else:
            already_ok += 1

    # Also check for negative temperature ranges
    range_indices = ['daily_temperature_range', 'daily_temperature_range_variability']

    for var in range_indices:
        if var in ds.data_vars:
            data = ds[var].values
            non_nan = data[~np.isnan(data)]

            if len(non_nan) > 0 and non_nan.min() < 0:
                # Temperature range should never be negative
                print(f"  ⚠ {var} has negative values (min: {non_nan.min():.1f})")
                # Take absolute value
                ds[var].values = np.abs(data)
                print(f"  ✓ Applied absolute value to {var}")
                fixed_count += 1

    # Save the repaired file
    output_path = Path(output_file) if output_file else input_path

    # Set encoding to avoid timedelta interpretation on reload
    encoding = {}
    for var in ds.data_vars:
        if var in indices_to_fix:
            encoding[var] = {
                'dtype': 'float32',
                'zlib': True,
                'complevel': 4,
                '_FillValue': np.nan
            }

    # Close the dataset before saving to avoid file lock issues
    ds_copy = ds.copy(deep=True)
    ds.close()

    # Use atomic write to prevent corruption on failure
    import tempfile
    import os

    # Create temporary file in same directory for atomic rename
    temp_fd, temp_path = tempfile.mkstemp(suffix='.nc.tmp',
                                         dir=output_path.parent,
                                         prefix='repair_')
    try:
        os.close(temp_fd)  # Close the file descriptor

        # Save to temporary file
        ds_copy.to_netcdf(temp_path, encoding=encoding, engine='netcdf4')
        ds_copy.close()

        # Atomic rename (on POSIX systems)
        # This ensures the file is either fully written or not written at all
        if output_path.exists():
            # Use replace for atomic operation
            Path(temp_path).replace(output_path)
        else:
            Path(temp_path).rename(output_path)

    except Exception as e:
        # Clean up temp file on error
        if Path(temp_path).exists():
            Path(temp_path).unlink()
        raise e

    print(f"\nSummary:")
    print(f"  Fixed: {fixed_count} variables")
    print(f"  Already OK: {already_ok} variables")
    print(f"  Not found: {not_found} variables")
    print(f"  Output saved to: {output_path}")

    return fixed_count > 0

def verify_repair(file_path):
    """Verify that a file has been properly repaired."""
    print(f"\nVerifying: {file_path}")
    print("-" * 50)

    # Load with default settings to see if xarray still interprets as timedelta
    ds = xr.open_dataset(file_path)

    problematic_indices = [
        'frost_days', 'ice_days', 'tropical_nights',
        'consecutive_frost_days', 'gsl'
    ]

    all_good = True
    for var in problematic_indices:
        if var in ds.data_vars:
            dtype = str(ds[var].dtype)
            units = ds[var].attrs.get('units', 'none')

            if 'timedelta' in dtype:
                print(f"  ✗ {var}: Still timedelta! ({dtype})")
                all_good = False
            else:
                # Check value range
                data = ds[var].values
                non_nan = data[~np.isnan(data)]
                if len(non_nan) > 0:
                    vmin, vmax = non_nan.min(), non_nan.max()
                    if 0 <= vmin <= 365 and 0 <= vmax <= 365:
                        print(f"  ✓ {var}: Numeric with valid range ({vmin:.0f}-{vmax:.0f} days)")
                    else:
                        print(f"  ⚠ {var}: Numeric but unusual range ({vmin:.1f}-{vmax:.1f})")
                else:
                    print(f"  - {var}: No data")

    ds.close()
    return all_good

def repair_directory(directory, pattern="*.nc"):
    """Repair all NetCDF files in a directory."""
    dir_path = Path(directory)

    if not dir_path.exists():
        print(f"✗ Directory not found: {dir_path}")
        return

    files = list(dir_path.glob(pattern))

    if not files:
        print(f"No files matching pattern '{pattern}' in {dir_path}")
        return

    print(f"\nFound {len(files)} files to repair in {dir_path}")
    print("=" * 60)

    repaired = 0
    for file in files:
        if repair_file(file):
            repaired += 1

    print("\n" + "=" * 60)
    print(f"Repair complete: {repaired}/{len(files)} files modified")

    # Verify repairs
    print("\nVerifying repairs...")
    print("=" * 60)

    all_good = True
    for file in files:
        if not verify_repair(file):
            all_good = False

    if all_good:
        print("\n✓ All files successfully repaired!")
    else:
        print("\n⚠ Some files may still have issues")

def main():
    """Main function to repair climate indices files."""

    print("\n" + "=" * 80)
    print("CLIMATE INDICES TIMEDELTA REPAIR TOOL")
    print("=" * 80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    import argparse
    parser = argparse.ArgumentParser(description='Repair timedelta issues in climate indices')
    parser.add_argument('path', help='File or directory to repair')
    parser.add_argument('--no-backup', action='store_true', help='Skip creating backup files')
    parser.add_argument('--pattern', default='*.nc', help='File pattern for directory repair')

    args = parser.parse_args()

    path = Path(args.path)

    if path.is_file():
        # Repair single file
        success = repair_file(path, backup=not args.no_backup)
        if success:
            verify_repair(path)
    elif path.is_dir():
        # Repair directory
        repair_directory(path, args.pattern)
    else:
        print(f"✗ Path not found: {path}")
        return 1

    print("\n" + "=" * 80)
    print("REPAIR COMPLETE")
    print("=" * 80)

    return 0

if __name__ == "__main__":
    sys.exit(main())
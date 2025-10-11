#!/usr/bin/env python3
"""
Validate production climate indices datasets.

Checks for common data corruption issues:
- Dimension consistency
- Units correctness
- Value ranges
- Data coverage
- File sizes

Usage:
    python scripts/validate_production_data.py outputs/production/temperature/
    python scripts/validate_production_data.py outputs/production/temperature/ --year 2003
"""

import argparse
import sys
from pathlib import Path
import xarray as xr
import numpy as np
from typing import List, Dict, Tuple


class DataValidator:
    """Validator for climate indices NetCDF files."""

    # Expected dimensions for temperature indices
    EXPECTED_DIMS = {
        'temperature': {'time': 1, 'lat': 621, 'lon': 1405},
        'precipitation': {'time': 1, 'lat': 621, 'lon': 1405},
        'agricultural': {'time': 1, 'lat': 621, 'lon': 1405},
        'drought': {'time': 1, 'lat': 621, 'lon': 1405},
    }

    # Expected index counts
    EXPECTED_INDEX_COUNTS = {
        'temperature': 35,
        'precipitation': 14,
        'agricultural': 5,
        'drought': 11,
    }

    # Count indices that should have units='days' and integer dtype
    COUNT_INDICES = [
        'summer_days', 'hot_days', 'ice_days', 'frost_days',
        'tropical_nights', 'consecutive_frost_days', 'tx90p', 'tx10p',
        'tn90p', 'tn10p', 'warm_spell_duration_index', 'cold_spell_duration_index',
        'heat_wave_index', 'cdd', 'cwd', 'r10mm', 'r20mm'
    ]

    def __init__(self, pipeline_type: str):
        self.pipeline_type = pipeline_type
        self.errors = []
        self.warnings = []

    def validate_file(self, file_path: Path) -> Dict:
        """Validate a single NetCDF file."""
        print(f"\nValidating: {file_path.name}")

        result = {
            'file': file_path.name,
            'errors': [],
            'warnings': [],
            'checks_passed': 0,
            'checks_failed': 0
        }

        try:
            ds = xr.open_dataset(file_path, decode_timedelta=False)

            # Check 1: Dimensions
            self._check_dimensions(ds, result)

            # Check 2: Index count
            self._check_index_count(ds, result)

            # Check 3: Units and data types
            self._check_units_and_dtypes(ds, result)

            # Check 4: Value ranges
            self._check_value_ranges(ds, result)

            # Check 5: Data coverage
            self._check_data_coverage(ds, result)

            # Check 6: File size
            self._check_file_size(file_path, result)

            ds.close()

        except Exception as e:
            result['errors'].append(f"Failed to open file: {e}")
            result['checks_failed'] += 1

        return result

    def _check_dimensions(self, ds: xr.Dataset, result: Dict):
        """Check if dimensions match expectations."""
        expected = self.EXPECTED_DIMS[self.pipeline_type]
        actual = dict(ds.dims)

        if actual == expected:
            result['checks_passed'] += 1
            print(f"  ✓ Dimensions correct: {actual}")
        else:
            result['checks_failed'] += 1
            result['errors'].append(f"Dimension mismatch: {actual} != {expected}")
            print(f"  ✗ Dimensions WRONG: {actual} (expected {expected})")

    def _check_index_count(self, ds: xr.Dataset, result: Dict):
        """Check if correct number of indices present."""
        expected = self.EXPECTED_INDEX_COUNTS.get(self.pipeline_type, None)
        actual = len(ds.data_vars)

        if expected is None:
            result['warnings'].append(f"No expected index count defined for {self.pipeline_type}")
            return

        if actual == expected:
            result['checks_passed'] += 1
            print(f"  ✓ Index count correct: {actual}")
        else:
            result['checks_failed'] += 1
            result['errors'].append(f"Index count mismatch: {actual} != {expected}")
            print(f"  ✗ Index count WRONG: {actual} (expected {expected})")

    def _check_units_and_dtypes(self, ds: xr.Dataset, result: Dict):
        """Check units and data types for count indices."""
        for var_name in ds.data_vars:
            if any(count_idx in var_name for count_idx in self.COUNT_INDICES):
                units = ds[var_name].attrs.get('units', None)
                dtype = ds[var_name].dtype

                # Check units
                if units == 'nanoseconds':
                    result['checks_failed'] += 1
                    result['errors'].append(f"{var_name}: units='nanoseconds' (should be 'days')")
                    print(f"  ✗ {var_name}: WRONG units (nanoseconds)")
                elif units == 'days':
                    result['checks_passed'] += 1
                else:
                    result['warnings'].append(f"{var_name}: unexpected units='{units}'")

                # Check for NaT values (timedelta corruption)
                sample = ds[var_name].values.ravel()
                if np.any(sample == -9223372036854775808):
                    result['checks_failed'] += 1
                    result['errors'].append(f"{var_name}: Contains NaT values (timedelta corruption)")
                    print(f"  ✗ {var_name}: Contains NaT values!")

    def _check_value_ranges(self, ds: xr.Dataset, result: Dict):
        """Check if values are in reasonable ranges."""
        for var_name in ds.data_vars:
            if any(count_idx in var_name for count_idx in self.COUNT_INDICES):
                values = ds[var_name].values
                valid_values = values[~np.isnan(values)]

                if len(valid_values) == 0:
                    result['warnings'].append(f"{var_name}: All NaN values")
                    continue

                # Count indices should be in [0, 366]
                if np.any(valid_values < 0) or np.any(valid_values > 366):
                    result['checks_failed'] += 1
                    result['errors'].append(f"{var_name}: Values out of range [0, 366]")
                    print(f"  ✗ {var_name}: Values out of range! min={valid_values.min()}, max={valid_values.max()}")
                else:
                    result['checks_passed'] += 1

    def _check_data_coverage(self, ds: xr.Dataset, result: Dict):
        """Check percentage of valid (non-NaN) data."""
        total_points = 0
        valid_points = 0

        for var_name in ds.data_vars:
            values = ds[var_name].values.ravel()
            total_points += len(values)
            valid_points += np.sum(~np.isnan(values))

        coverage = (valid_points / total_points) * 100

        if coverage < 10:
            result['checks_failed'] += 1
            result['errors'].append(f"Very low data coverage: {coverage:.1f}%")
            print(f"  ✗ Data coverage: {coverage:.1f}% (suspiciously low)")
        elif coverage < 30:
            result['warnings'].append(f"Low data coverage: {coverage:.1f}%")
            print(f"  ⚠ Data coverage: {coverage:.1f}%")
        else:
            result['checks_passed'] += 1
            print(f"  ✓ Data coverage: {coverage:.1f}%")

    def _check_file_size(self, file_path: Path, result: Dict):
        """Check if file size is reasonable."""
        size_mb = file_path.stat().st_size / (1024 * 1024)

        if size_mb < 1:
            result['checks_failed'] += 1
            result['errors'].append(f"File size too small: {size_mb:.1f} MB (likely corrupted)")
            print(f"  ✗ File size: {size_mb:.1f} MB (TOO SMALL)")
        elif size_mb < 10:
            result['warnings'].append(f"Small file size: {size_mb:.1f} MB")
            print(f"  ⚠ File size: {size_mb:.1f} MB (small)")
        else:
            result['checks_passed'] += 1
            print(f"  ✓ File size: {size_mb:.1f} MB")


def main():
    parser = argparse.ArgumentParser(
        description="Validate production climate indices datasets"
    )
    parser.add_argument(
        'data_dir',
        type=Path,
        help='Directory containing NetCDF files to validate'
    )
    parser.add_argument(
        '--year',
        type=int,
        help='Validate specific year only'
    )
    parser.add_argument(
        '--pipeline',
        choices=['temperature', 'precipitation', 'agricultural', 'drought'],
        default='temperature',
        help='Pipeline type (default: temperature)'
    )

    args = parser.parse_args()

    if not args.data_dir.exists():
        print(f"Error: Directory not found: {args.data_dir}")
        sys.exit(1)

    # Find NetCDF files
    if args.year:
        pattern = f"*indices_{args.year}_{args.year}.nc"
    else:
        pattern = "*indices_*.nc"

    files = sorted(args.data_dir.glob(pattern))
    # Exclude tile files
    files = [f for f in files if 'tile' not in f.name]

    if not files:
        print(f"No files found matching pattern: {pattern}")
        sys.exit(1)

    print(f"\nFound {len(files)} files to validate")
    print(f"Pipeline type: {args.pipeline}")
    print("=" * 60)

    # Validate each file
    validator = DataValidator(args.pipeline)
    results = []

    for file_path in files:
        result = validator.validate_file(file_path)
        results.append(result)

    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    total_errors = sum(len(r['errors']) for r in results)
    total_warnings = sum(len(r['warnings']) for r in results)
    total_passed = sum(r['checks_passed'] for r in results)
    total_failed = sum(r['checks_failed'] for r in results)

    print(f"\nFiles validated: {len(results)}")
    print(f"Total checks passed: {total_passed}")
    print(f"Total checks failed: {total_failed}")
    print(f"Total errors: {total_errors}")
    print(f"Total warnings: {total_warnings}")

    # List files with errors
    if total_errors > 0:
        print("\n❌ Files with ERRORS:")
        for result in results:
            if result['errors']:
                print(f"\n  {result['file']}:")
                for error in result['errors']:
                    print(f"    - {error}")

    # List files with warnings
    if total_warnings > 0:
        print("\n⚠️  Files with WARNINGS:")
        for result in results:
            if result['warnings']:
                print(f"\n  {result['file']}:")
                for warning in result['warnings']:
                    print(f"    - {warning}")

    if total_errors == 0:
        print("\n✅ All files passed validation!")
        sys.exit(0)
    else:
        print(f"\n❌ Validation FAILED: {total_errors} errors found")
        sys.exit(1)


if __name__ == '__main__':
    main()

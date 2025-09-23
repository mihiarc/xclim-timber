#!/usr/bin/env python
"""
Test script to verify timedelta conversion fix.
Processes one year of data and checks data types.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

import xarray as xr
import numpy as np
from pipeline_streaming import StreamingClimatePipeline

def test_single_year():
    """Process 2024 and check data types."""

    print("=" * 80)
    print("TESTING TIMEDELTA FIX - Processing 2024 only")
    print("=" * 80)

    # Use streaming pipeline to process just 2024
    config_path = Path(__file__).parent.parent / 'configs' / 'config_comprehensive_2001_2024.yaml'

    # Create test output directory
    test_output = Path('outputs/test_timedelta_fix')
    test_output.mkdir(parents=True, exist_ok=True)

    print("\n1. Processing 2024 with fixed indices calculator...")

    pipeline = StreamingClimatePipeline(str(config_path), chunk_years=1)

    # Override output path for test
    # The Config class uses a different structure, modify the output path directly
    import yaml
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)

    config_data['data']['output_path'] = str(test_output)

    # Save temporary config
    temp_config = test_output / 'test_config.yaml'
    with open(temp_config, 'w') as f:
        yaml.dump(config_data, f)

    # Re-initialize pipeline with modified config
    pipeline = StreamingClimatePipeline(str(temp_config), chunk_years=1)

    try:
        # Process just 2024
        results = pipeline.run_streaming(
            variables=['temperature'],
            start_year=2024,
            end_year=2024
        )

        if results['status'] != 'success':
            print(f"✗ Processing failed: {results.get('error')}")
            return False

    finally:
        pipeline.close()

    print("\n2. Checking output data types...")

    # Load the output file
    output_file = test_output / 'indices_2024_2024.nc'

    if not output_file.exists():
        print(f"✗ Output file not found: {output_file}")
        return False

    ds = xr.open_dataset(output_file)

    # Check data types of key indices
    problematic_indices = ['frost_days', 'ice_days', 'tropical_nights',
                          'consecutive_frost_days', 'gsl']

    print("\nData type check:")
    print("-" * 50)

    all_good = True
    for var in problematic_indices:
        if var in ds.data_vars:
            dtype = str(ds[var].dtype)
            is_numeric = 'float' in dtype or 'int' in dtype

            if 'timedelta' in dtype:
                status = "✗ STILL TIMEDELTA"
                all_good = False
            elif is_numeric:
                status = "✓ FIXED (numeric)"
            else:
                status = f"? Unknown: {dtype}"

            # Check value range
            data = ds[var].values
            non_nan = data[~np.isnan(data)]
            if len(non_nan) > 0:
                vmin, vmax = non_nan.min(), non_nan.max()
                print(f"{var:25s}: {dtype:15s} {status:20s} Range: {vmin:.1f}-{vmax:.1f}")
            else:
                print(f"{var:25s}: {dtype:15s} {status:20s} (all NaN)")
        else:
            print(f"{var:25s}: Not found in output")

    # Check a working index for comparison
    print("\nComparison with working indices:")
    print("-" * 50)

    for var in ['tg_mean', 'cooling_degree_days', 'heating_degree_days']:
        if var in ds.data_vars:
            dtype = str(ds[var].dtype)
            data = ds[var].values
            non_nan = data[~np.isnan(data)]
            if len(non_nan) > 0:
                vmin, vmax = non_nan.min(), non_nan.max()
                print(f"{var:25s}: {dtype:15s} Range: {vmin:.1f}-{vmax:.1f}")

    ds.close()

    return all_good

def main():
    """Run test and report results."""

    success = test_single_year()

    print("\n" + "=" * 80)
    if success:
        print("✓ TIMEDELTA FIX SUCCESSFUL!")
        print("All problematic indices now return numeric values.")
        print("\nNext steps:")
        print("1. Re-process full dataset with fix")
        print("2. Run QA/QC to verify all issues resolved")
    else:
        print("✗ TIMEDELTA FIX INCOMPLETE")
        print("Some indices still returning timedelta values.")
        print("Check the indices_calculator.py implementation.")
    print("=" * 80)

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
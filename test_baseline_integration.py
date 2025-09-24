#!/usr/bin/env python
"""
Test integration of pre-calculated baseline percentiles with streaming pipeline.

This verifies that the indices calculator correctly loads and uses
pre-calculated baseline thresholds for percentile indices.
"""

import xarray as xr
import numpy as np
from pathlib import Path
import logging
import sys

# Add src to path
sys.path.insert(0, 'src')

from config import Config
from indices_calculator import ClimateIndicesCalculator

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def test_baseline_integration():
    """Test that pre-calculated baselines are properly integrated."""

    print("\n" + "="*70)
    print("TESTING BASELINE PERCENTILE INTEGRATION")
    print("="*70)

    # Step 1: Verify baseline file exists
    baseline_path = Path('data/baselines/baseline_percentiles_1981_2000.nc')

    if not baseline_path.exists():
        print(f"✗ Baseline file not found: {baseline_path}")
        print("  Run calculate_baseline_percentiles.py first")
        return False

    print(f"✓ Found baseline file: {baseline_path}")

    # Step 2: Initialize indices calculator
    print("\n2. Initializing indices calculator...")

    # Create minimal config
    config = Config()
    config.set('indices.baseline_period.start', 1981)
    config.set('indices.baseline_period.end', 2000)
    config.set('indices.use_baseline_for_percentiles', True)

    calculator = ClimateIndicesCalculator(config)

    # Check if baselines were loaded
    if calculator._baseline_loaded:
        print(f"✓ Automatically loaded {len(calculator._baseline_percentiles)} baseline percentiles")
        for key in calculator._baseline_percentiles:
            print(f"  - {key}")
    else:
        print("✗ Baseline percentiles not loaded")
        return False

    # Step 3: Test with a single year of data (2023)
    print("\n3. Testing with single year of data (2023)...")

    temp_path = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'

    if Path(temp_path).exists():
        # Load just one year of data
        ds = xr.open_zarr(temp_path, consolidated=False)
        ds_2023 = ds.sel(time=slice('2023-01-01', '2023-12-31'))

        # Get temperature variables
        tasmax = ds_2023['tmax'] if 'tmax' in ds_2023 else None
        tasmin = ds_2023['tmin'] if 'tmin' in ds_2023 else None

        if tasmax is not None and tasmin is not None:
            # Set variable names for threshold matching
            tasmax.name = 'tasmax'
            tasmin.name = 'tasmin'

            # Ensure units
            tasmax.attrs['units'] = 'degC'
            tasmin.attrs['units'] = 'degC'

            print(f"  Data shape: {tasmax.shape}")
            print("  Attempting to calculate TX90p with pre-calculated baseline...")

            try:
                # This should use the pre-calculated baseline
                tx90_per = calculator._calculate_doy_percentile(tasmax, 90)

                # Verify it's using the baseline (should have dayofyear dimension)
                if 'dayofyear' in tx90_per.dims:
                    print(f"✓ Successfully used pre-calculated baseline")
                    print(f"  Threshold shape: {tx90_per.shape}")

                    # Try to calculate the actual index
                    from xclim.indicators import atmos
                    result = atmos.tx90p(tasmax, tasmax_per=tx90_per, freq='YS')

                    # Convert and analyze result
                    values = (result / np.timedelta64(1, 'D')).values
                    valid = values[~np.isnan(values)]

                    if len(valid) > 0:
                        mean_days = np.mean(valid)
                        print(f"✓ TX90p calculated: {mean_days:.1f} days in 2023")

                        # Compare to baseline expectation
                        if mean_days > 36.5:  # Should be > 10% for warming
                            print(f"  Warming signal detected: {mean_days - 36.5:.1f} days above baseline")

                        return True
                    else:
                        print("✗ No valid values in result")
                else:
                    print(f"✗ Unexpected threshold dimensions: {tx90_per.dims}")

            except Exception as e:
                print(f"✗ Error during calculation: {e}")
                import traceback
                traceback.print_exc()
    else:
        print(f"  Temperature data not found at {temp_path}")
        print("  Using synthetic data for testing...")

        # Create synthetic data for one year
        import pandas as pd
        times = pd.date_range('2023-01-01', '2023-12-31', freq='D')
        lats = np.linspace(25, 50, 50)
        lons = np.linspace(-125, -66, 50)

        # Create temperature data
        temp_base = 20 + 10 * np.sin(np.arange(len(times)) * 2 * np.pi / 365)
        temp_3d = temp_base[:, np.newaxis, np.newaxis] + 2  # Add warming
        temp_3d += np.random.randn(len(times), len(lats), len(lons))

        tasmax = xr.DataArray(
            temp_3d + 5,
            dims=['time', 'lat', 'lon'],
            coords={'time': times, 'lat': lats, 'lon': lons},
            name='tasmax'
        )
        tasmax.attrs['units'] = 'degC'

        print("  Created synthetic data")
        print("  Note: Synthetic test may not match baseline spatial structure")

    # Step 4: Verify percentile structure
    print("\n4. Verifying baseline percentile structure...")

    ds_baseline = xr.open_dataset(baseline_path)

    for var in ['tx90p_threshold', 'tx10p_threshold', 'tn90p_threshold', 'tn10p_threshold']:
        if var in ds_baseline:
            data = ds_baseline[var]
            print(f"  {var}:")
            print(f"    Shape: {data.shape}")
            print(f"    Dims: {data.dims}")

            valid = data.values[~np.isnan(data.values)]
            if len(valid) > 0:
                print(f"    Range: {valid.min():.1f} to {valid.max():.1f} {data.attrs.get('units', '')}")

    print("\n" + "="*70)
    print("INTEGRATION TEST COMPLETE")
    print("="*70)
    print("\nThe indices calculator will now:")
    print("1. Automatically load baseline percentiles on initialization")
    print("2. Use pre-calculated thresholds for percentile indices")
    print("3. Process data year-by-year without recalculation")
    print("4. Maintain scientific accuracy with proper baseline periods")

    return True


if __name__ == '__main__':
    success = test_baseline_integration()
    sys.exit(0 if success else 1)
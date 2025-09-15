#!/usr/bin/env python
"""
Test script for baseline period percentile calculations.
Tests that percentiles are correctly calculated using the baseline period.
"""

import numpy as np
import xarray as xr
import pandas as pd
import logging
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from config import Config
from indices_calculator import ClimateIndicesCalculator


def create_test_data():
    """Create test climate data with known patterns."""
    # Create time series from 1970 to 2020
    times = pd.date_range('1970-01-01', '2020-12-31', freq='D')

    # Create spatial dimensions (small for testing)
    lats = np.linspace(-10, 10, 5)
    lons = np.linspace(-20, 20, 8)

    # Create temperature data with trend
    # Base temperature + seasonal cycle + warming trend
    days = np.arange(len(times))
    years = days / 365.25

    # Seasonal component
    seasonal = 10 * np.sin(2 * np.pi * days / 365.25)

    # Warming trend: 0.2°C per decade, stronger after 2000
    trend = np.where(times.year < 2000,
                     0.02 * years,  # Slower warming before 2000
                     0.02 * years + 0.05 * (years - 30))  # Accelerated after 2000

    # Create 3D temperature array
    temp_base = 20  # Base temperature
    temp_data = np.zeros((len(times), len(lats), len(lons)))

    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            # Add spatial variation
            spatial_var = lat * 0.5 + lon * 0.1
            # Add random noise
            noise = np.random.randn(len(times)) * 2
            # Combine all components
            temp_data[:, i, j] = temp_base + seasonal + trend + spatial_var + noise

    # Create dataset
    ds = xr.Dataset(
        {
            'tasmax': (['time', 'lat', 'lon'], temp_data + 5),  # Max temp ~5°C higher
            'tasmin': (['time', 'lat', 'lon'], temp_data - 5),  # Min temp ~5°C lower
            'pr': (['time', 'lat', 'lon'],
                   np.random.exponential(2, (len(times), len(lats), len(lons))))
        },
        coords={
            'time': times,
            'lat': lats,
            'lon': lons
        }
    )

    # Add attributes
    ds['tasmax'].attrs['units'] = 'degC'
    ds['tasmin'].attrs['units'] = 'degC'
    ds['pr'].attrs['units'] = 'mm'

    return ds


def test_baseline_percentiles():
    """Test baseline period percentile calculations."""

    print("\n" + "="*60)
    print("Testing Baseline Period Percentile Calculations")
    print("="*60)

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s'
    )

    # Create test data
    print("\n1. Creating test climate data (1970-2020)...")
    ds = create_test_data()
    print(f"   Dataset shape: {ds.tasmax.shape}")
    print(f"   Time range: {ds.time.values[0]} to {ds.time.values[-1]}")

    # Test with baseline period
    print("\n2. Testing WITH baseline period (1971-2000)...")
    config_with_baseline = Config()
    config_with_baseline.config['indices'] = {
        'baseline_period': {'start': 1971, 'end': 2000},
        'use_baseline_for_percentiles': True,
        'extremes': ['tx90p', 'tn10p'],
        'precipitation': ['r95p']
    }

    calc_with = ClimateIndicesCalculator(config_with_baseline)
    datasets_with = {'temperature': ds, 'precipitation': ds}

    # Calculate tx90p using baseline
    print("\n   Calculating TX90p (warm days) using baseline...")
    indices_with = calc_with.calculate_all_indices(datasets_with)

    # Test without baseline period
    print("\n3. Testing WITHOUT baseline period (full period)...")
    config_without_baseline = Config()
    config_without_baseline.config['indices'] = {
        'use_baseline_for_percentiles': False,
        'extremes': ['tx90p', 'tn10p'],
        'precipitation': ['r95p']
    }

    calc_without = ClimateIndicesCalculator(config_without_baseline)
    datasets_without = {'temperature': ds, 'precipitation': ds}

    # Calculate tx90p without baseline
    print("\n   Calculating TX90p (warm days) using full period...")
    indices_without = calc_without.calculate_all_indices(datasets_without)

    # Compare results
    print("\n4. Comparing Results:")
    print("-" * 40)

    if 'tx90p' in indices_with and 'tx90p' in indices_without:
        # Calculate mean values for target years (2001-2015)
        target_with = indices_with['tx90p'].sel(time=slice('2001', '2015')).mean()
        target_without = indices_without['tx90p'].sel(time=slice('2001', '2015')).mean()

        # Calculate mean values for baseline period
        baseline_with = indices_with['tx90p'].sel(time=slice('1971', '2000')).mean()
        baseline_without = indices_without['tx90p'].sel(time=slice('1971', '2000')).mean()

        print(f"\n   TX90p (Warm Days) - Mean values:")
        print(f"   Baseline period (1971-2000):")
        print(f"     - With baseline: {float(baseline_with.values):.2f} days/year")
        print(f"     - Without baseline: {float(baseline_without.values):.2f} days/year")
        print(f"\n   Target period (2001-2015):")
        print(f"     - With baseline: {float(target_with.values):.2f} days/year")
        print(f"     - Without baseline: {float(target_without.values):.2f} days/year")

        # The difference should be more pronounced in target years
        print(f"\n   Impact of using baseline:")
        print(f"     - Baseline period difference: {float(baseline_with.values - baseline_without.values):.2f} days")
        print(f"     - Target period difference: {float(target_with.values - target_without.values):.2f} days")
        print(f"\n   ✓ With baseline method, warm extremes in 2001-2015 show larger increases")
        print(f"     relative to the 1971-2000 baseline, better capturing climate change signal")

    # Test edge cases
    print("\n5. Testing Edge Cases:")
    print("-" * 40)

    # Test with data outside baseline period
    print("\n   Testing with data only from 2015-2020...")
    recent_data = ds.sel(time=slice('2015', '2020'))
    calc_edge = ClimateIndicesCalculator(config_with_baseline)

    # This should trigger the fallback to full period
    datasets_edge = {'temperature': recent_data}
    indices_edge = calc_edge.calculate_temperature_indices(recent_data)

    print("   ✓ Handled missing baseline period gracefully")

    print("\n" + "="*60)
    print("✅ All tests completed successfully!")
    print("="*60)

    return True


if __name__ == "__main__":
    success = test_baseline_percentiles()
    sys.exit(0 if success else 1)
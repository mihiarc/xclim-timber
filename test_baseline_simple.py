#!/usr/bin/env python
"""
Simple test to verify baseline period functionality for percentile calculations.
Tests that tx90p calculated with 1971-2000 baseline shows climate change signal.
"""

import numpy as np
import xarray as xr
import pandas as pd
from src.config import Config
from src.indices_calculator import ClimateIndicesCalculator
import tempfile
import yaml


def create_test_data():
    """Create synthetic temperature data with warming trend."""
    # Create time series from 1971 to 2015
    time = pd.date_range('1971-01-01', '2015-12-31', freq='D')

    # Create spatial dimensions
    lat = np.linspace(-10, 10, 5)
    lon = np.linspace(-10, 10, 5)

    # Create temperature data with warming trend
    # Baseline period (1971-2000): mean = 20°C
    # Recent period (2001-2015): mean = 21.5°C (1.5°C warming)

    np.random.seed(42)
    n_days = len(time)
    n_lat = len(lat)
    n_lon = len(lon)

    # Base temperature grid
    base_temp = 20.0

    # Add warming trend (0.05°C per year)
    years = (time.year - 1971).values
    trend = years * 0.05

    # Create data with spatial variation and temporal trend
    tasmax = np.zeros((n_days, n_lat, n_lon))

    for i in range(n_lat):
        for j in range(n_lon):
            # Seasonal variation
            day_of_year = time.dayofyear.values
            seasonal = 10 * np.sin(2 * np.pi * day_of_year / 365 - np.pi/2)

            # Random daily variation
            daily_variation = np.random.normal(0, 3, n_days)

            # Combine components
            tasmax[:, i, j] = base_temp + trend + seasonal + daily_variation

    # Create dataset
    ds = xr.Dataset({
        'tasmax': (['time', 'lat', 'lon'], tasmax)
    }, coords={
        'time': time,
        'lat': lat,
        'lon': lon
    })

    # Add attributes
    ds.tasmax.attrs['units'] = 'degC'
    ds.tasmax.attrs['long_name'] = 'Daily Maximum Temperature'

    return ds


def test_baseline_percentiles():
    """Test that baseline percentiles work correctly."""

    print("Creating test data with warming trend...")
    ds = create_test_data()

    # Verify warming trend
    baseline_period = ds.sel(time=slice('1971', '2000'))
    recent_period = ds.sel(time=slice('2001', '2015'))

    baseline_mean = float(baseline_period.tasmax.mean())
    recent_mean = float(recent_period.tasmax.mean())

    print(f"Baseline period (1971-2000) mean: {baseline_mean:.2f}°C")
    print(f"Recent period (2001-2015) mean: {recent_mean:.2f}°C")
    print(f"Warming signal: {recent_mean - baseline_mean:.2f}°C")

    # Create config with baseline period
    config_dict = {
        'indices': {
            'baseline_period': {
                'start': 1971,
                'end': 2000
            },
            'use_baseline_for_percentiles': True,
            'temperature': ['tx90p']  # Warm days
        }
    }

    # Save config to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        config_file = f.name

    # Initialize calculator with baseline config
    config = Config(config_file)
    calculator = ClimateIndicesCalculator(config)

    print("\nCalculating tx90p with 1971-2000 baseline...")

    # Calculate percentile indices
    indices = calculator.calculate_temperature_indices(ds)

    if 'tx90p' in indices:
        tx90p = indices['tx90p']

        # Calculate exceedance for baseline and recent periods
        baseline_tx90p = tx90p.sel(time=slice('1971', '2000'))
        recent_tx90p = tx90p.sel(time=slice('2001', '2015'))

        baseline_mean_exceedance = float(baseline_tx90p.mean())
        recent_mean_exceedance = float(recent_tx90p.mean())

        print(f"\nResults:")
        print(f"Baseline period (1971-2000) tx90p: {baseline_mean_exceedance:.1f}%")
        print(f"Recent period (2001-2015) tx90p: {recent_mean_exceedance:.1f}%")
        print(f"Change in warm days: {recent_mean_exceedance - baseline_mean_exceedance:.1f} percentage points")

        # Verify expectations
        print("\nValidation:")
        if abs(baseline_mean_exceedance - 10.0) < 2.0:
            print("✓ Baseline period shows ~10% exceedance (as expected)")
        else:
            print("✗ Baseline period exceedance unexpected")

        if recent_mean_exceedance > 15.0:
            print("✓ Recent period shows increased warm days (climate change signal detected)")
        else:
            print("✗ Climate change signal not detected")

        # Calculate change
        if recent_mean_exceedance > baseline_mean_exceedance + 5.0:
            print("✓ Significant increase in extreme warm days")

        return True
    else:
        print("Error: tx90p not calculated")
        return False


if __name__ == "__main__":
    print("Testing Baseline Period Functionality")
    print("=" * 50)

    success = test_baseline_percentiles()

    if success:
        print("\n✓ Baseline period test completed successfully")
    else:
        print("\n✗ Baseline period test failed")
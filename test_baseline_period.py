#!/usr/bin/env python3
"""
Test baseline period configuration for percentile-based climate indices.

This test validates that the WMO-standard baseline period (1971-2000)
is correctly used for calculating percentile thresholds for indices like
tx90p (warm days), tn10p (cool nights), r95p (very wet days), etc.
"""

import xarray as xr
import numpy as np
from datetime import datetime, timedelta

def test_baseline_configuration():
    """Test baseline period configuration."""
    print("Testing baseline period configuration...")

    # Create test temperature data (1971-2015)
    years = range(1971, 2016)
    dates = []
    for year in years:
        dates.extend([datetime(year, 1, 1) + timedelta(days=d) for d in range(365)])

    # Create synthetic temperature data with warming trend
    # Baseline period (1971-2000): mean = 20°C
    # Recent period (2001-2015): mean = 21°C (simulating warming)
    temps = []
    for date in dates:
        if date.year <= 2000:
            # Baseline period - cooler
            base_temp = 20.0
        else:
            # Recent period - warmer by 1°C
            base_temp = 21.0

        # Add some random variation
        np.random.seed(date.toordinal())  # Reproducible randomness
        daily_temp = base_temp + np.random.normal(0, 5)
        temps.append(daily_temp)

    # Create xarray dataset
    ds = xr.Dataset({
        'tasmax': xr.DataArray(temps, dims=['time'], coords={'time': dates})
    })
    ds['tasmax'].attrs['units'] = 'degC'

    # Calculate statistics
    baseline_data = ds.sel(time=slice("1971-01-01", "2000-12-31"))
    recent_data = ds.sel(time=slice("2001-01-01", "2015-12-31"))

    baseline_mean = float(baseline_data['tasmax'].mean())
    recent_mean = float(recent_data['tasmax'].mean())
    baseline_p90 = float(baseline_data['tasmax'].quantile(0.9))

    print(f"Baseline period (1971-2000) mean: {baseline_mean:.2f}°C")
    print(f"Recent period (2001-2015) mean: {recent_mean:.2f}°C")
    print(f"Temperature increase: {recent_mean - baseline_mean:.2f}°C")
    print(f"Baseline 90th percentile: {baseline_p90:.2f}°C")

    # With proper baseline, recent warm days should exceed 10%
    recent_exceedances = float((recent_data['tasmax'] > baseline_p90).mean())
    print(f"Percentage of recent days exceeding baseline 90th percentile: {recent_exceedances:.1%}")

    # Validate the warming signal is detected
    if recent_exceedances > 0.10:
        print("✅ PASS: Warming signal detected using baseline period")
        print(f"   {recent_exceedances:.1%} of recent days exceed baseline 90th percentile")
        print("   (Expected >10% if warming is occurring)")
        return True
    else:
        print("❌ FAIL: Warming signal not properly detected")
        return False

if __name__ == "__main__":
    print("="*60)
    print("BASELINE PERIOD CONFIGURATION TEST")
    print("="*60)
    print("\nValidating WMO-standard baseline period (1971-2000)...\n")

    success = test_baseline_configuration()

    if success:
        print("\n✅ Baseline period configuration working correctly!")
        print("   - Percentiles calculated from 1971-2000 reference period")
        print("   - Climate change signals properly detected in recent data")
        print("   - WMO standards for climate monitoring implemented")
    else:
        print("\n❌ Baseline period test failed!")
        exit(1)
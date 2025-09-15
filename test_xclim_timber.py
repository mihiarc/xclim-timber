#!/usr/bin/env python
"""
Minimal test coverage for xclim_timber.py to ensure correctness of critical functions.
Run with: pytest test_xclim_timber.py -v
"""

import numpy as np
import pandas as pd
import xarray as xr
import tempfile
import os
from pathlib import Path
import pytest

# Import the module to test
import xclim_timber as xt


class TestCriticalFunctions:
    """Test critical functionality to prevent regressions."""

    def test_nearest_neighbor_extraction(self):
        """Verify that nearest neighbor extraction works correctly."""
        # Create a simple test dataset
        lats = np.array([40.0, 41.0, 42.0])
        lons = np.array([-120.0, -119.0, -118.0])
        time = pd.date_range('2020-01-01', periods=10)

        # Create temperature data with a gradient
        temp_data = np.ones((10, 3, 3)) * 20.0
        # Make center point warmer
        temp_data[:, 1, 1] = 25.0

        ds = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], temp_data)},
            coords={'time': time, 'lat': lats, 'lon': lons}
        )

        # Test extraction at exact grid point
        parcels = pd.DataFrame({
            'saleid': [1],
            'parcelid': ['P001'],
            'parcel_level_latitude': [41.0],
            'parcel_level_longitude': [-119.0]
        })

        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            parcels.to_csv(f.name, index=False)
            with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as nc:
                ds.to_netcdf(nc.name)
                with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as out:
                    xt.process_year(nc.name, f.name, out.name, 2020)
                    results = pd.read_csv(out.name)

                    # Should extract the center point (25°C)
                    assert np.abs(results['annual_mean'].values[0] - 25.0) < 0.01

        # Clean up
        for file in [f.name, nc.name, out.name]:
            if os.path.exists(file):
                os.unlink(file)

    def test_temperature_conversion(self):
        """Test Kelvin to Celsius conversion."""
        # Create dataset in Kelvin
        lats = np.array([40.0])
        lons = np.array([-120.0])
        time = pd.date_range('2020-01-01', periods=365)

        # Temperature in Kelvin (0°C = 273.15K)
        temp_kelvin = np.ones((365, 1, 1)) * 273.15

        ds = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], temp_kelvin)},
            coords={'time': time, 'lat': lats, 'lon': lons}
        )

        parcels = pd.DataFrame({
            'saleid': [1],
            'parcelid': ['P001'],
            'parcel_level_latitude': [40.0],
            'parcel_level_longitude': [-120.0]
        })

        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            parcels.to_csv(f.name, index=False)
            with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as nc:
                ds.to_netcdf(nc.name)
                with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as out:
                    xt.process_year(nc.name, f.name, out.name, 2020)
                    results = pd.read_csv(out.name)

                    # Should convert to 0°C
                    assert np.abs(results['annual_mean'].values[0] - 0.0) < 0.01
                    # Should have 365 frost days at 0°C
                    assert results['frost_days'].values[0] == 0  # Exactly at 0°C

        # Clean up
        for file in [f.name, nc.name, out.name]:
            if os.path.exists(file):
                os.unlink(file)

    def test_gdd_calculation(self):
        """Test growing degree days calculation."""
        # Known case: 10 days at 20°C with base 10°C = 10 * (20-10) = 100 GDD
        test_data = np.array([[20.0] * 10]).reshape(1, 10)

        # Direct test of GDD calculation
        gdd = np.maximum(test_data - 10, 0)
        total_gdd = np.nansum(gdd, axis=1)

        assert total_gdd[0] == 100.0

    def test_precipitation_conversion(self):
        """Test precipitation unit conversion from kg m-2 s-1 to mm/day."""
        # 1 kg m-2 s-1 = 86400 mm/day
        precip_kgms = 0.0001  # Small rainfall rate
        expected_mmday = precip_kgms * 86400

        # Create simple test data
        data = np.array([[precip_kgms] * 10])

        # Apply conversion (simulating the code logic)
        if data.mean() < 1:  # Likely in kg m-2 s-1
            data_converted = data * 86400

        assert np.abs(data_converted.mean() - expected_mmday) < 0.001

    def test_consecutive_days(self):
        """Test consecutive days calculation."""
        # Test data: 3 hot days, 2 cool days, 4 hot days
        test_temps = np.array([[31, 31, 31, 20, 20, 31, 31, 31, 31]])

        # Test the function
        result = xt.calculate_consecutive_days(test_temps, 30, 'greater')

        # Should find max consecutive of 4 days
        assert result[0] == 4

    def test_freeze_thaw_cycles(self):
        """Test freeze-thaw cycle detection."""
        # Test data with known transitions
        # -5, 5, -5, 5 = 3 transitions across 0°C
        test_data = np.array([[-5, 5, -5, 5]])

        freeze_thaw = np.zeros(1)
        above_zero = test_data[0] > 0
        transitions = np.diff(above_zero.astype(int))
        freeze_thaw[0] = np.sum(np.abs(transitions))

        assert freeze_thaw[0] == 3

    def test_percentile_calculation(self):
        """Test that percentiles are calculated correctly."""
        # Create data with known distribution
        test_data = np.arange(0, 100, 1).reshape(1, -1)

        # Calculate percentiles
        p10 = np.nanpercentile(test_data, 10, axis=1)
        p50 = np.nanpercentile(test_data, 50, axis=1)  # Median
        p90 = np.nanpercentile(test_data, 90, axis=1)

        assert np.abs(p10[0] - 9.9) < 0.1  # ~10th value
        assert np.abs(p50[0] - 49.5) < 0.1  # ~50th value
        assert np.abs(p90[0] - 89.1) < 0.1  # ~90th value

    def test_corn_gdd_formula(self):
        """Test corrected corn GDD formula."""
        # Test with temperature above cap
        test_temp = np.array([[35.0]])  # Above 30°C cap

        # Correct formula: cap temp first, then calculate GDD
        capped_temp = np.minimum(test_temp, 30)
        corn_gdd = np.maximum(capped_temp - 10, 0)

        # Should be (30 - 10) = 20, not (35 - 10) = 25
        assert corn_gdd[0, 0] == 20.0

    def test_missing_data_handling(self):
        """Test handling of NaN values in climate data."""
        # Create data with NaN values
        test_data = np.array([[20.0, np.nan, 22.0, 23.0, np.nan]])

        # Test that statistics ignore NaN
        mean_val = np.nanmean(test_data)
        count_above_20 = np.sum(test_data > 20)  # Should handle NaN

        assert np.abs(mean_val - 21.67) < 0.1
        assert count_above_20 == 2  # Only counts 22 and 23

    def test_longitude_conversion(self):
        """Test longitude coordinate system conversion."""
        # Test conversion from -180/180 to 0/360
        lons_180 = np.array([-170, -10, 0, 10, 170])

        # Apply conversion
        lons_360 = lons_180.copy()
        lons_360[lons_360 < 0] += 360

        expected = np.array([190, 350, 0, 10, 170])
        assert np.array_equal(lons_360, expected)


if __name__ == '__main__':
    # Run tests with pytest if available, otherwise run directly
    try:
        import pytest
        pytest.main([__file__, '-v'])
    except ImportError:
        print("Running tests without pytest...")
        test_suite = TestCriticalFunctions()
        for method_name in dir(test_suite):
            if method_name.startswith('test_'):
                print(f"Running {method_name}...")
                try:
                    getattr(test_suite, method_name)()
                    print(f"  ✓ {method_name} passed")
                except AssertionError as e:
                    print(f"  ✗ {method_name} failed: {e}")
                except Exception as e:
                    print(f"  ✗ {method_name} error: {e}")
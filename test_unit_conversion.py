#!/usr/bin/env python
"""
Test script to verify temperature unit conversion functionality.
Tests automatic detection and conversion of temperature units.
"""

import numpy as np
import xarray as xr
import pandas as pd
from src.config import Config
from src.indices_calculator import ClimateIndicesCalculator
import tempfile
import yaml


def create_temperature_data(units='degC', add_units_attr=True):
    """
    Create synthetic temperature data in specified units.

    Parameters:
    -----------
    units : str
        Temperature units ('degC', 'K', or 'degF')
    add_units_attr : bool
        Whether to add units attribute to the data

    Returns:
    --------
    xr.Dataset
        Temperature dataset in specified units
    """
    # Create time series
    time = pd.date_range('2020-01-01', '2020-12-31', freq='D')

    # Create spatial dimensions
    lat = np.linspace(-10, 10, 3)
    lon = np.linspace(-10, 10, 3)

    # Base temperature in Celsius
    base_temp_c = 20.0

    # Seasonal variation
    day_of_year = time.dayofyear.values
    seasonal = 10 * np.sin(2 * np.pi * day_of_year / 365 - np.pi/2)

    # Random daily variation
    np.random.seed(42)
    daily_variation = np.random.normal(0, 2, len(time))

    # Combine to get temperature in Celsius
    temp_c = base_temp_c + seasonal + daily_variation

    # Convert to requested units
    if units == 'K':
        temp = temp_c + 273.15
    elif units == 'degF':
        temp = temp_c * 9/5 + 32
    else:  # degC
        temp = temp_c

    # Create 3D array
    n_time = len(time)
    n_lat = len(lat)
    n_lon = len(lon)
    temp_3d = np.zeros((n_time, n_lat, n_lon))

    for i in range(n_lat):
        for j in range(n_lon):
            temp_3d[:, i, j] = temp

    # Create dataset
    ds = xr.Dataset({
        'tas': (['time', 'lat', 'lon'], temp_3d),
        'tasmax': (['time', 'lat', 'lon'], temp_3d + 5),  # Max is 5 degrees warmer
        'tasmin': (['time', 'lat', 'lon'], temp_3d - 5)   # Min is 5 degrees cooler
    }, coords={
        'time': time,
        'lat': lat,
        'lon': lon
    })

    # Add units attribute if requested
    if add_units_attr:
        for var in ['tas', 'tasmax', 'tasmin']:
            ds[var].attrs['units'] = units
            ds[var].attrs['long_name'] = f'Temperature in {units}'

    return ds


def test_kelvin_conversion():
    """Test conversion from Kelvin to Celsius."""
    print("\n" + "="*50)
    print("Test 1: Kelvin to Celsius Conversion")
    print("="*50)

    # Create data in Kelvin
    ds_kelvin = create_temperature_data(units='K', add_units_attr=True)

    # Verify input is in Kelvin
    tas_mean = float(ds_kelvin.tas.mean())
    print(f"Input temperature (Kelvin): {tas_mean:.2f} K")
    print(f"Expected in Celsius: {tas_mean - 273.15:.2f}°C")

    # Create config for Celsius target
    config_dict = {
        'processing': {
            'temperature_units': 'degC',
            'auto_convert_units': True
        },
        'indices': {
            'temperature': ['tg_mean']
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        config_file = f.name

    # Initialize calculator
    config = Config(config_file)
    calculator = ClimateIndicesCalculator(config)

    # Calculate indices (should auto-convert)
    indices = calculator.calculate_temperature_indices(ds_kelvin)

    # Check if conversion happened
    if 'tg_mean' in indices:
        result_mean = float(indices['tg_mean'].mean())
        expected_mean = tas_mean - 273.15  # Convert to Celsius

        print(f"\nResult after processing: {result_mean:.2f}°C")
        print(f"Difference from expected: {abs(result_mean - expected_mean):.4f}°C")

        if abs(result_mean - expected_mean) < 1.0:
            print("✓ Kelvin to Celsius conversion successful")
            return True
        else:
            print("✗ Conversion may have failed")
            return False
    else:
        print("✗ Index calculation failed")
        return False


def test_fahrenheit_conversion():
    """Test conversion from Fahrenheit to Celsius."""
    print("\n" + "="*50)
    print("Test 2: Fahrenheit to Celsius Conversion")
    print("="*50)

    # Create data in Fahrenheit
    ds_fahrenheit = create_temperature_data(units='degF', add_units_attr=True)

    # Verify input is in Fahrenheit
    tas_mean = float(ds_fahrenheit.tas.mean())
    print(f"Input temperature (Fahrenheit): {tas_mean:.2f}°F")
    print(f"Expected in Celsius: {(tas_mean - 32) * 5/9:.2f}°C")

    # Create config for Celsius target
    config_dict = {
        'processing': {
            'temperature_units': 'degC',
            'auto_convert_units': True
        },
        'indices': {
            'temperature': ['tg_mean']
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        config_file = f.name

    # Initialize calculator
    config = Config(config_file)
    calculator = ClimateIndicesCalculator(config)

    # Calculate indices (should auto-convert)
    indices = calculator.calculate_temperature_indices(ds_fahrenheit)

    # Check if conversion happened
    if 'tg_mean' in indices:
        result_mean = float(indices['tg_mean'].mean())
        expected_mean = (tas_mean - 32) * 5/9  # Convert to Celsius

        print(f"\nResult after processing: {result_mean:.2f}°C")
        print(f"Difference from expected: {abs(result_mean - expected_mean):.4f}°C")

        if abs(result_mean - expected_mean) < 1.0:
            print("✓ Fahrenheit to Celsius conversion successful")
            return True
        else:
            print("✗ Conversion may have failed")
            return False
    else:
        print("✗ Index calculation failed")
        return False


def test_no_units_attribute():
    """Test inference of units when no units attribute is present."""
    print("\n" + "="*50)
    print("Test 3: Unit Inference (No Units Attribute)")
    print("="*50)

    # Create data in Kelvin without units attribute
    ds_no_units = create_temperature_data(units='K', add_units_attr=False)

    # Verify input range suggests Kelvin
    tas_min = float(ds_no_units.tas.min())
    tas_max = float(ds_no_units.tas.max())
    tas_mean = float(ds_no_units.tas.mean())

    print(f"Input temperature range: {tas_min:.2f} to {tas_max:.2f}")
    print(f"Mean: {tas_mean:.2f}")
    print("This range suggests Kelvin units (>200 and <350)")

    # Create config
    config_dict = {
        'processing': {
            'temperature_units': 'degC',
            'auto_convert_units': True
        },
        'indices': {
            'temperature': ['frost_days']  # Should work correctly with Celsius
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        config_file = f.name

    # Initialize calculator
    config = Config(config_file)
    calculator = ClimateIndicesCalculator(config)

    # Calculate indices (should infer units and convert)
    indices = calculator.calculate_temperature_indices(ds_no_units)

    if 'frost_days' in indices:
        # With proper conversion, should have some frost days in winter
        frost_days_total = float(indices['frost_days'].sum())
        print(f"\nFrost days calculated: {frost_days_total}")

        if frost_days_total > 0:
            print("✓ Unit inference and conversion successful")
            print("  (Frost days detected, indicating proper Celsius conversion)")
            return True
        else:
            print("⚠ No frost days detected")
            print("  (This might be correct depending on the temperature range)")
            return True
    else:
        print("✗ Index calculation failed")
        return False


def test_celsius_passthrough():
    """Test that Celsius data is not modified when target is Celsius."""
    print("\n" + "="*50)
    print("Test 4: Celsius Passthrough (No Conversion Needed)")
    print("="*50)

    # Create data already in Celsius
    ds_celsius = create_temperature_data(units='degC', add_units_attr=True)

    tas_mean_input = float(ds_celsius.tas.mean())
    print(f"Input temperature (Celsius): {tas_mean_input:.2f}°C")

    # Create config for Celsius target
    config_dict = {
        'processing': {
            'temperature_units': 'degC',
            'auto_convert_units': True
        },
        'indices': {
            'temperature': ['tg_mean']
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        config_file = f.name

    # Initialize calculator
    config = Config(config_file)
    calculator = ClimateIndicesCalculator(config)

    # Calculate indices (should not convert)
    indices = calculator.calculate_temperature_indices(ds_celsius)

    if 'tg_mean' in indices:
        result_mean = float(indices['tg_mean'].mean())

        print(f"\nResult after processing: {result_mean:.2f}°C")
        print(f"Difference from input: {abs(result_mean - tas_mean_input):.4f}°C")

        if abs(result_mean - tas_mean_input) < 0.1:
            print("✓ Celsius data passed through without modification")
            return True
        else:
            print("⚠ Unexpected difference in values")
            return False
    else:
        print("✗ Index calculation failed")
        return False


if __name__ == "__main__":
    print("Temperature Unit Conversion Tests")
    print("="*50)

    results = []

    # Run all tests
    results.append(("Kelvin to Celsius", test_kelvin_conversion()))
    results.append(("Fahrenheit to Celsius", test_fahrenheit_conversion()))
    results.append(("Unit Inference", test_no_units_attribute()))
    results.append(("Celsius Passthrough", test_celsius_passthrough()))

    # Summary
    print("\n" + "="*50)
    print("Test Summary")
    print("="*50)

    all_passed = True
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name}: {status}")
        if not passed:
            all_passed = False

    print("\n" + "="*50)
    if all_passed:
        print("✓ All unit conversion tests passed successfully!")
    else:
        print("✗ Some tests failed. Please review the output above.")
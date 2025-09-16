#!/usr/bin/env python
"""
Detailed test to investigate unit conversion issues.
"""

import numpy as np
import xarray as xr
import pandas as pd
from src.config import Config
from src.indices_calculator import ClimateIndicesCalculator
import tempfile
import yaml
from xclim.core.units import convert_units_to


def test_unit_conversion_details():
    """Test unit conversion in detail to identify issues."""
    print("Detailed Unit Conversion Investigation")
    print("=" * 50)

    # Create test data with different scenarios
    time = pd.date_range('2020-01-01', '2020-12-31', freq='D')
    lat = np.array([40.0])
    lon = np.array([-100.0])

    # Test data: 15°C minimum, 25°C maximum (10°C range)
    temp_min = np.ones((len(time), 1, 1)) * 15.0
    temp_max = np.ones((len(time), 1, 1)) * 25.0
    temp_mean = np.ones((len(time), 1, 1)) * 20.0

    # Create dataset
    ds = xr.Dataset({
        'tas': (['time', 'lat', 'lon'], temp_mean),
        'tasmin': (['time', 'lat', 'lon'], temp_min),
        'tasmax': (['time', 'lat', 'lon'], temp_max),
    }, coords={
        'time': time,
        'lat': lat,
        'lon': lon
    })

    # Test with Celsius input
    print("\n1. Testing with Celsius input:")
    print("-" * 30)

    ds_celsius = ds.copy()
    ds_celsius.tas.attrs['units'] = 'degC'
    ds_celsius.tasmin.attrs['units'] = 'degC'
    ds_celsius.tasmax.attrs['units'] = 'degC'

    test_temperature_range_calculation(ds_celsius, "Celsius input")

    # Test with Kelvin input
    print("\n2. Testing with Kelvin input:")
    print("-" * 30)

    ds_kelvin = ds.copy()
    # Convert to Kelvin
    ds_kelvin['tas'] = ds_kelvin['tas'] + 273.15
    ds_kelvin['tasmin'] = ds_kelvin['tasmin'] + 273.15
    ds_kelvin['tasmax'] = ds_kelvin['tasmax'] + 273.15

    ds_kelvin.tas.attrs['units'] = 'K'
    ds_kelvin.tasmin.attrs['units'] = 'K'
    ds_kelvin.tasmax.attrs['units'] = 'K'

    test_temperature_range_calculation(ds_kelvin, "Kelvin input")

    # Test xclim direct calculation
    print("\n3. Testing xclim direct calculation:")
    print("-" * 40)

    from xclim import atmos

    # Test with Celsius
    print("Direct xclim with Celsius:")
    result_celsius = atmos.daily_temperature_range(ds_celsius.tasmin, ds_celsius.tasmax, freq='YS')
    print(f"  Result: {float(result_celsius.values):.2f} {result_celsius.attrs.get('units', 'unknown')}")

    # Test with Kelvin
    print("Direct xclim with Kelvin:")
    result_kelvin = atmos.daily_temperature_range(ds_kelvin.tasmin, ds_kelvin.tasmax, freq='YS')
    print(f"  Result: {float(result_kelvin.values):.2f} {result_kelvin.attrs.get('units', 'unknown')}")

    # Test manual conversion
    print("\nManual unit conversion test:")
    if result_kelvin.attrs.get('units') == 'K':
        converted = convert_units_to(result_kelvin, 'degC')
        print(f"  Converted result: {float(converted.values):.2f} {converted.attrs.get('units', 'unknown')}")


def test_temperature_range_calculation(ds, scenario_name):
    """Test temperature range calculation with given dataset."""
    print(f"\nScenario: {scenario_name}")

    # Create config
    config_dict = {
        'processing': {
            'temperature_units': 'degC'
        },
        'indices': {
            'temperature': ['tg_mean', 'tx_max', 'tn_min', 'daily_temperature_range']
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        config_file = f.name

    # Initialize calculator
    config = Config(config_file)
    calculator = ClimateIndicesCalculator(config)

    # Check input data
    print(f"Input data ranges:")
    print(f"  tas: {float(ds.tas.min()):.2f} to {float(ds.tas.max()):.2f} {ds.tas.attrs.get('units', 'unknown')}")
    print(f"  tasmin: {float(ds.tasmin.min()):.2f} to {float(ds.tasmin.max()):.2f} {ds.tasmin.attrs.get('units', 'unknown')}")
    print(f"  tasmax: {float(ds.tasmax.min()):.2f} to {float(ds.tasmax.max()):.2f} {ds.tasmax.attrs.get('units', 'unknown')}")

    # Calculate indices
    indices = calculator.calculate_temperature_indices(ds)

    # Check outputs
    print(f"Calculated indices:")
    for index_name, result in indices.items():
        if result is not None:
            value = float(result.values)
            units = result.attrs.get('units', 'unknown')
            print(f"  {index_name}: {value:.2f} {units}")

            # Validate daily temperature range specifically
            if index_name == 'daily_temperature_range':
                expected_range = 10.0  # 25°C - 15°C = 10°C
                if abs(value - expected_range) < 0.1:
                    print(f"    ✓ Correct temperature range")
                else:
                    print(f"    ✗ Incorrect temperature range (expected ~{expected_range}°C)")


if __name__ == "__main__":
    test_unit_conversion_details()
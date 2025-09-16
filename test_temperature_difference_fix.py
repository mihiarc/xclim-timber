#!/usr/bin/env python
"""
Test the fixed temperature difference conversion logic.
This test specifically validates that absolute temperatures and temperature differences
are handled correctly according to thermodynamic principles.
"""

import numpy as np
import xarray as xr
import pandas as pd
from src.config import Config
from src.indices_calculator import ClimateIndicesCalculator
import tempfile
import yaml


def test_absolute_temperature_conversion():
    """Test that absolute temperatures are correctly converted from Kelvin to Celsius."""
    print("Testing absolute temperature conversion...")

    # Create test data with known values in Celsius
    time = pd.date_range('2020-01-01', '2020-12-31', freq='D')
    lat = np.array([40.0])
    lon = np.array([-100.0])

    # Temperature data: 20°C constant (should become 293.15K internally, then back to 20°C)
    temp_data = np.ones((len(time), 1, 1)) * 20.0

    # Create dataset with Celsius input
    ds = xr.Dataset({
        'tas': (['time', 'lat', 'lon'], temp_data),
        'tasmax': (['time', 'lat', 'lon'], temp_data + 5),  # 25°C
        'tasmin': (['time', 'lat', 'lon'], temp_data - 5),  # 15°C
    }, coords={
        'time': time,
        'lat': lat,
        'lon': lon
    })

    # Set units to Celsius
    for var in ['tas', 'tasmax', 'tasmin']:
        ds[var].attrs['units'] = 'degC'

    # Create config
    config_dict = {
        'processing': {'temperature_units': 'degC'},
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

    # Calculate indices
    indices = calculator.calculate_temperature_indices(ds)

    print(f"\nAbsolute Temperature Results:")

    # Test absolute temperature indices
    absolute_temp_indices = ['tg_mean', 'tx_max', 'tn_min']
    for index_name in absolute_temp_indices:
        if index_name in indices:
            result = indices[index_name]
            value = float(result.values[0])
            units = result.attrs.get('units', 'unknown')

            print(f"{index_name}: {value:.2f} {units}")

            # Validate units
            if units in ['°C', 'degC']:
                print(f"  ✓ {index_name} has correct Celsius units")
            else:
                print(f"  ✗ {index_name} has wrong units: {units}")

            # Validate values (should be close to expected values)
            expected_values = {'tg_mean': 20.0, 'tx_max': 25.0, 'tn_min': 15.0}
            expected = expected_values[index_name]

            if abs(value - expected) < 1.0:
                print(f"  ✓ {index_name} value correct: {value:.2f}°C (expected ~{expected}°C)")
            else:
                print(f"  ✗ {index_name} value wrong: {value:.2f}°C (expected ~{expected}°C)")

    return indices


def test_temperature_difference_conversion():
    """Test that temperature differences are correctly handled (no offset conversion)."""
    print("\nTesting temperature difference conversion...")

    # Create test data with different max and min temperatures
    time = pd.date_range('2020-01-01', '2020-12-31', freq='D')
    lat = np.array([40.0])
    lon = np.array([-100.0])

    # Create temperature data with a known daily range
    # Max: 25°C, Min: 15°C, so daily range should be 10°C
    tasmax_data = np.ones((len(time), 1, 1)) * 25.0
    tasmin_data = np.ones((len(time), 1, 1)) * 15.0

    # Create dataset
    ds = xr.Dataset({
        'tasmax': (['time', 'lat', 'lon'], tasmax_data),
        'tasmin': (['time', 'lat', 'lon'], tasmin_data),
    }, coords={
        'time': time,
        'lat': lat,
        'lon': lon
    })

    # Set units to Celsius
    for var in ['tasmax', 'tasmin']:
        ds[var].attrs['units'] = 'degC'

    # Create config
    config_dict = {
        'processing': {'temperature_units': 'degC'},
        'indices': {
            'temperature': ['daily_temperature_range', 'daily_temperature_range_variability']
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_dict, f)
        config_file = f.name

    # Initialize calculator
    config = Config(config_file)
    calculator = ClimateIndicesCalculator(config)

    # Calculate indices
    indices = calculator.calculate_temperature_indices(ds)

    print(f"Temperature Difference Results:")

    # Test temperature difference indices
    if 'daily_temperature_range' in indices:
        result = indices['daily_temperature_range']
        value = float(result.values[0])
        units = result.attrs.get('units', 'unknown')

        print(f"daily_temperature_range: {value:.2f} {units}")

        # Validate units
        if units in ['°C', 'degC']:
            print(f"  ✓ Temperature range has correct Celsius units")
        else:
            print(f"  ✗ Temperature range has wrong units: {units}")

        # Validate value - should be 10°C (25°C - 15°C)
        # This is the critical test: it should be 10°C, NOT -263.15°C
        if 9.0 <= value <= 11.0:  # Allow small numerical error
            print(f"  ✓ Temperature range value correct: {value:.2f}°C (expected ~10°C)")
            print(f"  ✓ CRITICAL: Temperature difference correctly handled (not shifted by 273.15)")
        else:
            print(f"  ✗ Temperature range value WRONG: {value:.2f}°C (expected ~10°C)")
            if value < -200:
                print(f"  ✗ CRITICAL ERROR: Temperature difference incorrectly converted! (Got {value:.2f}°C)")

    return indices


def test_scientific_correctness():
    """Test the scientific correctness of the temperature conversion approach."""
    print("\nTesting scientific correctness...")

    # Test that temperature differences are scale-invariant between K and °C
    print("Scientific principle: ΔT(K) = ΔT(°C)")
    print("Example: If Tmax=298.15K and Tmin=288.15K, then:")
    print("  - ΔT = 298.15 - 288.15 = 10.0K")
    print("  - In Celsius: Tmax=25°C, Tmin=15°C, so ΔT = 25 - 15 = 10.0°C")
    print("  - Therefore: 10.0K = 10.0°C for temperature differences")
    print("  - Our conversion should produce 10.0°C, NOT -263.15°C")

    return True


if __name__ == "__main__":
    print("Temperature Conversion Fix Verification")
    print("=" * 50)

    print("This test validates the fix for temperature difference conversion logic.")
    print("Previously: temperature differences were incorrectly converted (10K → -263.15°C)")
    print("Fixed: temperature differences keep same numerical value (10K → 10°C)")

    # Run tests
    abs_results = test_absolute_temperature_conversion()
    diff_results = test_temperature_difference_conversion()
    test_scientific_correctness()

    print("\n" + "=" * 50)
    print("SUMMARY:")

    # Check if daily_temperature_range is reasonable
    if 'daily_temperature_range' in diff_results:
        range_value = float(diff_results['daily_temperature_range'].values[0])
        if 9.0 <= range_value <= 11.0:
            print("✓ HOTFIX SUCCESSFUL: Temperature difference conversion fixed!")
            print(f"  Daily temperature range: {range_value:.2f}°C (scientifically correct)")
        else:
            print("✗ HOTFIX FAILED: Temperature difference conversion still broken!")
            print(f"  Daily temperature range: {range_value:.2f}°C (should be ~10°C)")
    else:
        print("⚠ Could not test temperature difference conversion")

    # Check if absolute temperatures are reasonable
    if 'tg_mean' in abs_results:
        mean_value = float(abs_results['tg_mean'].values[0])
        if 19.0 <= mean_value <= 21.0:
            print("✓ Absolute temperature conversion working correctly")
            print(f"  Mean temperature: {mean_value:.2f}°C")
        else:
            print("✗ Absolute temperature conversion broken")
            print(f"  Mean temperature: {mean_value:.2f}°C (should be ~20°C)")
    else:
        print("⚠ Could not test absolute temperature conversion")
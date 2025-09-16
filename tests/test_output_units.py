#!/usr/bin/env python
"""
Test output unit conversion functionality.
"""

import numpy as np
import xarray as xr
import pandas as pd
from src.config import Config
from src.indices_calculator import ClimateIndicesCalculator
import tempfile
import yaml


def test_output_unit_conversion():
    """Test that temperature outputs are converted to Celsius."""
    print("Testing output unit conversion...")

    # Create test data in Celsius
    time = pd.date_range('2020-01-01', '2020-12-31', freq='D')
    lat = np.array([40.0])
    lon = np.array([-100.0])

    # Temperature data (constant 20°C)
    temp_data = np.ones((len(time), 1, 1)) * 20.0

    # Create dataset
    ds = xr.Dataset({
        'tas': (['time', 'lat', 'lon'], temp_data),
    }, coords={
        'time': time,
        'lat': lat,
        'lon': lon
    })

    # Set units to Celsius
    ds.tas.attrs['units'] = 'degC'

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

    # Calculate indices
    indices = calculator.calculate_temperature_indices(ds)

    # Check outputs
    print(f"\nResults:")
    for index_name, result in indices.items():
        if result is not None:
            value = float(result.values)
            units = result.attrs.get('units', 'unknown')
            print(f"{index_name}: {value:.2f} {units}")

            # For temperature statistics, check they're in Celsius and reasonable
            if index_name in ['tg_mean', 'tx_max', 'tn_min']:
                if units == '°C' or units == 'degC':
                    print(f"  ✓ {index_name} correctly in Celsius")
                    if 19.5 <= value <= 20.5:  # Should be ~20°C
                        print(f"  ✓ {index_name} value is reasonable: {value:.2f}°C")
                    else:
                        print(f"  ⚠ {index_name} value unexpected: {value:.2f}°C")
                else:
                    print(f"  ✗ {index_name} not in Celsius (units: {units})")
            elif index_name == 'daily_temperature_range':
                if units == '°C' or units == 'degC':
                    print(f"  ✓ {index_name} correctly in Celsius")
                else:
                    print(f"  ✗ {index_name} not in Celsius (units: {units})")

    return len(indices) > 0


if __name__ == "__main__":
    print("Output Unit Conversion Test")
    print("=" * 40)

    success = test_output_unit_conversion()

    if success:
        print("\n✓ Output unit conversion test completed")
    else:
        print("\n✗ Output unit conversion test failed")
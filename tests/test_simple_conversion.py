#!/usr/bin/env python
"""
Simple test to check xclim's unit handling behavior.
"""

import numpy as np
import xarray as xr
from xclim import atmos
from xclim.core.units import convert_units_to

# Create simple temperature data
time = np.arange(10)
data_celsius = np.array([0, 5, 10, 15, 20, 25, 30, 25, 20, 15])  # Celsius
data_kelvin = data_celsius + 273.15  # Kelvin

# Create DataArrays
da_celsius = xr.DataArray(data_celsius, dims=['time'], coords={'time': time})
da_celsius.attrs['units'] = 'degC'

da_kelvin = xr.DataArray(data_kelvin, dims=['time'], coords={'time': time})
da_kelvin.attrs['units'] = 'K'

print("Original data:")
print(f"Celsius: {da_celsius.values}")
print(f"Kelvin: {da_kelvin.values}")
print()

# Test xclim's tg_mean function
print("Testing tg_mean:")
try:
    result_c = atmos.tg_mean(da_celsius, freq='YS')
    print(f"Result from Celsius input: {result_c.values[0]:.2f} (units: {result_c.attrs.get('units', 'unknown')})")
except Exception as e:
    print(f"Error with Celsius: {e}")

try:
    result_k = atmos.tg_mean(da_kelvin, freq='YS')
    print(f"Result from Kelvin input: {result_k.values[0]:.2f} (units: {result_k.attrs.get('units', 'unknown')})")
except Exception as e:
    print(f"Error with Kelvin: {e}")

print()

# Test unit conversion
print("Testing convert_units_to:")
da_k_to_c = convert_units_to(da_kelvin, 'degC')
print(f"Kelvin to Celsius: {da_k_to_c.values}")
print(f"Units after conversion: {da_k_to_c.attrs.get('units', 'unknown')}")

da_c_to_k = convert_units_to(da_celsius, 'K')
print(f"Celsius to Kelvin: {da_c_to_k.values}")
print(f"Units after conversion: {da_c_to_k.attrs.get('units', 'unknown')}")
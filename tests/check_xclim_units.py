#!/usr/bin/env python
"""Check how xclim handles units internally."""

import pandas as pd
import xarray as xr
from xclim import atmos

# Create proper datetime index
time = pd.date_range('2020-01-01', periods=365, freq='D')

# Create temperature data in Celsius
temp_celsius = xr.DataArray(
    [20.0] * 365,  # Constant 20°C
    dims=['time'],
    coords={'time': time}
)
temp_celsius.attrs['units'] = 'degC'

# Create same data in Kelvin
temp_kelvin = xr.DataArray(
    [293.15] * 365,  # Same as 20°C
    dims=['time'],
    coords={'time': time}
)
temp_kelvin.attrs['units'] = 'K'

print("Input data:")
print(f"Celsius: {temp_celsius.values[0]:.2f} {temp_celsius.attrs['units']}")
print(f"Kelvin: {temp_kelvin.values[0]:.2f} {temp_kelvin.attrs['units']}")
print()

# Calculate mean with both inputs
mean_from_c = atmos.tg_mean(temp_celsius, freq='YS')
mean_from_k = atmos.tg_mean(temp_kelvin, freq='YS')

print("tg_mean results:")
print(f"From Celsius input: {mean_from_c.values[0]:.2f} {mean_from_c.attrs.get('units', 'no units')}")
print(f"From Kelvin input: {mean_from_k.values[0]:.2f} {mean_from_k.attrs.get('units', 'no units')}")
print()

# Check if they're the same
if abs(mean_from_c.values[0] - mean_from_k.values[0]) < 0.01:
    print("✓ xclim correctly handles different input units!")
else:
    print("✗ xclim produces different results for different units")
    print(f"  Difference: {abs(mean_from_c.values[0] - mean_from_k.values[0]):.2f}")
# Issue #003: Daily temperature range showing impossible negative values

## Problem Description
The `daily_temperature_range` and `daily_temperature_range_variability` indices are showing impossible negative values around -260°C to -270°C, suggesting these are Kelvin values that were incorrectly processed or had 273.15 subtracted twice.

## Current Behavior
```
QA/QC Output:
daily_temperature_range:             Min: -271.5°C, Max: -249.6°C, Mean: -260.2°C
daily_temperature_range_variability: Min: -272.8°C, Max: -268.0°C, Mean: -270.3°C
```

## Expected Behavior
```
daily_temperature_range:             Min: 2.0°C, Max: 25.0°C, Mean: 12.0°C
daily_temperature_range_variability: Min: 0.5°C, Max: 8.0°C, Mean: 2.5°C
```

Temperature ranges should ALWAYS be positive (it's the difference between max and min temperatures).

## Impact
- ❌ Physically impossible values make indices unusable
- ❌ Indicates fundamental error in calculation or unit conversion
- ❌ Affects derived statistics and any analysis using these indices
- ❌ May indicate broader issues with temperature handling

## Root Cause Analysis

### Hypothesis 1: Double Conversion from Kelvin
```python
# Possible error flow:
# 1. Data in Kelvin: tmax=293K, tmin=283K
# 2. Range calculated: 293-283 = 10K (correct)
# 3. Incorrectly converted: 10K - 273.15 = -263.15°C (WRONG!)
# Should just be: 10K = 10°C (for temperature differences)
```

### Hypothesis 2: Order of Operations Error
```python
# Possible error:
# Converting before calculating range
tmax_C = tmax_K - 273.15  # OK
tmin_C = tmin_K - 273.15  # OK
range = tmin_C - tmax_C   # WRONG ORDER! Should be tmax_C - tmin_C
```

### Hypothesis 3: xclim Return Type Issue
The xclim library might be returning the range in unexpected units or format.

## Code Investigation Needed

Check in `src/indices_calculator.py`:
```python
# Look for daily_temperature_range calculation
# Should be something like:
dtr = tasmax - tasmin  # Must ensure tasmax > tasmin

# Temperature differences don't need unit conversion
# 10K difference = 10°C difference
```

## Proposed Solution

### Fix in indices_calculator.py:
```python
def calculate_temperature_indices(self, ds):
    # ...

    # Daily temperature range
    if 'daily_temperature_range' in configured_indices:
        try:
            # Ensure we're using max - min (not min - max)
            dtr = atmos.daily_temperature_range(tasmin, tasmax, freq='YS')

            # Check if result is negative (impossible)
            if dtr.min() < 0:
                logger.warning("Negative DTR detected, taking absolute value")
                dtr = abs(dtr)

            # Ensure units are handled correctly
            # Temperature DIFFERENCES don't need K->C conversion
            if 'K' in str(dtr.units):
                dtr.attrs['units'] = 'degC'  # K difference = C difference

            indices['daily_temperature_range'] = dtr
        except Exception as e:
            logger.error(f"Error calculating DTR: {e}")
```

## Validation Tests
```python
# Test cases
assert all(dtr >= 0), "Temperature range cannot be negative"
assert all(dtr <= 50), "Daily range >50°C is unrealistic"
assert dtr.mean() > 5 and dtr.mean() < 20, "Mean DTR should be 5-20°C"
```

## Files to Modify
- `src/indices_calculator.py` - Fix temperature range calculations
- Add specific unit tests for temperature range indices

## Priority
**HIGH** - These are fundamental climate indices that should never be negative. This error suggests a serious calculation bug.

## Related Indices to Check
- Any index involving temperature differences
- Diurnal temperature indices
- Temperature variability metrics

## Scientific Context
- Daily Temperature Range (DTR) = Daily Maximum - Daily Minimum
- Always positive by definition
- Typical ranges: 5-15°C in humid areas, 10-25°C in arid areas
- Important for agriculture, human health studies

## References
- WMO guidelines on DTR calculation
- xclim.indicators.atmos.daily_temperature_range documentation
- Physical constraints on temperature measurements
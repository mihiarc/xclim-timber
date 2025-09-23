# Issue #001: Climate indices returning timedelta64 instead of numeric values

## Problem Description
Several climate indices that should return simple day counts are instead returning `timedelta64[ns]` data types, causing massive numeric values (e.g., 20044800000000000.0 nanoseconds) and breaking downstream analysis.

## Affected Indices
- `consecutive_frost_days`
- `frost_days`
- `ice_days`
- `tropical_nights`
- `gsl` (growing season length)

## Current Behavior
```python
# Example output
consecutive_frost_days: timedelta64[ns]
Value: 20044800000000000.0  # This is nanoseconds, not days!
```

## Expected Behavior
```python
# Should be
consecutive_frost_days: float32 or int
Value: 232.0  # Simple day count
```

## Root Cause
The xclim library returns these indices as timedelta objects to preserve temporal precision, but our pipeline doesn't convert them to numeric days before saving.

## Impact
- ❌ QA/QC validation fails with unrealistic values
- ❌ Statistical analysis produces incorrect results
- ❌ Visualization tools cannot properly display the data
- ❌ 5 out of 13 indices are unusable

## Reproduction Steps
1. Run pipeline: `python scripts/run_comprehensive_indices.py --yes`
2. Check output: `xr.open_dataset('outputs/comprehensive_2001_2024/combined_indices.nc')`
3. Observe data types: `ds['frost_days'].dtype` returns `timedelta64[ns]`

## Proposed Solution

### Option 1: Fix in indices_calculator.py (Recommended)
```python
# In calculate_temperature_indices() method
if 'frost_days' in configured_indices:
    result = atmos.frost_days(tasmin, freq='YS')
    # Convert timedelta to days
    if result.dtype == 'timedelta64[ns]':
        indices['frost_days'] = result / np.timedelta64(1, 'D')
    else:
        indices['frost_days'] = result
```

### Option 2: Post-processing fix
Create a repair script to fix existing files by converting timedelta columns to numeric days.

## Files to Modify
- `src/indices_calculator.py` (lines where day-count indices are calculated)
- Specifically methods:
  - `calculate_temperature_indices()`
  - `calculate_extreme_indices()`

## Testing Required
- Verify all day-count indices return numeric types
- Confirm values are in reasonable ranges (0-365 days)
- Ensure no loss of data during conversion

## Priority
**HIGH** - This affects 38% of calculated indices and makes them unusable for analysis.

## Related Issues
- May be related to xclim version compatibility
- Could affect other pipelines using the same calculator

## References
- xclim documentation on return types: https://xclim.readthedocs.io/en/stable/
- NumPy timedelta64: https://numpy.org/doc/stable/reference/arrays.datetime.html
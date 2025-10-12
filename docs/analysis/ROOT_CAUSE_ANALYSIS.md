# Temperature Pipeline Data Corruption - Root Cause Analysis

**Date:** October 11, 2025
**Analyzed by:** Claude Code
**Branch:** fix/critical-bugs-temperature-pipeline

## Executive Summary

The temperature pipeline data corruption affecting ALL 44 years (1981-2024) is caused by **xarray's automatic timedelta encoding** when writing NetCDF files, NOT by xclim version differences.

**Critical Finding:** xclim 0.56.0 (current) and 0.58.1 (latest) return `float64` with `units='days'` correctly, but xarray interprets this as a timedelta when writing to NetCDF, converting values to `timedelta64[ns]`.

## Symptoms

### Production Data Issues
- 15 count indices have `units='nanoseconds'` in NetCDF metadata
- Dtype: `timedelta64[ns]` instead of `float64` or `int64`
- Values: `-9223372036854775808` (NaT - Not a Time) instead of [0-366]
- **Impact:** ALL 44 years (1981-2024) affected

### Affected Indices
1. summer_days
2. hot_days
3. ice_days
4. frost_days
5. tropical_nights
6. consecutive_frost_days
7. frost_season_length
8. frost_free_season_length
9. tx90p (warm days)
10. tx10p (cool days)
11. tn90p (warm nights)
12. tn10p (cool nights)
13. warm_spell_duration_index
14. cold_spell_duration_index
15. heat_wave_index

## Root Cause Analysis

### Test Results

**xclim Calculation (CORRECT):**
```python
result = atmos.tx_days_above(ds.tasmax, thresh='25 degC', freq='YS')
# Type: xarray.DataArray
# Dtype: float64
# Units: 'days'
# Values: [90, 92, 103, 88, ...] ✓
```

**After NetCDF Write (CORRUPTED):**
```python
ds_saved = xr.open_dataset('output.nc')
# Type: xarray.DataArray
# Dtype: timedelta64[ns]
# Units: 'nanoseconds' (from NetCDF metadata)
# Values: [NaT, NaT, NaT, ...] ✗
```

### The Problem

When xarray writes a DataArray with `units='days'` to NetCDF:

1. xclim returns float64 array with `units='days'` attribute
2. xarray.to_netcdf() interprets 'days' as a CF timedelta unit
3. xarray converts float64 → timedelta64[ns] during encoding
4. NetCDF file stores as int64 with units='nanoseconds'
5. On read, xarray decodes as timedelta64[ns] with all NaT values

**This is a CF conventions misinterpretation!** 'days' as a unit for counts should NOT be treated as timedelta.

## Version Information

| Software | Current | Latest | Notes |
|----------|---------|--------|-------|
| xclim | 0.56.0 | 0.58.1 | Both return float64 correctly |
| xarray | Unknown | Unknown | Needs investigation |
| Python | 3.12 | 3.13 | Compatible |

**Key Discovery:** The bug exists in current xclim 0.56.0 - it's NOT a legacy version issue!

## Solution Options

### Option 1: Modify Units Before Writing (RECOMMENDED)
```python
# After calculating indices, change units to prevent timedelta encoding
for idx_name in COUNT_INDICES:
    if idx_name in ds.data_vars:
        # Change 'days' to something xarray won't interpret as timedelta
        ds[idx_name].attrs['units'] = '1'  # dimensionless
        ds[idx_name].attrs['long_name'] = f"{ds[idx_name].attrs.get('long_name', '')} (days)"
```

**Pros:**
- Fixes root cause
- No data loss
- CF-compliant (dimensionless counts)

**Cons:**
- Requires code changes
- Need to update all count indices

### Option 2: Decode Timedelta=False During Read
```python
ds = xr.open_dataset('file.nc', decode_timedelta=False)
```

**Pros:**
- No pipeline changes needed
- Can read existing files

**Cons:**
- Doesn't fix source data
- Must remember to use flag everywhere
- Still writes corrupted files

### Option 3: Convert to Integer Before Writing
```python
for idx_name in COUNT_INDICES:
    if idx_name in ds.data_vars:
        # Convert float64 to int32
        ds[idx_name] = ds[idx_name].astype('int32')
        ds[idx_name].attrs['units'] = 'days'
```

**Pros:**
- More efficient storage
- Clearer intent (counts are integers)

**Cons:**
- May still be interpreted as timedelta by xarray
- Need to handle NaN values

## Recommended Fix

**Combine Options 1 & 3:**

```python
# In temperature_pipeline.py, after calculating all indices:

COUNT_INDICES = [
    'summer_days', 'hot_days', 'ice_days', 'frost_days',
    'tropical_nights', 'consecutive_frost_days',
    'frost_season_length', 'frost_free_season_length',
    'tx90p', 'tx10p', 'tn90p', 'tn10p',
    'warm_spell_duration_index', 'cold_spell_duration_index',
    'heat_wave_index'
]

def fix_count_indices(ds: xr.Dataset) -> xr.Dataset:
    """
    Fix count indices to prevent timedelta encoding.

    Converts float64 to int32 and sets units to '1' (dimensionless)
    to prevent xarray from interpreting 'days' as timedelta.
    """
    for idx_name in COUNT_INDICES:
        if idx_name in ds.data_vars:
            # Convert to integer (counts are whole numbers)
            ds[idx_name] = ds[idx_name].fillna(-999).astype('int32')

            # Set as dimensionless to avoid timedelta encoding
            original_units = ds[idx_name].attrs.get('units', 'days')
            ds[idx_name].attrs['units'] = '1'  # dimensionless
            ds[idx_name].attrs['original_units'] = original_units
            ds[idx_name].attrs['comment'] = f'Count of days (stored as dimensionless to avoid timedelta encoding)'

            # Use -999 as fill value instead of NaN
            ds[idx_name].attrs['_FillValue'] = -999

    return ds

# Apply before saving:
indices_ds = fix_count_indices(indices_ds)
indices_ds.to_netcdf(output_file, ...)
```

## Testing Requirements

### Unit Test
```python
def test_count_indices_no_timedelta():
    """Verify count indices don't become timedeltas."""
    ds = calculate_temperature_indices(test_data)
    ds = fix_count_indices(ds)

    # Save and reload
    ds.to_netcdf('/tmp/test.nc')
    ds_reloaded = xr.open_dataset('/tmp/test.nc')

    for idx in COUNT_INDICES:
        assert ds_reloaded[idx].dtype in [np.int32, np.int64]
        assert ds_reloaded[idx].attrs['units'] == '1'
        assert not np.any(ds_reloaded[idx] == -9223372036854775808)
```

### Integration Test
```bash
# Test with single year
python temperature_pipeline.py --start-year 2024 --end-year 2024 --output-dir outputs/test

# Validate output
python scripts/validate_production_data.py outputs/test --pipeline temperature

# Expected: 0 errors
```

## Reprocessing Plan

**IMPORTANT:** All 44 years must be reprocessed with the fix!

### Phase 1: Verify Fix (30 min)
1. Implement fix in temperature_pipeline.py
2. Test with year 2024
3. Validate output with validation script
4. Commit fix to new branch

### Phase 2: Reprocess All Years (2-3 hours)
```bash
for year in {1981..2024}; do
    python temperature_pipeline.py \
        --start-year $year \
        --end-year $year \
        --output-dir outputs/production_v2/temperature
done
```

### Phase 3: Validation (15 min)
```bash
python scripts/validate_production_data.py \
    outputs/production_v2/temperature \
    --pipeline temperature
```

### Phase 4: Backup & Replace (5 min)
```bash
mv outputs/production/temperature outputs/production/temperature_backup_corrupted
mv outputs/production_v2/temperature outputs/production/temperature
```

## Impact Assessment

### Data Integrity
- ❌ **CRITICAL:** All 44 years currently unusable for count indices
- ❌ **HIGH:** Affects 15 of 35 indices (43%)
- ✅ **NONE:** Temperature stats (mean, min, max) unaffected

### Scientific Impact
- ❌ Frost risk assessments invalid
- ❌ Heat wave analyses incorrect
- ❌ Growing season calculations wrong
- ❌ Percentile-based extremes corrupted

### Timeline
- **Fix implementation:** 1-2 hours
- **Testing:** 30 minutes
- **Reprocessing:** 2-3 hours
- **Total:** 4-6 hours

## References

- xclim documentation: https://xclim.readthedocs.io/
- CF Conventions: http://cfconventions.org/
- xarray timedelta encoding: https://docs.xarray.dev/en/stable/user-guide/io.html
- GitHub Issue #70: Count indices nanoseconds corruption

## Related Files

- `temperature_pipeline.py:229-245` - Basic count indices
- `temperature_pipeline.py:279-293` - Frost season indices
- `temperature_pipeline.py:313-356` - Extreme percentile indices
- `temperature_pipeline.py:509-514` - Heat wave index
- `scripts/validate_production_data.py` - Validation script
- `docs/analysis/CRITICAL_BUGS_SUMMARY.md` - Original analysis

---

## Fix Implementation Status

**Status:** ✅ **SUCCESSFULLY IMPLEMENTED AND TESTED**

**Date Implemented:** October 12, 2025

### Implementation Summary

The fix has been successfully implemented in `temperature_pipeline.py` (v5.1) and validated:

**Files Modified:**
- `temperature_pipeline.py:523-559` - Added `fix_count_indices()` method
- `temperature_pipeline.py:632` - Applied fix before tile NetCDF writes
- `temperature_pipeline.py:683` - Applied fix before merged dataset NetCDF write
- `scripts/validate_production_data.py:140-143` - Updated validation to accept `units='1'`

**Test Results (2024 data):**
```
✅ All validation checks passed (30/30)
✅ Total errors: 0
✅ Total warnings: 0
✅ Count indices: units='1', dtype=float64
✅ Value ranges: 0-366 days (valid)
✅ No NaT corruption detected
```

**Sample Fixed Output:**
```python
# summer_days:
#   units = "1" (dimensionless)
#   dtype = float64
#   comments = "Count of days (dimensionless to avoid CF timedelta encoding). Original units: days"
#   values: 0.0 - 331.0 (valid range)
```

### Next Steps

1. ✅ Fix implemented and tested with 2024 data
2. ⏳ Commit fix to branch `fix/critical-bugs-temperature-pipeline`
3. ⏳ Reprocess all 44 years (1981-2024) with fixed pipeline
4. ⏳ Update GitHub Issue #70 with resolution
5. ⏳ Create PR for review and merging

---

**Conclusion:** The bug was in xarray's NetCDF encoding, not xclim. The fix changes count index units from 'days' to '1' (dimensionless) before writing to prevent timedelta interpretation. Fix successfully tested and validated. All 44 years require reprocessing with the fixed pipeline.

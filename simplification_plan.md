# Simplification Plan - Remove Monthly & Backward Compatibility

## Code to Delete Immediately

### 1. Remove Monthly Processing (indices_calculator.py)
- **Lines 675-686**: SPI monthly calculation
- **Lines 694-697**: Monthly PET calculation
- **Lines 741-747**: SPEI monthly calculation

### 2. Remove Temporal Resampling (preprocessor.py)
- **Lines 377-435**: Entire `resample_temporal()` method
- **Pipeline.py lines 152-155**: Resampling calls

### 3. Remove Complex Unit Handling (indices_calculator.py)
- **Lines 829-861**: `_infer_temperature_units()` method
- **Lines 863-907**: Complex unit conversion logic
- **Lines 909-953**: Output unit conversion

### 4. Delete Entire Files
- `/src/config_noresm.py` - 221 lines of unused model config
- `/scripts/format_csv_example.py` - Example file not needed
- Any legacy extraction scripts

### 5. Simplify Configuration (config.py)
Remove from default configuration:
```python
# DELETE THESE:
'resampling': {
    'temporal': 'D',
    'spatial_resolution': None
},
'dask': {
    'n_workers': 4,
    'threads_per_worker': 1,
    'memory_limit': '2GB'
}
```

## Code to Keep/Enhance

### Annual Indices Only
Focus on these annual calculations:
- Annual mean temperature (tg_mean)
- Frost days (annual count)
- Growing degree days (annual sum)
- Annual precipitation total
- Extreme indices (annual counts)

### Simple Configuration
```yaml
# config_simple.yaml becomes:
data:
  input_path: /path/to/data
  output_path: ./outputs

indices:
  temperature:
    - annual_mean
    - frost_days
    - growing_degree_days

output:
  format: 'csv'  # Direct CSV for parcels
```

## Benefits After Simplification

1. **37% less code** to maintain
2. **No monthly processing** overhead
3. **No backward compatibility** debt
4. **Clearer purpose**: Annual climate indices for parcels
5. **Faster execution**: No unnecessary resampling
6. **Easier to understand**: Single clear workflow

## Implementation Priority

1. **TODAY**: Remove monthly indices (SPI, SPEI, monthly PET)
2. **TODAY**: Delete config_noresm.py
3. **NEXT**: Remove temporal resampling infrastructure
4. **NEXT**: Simplify unit handling to use xclim directly
5. **LATER**: Clean up quality control methods
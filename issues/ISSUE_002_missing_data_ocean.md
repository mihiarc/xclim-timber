# Issue #002: High missing data percentage (44.8%) due to ocean/water areas

## Problem Description
All climate indices show 44.8% missing data because the PRISM dataset includes ocean areas where terrestrial climate indices are not applicable. The pipeline doesn't mask out water bodies, leading to NaN values that affect statistics and analysis.

## Current Behavior
```
Missing data analysis:
✗ consecutive_frost_days: 44.8% missing
✗ frost_days: 44.8% missing
✗ tg_mean: 44.8% missing
... (all 13 indices show same percentage)
```

## Expected Behavior
- Ocean/water areas should be explicitly masked
- Missing data percentage should only reflect actual data gaps over land
- Statistics should be computed only for valid land areas

## Impact
- ⚠️ Misleading QA/QC reports suggesting poor data quality
- ⚠️ Inefficient storage of NaN values for ocean areas
- ⚠️ Biased spatial statistics if ocean areas are included
- ⚠️ Confusion about actual data coverage

## Root Cause Analysis
1. PRISM data covers a rectangular grid including Pacific Ocean areas
2. The spatial extent (621 × 1405 grid points) includes significant ocean coverage
3. No land/ocean mask is applied during processing

## Proposed Solutions

### Solution 1: Apply Land Mask During Processing
```python
# In data_loader.py or preprocessor.py
def apply_land_mask(ds, land_mask_file):
    """Apply land mask to exclude ocean areas."""
    mask = xr.open_dataset(land_mask_file)

    # Set ocean areas to NaN consistently
    for var in ds.data_vars:
        ds[var] = ds[var].where(mask.land == 1)

    # Add mask as coordinate for reference
    ds.coords['land_mask'] = mask.land

    return ds
```

### Solution 2: Use PRISM's Built-in Land Mask
PRISM data may include a land mask variable. Check if available:
```python
# Check for mask in PRISM data
ds = xr.open_zarr('/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature')
if 'mask' in ds.variables or 'land' in ds.variables:
    # Use existing mask
```

### Solution 3: Generate Land Mask from Data
```python
# Create mask based on valid data points
# Areas with >90% missing data across all time steps are likely ocean
def generate_land_mask(ds, threshold=0.9):
    valid_frac = (~ds['tmean'].isnull()).mean(dim='time')
    land_mask = valid_frac > threshold
    return land_mask
```

## Implementation Steps
1. Identify or create appropriate land mask for PRISM domain
2. Add mask loading to pipeline configuration
3. Apply mask in preprocessor before index calculation
4. Update QA/QC to report land-only statistics

## Files to Modify
- `src/preprocessor.py` - Add land masking step
- `src/data_loader.py` - Load land mask if separate file
- `configs/config_comprehensive_2001_2024.yaml` - Add land mask path
- `scripts/qa_qc_indices.py` - Separate land/ocean statistics

## Benefits of Fixing
- ✅ Accurate missing data percentages
- ✅ Smaller file sizes (can use _FillValue efficiently)
- ✅ Clearer QA/QC reports
- ✅ More accurate spatial statistics

## Testing Required
- Verify mask correctly identifies land/ocean boundaries
- Ensure mask aligns with PRISM grid
- Check that indices are only computed over land
- Validate that statistics exclude ocean areas

## Priority
**MEDIUM** - Doesn't affect calculation accuracy but impacts interpretation and storage efficiency.

## Geographic Context
The PRISM domain covers the continental US including:
- Pacific Ocean to the west
- Atlantic Ocean to the east
- Gulf of Mexico to the south
- Great Lakes (may want to keep or mask depending on use case)

## References
- PRISM documentation on spatial coverage
- CF conventions for mask variables
- Best practices for handling ocean areas in terrestrial climate data
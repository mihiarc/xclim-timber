# Issue #007: Run comprehensive QA/QC validation on repaired data

## Problem Description
After fixing timedelta and temperature range issues, we need to run a complete QA/QC validation to ensure all data quality issues have been resolved and no new issues were introduced.

## Validation Checklist

### 1. Data Type Validation
- [ ] All indices return numeric types (no timedelta64)
- [ ] Correct data types (float32 or float64)
- [ ] No unexpected string or object types

### 2. Value Range Validation
- [ ] Temperature indices: -60°C to 60°C
- [ ] Day count indices: 0 to 365/366 days
- [ ] Degree day indices: 0 to 10,000
- [ ] Temperature ranges: Always positive
- [ ] No infinite values

### 3. Missing Data Analysis
- [ ] Document % missing for each index
- [ ] Identify spatial patterns in missing data
- [ ] Separate land vs ocean missing data (once mask applied)

### 4. Temporal Consistency
- [ ] No unrealistic year-to-year jumps
- [ ] Smooth temporal evolution
- [ ] No data gaps between years

### 5. Spatial Coherence
- [ ] No isolated extreme values
- [ ] Smooth spatial gradients
- [ ] Coastline effects reasonable

### 6. Inter-Index Relationships
- [ ] tmin ≤ tmean ≤ tmax
- [ ] frost_days ≥ ice_days
- [ ] Degree days correlate with temperature

## Validation Script Requirements

```python
def comprehensive_qa_qc(dataset_path):
    """Run full QA/QC suite on climate indices."""

    tests = {
        'data_types': check_data_types(),
        'value_ranges': check_value_ranges(),
        'missing_data': analyze_missing_data(),
        'temporal': check_temporal_consistency(),
        'spatial': check_spatial_patterns(),
        'relationships': check_index_relationships()
    }

    # Generate report
    report = generate_qa_report(tests)

    # Flag critical issues
    critical_issues = identify_critical_issues(tests)

    return report, critical_issues
```

## Expected Outcomes

### Before Fixes
- 5/13 indices with timedelta types
- 2 indices with negative values
- 44.8% missing data (including ocean)

### After Fixes
- 13/13 indices with numeric types ✓
- All temperature ranges positive ✓
- Missing data only in ocean areas

## Report Format

```
================================================================================
CLIMATE INDICES QA/QC REPORT - POST REPAIR
================================================================================
Generated: 2025-09-23

SUMMARY
-------
✅ Data Types: PASS - All indices numeric
✅ Value Ranges: PASS - All within physical limits
⚠️ Missing Data: 44.8% (expected due to ocean areas)
✅ Temporal: PASS - No anomalies detected
✅ Spatial: PASS - Patterns physically plausible
✅ Relationships: PASS - All constraints satisfied

RECOMMENDATION: Data ready for scientific analysis
================================================================================
```

## Success Criteria
- All data type tests pass
- All value range tests pass
- No critical issues identified
- Report clearly documents data quality

## Priority
**HIGH** - Must validate fixes before data can be used

## Estimated Effort
1-2 hours

## Dependencies
- Issue #001, #003 fixes must be applied
- Issue #006 (recombination) should be complete

## Testing
- Run on repaired combined dataset
- Compare with original QA/QC results
- Document improvements
# Climate Pipeline Issues Tracker

## Overview
This directory contains detailed issue reports from QA/QC analysis of the climate indices pipeline output. These issues were identified during quality control testing on 2025-09-23.

## Critical Issues (Priority: HIGH)

### 🔴 [ISSUE_001](./ISSUE_001_timedelta_data_type.md): Timedelta Data Type Problem
**Impact**: 5 of 13 indices unusable
**Problem**: Day-count indices returning timedelta64[ns] instead of numeric values
**Symptoms**: Values like 20044800000000000.0 instead of reasonable day counts (0-365)
**Affected Indices**: frost_days, ice_days, tropical_nights, consecutive_frost_days, gsl

### 🔴 [ISSUE_003](./ISSUE_003_negative_temperature_range.md): Negative Temperature Range Values
**Impact**: 2 indices physically impossible
**Problem**: Daily temperature range showing negative values (-271°C)
**Symptoms**: Temperature differences incorrectly processed, possible Kelvin conversion error
**Affected Indices**: daily_temperature_range, daily_temperature_range_variability

### 🔴 [ISSUE_005](./ISSUE_005_unit_conversion_chain.md): Unit Conversion Chain Errors
**Impact**: Systematic errors across multiple indices
**Problem**: Inconsistent unit handling throughout processing pipeline
**Symptoms**: Mixed units in output, incorrect conversions, incompatible data types
**Affected Components**: data_loader.py, preprocessor.py, indices_calculator.py

## Important Issues (Priority: MEDIUM)

### 🟡 [ISSUE_002](./ISSUE_002_missing_data_ocean.md): High Missing Data from Ocean Areas
**Impact**: 44.8% missing data reported
**Problem**: Ocean areas included in grid but not masked
**Symptoms**: Misleading QA/QC statistics, larger file sizes
**Solution**: Implement land masking

### 🟡 [ISSUE_004](./ISSUE_004_land_mask_implementation.md): Land Mask Implementation (Feature)
**Impact**: Data quality and storage improvement
**Problem**: No separation between land and ocean in processing
**Benefits**: Accurate statistics, 45% file size reduction, clearer analysis
**Type**: Enhancement request

## Quick Status Summary

| Issue | Type | Priority | Status | Est. Effort |
|-------|------|----------|--------|------------|
| 001 | Bug | HIGH | Open | 2-4 hours |
| 002 | Bug | MEDIUM | Open | 2-3 hours |
| 003 | Bug | HIGH | Open | 1-2 hours |
| 004 | Feature | MEDIUM | Open | 4-6 hours |
| 005 | Bug | HIGH | Open | 3-4 hours |

## Resolution Order

Recommended order for fixing issues:

1. **First**: Fix ISSUE_005 (unit conversion) - This may resolve other issues
2. **Second**: Fix ISSUE_001 (timedelta types) - Critical for usability
3. **Third**: Fix ISSUE_003 (negative ranges) - May be fixed by #005
4. **Fourth**: Implement ISSUE_004 (land masking) - Improves overall quality
5. **Last**: Verify ISSUE_002 resolved by land masking

## Testing After Fixes

After implementing fixes, run:

```bash
# Re-process one year to test
python scripts/run_comprehensive_indices.py --chunk-years 1 --yes

# Run QA/QC
python scripts/qa_qc_indices.py

# Expected outcomes:
# - All indices should be numeric (no timedelta)
# - Temperature ranges should be positive
# - Missing data should be <5% over land areas
# - All values in physically plausible ranges
```

## Root Cause Summary

Most issues stem from:
1. **xclim library return types** not being handled properly
2. **Unit metadata mismatches** between PRISM and xclim
3. **Lack of data validation** in the processing chain
4. **No land/ocean separation** in the spatial domain

## Prevention

To prevent similar issues:
1. Add unit tests for each index calculation
2. Implement data validation at each pipeline stage
3. Add type checking for index outputs
4. Include QA/QC as part of the pipeline
5. Document expected ranges for all indices

## Contact

For questions about these issues, refer to the main project documentation in `/home/mihiarc/xclim-timber/CLAUDE.md`
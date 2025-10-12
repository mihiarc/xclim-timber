# Temperature Dataset Inconsistency Analysis

**Analysis Date**: 2025-10-11 11:40:58

**Dataset Location**: `outputs/production/temperature/`

**Files Analyzed**: 8 files spanning 1981-2024

## Executive Summary

### 🚨 CRITICAL DATA TYPE ERROR DETECTED

**Multiple count-based indices have incorrect units (nanoseconds instead of days)**

This is a fundamental pipeline error that affects data interpretation:
- Variables like `hot_days`, `summer_days`, `frost_days`, etc. are stored with 'nanoseconds' units
- This makes the data values incorrect by a factor of ~8.64×10¹³
- Logical relationship validation between indices is impossible
- **IMMEDIATE ACTION REQUIRED**: Fix xclim processing pipeline unit handling

⚠️ **CRITICAL**: 222 critical issues found that require immediate attention.

Total issues identified: **474**
- Critical: 222
- High: 61
- Medium: 191
- Low: 0

## Key Findings

### Structural Issues (2 issues)

- Dimension mismatch in 2003
- 2003: Grid shape mismatch

### Unit/Data Type Errors (150 issues)

- 2003/hot_days: Incorrect units - 'nanoseconds' instead of 'days'
- 2003/summer_days: Incorrect units - 'nanoseconds' instead of 'days'
- 2003/frost_days: Incorrect units - 'nanoseconds' instead of 'days'
- ... and 147 more

### Invalid Values (70 issues)

- 2003/summer_days: Negative count values
- 2003/hot_days: Negative count values
- 2003/ice_days: Negative count values
- ... and 67 more

## Detailed Findings by Severity

### Critical Issues (222)

**Dimension mismatch in 2003**
```json
{
  "reference": {
    "lat": 621,
    "lon": 1405
  },
  "current": {
    "lat": 1242,
    "lon": 1405
  }
}
```

**hot_days** (15 occurrences)
```json
{
  "variable": "hot_days",
  "incorrect_units": "nanoseconds",
  "expected_units": "days"
}
```

**summer_days** (15 occurrences)
```json
{
  "variable": "summer_days",
  "incorrect_units": "nanoseconds",
  "expected_units": "days"
}
```

**frost_days** (15 occurrences)
```json
{
  "variable": "frost_days",
  "incorrect_units": "nanoseconds",
  "expected_units": "days"
}
```

**ice_days** (15 occurrences)
```json
{
  "variable": "ice_days",
  "incorrect_units": "nanoseconds",
  "expected_units": "days"
}
```

**tropical_nights** (15 occurrences)
```json
{
  "variable": "tropical_nights",
  "incorrect_units": "nanoseconds",
  "expected_units": "days"
}
```

**consecutive_frost_days** (15 occurrences)
```json
{
  "variable": "consecutive_frost_days",
  "incorrect_units": "nanoseconds",
  "expected_units": "days"
}
```

**frost_season_length** (10 occurrences)
```json
{
  "variable": "frost_season_length",
  "incorrect_units": "nanoseconds",
  "expected_units": "days"
}
```

**frost_free_season_length** (15 occurrences)
```json
{
  "variable": "frost_free_season_length",
  "incorrect_units": "nanoseconds",
  "expected_units": "days"
}
```

**tx90p** (15 occurrences)
```json
{
  "variable": "tx90p",
  "incorrect_units": "nanoseconds",
  "expected_units": "days"
}
```

### High Issues (61)

**Irregular latitude spacing in 2003**
```json
{
  "max_diff": 25.87500000000294
}
```

**Irregular latitude spacing in 2005**
```json
{
  "max_diff": 25.87500000000294
}
```

**35 variables with >30% NaN** (3 occurrences)

**tg_mean** (9 occurrences)

**tx_max** (9 occurrences)

**tn_min** (9 occurrences)

**2003/growing_degree_days: High NaN percentage (72.4%)**

**2003/heating_degree_days: High NaN percentage (72.4%)**

**2003/cooling_degree_days: High NaN percentage (72.4%)**

**2003/freezing_degree_days: High NaN percentage (72.4%)**

### Medium Issues (191)

**tg_mean** (7 occurrences)

**tx_max** (7 occurrences)

**summer_days** (3 occurrences)

**hot_days** (3 occurrences)

**ice_days** (3 occurrences)

**tn_min** (7 occurrences)

**frost_days** (3 occurrences)

**tropical_nights** (3 occurrences)

**consecutive_frost_days** (3 occurrences)

**growing_degree_days** (7 occurrences)

## Analysis Coverage

### Checks Performed

1. **Metadata Consistency**
   - ✓ Dimension alignment across years
   - ✓ Variable presence and naming
   - ✓ Coordinate system consistency
   - ✓ Unit validation for all indices

2. **Data Quality**
   - ✓ NaN coverage analysis
   - ✓ Tile artifact detection
   - ✓ Spatial pattern analysis

3. **Physical Constraints**
   - ✓ Temperature relationships (tg between tn and tx)
   - ✓ Count indices bounds (0-366 days)
   - ✓ Degree-day non-negativity

4. **Statistical Validation**
   - ✓ Outlier detection (3*IQR method)
   - ✓ Temporal consistency between years
   - ✓ Value range validation

## Recommendations

### 🔴 IMMEDIATE ACTION REQUIRED

1. **Fix Data Type/Units Issue in Pipeline**
   - Review xclim function outputs for count-based indices
   - Ensure proper conversion from timedelta to integer days
   - Verify units are correctly set in NetCDF attributes
   - Re-process ALL years with corrected pipeline

2. **Validation Before Production**
   - Add unit checks in the processing pipeline
   - Implement automated validation for logical relationships
   - Create test cases for edge conditions

### 🟠 High Priority Improvements

- **Data Coverage**: Address high NaN percentages (>50% in multiple variables)
  - Review land/ocean masking strategy
  - Check source data completeness

### 🟡 Medium Priority Optimizations

- Review moderate NaN coverage (30-50%) for potential improvements
- Investigate statistical outliers for data quality issues
- Document expected value ranges for all indices

## Technical Details

### Files Analyzed
- `temperature_indices_1981_1981.nc` - Shape: Frozen({'lat': 621, 'lon': 1405, 'time': 1})
- `temperature_indices_1990_1990.nc` - Shape: Frozen({'lat': 621, 'lon': 1405, 'time': 1})
- `temperature_indices_2000_2000.nc` - Shape: Frozen({'lat': 621, 'lon': 1405, 'time': 1})
- `temperature_indices_2003_2003.nc` - Shape: Frozen({'time': 1, 'lat': 1242, 'lon': 1405})
- `temperature_indices_2005_2005.nc` - Shape: Frozen({'time': 1, 'lat': 621, 'lon': 1405})
- `temperature_indices_2010_2010.nc` - Shape: Frozen({'time': 1, 'lat': 621, 'lon': 1405})
- `temperature_indices_2020_2020.nc` - Shape: Frozen({'time': 1, 'lat': 621, 'lon': 1405})
- `temperature_indices_2024_2024.nc` - Shape: Frozen({'time': 1, 'lat': 621, 'lon': 1405})

### Critical Variables Requiring Attention

Variables with incorrect units (nanoseconds instead of days):
- `cold_spell_duration_index`
- `consecutive_frost_days`
- `frost_days`
- `frost_free_season_length`
- `frost_season_length`
- `heat_wave_index`
- `hot_days`
- `ice_days`
- `summer_days`
- `tn10p`
- `tn90p`
- `tropical_nights`
- `tx10p`
- `tx90p`
- `warm_spell_duration_index`

### Analysis Timestamp
2025-10-11 11:40:58 UTC

# Scientific Review Report: xclim-timber Climate Indices Implementation

## Executive Summary

A critical scientific review of the climate indices implementation in `xclim_timber.py` (lines 150-350) reveals **12 significant issues**, including **6 critical errors** that produce scientifically incorrect results. These errors must be corrected before using the tool for economic analysis.

## Critical Errors Found

### 1. ❌ **Corn Growing Degree Days (Line 286) - INCORRECT FORMULA**

**Current Implementation:**
```python
corn_gdd = np.minimum(np.maximum(data - 10, 0), 20)  # Cap at 30-10=20
```

**Issue:** The code caps the degree-days at 20, not the temperature at 30°C.

**Correct Formula:**
```python
corn_gdd = np.maximum(np.minimum(data, 30) - 10, 0)
```

**Impact:** Severely underestimates corn GDD, affecting agricultural productivity models.

### 2. ❌ **Bioclimatic Variables (Lines 330-346) - NOT FOLLOWING BIOCLIM STANDARDS**

**Current Implementation:**
- Uses "warmest 30 days" instead of warmest month
- Mixes days from different months

**Issues:**
- BIO5 should be "Max Temperature of Warmest Month" (not mean of 30 warmest days)
- BIO6 should be "Min Temperature of Coldest Month" (not mean of 30 coldest days)
- BIO4 uses daily standard deviation instead of monthly means

**Impact:** Results are not comparable with standard BIOCLIM datasets used in species distribution modeling.

### 3. ❌ **Freeze-Thaw Cycles (Lines 324-328) - LOGICALLY FLAWED**

**Current Implementation:**
```python
transitions = np.diff(np.sign(data[i]))
freeze_thaw[i] = np.sum(np.abs(transitions) > 0) / 2
```

**Issues:**
- Counts ANY sign change, not just crossings of 0°C
- Incorrect division by 2

**Impact:** Overestimates freeze-thaw cycles, critical for infrastructure and tree damage assessment.

### 4. ❌ **Temperature Seasonality BIO4 (Line 348) - INCORRECT CALCULATION**

**Current Implementation:**
```python
results['bio4_temp_seasonality'] = np.nanstd(data, axis=1) * 100
```

**Issue:** Should use standard deviation of monthly means, not daily temperatures.

**Impact:** Inflates seasonality measure by 3-5x, misrepresenting climate variability.

### 5. ❌ **Cold/Warm Spell Duration (Lines 311-314) - USES GLOBAL PERCENTILES**

**Current Implementation:**
```python
temp_p10 = np.nanpercentile(data, 10)  # Global 10th percentile
temp_p90 = np.nanpercentile(data, 90)  # Global 90th percentile
```

**Issue:** Uses single global percentiles instead of location-specific baselines.

**Impact:** Misidentifies extreme events, especially problematic across diverse climate zones.

### 6. ❌ **Diurnal Temperature Range (Line 302) - CANNOT BE CALCULATED**

**Current Implementation:**
```python
results['mean_diurnal_range'] = np.nanstd(data, axis=1) * 2  # Approximation
```

**Issue:** DTR requires daily min/max temperatures, not available from daily means.

**Impact:** Produces meaningless values that don't represent actual diurnal variation.

## Minor Issues and Improvements Needed

### 7. ⚠️ **Growing Degree Days (Lines 269-272) - MISSING UPPER CAP**

**Current Implementation:** No upper temperature threshold

**Recommendation:** Add optional upper cap for more accurate crop-specific calculations:
```python
gdd = np.maximum(np.minimum(data, cap_temp) - base_temp, 0)
```

### 8. ⚠️ **Precipitation Wet Days Threshold (Lines 58, 68-69)**

**Issue:** Uses `>=` for counting but `<` and `>` for consecutive days (inconsistent)

**Recommendation:** Standardize to `>=` for wet days (≥1mm) and `<` for dry days (<1mm)

### 9. ⚠️ **NaN Handling in Consecutive Days**

**Issue:** NaN values break consecutive counts implicitly

**Recommendation:** Add explicit NaN handling option

### 10. ⚠️ **Precipitation Unit Detection (Line 207)**

**Current Method:** `if data.mean() < 1`

**Issue:** Crude heuristic that could fail in very dry regions

**Recommendation:** Check metadata or use more robust detection

## Missing Critical Indices for Timber/Forestry

The following essential indices for forestry applications are absent:

1. **Vapor Pressure Deficit (VPD)** - Critical for tree water stress
2. **Potential Evapotranspiration (PET)** - Water balance calculations
3. **Fire Weather Index components** - Forest fire risk
4. **Growing Season Length** - Properly defined start/end
5. **Spring/Fall Frost Dates** - Critical for phenology
6. **Utah Chill Units** - More accurate than simple chill days
7. **Water Stress Index** - Combined temperature-precipitation metric
8. **Drought indices** (SPI, SPEI) - Long-term moisture deficits

## Verification of Correct Implementations

### ✅ **Correctly Implemented:**

1. **Precipitation unit conversion** (kg m⁻² s⁻¹ to mm/day): Factor of 86400 is correct
2. **Basic temperature percentiles**: Properly calculated
3. **Basic statistics**: Mean, min, max, std calculations are correct
4. **Vernalization days**: Temperature range (0-10°C) is appropriate
5. **Simple threshold counts**: Frost days, summer days, etc. are correct

## Recommendations

### Immediate Actions Required:

1. **Fix critical calculation errors** (items 1-6 above)
2. **Implement proper BIOCLIM variables** following WorldClim standards
3. **Add location-specific baseline calculations** for extremes
4. **Remove or flag DTR calculation** as unavailable without min/max data

### Enhancement Priorities:

1. **Add missing forestry indices** (see climate_indices_corrections.py)
2. **Implement proper growing season metrics**
3. **Add water balance indicators**
4. **Include fire weather components**

## Impact Assessment

Using the current implementation for economic analysis would lead to:

- **Biased agricultural productivity estimates** (incorrect GDD)
- **Misrepresented climate extremes** (wrong percentile baselines)
- **Incomparable bioclimatic variables** (non-standard calculations)
- **Overestimated infrastructure stress** (wrong freeze-thaw cycles)
- **Missing critical drought/water stress indicators**

## Validation Data

To validate corrections, compare outputs with:
- **BIOCLIM**: WorldClim 2.1 bioclimatic variables
- **Growing Degree Days**: NOAA Climate Data Online
- **Extremes**: ETCCDI climate indices
- **Agricultural indices**: AgMERRA or AgERA5 datasets

## Conclusion

The implementation contains fundamental scientific errors that must be corrected before use in economic analysis. The provided `climate_indices_corrections.py` file contains scientifically accurate implementations of the corrected formulas and additional timber-specific indices.

**Recommendation:** Do not use the current implementation for research until critical errors are fixed. Use the corrected implementations provided as a reference for updating the codebase.
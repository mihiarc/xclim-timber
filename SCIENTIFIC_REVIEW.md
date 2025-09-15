# Scientific Review of xclim-timber Climate Indices Implementation

## Executive Summary

This review assesses the scientific accuracy of climate indices implementation in the xclim-timber tool. The analysis covers threshold values, calculation methods, xclim function usage, variable handling, and compliance with WMO standards.

## 1. SCIENTIFIC CORRECTNESS OF CLIMATE INDICES

### 1.1 Temperature Threshold Values

#### ✅ Correctly Implemented Thresholds:
- **Tropical nights (>20°C)**: CORRECT - Standard WMO definition
- **Frost days (<0°C)**: CORRECT - Standard definition for minimum temperature below freezing
- **Ice days (<0°C)**: CORRECT - Maximum temperature below freezing
- **Summer days (>25°C)**: CORRECT - Standard European definition
- **Hot days (>30°C)**: CORRECT - Widely accepted threshold
- **Very hot days (>35°C)**: CORRECT - Appropriate for extreme heat

#### ⚠️ Questionable Threshold:
- **Warm nights (>15°C)**: SCIENTIFICALLY QUESTIONABLE
  - **Issue**: 15°C is not a standard WMO threshold for warm nights
  - **Standard**: Warm nights typically defined as TN > 90th percentile (tn90p)
  - **Recommendation**: Consider removing fixed 15°C threshold or rename to "mild nights"

### 1.2 Degree Day Base Temperatures

#### ✅ Correctly Implemented:
- **Growing degree days (10°C base)**: CORRECT - Standard agricultural base temperature
- **Heating degree days (17°C base)**: ACCEPTABLE - Common in some regions (though 18°C or 65°F more common)
- **Cooling degree days (18°C base)**: CORRECT - Standard base temperature

### 1.3 Humidity Calculations

#### ❌ Critical Error in Dewpoint Calculation:
**Line 510-512**: The code incorrectly uses `atmos.relative_humidity_from_dewpoint()` to calculate dewpoint
```python
indices['dewpoint_temperature'] = atmos.relative_humidity_from_dewpoint(
    hus, ps, method='sonntag90'
)
```
- **Issue**: This function calculates relative humidity FROM dewpoint, not dewpoint itself
- **Correct approach**: Should use `atmos.dewpoint_from_specific_humidity()` or similar

#### ✅ Correct Implementation:
- **Relative humidity from specific humidity**: CORRECT methodology (lines 525-528)
- **Heat index calculation**: CORRECT - properly uses temperature and relative humidity
- **Humidex calculation**: CORRECT - Canadian standard implementation

### 1.4 Precipitation Thresholds

#### ✅ All Correctly Implemented:
- **Heavy precipitation (≥10mm)**: CORRECT - Standard R10mm definition
- **Very heavy precipitation (≥20mm)**: CORRECT - Standard R20mm definition
- **Very wet days (95th percentile)**: CORRECT - Standard R95p definition
- **Extremely wet days (99th percentile)**: CORRECT - Standard R99p definition
- **Consecutive dry days (<1mm)**: CORRECT - Standard CDD definition
- **Consecutive wet days (≥1mm)**: CORRECT - Standard CWD definition

## 2. XCLIM USAGE ANALYSIS

### 2.1 Frequency Parameters

#### ✅ Appropriate Usage:
- **Annual frequency ('YS')**: Correctly used for most indices
- **Monthly frequency ('MS')**: Correctly used for SPI and SPEI (lines 609, 625, 675)
- **Daily frequency ('D')**: Correctly used for PET calculation before SPEI (line 667)

### 2.2 Percentile Calculations

#### ⚠️ Methodological Issue:
**Lines 273-276, 286-289, etc.**: Percentiles calculated on entire time series
```python
tasmax_per=tasmax.quantile(0.9, dim='time')
```
- **Issue**: Using fixed percentiles from entire dataset rather than baseline period
- **Standard**: WMO recommends using 1961-1990 or 1981-2010 baseline
- **Impact**: May underestimate extremes in warming climate
- **Recommendation**: Add baseline period configuration option

### 2.3 SPEI Implementation

#### ⚠️ Simplified Implementation:
**Lines 666-678**: SPEI calculation uses Thornthwaite PET
- **Issue**: Thornthwaite method only uses temperature, ignoring radiation, wind, humidity
- **Standard**: FAO-56 Penman-Monteith preferred for PET
- **Impact**: May underestimate PET in arid regions
- **Acceptable for**: Data-limited situations

## 3. VARIABLE HANDLING

### 3.1 Unit Conversions

#### ⚠️ Missing Explicit Unit Conversion:
- **Temperature units**: Code assumes correct units but doesn't explicitly convert
- **Lines 741-751**: Only validates range, doesn't convert Kelvin to Celsius
- **Risk**: xclim functions expect specific units (typically Kelvin for temperature)
- **Recommendation**: Add explicit unit conversion using `convert_units_to()`

### 3.2 Variable Range Validation

#### ✅ Good Practice:
- Temperature range validation (lines 741-751): Appropriate checks
- Precipitation non-negative check (line 756): Correct
- Relative humidity 0-100% check (line 765): Correct

#### ⚠️ Questionable Range:
- **Specific humidity range (0-0.1 kg/kg)**:
  - Line 768: Upper limit of 0.1 kg/kg is too high
  - **Typical range**: 0-0.03 kg/kg (up to 0.04 in extreme tropical conditions)
  - **Recommendation**: Change warning threshold to 0.04 kg/kg

### 3.3 Missing Data Handling

#### ✅ Good Implementation:
- Missing data fraction reporting (lines 773-777)
- Warning when >50% missing data
- Info message when >10% missing data

## 4. MULTIVARIATE INDICES

### 4.1 Temperature-Precipitation Combinations

#### ✅ Scientifically Sound:
- **Cold and dry days**: 10th percentile thresholds appropriate
- **Warm and wet days**: 90th percentile thresholds appropriate
- **Methodology**: Percentile-based approach is standard

#### ⚠️ Missing Context:
- No consideration of seasonal variations
- No lag effects between temperature and precipitation
- Could benefit from compound event metrics

### 4.2 Evapotranspiration

#### ⚠️ Limited Implementation:
- **Thornthwaite method**: Temperature-only approach
- **Missing**: Hargreaves, Priestley-Taylor, or Penman-Monteith options
- **Impact**: May not capture water stress accurately in all climates

## 5. WMO STANDARDS COMPLIANCE

### 5.1 Index Definitions

#### ✅ WMO-Compliant Indices:
- TX90p, TN90p, TX10p, TN10p
- WSDI, CSDI
- R10mm, R20mm, R95p, R99p
- CDD, CWD
- Rx1day, Rx5day
- SDII, PRCPTOT

#### ⚠️ Non-Standard Implementations:
- "Warm nights" with fixed 15°C threshold (should be percentile-based)
- Missing baseline period specification for percentile indices

### 5.2 Naming Conventions

#### ✅ Generally Correct:
- Most indices use standard WMO/ETCCDI abbreviations
- Clear distinction between similar indices (hot_days vs very_hot_days)

## 6. CRITICAL ISSUES TO ADDRESS

### 🔴 High Priority:
1. **Fix dewpoint calculation** (line 510-512) - Wrong function being used
2. **Add explicit unit conversions** - Ensure all temperature data in correct units
3. **Fix specific humidity range check** - Upper limit too high (0.1 → 0.04)

### 🟡 Medium Priority:
1. **Add baseline period configuration** for percentile calculations
2. **Reconsider "warm nights" threshold** or rename the index
3. **Document unit expectations** clearly in code

### 🟢 Low Priority (Enhancements):
1. Add more PET methods beyond Thornthwaite
2. Implement seasonal percentile calculations
3. Add compound event indices

## 7. DOCUMENTATION ACCURACY

### README.md Review:
- **Line 145**: States "84 comprehensive climate indices" but implementation shows fewer
- **Line 227**: Lists "reference_evapotranspiration" as FAO-56 but not implemented
- **Line 181**: Growing degree days description accurate
- **Line 258**: SPEI description notes "requires additional data" - correctly implemented

## 8. RECOMMENDATIONS

### Immediate Actions:
1. Fix the dewpoint temperature calculation function
2. Add unit conversion checks and transformations
3. Adjust specific humidity validation range
4. Update documentation to match actual implementation

### Future Improvements:
1. Implement configurable baseline periods for percentile indices
2. Add more sophisticated PET calculation methods
3. Consider adding seasonal analysis capabilities
4. Implement proper FAO-56 reference ET if data available

## 9. OVERALL ASSESSMENT

**Strengths:**
- Comprehensive index coverage
- Good error handling and logging
- Flexible variable name recognition
- Proper use of xclim library functions for most indices

**Weaknesses:**
- Critical error in dewpoint calculation
- Missing explicit unit conversions
- Simplified PET calculation
- Fixed percentile baselines

**Scientific Validity Score: 7.5/10**
- Most implementations are scientifically sound
- Critical fixes needed for dewpoint and units
- Would benefit from enhanced PET methods and baseline period configuration

## 10. CODE QUALITY OBSERVATIONS

**Positive Aspects:**
- Well-structured and modular code
- Comprehensive error handling
- Good logging implementation
- Clear separation of concerns

**Areas for Improvement:**
- Add type hints for better code documentation
- Consider configuration validation for scientific parameters
- Add unit tests for critical calculations

---

*Review conducted: 2025-09-15*
*Reviewer: Climate Science Expert with xclim expertise*
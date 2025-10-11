# Climate Indices Data Dictionary
## xclim-timber: Comprehensive Climate Metrics Reference Guide

> **⚠️ PARTIALLY OUTDATED - Index counts need updating**
> **Current status:** 80/80 indices implemented (100% complete)
> **For authoritative index catalog:** See [ACHIEVABLE_INDICES_ROADMAP.md](ACHIEVABLE_INDICES_ROADMAP.md)
> **This dictionary structure is good but data is incomplete - update in progress**

This document provides a complete reference for all climate indices calculated by the xclim-timber pipeline, organized by category following World Meteorological Organization (WMO) standards.

**~~Total Indices: 42+ core indices~~ ACTUAL: 80 indices** across ~~8~~ **7 categories** (COMPLETE)

## Quick Reference

### Index Categories Summary
- **Temperature Indices**: 17 indices for temperature patterns and extremes
- **Precipitation Indices**: 10 indices for rainfall patterns and intensity
- **Extreme Weather Indices**: 6 percentile-based extremes (tx90p, tn90p, tx10p, tn10p, WSDI, CSDI)
- **Humidity Indices**: 2 indices for moisture conditions
- **Human Comfort Indices**: 2 indices for heat stress assessment
- **Evapotranspiration Indices**: 3 indices for water balance
- **Multivariate Indices**: 4 combined temperature-precipitation events
- **Agricultural Indices**: 3 specialized farming/crop indices

### Most Commonly Used Indices
1. **Temperature**: `tg_mean`, `frost_days`, `summer_days`, `growing_degree_days`
2. **Precipitation**: `prcptot`, `rx1day`, `cdd`, `cwd`
3. **Extremes**: `tx90p` (warm days), `tn10p` (cool nights)
4. **Agriculture**: `gsl` (growing season length), `spi_3` (drought index)

---

## Table of Contents
1. [Temperature Indices (17 indices)](#temperature-indices)
2. [Precipitation Indices (10 indices)](#precipitation-indices)
3. [Extreme Weather Indices (6 indices)](#extreme-weather-indices)
4. [Humidity Indices (2 indices)](#humidity-indices)
5. [Human Comfort Indices (2 indices)](#human-comfort-indices)
6. [Evapotranspiration Indices (3 indices)](#evapotranspiration-indices)
7. [Multivariate Indices (4 indices)](#multivariate-indices)
8. [Agricultural Indices (3 indices)](#agricultural-indices)
9. [Input Variables Reference](#input-variables-reference)

---

## Temperature Indices

### Basic Temperature Statistics

| Index ID | Index Name | Description | Units | Formula/Threshold | WMO Standard |
|----------|------------|-------------|-------|-------------------|--------------|
| `tg_mean` | Annual Mean Temperature | Average of daily mean temperature over the year | °C | Σ(tas)/n | Yes |
| `tx_max` | Annual Maximum Temperature | Highest daily maximum temperature in the year | °C | max(tasmax) | Yes |
| `tn_min` | Annual Minimum Temperature | Lowest daily minimum temperature in the year | °C | min(tasmin) | Yes |
| `daily_temperature_range` | Mean Daily Temperature Range | Average difference between daily maximum and minimum | °C | mean(tasmax - tasmin) | Yes |
| `daily_temperature_range_variability` | Temperature Range Variability | Standard deviation of daily temperature range | °C | std(tasmax - tasmin) | Yes |

### Threshold-Based Temperature Counts

| Index ID | Index Name | Description | Units | Threshold | WMO Standard |
|----------|------------|-------------|-------|-----------|--------------|
| `tropical_nights` | Tropical Nights | Annual count of days when minimum temperature > 20°C | days | Tmin > 20°C | Yes |
| `frost_days` | Frost Days | Annual count of days when minimum temperature < 0°C | days | Tmin < 0°C | Yes |
| `ice_days` | Ice Days | Annual count of days when maximum temperature < 0°C | days | Tmax < 0°C | Yes |
| `summer_days` | Summer Days | Annual count of days when maximum temperature > 25°C | days | Tmax > 25°C | Yes |
| `hot_days` | Hot Days | Annual count of days when maximum temperature > 30°C | days | Tmax > 30°C | Regional |
| `very_hot_days` | Very Hot Days | Annual count of days when maximum temperature > 35°C | days | Tmax > 35°C | Regional |
| `warm_nights` | Warm Nights | Annual count of nights when minimum temperature > 15°C | days | Tmin > 15°C | Regional |
| `consecutive_frost_days` | Maximum Consecutive Frost Days | Maximum number of consecutive frost days | days | Consecutive Tmin < 0°C | Yes |

### Degree Day Metrics

| Index ID | Index Name | Description | Units | Base Temperature | Application |
|----------|------------|-------------|-------|------------------|-------------|
| `growing_degree_days` | Growing Degree Days | Accumulated temperature above threshold for crop development | °C·days | 10°C | Agriculture |
| `heating_degree_days` | Heating Degree Days | Energy demand indicator for heating | °C·days | 17°C | Energy Planning |
| `cooling_degree_days` | Cooling Degree Days | Energy demand indicator for cooling | °C·days | 18°C | Energy Planning |

---

## Precipitation Indices

### Basic Precipitation Statistics

| Index ID | Index Name | Description | Units | Formula | WMO Standard |
|----------|------------|-------------|-------|---------|--------------|
| `prcptot` | Total Precipitation | Annual total precipitation from wet days (≥ 1mm) | mm | Σ(pr) where pr ≥ 1mm | Yes |
| `rx1day` | Max 1-Day Precipitation | Maximum 1-day precipitation amount | mm | max(pr) | Yes |
| `rx5day` | Max 5-Day Precipitation | Maximum 5-day precipitation amount | mm | max(Σ5-day(pr)) | Yes |
| `sdii` | Simple Daily Intensity Index | Average precipitation on wet days | mm/day | Σ(pr)/count(wet days) | Yes |

### Consecutive Precipitation Events

| Index ID | Index Name | Description | Units | Threshold | WMO Standard |
|----------|------------|-------------|-------|-----------|--------------|
| `cdd` | Consecutive Dry Days | Maximum number of consecutive dry days | days | pr < 1mm | Yes |
| `cwd` | Consecutive Wet Days | Maximum number of consecutive wet days | days | pr ≥ 1mm | Yes |

### Precipitation Intensity Events

| Index ID | Index Name | Description | Units | Threshold | WMO Standard |
|----------|------------|-------------|-------|-----------|--------------|
| `r10mm` | Heavy Precipitation Days | Annual count of days with precipitation ≥ 10mm | days | pr ≥ 10mm | Yes |
| `r20mm` | Very Heavy Precipitation Days | Annual count of days with precipitation ≥ 20mm | days | pr ≥ 20mm | Yes |
| `r95p` | Very Wet Days | Precipitation from days above 95th percentile | mm | pr > 95th percentile | Yes |
| `r99p` | Extremely Wet Days | Precipitation from days above 99th percentile | mm | pr > 99th percentile | Yes |

---

## Extreme Weather Indices

### Temperature Extremes (Percentile-Based)

| Index ID | Index Name | Description | Units | Calculation | WMO Standard |
|----------|------------|-------------|-------|-------------|--------------|
| `tx90p` | Warm Days | Percentage of days when Tmax > 90th percentile | % or days | Tmax > 90th percentile of baseline | Yes |
| `tn90p` | Warm Nights | Percentage of days when Tmin > 90th percentile | % or days | Tmin > 90th percentile of baseline | Yes |
| `tx10p` | Cool Days | Percentage of days when Tmax < 10th percentile | % or days | Tmax < 10th percentile of baseline | Yes |
| `tn10p` | Cool Nights | Percentage of days when Tmin < 10th percentile | % or days | Tmin < 10th percentile of baseline | Yes |

### Spell Duration Indices

| Index ID | Index Name | Description | Units | Definition | WMO Standard |
|----------|------------|-------------|-------|------------|--------------|
| `warm_spell_duration_index` (WSDI) | Warm Spell Duration | Annual count of days in warm spells | days | ≥6 consecutive days with Tmax > 90th percentile | Yes |
| `cold_spell_duration_index` (CSDI) | Cold Spell Duration | Annual count of days in cold spells | days | ≥6 consecutive days with Tmin < 10th percentile | Yes |

---

## Humidity Indices

| Index ID | Index Name | Description | Units | Input Variables | Application |
|----------|------------|-------------|-------|-----------------|-------------|
| `dewpoint_temperature` | Dewpoint Temperature | Temperature at which air becomes saturated | °C | Specific humidity (hus), Temperature (tas), Pressure | Comfort, Agriculture |
| `relative_humidity` | Relative Humidity | Ratio of water vapor pressure to saturation pressure | % | Specific humidity (hus), Temperature (tas) | General Climate |

---

## Human Comfort Indices

| Index ID | Index Name | Description | Units | Formula Components | Application |
|----------|------------|-------------|-------|-------------------|-------------|
| `heat_index` | Heat Index | Apparent temperature combining heat and humidity | °C | Temperature + Humidity effect | Public Health |
| `humidex` | Humidex | Canadian humidity index for perceived temperature | °C | Temperature + 5/9 × (e - 10) | Public Health |

---

## Evapotranspiration Indices

| Index ID | Index Name | Description | Units | Method | Application |
|----------|------------|-------------|-------|--------|-------------|
| `potential_evapotranspiration` | Potential Evapotranspiration | Maximum water loss under optimal conditions | mm | Thornthwaite method | Water Balance |
| `reference_evapotranspiration` | Reference ET (ET₀) | Evapotranspiration from reference surface | mm | FAO-56 Penman-Monteith | Irrigation Planning |
| `spei_3` | 3-Month SPEI | Standardized Precipitation-Evapotranspiration Index | dimensionless | P - PET standardized | Drought Monitoring |

---

## Multivariate Indices

### Combined Temperature-Precipitation Events

| Index ID | Index Name | Description | Units | Temperature Threshold | Precipitation Threshold |
|----------|------------|-------------|-------|----------------------|------------------------|
| `cold_and_dry_days` | Cold & Dry Days | Days with low temperature and low precipitation | days | T < 25th percentile | P < 25th percentile |
| `cold_and_wet_days` | Cold & Wet Days | Days with low temperature and high precipitation | days | T < 25th percentile | P > 75th percentile |
| `warm_and_dry_days` | Warm & Dry Days | Days with high temperature and low precipitation | days | T > 75th percentile | P < 25th percentile |
| `warm_and_wet_days` | Warm & Wet Days | Days with high temperature and high precipitation | days | T > 75th percentile | P > 75th percentile |

---

## Agricultural Indices

| Index ID | Index Name | Description | Units | Calculation | Application |
|----------|------------|-------------|-------|-------------|-------------|
| `gsl` | Growing Season Length | Length of period suitable for crop growth | days | Period between first span of 6+ days > 5°C and first span after July 1st of 6+ days < 5°C | Crop Planning |
| `spi_3` | 3-Month SPI | Standardized Precipitation Index | dimensionless | Precipitation standardized over 3 months | Drought Assessment |
| `spei_3` | 3-Month SPEI | Enhanced drought index with evapotranspiration | dimensionless | (P - PET) standardized over 3 months | Water Stress |

---

## Input Variables Reference

### Primary Climate Variables

| Variable ID | Variable Name | Description | Standard Units | Alternative Names |
|-------------|---------------|-------------|----------------|-------------------|
| `tas` | Near-Surface Air Temperature | Daily mean temperature at 2m height | K or °C | temperature, temp, tmean |
| `tasmax` | Maximum Near-Surface Air Temperature | Daily maximum temperature at 2m height | K or °C | tmax, temperature_max |
| `tasmin` | Minimum Near-Surface Air Temperature | Daily minimum temperature at 2m height | K or °C | tmin, temperature_min |
| `pr` | Precipitation | Daily precipitation amount (liquid + solid) | mm/day or kg/m²/s | precipitation, precip, prcp |
| `hus` | Specific Humidity | Mass of water vapor per unit mass of air | kg/kg | specific_humidity, huss |
| `hurs` | Relative Humidity | Percentage of saturation humidity | % | relative_humidity, rh |
| `ps` | Surface Air Pressure | Atmospheric pressure at surface | Pa | pressure, surface_pressure |

### Derived Variables

| Variable | Description | Derivation | Units |
|----------|-------------|------------|-------|
| `evspsbl` | Evaporation | Calculated from temperature and humidity | mm/day |
| `tas_range` | Daily Temperature Range | tasmax - tasmin | °C |
| `wet_days` | Wet Day Indicator | pr ≥ 1mm | boolean |

---

## Technical Specifications

### Baseline Period Configuration
- **WMO Standard Baseline**: 1971-2000 (30 years)
- **Alternative Baseline**: 1981-2010 (climate normal period)
- **Minimum Baseline**: 10 years (with warning)
- **Percentile Calculation**: Uses baseline period for extreme indices

### Frequency Options
- **YS**: Year Start (January 1) - Default for annual indices
- **YE**: Year End (December 31)
- **MS**: Month Start - For monthly aggregations
- **QS**: Quarter Start - For seasonal analysis

### Quality Control
- **Missing Data Handling**: Indices calculated only with sufficient data coverage (typically >90%)
- **Unit Conversion**: Automatic conversion to standard units (°C for temperature, mm for precipitation)
- **CF Compliance**: All outputs follow Climate and Forecast (CF) metadata conventions

### Performance Considerations
- **Chunking**: Temporal chunking (365 days) and spatial chunking (100x100) for memory efficiency
- **Caching**: Percentile values cached for repeated calculations
- **Parallel Processing**: Dask-enabled for multi-core computation

---

## Usage Examples

### Temperature Index Calculation
```python
# Annual mean temperature
tg_mean = atmos.tg_mean(tas, freq='YS')

# Frost days with custom threshold
frost_days = atmos.frost_days(tasmin, thresh='-2 degC', freq='YS')
```

### Precipitation Index Calculation
```python
# Total precipitation
prcptot = atmos.prcptot(pr, thresh='1 mm/day', freq='YS')

# Consecutive dry days
cdd = atmos.maximum_consecutive_dry_days(pr, thresh='1 mm/day', freq='YS')
```

### Extreme Index with Baseline
```python
# Calculate 90th percentile using baseline period
baseline_data = tasmax.sel(time=slice('1971', '2000'))
tx90_per = baseline_data.quantile(0.9, dim='time')

# Apply to full dataset
tx90p = atmos.tx90p(tasmax, tasmax_per=tx90_per, freq='YS')
```

---

## References

1. **WMO Guidelines on the Calculation of Climate Normals** (WMO-No. 1203, 2017)
2. **ETCCDI Climate Change Indices** - Expert Team on Climate Change Detection and Indices
3. **xclim Documentation**: https://xclim.readthedocs.io/
4. **CF Conventions**: http://cfconventions.org/
5. **FAO-56 Penman-Monteith**: FAO Irrigation and Drainage Paper No. 56

---

## Version Information

- **Document Version**: 1.0.0
- **Pipeline Version**: xclim-timber v1.0
- **xclim Version**: ≥0.48.0
- **Last Updated**: 2025-09-16
- **Maintained By**: xclim-timber Development Team

---

## Notes

- All indices are calculated at annual frequency (YS) by default
- Temperature indices require conversion to Celsius before calculation
- Precipitation totals exclude trace amounts (<1mm) unless specified
- Percentile-based indices use the WMO standard 1971-2000 baseline period
- Missing data thresholds follow WMO recommendations (>10% missing = no calculation)
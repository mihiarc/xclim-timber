# Climate Indices Data Dictionary
## xclim-timber: Comprehensive Climate Metrics Reference Guide

> **✅ UPDATED 2025-10-11 - Complete 80-index catalog**
> **Current status:** 80/80 indices implemented (100% complete)
> **Authoritative source:** This dictionary is synchronized with [ACHIEVABLE_INDICES_ROADMAP.md](ACHIEVABLE_INDICES_ROADMAP.md)

This document provides a complete reference for all climate indices calculated by the xclim-timber pipeline, organized by category following World Meteorological Organization (WMO) standards.

**Total Indices: 80** across 7 categories (COMPLETE)

## Quick Reference

### Index Categories Summary
- **Temperature Indices**: 35 indices for temperature patterns and extremes
- **Precipitation Indices**: 13 indices for rainfall patterns and intensity
- **Humidity Indices**: 8 indices for moisture conditions
- **Human Comfort Indices**: 3 indices for heat stress assessment
- **Multivariate Indices**: 4 combined temperature-precipitation events
- **Agricultural Indices**: 5 specialized farming/crop indices
- **Drought Indices**: 12 drought monitoring indices

### Most Commonly Used Indices
1. **Temperature**: `tg_mean`, `frost_days`, `summer_days`, `growing_degree_days`
2. **Precipitation**: `prcptot`, `rx1day`, `cdd`, `cwd`
3. **Extremes**: `tx90p` (warm days), `tn10p` (cool nights), `warm_spell_duration_index`
4. **Drought**: `spi_3month`, `cdd`, `dry_days`
5. **Agriculture**: `growing_season_length`, `potential_evapotranspiration`, `corn_heat_units`

---

## Table of Contents
1. [Temperature Indices (35 indices)](#temperature-indices)
2. [Precipitation Indices (13 indices)](#precipitation-indices)
3. [Humidity Indices (8 indices)](#humidity-indices)
4. [Human Comfort Indices (3 indices)](#human-comfort-indices)
5. [Multivariate Indices (4 indices)](#multivariate-indices)
6. [Agricultural Indices (5 indices)](#agricultural-indices)
7. [Drought Indices (12 indices)](#drought-indices)
8. [Input Variables Reference](#input-variables-reference)

---

## Temperature Indices

### Basic Temperature Statistics (5 indices)

| Index ID | Index Name | Description | Units | Formula/Threshold | WMO Standard |
|----------|------------|-------------|-------|-------------------|--------------|
| `tg_mean` | Annual Mean Temperature | Average of daily mean temperature over the year | °C | Σ(tas)/n | Yes |
| `tx_max` | Annual Maximum Temperature | Highest daily maximum temperature in the year | °C | max(tasmax) | Yes |
| `tn_min` | Annual Minimum Temperature | Lowest daily minimum temperature in the year | °C | min(tasmin) | Yes |
| `daily_temperature_range` | Mean Daily Temperature Range | Average difference between daily maximum and minimum | °C | mean(tasmax - tasmin) | Yes |
| `extreme_temperature_range` | Extreme Temperature Range | Annual range between maximum and minimum temperatures | °C | max(tasmax) - min(tasmin) | Yes |

### Threshold-Based Temperature Counts (6 indices)

| Index ID | Index Name | Description | Units | Threshold | WMO Standard |
|----------|------------|-------------|-------|-----------|--------------|
| `frost_days` | Frost Days | Annual count of days when minimum temperature < 0°C | days | Tmin < 0°C | Yes |
| `ice_days` | Ice Days | Annual count of days when maximum temperature < 0°C | days | Tmax < 0°C | Yes |
| `summer_days` | Summer Days | Annual count of days when maximum temperature > 25°C | days | Tmax > 25°C | Yes |
| `hot_days` | Hot Days | Annual count of days when maximum temperature > 30°C | days | Tmax > 30°C | Regional |
| `tropical_nights` | Tropical Nights | Annual count of nights when minimum temperature > 20°C | days | Tmin > 20°C | Yes |
| `consecutive_frost_days` | Maximum Consecutive Frost Days | Maximum number of consecutive frost days | days | Consecutive Tmin < 0°C | Yes |

### Frost Season Metrics (4 indices)

| Index ID | Index Name | Description | Units | Definition | Application |
|----------|------------|-------------|-------|------------|-------------|
| `frost_season_length` | Frost Season Length | Duration from first to last frost | days | Period with frost days | Agriculture, Climate |
| `frost_free_season_start` | Frost-Free Season Start | Last spring frost date (Julian day) | day of year | Last Tmin < 0°C before growing season | Planting timing |
| `frost_free_season_end` | Frost-Free Season End | First fall frost date (Julian day) | day of year | First Tmin < 0°C after growing season | Harvest timing |
| `frost_free_season_length` | Frost-Free Season Length | Days between last spring and first fall frost | days | End date - Start date | Growing season |

### Degree Day Metrics (4 indices)

| Index ID | Index Name | Description | Units | Base Temperature | Application |
|----------|------------|-------------|-------|------------------|-------------|
| `growing_degree_days` | Growing Degree Days | Accumulated temperature above threshold for crop development | °C·days | 10°C | Agriculture, phenology |
| `heating_degree_days` | Heating Degree Days | Energy demand indicator for heating | °C·days | 17°C | Energy planning |
| `cooling_degree_days` | Cooling Degree Days | Energy demand indicator for cooling | °C·days | 18°C | Energy planning |
| `freezing_degree_days` | Freezing Degree Days | Accumulated temperature below 0°C | °C·days | 0°C | Infrastructure, permafrost |

### Percentile-Based Extremes (6 indices)

| Index ID | Index Name | Description | Units | Calculation | WMO Standard |
|----------|------------|-------------|-------|-------------|--------------|
| `tx90p` | Warm Days | Days when Tmax exceeds 90th percentile | days | Tmax > 90th percentile of baseline (1981-2000) | Yes |
| `tx10p` | Cool Days | Days when Tmax below 10th percentile | days | Tmax < 10th percentile of baseline (1981-2000) | Yes |
| `tn90p` | Warm Nights | Days when Tmin exceeds 90th percentile | days | Tmin > 90th percentile of baseline (1981-2000) | Yes |
| `tn10p` | Cool Nights | Days when Tmin below 10th percentile | days | Tmin < 10th percentile of baseline (1981-2000) | Yes |
| `warm_spell_duration_index` | Warm Spell Duration (WSDI) | Total days in warm spells | days | ≥6 consecutive days with Tmax > 90th percentile | Yes |
| `cold_spell_duration_index` | Cold Spell Duration (CSDI) | Total days in cold spells | days | ≥6 consecutive days with Tmin < 10th percentile | Yes |

### Advanced Temperature Indices (8 indices)

| Index ID | Index Name | Description | Units | Threshold/Method | Application |
|----------|------------|-------------|-------|------------------|-------------|
| `growing_season_start` | Growing Season Start | First day of sustained warmth | day of year | First 5+ days > 5°C (ETCCDI) | Agriculture |
| `growing_season_end` | Growing Season End | First day of sustained cold | day of year | First 5+ days < 5°C after July 1 | Agriculture |
| `cold_spell_frequency` | Cold Spell Frequency | Number of cold spell events | count | Events of tas < -10°C for 5+ days | Climate monitoring |
| `hot_spell_frequency` | Hot Spell Frequency | Number of hot spell events | count | Events of tasmax > 30°C for 3+ days | Public health |
| `heat_wave_frequency` | Heat Wave Frequency | Number of heat wave events | count | Events of tasmin > 22°C AND tasmax > 30°C for 3+ days | Public health |
| `freezethaw_spell_frequency` | Freeze-Thaw Cycles | Number of freeze-thaw events | count | Days with tasmax > 0°C AND tasmin ≤ 0°C | Infrastructure |
| `last_spring_frost` | Last Spring Frost | Date of last spring frost | day of year | Last Tmin < 0°C in spring | Planting |
| `daily_temperature_range_variability` | DTR Variability | Day-to-day variation in temperature range | °C | Std dev of daily (Tmax - Tmin) differences | Climate stability |

### Temperature Variability (2 indices)

| Index ID | Index Name | Description | Units | Calculation | Application |
|----------|------------|-------------|-------|-------------|-------------|
| `temperature_seasonality` | Temperature Seasonality | Annual temperature coefficient of variation | % | (Std dev / Mean) × 100 of tas | Climate classification (ANUCLIM BIO4) |
| `heat_wave_index` | Heat Wave Index | Total days in heat waves | days | Days in events of 5+ consecutive days with tasmax > 25°C | Heatwave duration |

---

## Precipitation Indices

### Basic Precipitation Statistics (4 indices)

| Index ID | Index Name | Description | Units | Formula | WMO Standard |
|----------|------------|-------------|-------|---------|--------------|
| `prcptot` | Total Precipitation | Annual total precipitation from wet days | mm | Σ(pr) where pr ≥ 1mm | Yes |
| `rx1day` | Max 1-Day Precipitation | Maximum 1-day precipitation amount | mm | max(pr) | Yes |
| `rx5day` | Max 5-Day Precipitation | Maximum 5-day precipitation amount | mm | max(Σ5-day(pr)) | Yes |
| `sdii` | Simple Daily Intensity Index | Average precipitation on wet days | mm/day | Σ(pr)/count(wet days where pr ≥ 1mm) | Yes |

### Consecutive Precipitation Events (2 indices)

| Index ID | Index Name | Description | Units | Threshold | WMO Standard |
|----------|------------|-------------|-------|-----------|--------------|
| `cdd` | Consecutive Dry Days | Maximum number of consecutive dry days | days | pr < 1mm | Yes |
| `cwd` | Consecutive Wet Days | Maximum number of consecutive wet days | days | pr ≥ 1mm | Yes |

### Precipitation Extremes (4 indices)

| Index ID | Index Name | Description | Units | Threshold | WMO Standard |
|----------|------------|-------------|-------|-----------|--------------|
| `r10mm` | Heavy Precipitation Days | Annual count of days with precipitation ≥ 10mm | days | pr ≥ 10mm | Yes |
| `r20mm` | Very Heavy Precipitation Days | Annual count of days with precipitation ≥ 20mm | days | pr ≥ 20mm | Yes |
| `r95p` | Very Wet Days | Total precipitation from days above 95th percentile | mm | pr > 95th percentile of wet days (1981-2000) | Yes |
| `r99p` | Extremely Wet Days | Total precipitation from days above 99th percentile | mm | pr > 99th percentile of wet days (1981-2000) | Yes |

### Enhanced Precipitation Analysis (3 indices)

| Index ID | Index Name | Description | Units | Threshold | Application |
|----------|------------|-------------|-------|-----------|-------------|
| `dry_days` | Dry Days | Total number of dry days | days | pr < 1mm | Drought monitoring |
| `wetdays` | Wet Days | Total number of wet days | days | pr ≥ 1mm | Water availability |
| `wetdays_prop` | Wet Days Proportion | Fraction of days that are wet | fraction (0-1) | count(pr ≥ 1mm) / count(all days) | Climate characterization |

---

## Humidity Indices

### Dewpoint Temperature Statistics (4 indices)

| Index ID | Index Name | Description | Units | Details | Application |
|----------|------------|-------------|-------|---------|-------------|
| `dewpoint_mean` | Mean Dewpoint Temperature | Annual mean dewpoint temperature | °C | Average moisture content indicator | Comfort, Agriculture |
| `dewpoint_min` | Minimum Dewpoint Temperature | Annual minimum dewpoint temperature | °C | Driest conditions | Climate extremes |
| `dewpoint_max` | Maximum Dewpoint Temperature | Annual maximum dewpoint temperature | °C | Most humid conditions | Heat stress |
| `humid_days` | Humid Days | Days with high humidity | days | Dewpoint > 18°C (uncomfortable threshold) | Public health |

### Vapor Pressure Deficit Statistics (4 indices)

| Index ID | Index Name | Description | Units | Threshold | Application |
|----------|------------|-------------|-------|-----------|-------------|
| `vpdmax_mean` | Mean Maximum VPD | Annual mean of daily maximum VPD | kPa | Average evaporative demand | Agriculture, Water |
| `vpdmin_mean` | Mean Minimum VPD | Annual mean of daily minimum VPD | kPa | Average minimum moisture stress | Agriculture |
| `extreme_vpd_days` | Extreme VPD Days | Days with extreme VPD | days | VPD > 4 kPa (high plant stress) | Agriculture, Drought |
| `low_vpd_days` | Low VPD Days | Days with low VPD | days | VPD < 0.5 kPa (fog/high moisture) | Agriculture, Aviation |

---

## Human Comfort Indices

| Index ID | Index Name | Description | Units | Formula Components | Application |
|----------|------------|-------------|-------|-------------------|-------------|
| `heat_index` | Heat Index | Apparent temperature combining heat and humidity | °C | Temperature + Humidity effect (Steadman 1979) | Public health, Heat stress |
| `humidex` | Humidex | Canadian humidity index for perceived temperature | °C | Temperature + 5/9 × (e - 10) where e = vapor pressure | Public health, Canada |
| `relative_humidity` | Relative Humidity | Ratio of water vapor pressure to saturation pressure | % | Derived from specific humidity and temperature | General comfort |

---

## Multivariate Indices

### Combined Temperature-Precipitation Events (4 indices)

| Index ID | Index Name | Description | Units | Temperature Threshold | Precipitation Threshold | Application |
|----------|------------|-------------|-------|-----------------------|------------------------|-------------|
| `cold_and_dry_days` | Cold & Dry Days | Days with low temperature AND low precipitation | days | tas < 25th percentile | pr < 25th percentile | Compound drought |
| `cold_and_wet_days` | Cold & Wet Days | Days with low temperature AND high precipitation | days | tas < 25th percentile | pr > 75th percentile | Flooding risk |
| `warm_and_dry_days` | Warm & Dry Days | Days with high temperature AND low precipitation | days | tas > 75th percentile | pr < 25th percentile | Drought/fire risk |
| `warm_and_wet_days` | Warm & Wet Days | Days with high temperature AND high precipitation | days | tas > 75th percentile | pr > 75th percentile | Compound extremes |

**Note:** Percentile thresholds calculated from 1981-2000 baseline period using day-of-year percentiles.

---

## Agricultural Indices

| Index ID | Index Name | Description | Units | Calculation | Application |
|----------|------------|-------------|-------|-------------|-------------|
| `growing_season_length` | Growing Season Length | Total days between first and last occurrence of sustained warmth | days | Period between first and last span of 6+ days > 5°C (ETCCDI) | Crop planning, variety selection |
| `potential_evapotranspiration` | Potential Evapotranspiration (PET) | Annual potential evapotranspiration | kg m⁻² s⁻¹ | Baier-Robertson (1965) temperature-only method | Irrigation planning, water budget |
| `corn_heat_units` | Corn Heat Units (CHU) | Annual accumulated heat units for corn development | dimensionless | USDA standard corn-specific formula | Corn maturity prediction, hybrid selection |
| `thawing_degree_days` | Thawing Degree Days (TDD) | Sum of degree-days above 0°C | K days | Σ(tas - 0°C) where tas > 0°C | Permafrost monitoring, spring melt |
| `growing_season_precipitation` | Growing Season Precipitation | Total precipitation during growing season | mm | Σ(pr) for April-October (northern hemisphere) | Water availability assessment |

---

## Drought Indices

### Standardized Precipitation Index - SPI (5 windows)

| Index ID | Index Name | Description | Units | Window | Application |
|----------|------------|-------------|-------|--------|-------------|
| `spi_1month` | 1-Month SPI | Short-term agricultural drought | dimensionless | 1 month | Immediate moisture deficit |
| `spi_3month` | 3-Month SPI | Seasonal agricultural drought | dimensionless | 3 months | Most common for agriculture |
| `spi_6month` | 6-Month SPI | Medium-term agricultural/hydrological drought | dimensionless | 6 months | Water supply impacts |
| `spi_12month` | 12-Month SPI | Long-term hydrological drought | dimensionless | 12 months | Reservoir levels, streamflow |
| `spi_24month` | 24-Month SPI | Multi-year persistent drought | dimensionless | 24 months | Severe prolonged drought |

**Methodology:** McKee et al. (1993) using gamma distribution fitting on 1981-2010 calibration period (WMO standard).

**Interpretation:**
- SPI > 2.0: Extremely wet
- SPI 1.5 to 2.0: Very wet
- SPI 1.0 to 1.5: Moderately wet
- SPI -1.0 to 1.0: Near normal
- SPI -1.5 to -1.0: Moderately dry
- SPI -2.0 to -1.5: Severely dry
- SPI < -2.0: Extremely dry

### Dry Spell Analysis (4 indices)

| Index ID | Index Name | Description | Units | Threshold | Application |
|----------|------------|-------------|-------|-----------|-------------|
| `cdd` | Maximum Consecutive Dry Days | Longest dry spell | days | pr < 1mm consecutive | Drought severity (ETCCDI) |
| `dry_spell_frequency` | Dry Spell Frequency | Number of dry spell events | count | Events of ≥3 consecutive days < 1mm | Drought event monitoring |
| `dry_spell_total_length` | Dry Spell Total Length | Total days in all dry spells | days | Sum of all dry spell durations | Cumulative drought exposure |
| `dry_days` | Dry Days | Total number of dry days | days | pr < 1mm | Overall aridity |

### Precipitation Intensity (3 indices)

| Index ID | Index Name | Description | Units | Calculation | Application |
|----------|------------|-------------|-------|-------------|-------------|
| `sdii` | Simple Daily Intensity Index | Average precipitation on wet days | mm/day | Σ(pr)/count(wet days) where pr ≥ 1mm | Intensity monitoring (ETCCDI) |
| `max_7day_pr_intensity` | Maximum 7-Day Precipitation | Maximum 7-day rolling precipitation sum | mm | max(Σ7-day(pr)) | Heavy precipitation events |
| `fraction_heavy_precip` | Heavy Precipitation Fraction | Fraction of annual precipitation from heavy events | fraction (0-1) | Σ(pr > 75th percentile) / Σ(all pr) | Precipitation concentration |

---

## Input Variables Reference

### Primary Climate Variables

| Variable ID | Variable Name | Description | Standard Units | Alternative Names | Source |
|-------------|---------------|-------------|----------------|-------------------|--------|
| `tas` | Near-Surface Air Temperature | Daily mean temperature at 2m height | K or °C | temperature, temp, tmean | PRISM tmean |
| `tasmax` | Maximum Near-Surface Air Temperature | Daily maximum temperature at 2m height | K or °C | tmax, temperature_max | PRISM tmax |
| `tasmin` | Minimum Near-Surface Air Temperature | Daily minimum temperature at 2m height | K or °C | tmin, temperature_min | PRISM tmin |
| `pr` | Precipitation | Daily precipitation amount (liquid + solid) | mm/day or kg/m²/s | precipitation, precip, prcp, ppt | PRISM ppt |
| `tdew` | Dewpoint Temperature | Daily mean dewpoint temperature | °C | tdmean, dewpoint | PRISM tdmean |
| `vpdmax` | Maximum Vapor Pressure Deficit | Daily maximum VPD | hPa or kPa | vpd_max | PRISM vpdmax |
| `vpdmin` | Minimum Vapor Pressure Deficit | Daily minimum VPD | hPa or kPa | vpd_min | PRISM vpdmin |

### Derived Variables

| Variable | Description | Derivation | Units | Used By |
|----------|-------------|------------|-------|---------|
| `hurs` | Relative Humidity | Calculated from dewpoint and temperature | % | Human comfort indices |
| `tas_range` | Daily Temperature Range | tasmax - tasmin | °C | Temperature variability indices |
| `wet_days` | Wet Day Indicator | pr ≥ 1mm | boolean | Precipitation intensity |
| `dayofyear` | Day of Year | Calendar day (1-366) | day | Percentile thresholds |

---

## Technical Specifications

### Baseline Period Configuration
- **Project Baseline**: 1981-2000 (20 years) for PRISM data compatibility
- **WMO Standard**: 1971-2000 or 1981-2010 (30 years) recommended
- **Drought Calibration**: 1981-2010 (30 years) for SPI following WMO guidelines
- **Percentile Calculation**: Day-of-year percentiles using 5-day window
- **Pre-calculated**: Baseline percentiles computed once and reused

### Frequency Options
- **YS**: Year Start (January 1) - Default for annual indices
- **YE**: Year End (December 31)
- **MS**: Month Start - For monthly aggregations (e.g., SPI)
- **QS**: Quarter Start - For seasonal analysis

### Spatial Coverage
- **Domain**: PRISM CONUS (Contiguous United States)
- **Resolution**: ~4 km (800m nominal)
- **Grid**: 621 latitude × 1405 longitude points
- **Projection**: Geographic (WGS84)

### Quality Control
- **Missing Data Handling**: Indices calculated only with sufficient data coverage (typically >90%)
- **Unit Conversion**: Automatic conversion to standard units (°C for temperature, mm for precipitation)
- **CF Compliance**: All outputs follow Climate and Forecast (CF) metadata conventions
- **Validation**: Temperature ranges checked, percentiles verified (90th > 10th)

### Performance Considerations
- **Temporal Chunking**: 1-year or 4-year chunks depending on memory requirements
- **Spatial Chunking**: lat (103) × lon (201) chunks for 6×7 grid subdivision
- **Memory Usage**: 18-22 GB peak for baseline-dependent pipelines
- **Processing Time**: ~2 min/year for temperature, ~5 min/year for drought
- **Scheduler**: Threaded (not distributed) for memory efficiency

---

## Usage Examples

### Temperature Index Calculation
```python
from xclim import atmos

# Annual mean temperature
tg_mean = atmos.tg_mean(tas, freq='YS')

# Frost days with custom threshold
frost_days = atmos.frost_days(tasmin, thresh='-2 degC', freq='YS')

# Growing degree days
gdd = atmos.growing_degree_days(tas, thresh='10 degC', freq='YS')
```

### Precipitation Index Calculation
```python
# Total precipitation
prcptot = atmos.prcptot(pr, thresh='1 mm/day', freq='YS')

# Consecutive dry days
cdd = atmos.maximum_consecutive_dry_days(pr, thresh='1 mm/day', freq='YS')

# Simple daily intensity
sdii = atmos.daily_pr_intensity(pr, thresh='1 mm/day', freq='YS')
```

### Percentile-Based Extreme Indices
```python
# Load pre-calculated baseline percentiles
baseline = xr.open_dataset('data/baselines/baseline_percentiles_1981_2000.nc')

# Calculate warm days using baseline 90th percentile
tx90p = atmos.tx_days_above(
    tasmax,
    tasmax_per=baseline.tx90p_threshold,
    freq='YS'
)

# Calculate warm spell duration
wsdi = atmos.warm_spell_duration_index(
    tasmax,
    tasmax_per=baseline.tx90p_threshold,
    window=6,
    freq='YS'
)
```

### Multivariate Indices
```python
# Load both datasets
temp_ds = xr.open_zarr('temperature.zarr')
precip_ds = xr.open_zarr('precipitation.zarr')

# Merge with coordinate validation
combined = xr.merge([temp_ds, precip_ds])

# Calculate compound dry days
warm_dry = atmos.warm_and_dry_days(
    tas=combined.tas,
    pr=combined.pr,
    tas_per=baseline.tas_75p_threshold,
    pr_per=baseline.pr_25p_threshold,
    freq='YS'
)
```

### Drought Index Calculation (SPI)
```python
from xclim.indices import standardized_precipitation_index

# Calculate 3-month SPI
spi_3 = standardized_precipitation_index(
    pr=pr,
    freq='MS',  # Monthly
    window=3,
    dist='gamma',
    method='ML',
    cal_start='1981',
    cal_end='2010'
)

# Filter to target period
spi_3_filtered = spi_3.sel(time=slice('1981', '2024'))
```

---

## References

1. **WMO Guidelines on the Calculation of Climate Normals** (WMO-No. 1203, 2017)
2. **ETCCDI Climate Change Indices** - Expert Team on Climate Change Detection and Indices
   http://etccdi.pacificclimate.org/
3. **McKee, T.B., N.J. Doesken, and J. Kleist (1993).** "The relationship of drought frequency and duration to time scales." Proceedings of the 8th Conference on Applied Climatology, 17-22 January, Anaheim, CA, pp. 179-183.
4. **Zhang, X., et al. (2005).** "Avoiding inhomogeneity in percentile-based indices of temperature extremes." Journal of Climate, 18(11), 1641-1651.
5. **Baier, W., and G.W. Robertson (1965).** "Estimation of latent evaporation from simple weather observations." Canadian Journal of Plant Science, 45(3), 276-284.
6. **xclim Documentation**: https://xclim.readthedocs.io/
7. **CF Conventions**: http://cfconventions.org/
8. **PRISM Climate Group**: https://prism.oregonstate.edu/

---

## Version Information

- **Document Version**: 2.0.0
- **Pipeline Version**: xclim-timber v2.0 (production)
- **xclim Version**: ≥0.56.0
- **Last Updated**: 2025-10-11
- **Maintained By**: xclim-timber Development Team

---

## Notes

- All indices are calculated at annual frequency (YS) by default unless specified (e.g., SPI uses monthly)
- Temperature indices require conversion to Celsius before calculation (xclim handles automatically)
- Precipitation totals exclude trace amounts (<1mm) unless specified
- Percentile-based indices use pre-calculated day-of-year thresholds from 1981-2000 baseline period
- Missing data thresholds follow WMO recommendations (>10% missing = no calculation)
- SPI indices require 30-year calibration period (1981-2010) for proper distribution fitting
- Some indices appear in multiple categories (e.g., CDD in both Precipitation and Drought) based on application context
- All 80 indices have been validated with 2023 test data and are ready for production use

# Climate Indices Reference Guide

> **⚠️ SEVERELY OUTDATED - This document is from early project phases**
> **Current status:** 80/80 indices implemented (100% complete)
> **For current index catalog:** See [ACHIEVABLE_INDICES_ROADMAP.md](ACHIEVABLE_INDICES_ROADMAP.md)
> **This document will be archived or completely rewritten**

## Overview
~~The xclim-timber pipeline suite calculates **26 distinct climate indices** across three specialized pipelines.~~ **[OUTDATED: Now 80 indices across 7 pipelines]** Each index is scientifically validated and follows CF (Climate and Forecast) conventions for metadata.

**~~Total Indices: 26~~ ACTUAL: 80 indices** (100% complete!)
- ~~Temperature Pipeline: 12 indices~~ **Temperature: 35 indices**
- ~~Precipitation Pipeline: 6 indices~~ **Precipitation: 13 indices**
- ~~Humidity Pipeline: 8 indices~~ **Humidity: 8 indices**
- **Human Comfort: 3 indices** (NEW)
- **Multivariate: 4 indices** (NEW)
- **Agricultural: 5 indices** (NEW)
- **Drought: 12 indices** (NEW)

---

## Temperature Pipeline (12 indices)

### Basic Temperature Statistics (3)
| Index | Variable Name | Description | Units | Threshold/Method |
|-------|--------------|-------------|-------|------------------|
| 1 | `tg_mean` | Annual mean temperature | °C | Annual average of daily mean temperature |
| 2 | `tx_max` | Annual maximum temperature | °C | Highest daily maximum temperature in year |
| 3 | `tn_min` | Annual minimum temperature | °C | Lowest daily minimum temperature in year |

### Temperature Threshold Days (6)
| Index | Variable Name | Description | Units | Threshold |
|-------|--------------|-------------|-------|-----------|
| 4 | `summer_days` | Days with maximum temperature >25°C | days | Tmax > 25°C |
| 5 | `hot_days` | Days with maximum temperature >30°C | days | Tmax > 30°C |
| 6 | `ice_days` | Ice days (maximum temperature <0°C) | days | Tmax < 0°C |
| 7 | `frost_days` | Frost days (minimum temperature <0°C) | days | Tmin < 0°C |
| 8 | `tropical_nights` | Nights with minimum temperature >20°C | days | Tmin > 20°C |
| 9 | `consecutive_frost_days` | Maximum consecutive days with frost | days | Tmin < 0°C (consecutive) |

### Degree Day Metrics (3)
| Index | Variable Name | Description | Units | Base Temperature | Application |
|-------|--------------|-------------|-------|------------------|-------------|
| 10 | `growing_degree_days` | Accumulated temperature for crop growth | °C·days | 10°C | Agriculture, phenology |
| 11 | `heating_degree_days` | Heating energy demand indicator | °C·days | 17°C | Energy planning |
| 12 | `cooling_degree_days` | Cooling energy demand indicator | °C·days | 18°C | Energy planning |

---

## Precipitation Pipeline (6 indices)

### Precipitation Amounts (3)
| Index | Variable Name | Description | Units | Details |
|-------|--------------|-------------|-------|---------|
| 13 | `prcptot` | Total annual precipitation (wet days only) | mm | Sum of precipitation on days ≥1mm |
| 14 | `rx1day` | Maximum 1-day precipitation | mm | Highest daily precipitation amount |
| 15 | `rx5day` | Maximum 5-day precipitation | mm | Highest 5-consecutive-day total |

### Consecutive Events (2)
| Index | Variable Name | Description | Units | Threshold |
|-------|--------------|-------------|-------|-----------|
| 16 | `cdd` | Maximum consecutive dry days | days | <1 mm/day |
| 17 | `cwd` | Maximum consecutive wet days | days | ≥1 mm/day |

### Precipitation Intensity (1)
| Index | Variable Name | Description | Units | Calculation |
|-------|--------------|-------------|-------|-------------|
| 18 | `sdii` | Simple Daily Intensity Index | mm/day | Mean precipitation on wet days (≥1mm) |

---

## Humidity Pipeline (8 indices)

### Dewpoint Temperature Statistics (4)
| Index | Variable Name | Description | Units | Details |
|-------|--------------|-------------|-------|---------|
| 19 | `dewpoint_mean` | Annual mean dewpoint temperature | °C | Average moisture content indicator |
| 20 | `dewpoint_min` | Annual minimum dewpoint temperature | °C | Driest conditions |
| 21 | `dewpoint_max` | Annual maximum dewpoint temperature | °C | Most humid conditions |
| 22 | `humid_days` | Days with high humidity | days | Dewpoint > 18°C (uncomfortable) |

### Vapor Pressure Deficit Statistics (4)
| Index | Variable Name | Description | Units | Details |
|-------|--------------|-------------|-------|---------|
| 23 | `vpdmax_mean` | Mean daily maximum VPD | kPa | Average evaporative demand |
| 24 | `extreme_vpd_days` | Days with extreme VPD | days | VPD > 4 kPa (high plant stress) |
| 25 | `vpdmin_mean` | Mean daily minimum VPD | kPa | Average minimum moisture stress |
| 26 | `low_vpd_days` | Days with low VPD | days | VPD < 0.5 kPa (fog/high moisture) |

---

## Applications by Sector

### Agriculture & Forestry
- **Crop Planning**: Growing degree days (#9), frost days (#7), extreme VPD days (#23)
- **Irrigation Management**: CDD (#15), SDII (#17), VPD metrics (#22-25)
- **Harvest Timing**: Summer days (#4), precipitation patterns (#12-17)

### Energy & Infrastructure
- **Energy Demand**: Heating degree days (#10), cooling degree days (#11)
- **Grid Planning**: Hot days (#5), tropical nights (#8), ice days (#6)
- **Building Design**: Temperature extremes (#2-3), humidity levels (#18-21)

### Water Resources
- **Drought Monitoring**: CDD (#15), low VPD days (#25), dewpoint minimum (#19)
- **Flood Risk**: Rx1day (#13), rx5day (#14), CWD (#16)
- **Reservoir Management**: Total precipitation (#12), seasonal patterns

### Public Health
- **Heat Stress**: Hot days (#5), tropical nights (#8), humid days (#21)
- **Cold Exposure**: Frost days (#7), ice days (#6), minimum temperature (#3)
- **Comfort Indices**: Dewpoint mean (#18), VPD conditions (#22-25)

### Environmental Monitoring
- **Climate Trends**: All temperature statistics (#1-3)
- **Ecosystem Stress**: Extreme VPD days (#23), consecutive dry days (#15)
- **Phenology**: Growing degree days (#9), frost-free period

---

## Technical Details

### Input Data Requirements
- **Temperature Pipeline**: Daily tmax, tmin, tmean (°C)
- **Precipitation Pipeline**: Daily precipitation (mm)
- **Humidity Pipeline**: Daily dewpoint, VPD max/min (°C, kPa)

### Processing Configuration
- **Temporal Resolution**: Annual aggregation (YS frequency)
- **Spatial Coverage**: PRISM CONUS domain (~4km resolution)
- **Memory Usage**: 3-4 GB per pipeline for 24-year processing
- **Output Format**: CF-compliant NetCDF4 with zlib compression

### Quality Assurance
- All indices use scientifically validated xclim algorithms
- Thresholds follow WMO and international standards
- Missing data handled appropriately (NaN propagation)
- Metadata includes units, standard_names, and descriptions

---

## Usage Example

```bash
# Calculate all 25 indices for 2020-2023
for pipeline in temperature precipitation humidity; do
    python ${pipeline}_pipeline.py --start-year 2020 --end-year 2023
done

# Combine outputs for analysis
python -c "
import xarray as xr
temp = xr.open_dataset('outputs/temperature_indices_2020_2023.nc')
precip = xr.open_dataset('outputs/precipitation_indices_2020_2023.nc')
humid = xr.open_dataset('outputs/humidity_indices_2020_2023.nc')
combined = xr.merge([temp, precip, humid])
print(f'Total indices: {len(combined.data_vars)}')
"
```

---

## References
- ETCCDI Climate Change Indices: http://etccdi.pacificclimate.org/
- WMO Guidelines on Climate Metadata: https://library.wmo.int/
- CF Conventions: https://cfconventions.org/
- xclim Documentation: https://xclim.readthedocs.io/
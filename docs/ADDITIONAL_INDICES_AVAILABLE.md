# Additional Climate Indices Available in xclim

## Overview
xclim provides **151 total climate indicators** in the `xclim.indicators.atmos` module. We currently implement **50 indices** across four specialized pipelines (was 46, adding 4 multivariate in Phase 5). This document tracks **101 additional indices** that could be added to enhance our climate analysis capabilities.

## Current Implementation Status (50 indices)

### ✅ Temperature Pipeline (25 indices)
**Basic Statistics (5):**
- `tg_mean` - Annual mean temperature
- `tx_max` - Annual maximum temperature
- `tn_min` - Annual minimum temperature
- `daily_temperature_range` - Mean daily temperature range
- `extreme_temperature_range` - Annual extreme temperature range

**Threshold-Based Counts (6):**
- `frost_days` - Days with Tmin < 0°C
- `ice_days` - Days with Tmax < 0°C
- `summer_days` - Days with Tmax > 25°C
- `hot_days` - Days with Tmax > 30°C
- `tropical_nights` - Nights with Tmin > 20°C
- `consecutive_frost_days` - Maximum consecutive frost days ✨ *Phase 1 Extension*

**Frost Season Metrics (4):**
- `frost_season_length` - Duration of frost season
- `frost_free_season_length` - Growing season length
- `frost_free_season_start` - Last spring frost
- `frost_free_season_end` - First fall frost

**Degree Day Metrics (3):**
- `growing_degree_days` - GDD base 10°C (crop development)
- `heating_degree_days` - HDD base 17°C (energy demand)
- `cooling_degree_days` - CDD base 18°C (cooling energy)
- `freezing_degree_days` - FDD (infrastructure impacts) ✨ *Phase 1 Extension*

**Extreme Percentile-Based (6):** *Uses 1981-2000 Baseline*
- `tx90p` - Warm days (Tmax > 90th percentile)
- `tx10p` - Cool days (Tmax < 10th percentile)
- `tn90p` - Warm nights (Tmin > 90th percentile)
- `tn10p` - Cool nights (Tmin < 10th percentile)
- `warm_spell_duration_index` - WSDI (consecutive warm periods)
- `cold_spell_duration_index` - CSDI (consecutive cold periods)

### ✅ Precipitation Pipeline (10 indices)
**Basic Statistics (4):**
- `prcptot` - Total annual precipitation
- `rx1day` - Maximum 1-day precipitation
- `rx5day` - Maximum 5-day precipitation
- `sdii` - Simple daily intensity index (average wet day amount)

**Consecutive Events (2):**
- `cdd` - Maximum consecutive dry days (< 1mm)
- `cwd` - Maximum consecutive wet days (≥ 1mm)

**Extreme Precipitation (4):** *Uses 1981-2000 Baseline*
- `r10mm` - Heavy precipitation days (≥ 10mm)
- `r20mm` - Very heavy precipitation days (≥ 20mm)
- `r95p` - Very wet days (> 95th percentile)
- `r99p` - Extremely wet days (> 99th percentile)

### ✅ Humidity Pipeline (8 indices)
**Note:** Implementation details from humidity pipeline would go here
- Various humidity and moisture metrics
- (Specific index names to be documented)

### ✅ Human Comfort Pipeline (3 indices)
**Heat Stress Assessment:**
- `heat_index` - Heat index (temperature + humidity effects)
- `humidex` - Canadian humidex for apparent temperature
- `relative_humidity` - Derived relative humidity

### ✅ Multivariate Pipeline (4 indices) ✨ **Phase 5 - NEW**
**Compound Climate Extremes:** *Uses 1981-2000 Baseline*
- `cold_and_dry_days` - Combined cold + dry conditions (compound drought)
- `cold_and_wet_days` - Combined cold + wet conditions (flooding risk)
- `warm_and_dry_days` - Combined warm + dry conditions (drought/fire risk)
- `warm_and_wet_days` - Combined warm + wet conditions (compound extremes)

---

## Implementation Progress Timeline

### Phase 1 (Complete): Temperature Extensions
- Added `consecutive_frost_days` (2025-09)
- Added `freezing_degree_days` (2025-09)
- Added 5 additional temperature metrics
- **Result:** 19 → 25 temperature indices

### Phase 4 (Complete): Human Comfort
- Added `heat_index`, `humidex`, `relative_humidity`
- **Result:** 46 total indices

### Phase 5 (In Progress): Multivariate Extremes
- Adding 4 compound climate extreme indices
- **Result:** 46 → 50 total indices (60% of 84-index goal)

---

## High-Priority Additions (Recommended Next)

### Temperature Extremes (Still Available - 9 indices)
| Index | Function Name | Description | Application |
|-------|--------------|-------------|-------------|
| 1 | `daily_temperature_range_variability` | Variability of DTR | Climate stability |
| 2 | `heat_wave_frequency` | Number of heat wave events | Public health |
| 3 | `heat_wave_max_length` | Longest heat wave duration | Infrastructure stress |
| 4 | `thawing_degree_days` | TDD > 0°C | Permafrost monitoring |
| 5 | `frost_free_season_length` | Growing season | Agriculture |
| 6 | `growing_season_length` | Temperature-based growing season | Crop planning |
| 7 | `late_frost_days` | Frost after season start | Crop damage |
| 8 | `last_spring_frost` | Date of last spring frost | Planting |
| 9 | `first_fall_frost` | Date of first fall frost | Harvest |

### Precipitation Enhancements (12 indices)
| Index | Function Name | Description | Application |
|-------|--------------|-------------|-------------|
| 10 | `max_pr_intensity` | Maximum precipitation intensity | Storm severity |
| 11 | `wetdays` | Number of wet days (≥1mm) | Water availability |
| 12 | `wetdays_prop` | Proportion of wet days | Climate characterization |
| 13 | `dry_spell_frequency` | Number of dry spell events | Drought monitoring |
| 14 | `dry_spell_max_length` | Longest dry spell | Severe drought |
| 15 | `wet_spell_frequency` | Number of wet spell events | Flood risk |
| 16 | `wet_spell_max_length` | Longest wet spell | Flood duration |
| 17 | `days_over_precip_thresh` | Days above percentile | Extreme precip |
| 18 | `fraction_over_precip_thresh` | Fraction from extremes | Climate extremes |
| 19 | `rprctot` | Heavy day proportion | Precip concentration |
| 20 | `liquid_precip_ratio` | Rain vs snow | Hydrology |
| 21 | `precip_accumulation` | Accumulated precipitation | Water budget |

### Agricultural Indices (8 indices)
| Index | Function Name | Description | Application |
|-------|--------------|-------------|-------------|
| 22 | `biologically_effective_degree_days` | BEDD for viticulture | Wine grapes |
| 23 | `huglin_index` | Heat for viticulture | Wine regions |
| 24 | `corn_heat_units` | CHU for corn | Corn cultivation |
| 25 | `chill_units` | Winter chill | Fruit trees |
| 26 | `chill_portions` | Dynamic chill model | Orchard mgmt |
| 27 | `latitude_temperature_index` | Lat-temp metric | Crop suitability |
| 28 | `late_frost_days` | Post-season frost | Crop damage |
| 29 | `first_spring_frost` | Last frost date | Planting |

---

## Specialized Indices by Category

### Fire Weather & Drought (12 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 30 | `cffwis_indices` | Canadian Fire Weather Index |
| 31 | `drought_code` | DC - Deep duff dryness |
| 32 | `duff_moisture_code` | DMC - Organic layer |
| 33 | `keetch_byram_drought_index` | KBDI - Soil deficit |
| 34 | `mcarthur_forest_fire_danger_index` | Australian FDI |
| 35 | `griffiths_drought_factor` | Australian drought |
| 36 | `antecedent_precipitation_index` | API - Prior rainfall |
| 37 | `dryness_index` | Budyko dryness |
| 38 | `standardized_precipitation_index` | SPI - Drought |
| 39 | `standardized_precipitation_evapotranspiration_index` | SPEI |
| 40 | `fire_season` | Fire season mask |
| 41 | `water_cycle_intensity` | Precip recycling |

### Snow & Winter Indices (11 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 42 | `days_with_snow` | Snow days |
| 43 | `first_snowfall` | First snow date |
| 44 | `last_snowfall` | Last snow date |
| 45 | `snowfall_frequency` | Snow events |
| 46 | `snowfall_intensity` | Snow per event |
| 47 | `solid_precip_accumulation` | Total snow |
| 48 | `liquid_precip_accumulation` | Total rain |
| 49 | `rain_on_frozen_ground_days` | Rain-on-snow |
| 50 | `daily_freezethaw_cycles` | Freeze-thaw |
| 51 | `freezethaw_spell_frequency` | F-T events |
| 52 | `freezing_degree_days` | FDD ✅ *IMPLEMENTED* |

### Wind Indices (4 indices)
*Requires additional wind speed data*
| Index | Function Name | Description |
|-------|--------------|-------------|
| 53 | `sfcWind_mean` | Mean wind speed |
| 54 | `sfcWind_max` | Max wind speed |
| 55 | `windy_days` | High wind days |
| 56 | `calm_days` | Low wind days |

### Extreme Event Timing (12 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 57-62 | `first_day_t[x/n/g]_above/below` | First occurrences |
| 63-68 | `last_day_t[x/n/g]_above/below` | Last occurrences |
| 69 | `degree_days_exceedance_date` | GDD threshold |
| 70 | `freshet_start` | Spring melt |
| 71 | `frost_free_season_start` | ✅ *IMPLEMENTED* |
| 72 | `frost_free_season_end` | ✅ *IMPLEMENTED* |

### Additional Compound Events (4 indices - partial overlap)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 73 | `cold_and_dry_days` | ✅ *IMPLEMENTED Phase 5* |
| 74 | `cold_and_wet_days` | ✅ *IMPLEMENTED Phase 5* |
| 75 | `warm_and_dry_days` | ✅ *IMPLEMENTED Phase 5* |
| 76 | `warm_and_wet_days` | ✅ *IMPLEMENTED Phase 5* |
| 77 | `high_precip_low_temp` | Rain during cold |
| 78 | `tx_tn_days_above` | Both max/min above |
| 79 | `hot_spell_max_magnitude` | Hot spell intensity |
| 80 | `heat_wave_index` | Combined heat metric |

### Climate Zones & Classifications (4 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 81 | `usda_hardiness_zones` | USDA plant zones |
| 82 | `australian_hardiness_zones` | Australian zones |
| 83 | `rain_season` | Rainy season |
| 84 | `cool_night_index` | Wine classification |

---

## Implementation Priority Matrix

### Immediate Implementation (Low Effort, High Value)
1. ~~**Temperature percentiles**~~ (`tx90p`, `tn90p`, `tx10p`, `tn10p`) ✅ IMPLEMENTED
2. ~~**Spell duration indices**~~ (WSDI, CSDI) ✅ IMPLEMENTED
3. ~~**Growing season metrics**~~ ✅ PARTIALLY IMPLEMENTED
4. **Enhanced precipitation** (`wetdays`, `dry_spell_frequency`) - **NEXT PRIORITY**

### Medium-Term Implementation (Moderate Effort, High Value)
1. **Fire weather indices** - Critical for wildfire management
2. **Agricultural specialties** - Chill portions, Huglin index
3. **Snow metrics** - Water resources (requires snow data)
4. ~~**Compound events**~~ ✅ IMPLEMENTED Phase 5

### Long-Term Implementation (High Effort or Specialized)
1. **Drought indices** (SPI, SPEI) - Require complex baseline
2. **Wind indices** - Need additional wind data
3. **Climate classifications** - Complex zoning
4. **Hydrological indices** - May need water balance data

---

## Data Requirements

### Using Current PRISM Data ✅
Currently implemented indices use:
- **Temperature** (tmax, tmin, tmean): 25 indices
- **Precipitation** (ppt): 10 indices
- **Humidity** (derived): 8 indices
- **Multi-source** (temp + precip): 4 indices + 3 comfort indices

### Additional Data Needed for Expansion
- **Wind**: Surface wind speed (4+ indices)
- **Snow**: Snow depth/SWE (11+ indices)
- **Solar**: Radiation for full ET (some indices)
- **Soil**: For advanced drought indices

---

## Progress Tracking

### Implementation Milestones
- [x] **Phase 1**: Temperature Extensions (19 → 25 indices)
- [x] **Phase 2**: Precipitation WMO Standards (10 indices)
- [x] **Phase 3**: Humidity Integration (8 indices)
- [x] **Phase 4**: Human Comfort (3 indices)
- [x] **Phase 5**: Multivariate Extremes (4 indices)
- [ ] **Phase 6**: Enhanced Precipitation (12 indices) - **PROPOSED**
- [ ] **Phase 7**: Agricultural Specialties (8 indices) - **PROPOSED**
- [ ] **Phase 8**: Fire Weather (requires discussion)

### Current vs Goal
- **Implemented:** 50 / 84 indices (59.5% ✨)
- **Remaining:** 34 indices to reach goal
- **Available in xclim:** 101 additional indices beyond goal

---

## Summary

- **Current Implementation**: 50 of 151 available indices (33.1%)
- **Project Goal**: 84 indices (59.5% complete ✨)
- **High-Priority Additions**: 29 indices recommended
- **Total Potential**: 101 additional indices beyond goal
- **Data Compatibility**: ~100 indices can use existing PRISM data

The extensive xclim library provides tremendous opportunity to enhance our climate analysis capabilities. With Phase 5 complete, we've reached 60% of our 84-index goal, with most remaining indices requiring only moderate implementation effort using our existing pipeline architecture.

## Recent Updates

**2025-10 - Phase 5 Implementation:**
- Added 4 multivariate compound extreme indices
- Implemented baseline percentile system for multivariate thresholds
- Reached 50 total indices (59.5% of 84-index goal)
- Document updated to reflect current implementation status

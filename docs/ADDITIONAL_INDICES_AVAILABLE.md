# Additional Climate Indices Available in xclim

## Overview
xclim provides **151 total climate indicators** in the `xclim.indicators.atmos` module. We currently use **26 indices** across our three pipelines (was 25, now incrementally adding more). This document tracks **125 additional indices** that could be added to enhance our climate analysis capabilities.

## Implementation Progress
✅ **Implemented** (1 new addition)
- `consecutive_frost_days` - Added to temperature pipeline (2025-09-25)

---

## High-Priority Additions (Recommended)

These indices would significantly enhance our current pipelines with minimal additional data requirements:

### Temperature Extremes & Variability (15 indices)
| Index | Function Name | Description | Application |
|-------|--------------|-------------|-------------|
| 1 | `daily_temperature_range` | Mean daily temperature range | Climate variability assessment |
| 2 | `daily_temperature_range_variability` | Variability of daily temperature range | Climate stability |
| 3 | `extreme_temperature_range` | Annual extreme temperature range | Continental climate indicator |
| 4 | `tx90p` | Warm days (>90th percentile) | Heat wave analysis |
| 5 | `tn90p` | Warm nights (>90th percentile) | Urban heat island |
| 6 | `tx10p` | Cool days (<10th percentile) | Cold snap analysis |
| 7 | `tn10p` | Cool nights (<10th percentile) | Frost risk |
| 8 | `cold_spell_duration_index` | CSDI - consecutive cold days | Cold wave assessment |
| 9 | `warm_spell_duration_index` | WSDI - consecutive warm days | Heat wave assessment |
| 10 | `heat_wave_frequency` | Number of heat wave events | Public health planning |
| 11 | `heat_wave_max_length` | Longest heat wave duration | Infrastructure stress |
| 12 | ~~`consecutive_frost_days`~~ | ~~Maximum consecutive frost days~~ | ~~Agricultural planning~~ | ✅ IMPLEMENTED
| 13 | `frost_free_season_length` | Growing season duration | Agriculture |
| 14 | `growing_season_length` | Temperature-based growing season | Crop planning |
| 15 | `thawing_degree_days` | Accumulated temperature >0°C | Permafrost monitoring |

### Enhanced Precipitation Analysis (12 indices)
| Index | Function Name | Description | Application |
|-------|--------------|-------------|-------------|
| 16 | `rx1day` | Max 1-day precipitation (enhanced) | Flood risk |
| 17 | `max_pr_intensity` | Maximum precipitation intensity | Storm severity |
| 18 | `wetdays` | Number of wet days | Water availability |
| 19 | `wetdays_prop` | Proportion of wet days | Climate characterization |
| 20 | `dry_spell_frequency` | Number of dry spell events | Drought monitoring |
| 21 | `dry_spell_max_length` | Longest dry spell | Severe drought |
| 22 | `wet_spell_frequency` | Number of wet spell events | Flood risk periods |
| 23 | `wet_spell_max_length` | Longest wet spell | Flood duration |
| 24 | `days_over_precip_thresh` | Days above percentile threshold | Extreme precipitation |
| 25 | `fraction_over_precip_thresh` | Fraction of precipitation from extreme events | Climate extremes |
| 26 | `rprctot` | Proportion of annual total from heavy days | Precipitation concentration |
| 27 | `liquid_precip_ratio` | Rain vs snow ratio | Hydrological planning |

### Agricultural Indices (8 indices)
| Index | Function Name | Description | Application |
|-------|--------------|-------------|-------------|
| 28 | `biologically_effective_degree_days` | BEDD for viticulture | Wine grape growing |
| 29 | `huglin_index` | Heat accumulation for viticulture | Wine regions |
| 30 | `corn_heat_units` | CHU for corn growing | Corn cultivation |
| 31 | `chill_units` | Winter chill accumulation | Fruit tree dormancy |
| 32 | `chill_portions` | Dynamic chill model | Orchard management |
| 33 | `latitude_temperature_index` | Combined lat-temp metric | Crop suitability |
| 34 | `last_spring_frost` | Date of last spring frost | Planting dates |
| 35 | `late_frost_days` | Frost after growing season start | Crop damage risk |

---

## Specialized Indices by Category

### Fire Weather & Drought (12 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 36 | `cffwis_indices` | Canadian Fire Weather Index System |
| 37 | `drought_code` | DC - Deep duff dryness |
| 38 | `duff_moisture_code` | DMC - Organic layer moisture |
| 39 | `keetch_byram_drought_index` | KBDI - Soil moisture deficit |
| 40 | `mcarthur_forest_fire_danger_index` | Australian fire danger |
| 41 | `griffiths_drought_factor` | Australian drought factor |
| 42 | `antecedent_precipitation_index` | API - Prior rainfall effect |
| 43 | `dryness_index` | Budyko dryness index |
| 44 | `standardized_precipitation_index` | SPI - Drought monitoring |
| 45 | `standardized_precipitation_evapotranspiration_index` | SPEI - Enhanced drought |
| 46 | `fire_season` | Fire season mask |
| 47 | `water_cycle_intensity` | Precipitation recycling |

### Snow & Winter Indices (11 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 48 | `days_with_snow` | Snow days count |
| 49 | `first_snowfall` | First snow date |
| 50 | `last_snowfall` | Last snow date |
| 51 | `snowfall_frequency` | Snow event frequency |
| 52 | `snowfall_intensity` | Snow amount per event |
| 53 | `solid_precip_accumulation` | Total solid precipitation |
| 54 | `liquid_precip_accumulation` | Total liquid precipitation |
| 55 | `rain_on_frozen_ground_days` | Rain-on-snow events |
| 56 | `daily_freezethaw_cycles` | Freeze-thaw transitions |
| 57 | `freezethaw_spell_frequency` | Freeze-thaw spell events |
| 58 | `freezing_degree_days` | FDD - Accumulated cold |

### Wind Indices (4 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 59 | `sfcWind_mean` | Mean surface wind speed |
| 60 | `sfcWind_max` | Maximum wind speed |
| 61 | `windy_days` | Days above wind threshold |
| 62 | `calm_days` | Days with low wind |

### Extreme Event Timing (12 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 63-68 | `first_day_t[x/n/g]_above/below` | First occurrence dates |
| 69-74 | `last_day_t[x/n/g]_above/below` | Last occurrence dates |
| 75 | `degree_days_exceedance_date` | Date reaching GDD threshold |
| 76 | `freshet_start` | Spring melt onset |
| 77 | `frost_free_season_start` | Last spring frost |
| 78 | `frost_free_season_end` | First fall frost |
| 79 | `growing_season_start` | Temperature-based start |
| 80 | `growing_season_end` | Temperature-based end |

### Compound Events (8 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 81 | `cold_and_dry_days` | Combined cold-dry conditions |
| 82 | `cold_and_wet_days` | Combined cold-wet conditions |
| 83 | `warm_and_dry_days` | Combined warm-dry conditions |
| 84 | `warm_and_wet_days` | Combined warm-wet conditions |
| 85 | `high_precip_low_temp` | Rain during cold |
| 86 | `tx_tn_days_above` | Both max and min above threshold |
| 87 | `hot_spell_max_magnitude` | Intensity of hot spells |
| 88 | `heat_wave_index` | Combined heat wave metric |

### Climate Zones & Classifications (4 indices)
| Index | Function Name | Description |
|-------|--------------|-------------|
| 89 | `usda_hardiness_zones` | USDA plant hardiness zones |
| 90 | `australian_hardiness_zones` | Australian plant zones |
| 91 | `rain_season` | Rainy season timing |
| 92 | `cool_night_index` | Wine region classification |

### Additional Statistics (25 indices)
| Category | Indices |
|----------|---------|
| Temperature percentiles | `tg10p`, `tg90p` (2) |
| Temperature counts | `tg_days_above/below`, `tn_days_above/below`, `tx_days_above/below` (6) |
| Temperature extremes | `tg_max/mean/min`, `tn_max/mean`, `tx_mean` (6) |
| Precipitation stats | `precip_accumulation`, `precip_average` (2) |
| Spell statistics | Various spell mean lengths and totals (9) |

---

## Implementation Priority Matrix

### Immediate Implementation (Low Effort, High Value)
1. **Temperature percentiles** (`tx90p`, `tn90p`, `tx10p`, `tn10p`) - Essential for climate extremes
2. **Spell duration indices** (WSDI, CSDI) - Standard ETCCDI indices
3. **Growing season metrics** - Direct agricultural applications
4. **Enhanced precipitation** (`wetdays`, `dry_spell_frequency`) - Water resource planning

### Medium-Term Implementation (Moderate Effort, High Value)
1. **Fire weather indices** - Critical for wildfire management
2. **Agricultural specialties** (chill portions, Huglin index) - Specific crop applications
3. **Snow metrics** - Important for water resources and winter operations
4. **Compound events** - Climate change impact assessment

### Long-Term Implementation (High Effort or Specialized)
1. **Drought indices** (SPI, SPEI) - Require baseline calculations
2. **Wind indices** - Need additional wind data
3. **Climate classifications** - Complex zoning algorithms
4. **Hydrological indices** - May need additional water balance data

---

## Data Requirements

### Using Current PRISM Data
Most indices can be calculated with existing PRISM variables:
- **Temperature indices**: 90+ can use tmax, tmin, tmean
- **Precipitation indices**: 30+ can use daily precipitation
- **Humidity indices**: Limited by VPD and dewpoint availability

### Additional Data Needed
Some indices require data not in standard PRISM:
- **Wind indices**: Surface wind speed data
- **Snow indices**: Snow depth or snow water equivalent
- **Evapotranspiration**: Solar radiation, wind (for Penman-Monteith)
- **Fire indices**: Additional humidity, wind for full FWI system

---

## Usage Example for Adding New Indices

```python
# Example: Adding temperature percentile indices to temperature_pipeline.py

def calculate_enhanced_temperature_indices(self, ds: xr.Dataset) -> dict:
    indices = {}

    # Existing indices...

    # Add percentile-based indices
    if 'tasmax' in ds:
        logger.info("  - Calculating warm days (>90th percentile)...")
        indices['tx90p'] = atmos.tx90p(ds.tasmax, freq='YS')

        logger.info("  - Calculating cool days (<10th percentile)...")
        indices['tx10p'] = atmos.tx10p(ds.tasmax, freq='YS')

    if 'tasmin' in ds:
        logger.info("  - Calculating warm nights (>90th percentile)...")
        indices['tn90p'] = atmos.tn90p(ds.tasmin, freq='YS')

        logger.info("  - Calculating cool nights (<10th percentile)...")
        indices['tn10p'] = atmos.tn10p(ds.tasmin, freq='YS')

    # Add spell duration indices
    if 'tasmax' in ds:
        logger.info("  - Calculating warm spell duration index...")
        indices['wsdi'] = atmos.warm_spell_duration_index(ds.tasmax, freq='YS')

        logger.info("  - Calculating cold spell duration index...")
        indices['csdi'] = atmos.cold_spell_duration_index(ds.tasmax, freq='YS')

    return indices
```

---

## Summary

- **Current Usage**: 25 of 151 available indices (16.6%)
- **High-Priority Additions**: 35 indices recommended for immediate value
- **Total Potential**: 126 additional indices available
- **Implementation Strategy**: Phased approach based on effort and value
- **Data Compatibility**: ~100 indices can use existing PRISM data

The extensive xclim library provides tremendous opportunity to enhance our climate analysis capabilities beyond the current 25 indices, with most additions requiring minimal code changes to our existing pipeline architecture.
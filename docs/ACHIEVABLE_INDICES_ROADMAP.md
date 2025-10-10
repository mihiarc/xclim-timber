# Achievable Indices Roadmap (Data-Constrained)

**Last Updated:** 2025-10-10 (Post Phase 8)
**Current Progress:** 66/80 indices (82.5%)
**Remaining Achievable:** 14 indices

---

## Available Data Sources

**PRISM Zarr Stores (1981-2024, Daily):**
- `/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature`
  - Variables: `tmax`, `tmin`, `tmean` (°C)
- `/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation`
  - Variables: `ppt` (mm/day)
- `/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/humidity`
  - Variables: `tdmean` (dewpoint, °C), `vpdmax`, `vpdmin` (hPa)

**Derived/Computed:**
- Baseline percentiles (10 total): tx90p, tx10p, tn90p, tn10p, pr95p, pr99p, tas_25p, tas_75p, pr_25p, pr_75p

---

## Currently Implemented (66 indices)

### Temperature Pipeline (33 indices) ✅ Phase 7 Complete
**Basic Statistics (5):**
- tg_mean, tx_max, tn_min, daily_temperature_range, extreme_temperature_range

**Threshold Counts (6):**
- frost_days, ice_days, summer_days, hot_days, tropical_nights, consecutive_frost_days

**Frost Season (4):**
- frost_season_length, frost_free_season_start, frost_free_season_end, frost_free_season_length

**Degree Days (4):**
- growing_degree_days, heating_degree_days, cooling_degree_days, freezing_degree_days

**Percentile Extremes (6):**
- tx90p, tx10p, tn90p, tn10p, warm_spell_duration_index, cold_spell_duration_index

### Precipitation Pipeline (13 indices) ✅ Phase 6 Complete
**Basic Statistics (4):**
- prcptot, rx1day, rx5day, sdii

**Consecutive Events (2):**
- cdd, cwd

**Extreme Events (4):**
- r10mm, r20mm, r95p, r99p

**Enhanced Analysis - Phase 6 (3):**
- dry_days, wetdays, wetdays_prop

### Humidity Pipeline (8 indices) ✅
**Dewpoint (4):**
- dewpoint_mean, dewpoint_min, dewpoint_max, humid_days

**VPD (4):**
- vpdmax_mean, vpdmin_mean, extreme_vpd_days, low_vpd_days

### Human Comfort Pipeline (3 indices) ✅
- heat_index, humidex, relative_humidity

### Multivariate Pipeline (4 indices) ✅
- cold_and_dry_days, cold_and_wet_days, warm_and_dry_days, warm_and_wet_days

### Agricultural Pipeline (5 indices) ✅ Phase 8 Complete
**Growing Season (1):**
- growing_season_length

**Water Balance (1):**
- potential_evapotranspiration (Baier-Robertson 1965)

**Crop-Specific (1):**
- corn_heat_units

**Spring Thaw (1):**
- thawing_degree_days

**Seasonal Precipitation (1):**
- growing_season_precipitation

---

## Remaining Achievable Indices (14 estimated)

### ✅ Phase 6: Enhanced Precipitation Analysis - COMPLETE
**Status:** COMPLETED 2025-10-10 | **Implemented:** 3 indices

**Completed Indices:**
1. ✅ `dry_days` - Total number of dry days (pr < 1mm)
2. ✅ `wetdays` - Total number of wet days (pr >= 1mm)
3. ✅ `wetdays_prop` - Proportion of days that are wet

**Notes:**
- Spell frequency indices (counting discrete events) would require custom temporal logic beyond standard xclim functions
- Deferred to future phases if needed
- Focus was on distinct, non-redundant indices using xclim's validated functions

**Data Used:** Only `ppt` ✅
**Baseline Updates:** None required ✅

---

### ✅ Phase 7: Advanced Temperature Extremes - COMPLETE
**Status:** COMPLETED 2025-10-10 | **Implemented:** 8 indices

**Completed Indices:**
1. ✅ `growing_season_start` - First day when temperature exceeds 5°C for 5+ consecutive days (ETCCDI standard)
2. ✅ `growing_season_end` - First day after July 1st when temperature drops below 5°C for 5+ consecutive days
3. ✅ `cold_spell_frequency` - Number of cold spell events (tas < -10°C for 5+ days)
4. ✅ `hot_spell_frequency` - Number of hot spell events (tasmax > 30°C for 3+ days)
5. ✅ `heat_wave_frequency` - Number of heat wave events (tasmin > 22°C AND tasmax > 30°C for 3+ days)
6. ✅ `freezethaw_spell_frequency` - Number of freeze-thaw cycles (tasmax > 0°C AND tasmin ≤ 0°C)
7. ✅ `last_spring_frost` - Last day in spring when tasmin < 0°C (critical for agriculture)
8. ✅ `daily_temperature_range_variability` - Average day-to-day variation in DTR (climate stability)

**Notes:**
- All indices use fixed thresholds (no baseline percentiles required) ✅
- ETCCDI-aligned growing season indices provide agricultural planning value
- Spell frequency indices count discrete events (complementary to existing spell duration indices)
- Temperature variability adds climate stability assessment
- All indices CF-compliant with comprehensive metadata

**Data Used:** `tas`, `tmax`, `tmin` ✅
**Baseline Updates:** None required ✅
**Actual Implementation Time:** ~3 hours

---

### ✅ Phase 8: Growing Season & Agricultural Basics - COMPLETE
**Status:** COMPLETED 2025-10-10 | **Implemented:** 5 indices

**Completed Indices:**
1. ✅ `growing_season_length` - Total days between first and last occurrence of 6+ consecutive days with temperature above 5°C (ETCCDI standard)
2. ✅ `potential_evapotranspiration` - Annual potential evapotranspiration using Baier-Robertson 1965 method (temperature-only)
3. ✅ `corn_heat_units` - Annual accumulated corn heat units for crop development (USDA standard)
4. ✅ `thawing_degree_days` - Sum of degree-days above 0°C (permafrost monitoring, spring melt timing)
5. ✅ `growing_season_precipitation` - Total precipitation during growing season (April-October)

**Notes:**
- All indices use fixed thresholds or simple aggregation (no baseline percentiles required) ✅
- PET uses Baier-Robertson (1965) method - temperature-only, no wind/radiation needed
- CHU is USDA standard widely used in North American agriculture
- Growing season start/end were already implemented in Phase 7
- Created new `agricultural_pipeline.py` for temperature + precipitation integration
- All indices CF-compliant with comprehensive metadata

**Data Used:** `tas`, `tmax`, `tmin`, `ppt` ✅
**Baseline Updates:** None required ✅
**Actual Implementation Time:** ~2 hours

---

### 🟠 Phase 9: Temperature Variability (3-4 indices)
**Priority:** MEDIUM-LOW | **Complexity:** LOW-MEDIUM | **Time:** 2 hours

1. `daily_temperature_range_variability` - Std dev of DTR (climate stability)
2. `temperature_seasonality` - Coefficient of variation of monthly temperatures
3. `diurnal_temperature_range_mean` - Already have daily_temperature_range ✅
4. `extreme_heat_wave_days` - Days in heat waves (may need new baseline threshold)

**Data Required:** `tmax`, `tmin` ✅

---

### 🔴 Phase 10: Specialized Drought Indices (2-3 indices)
**Priority:** LOW | **Complexity:** HIGH | **Time:** 4-6 hours

1. `standardized_precipitation_index (SPI-3)` - Requires statistical distribution fitting (scipy.stats)
2. `standardized_precipitation_index (SPI-6)` - 6-month window
3. `consecutive_dry_days_variability` - Interannual variability of CDD

**Data Required:** `ppt` ✅
**Technical Complexity:** Requires gamma distribution fitting, statistical methods
**Baseline Updates:** May need long-term statistics

**Note:** Full SPEI requires PET which needs wind+solar ❌

---

## What We CANNOT Implement (Missing Required Data)

### ❌ Wind-Dependent Indices (~15 indices)
**Missing:** `sfcWind` (surface wind speed)
- All wind statistics (sfcWind_mean, sfcWind_max, calm_days, windy_days)
- Wind chill indices
- Full fire weather indices (FFMC, ISI, FWI require wind)
- Full evapotranspiration (Penman-Monteith needs wind)

### ❌ Snow Indices (~12 indices)
**Missing:** `prsn` (snowfall), `snd` (snow depth)
- days_with_snow, first_snowfall, last_snowfall
- snowfall_frequency, snowfall_intensity
- solid_precip_accumulation
- rain_on_frozen_ground_days (needs snow)
- freshet_start (spring melt)

### ❌ Solar Radiation Indices (~5 indices)
**Missing:** `rsds` (shortwave radiation), `rlds` (longwave radiation)
- Solar radiation statistics
- Net radiation calculations
- Full PET (FAO-56 Penman-Monteith)

### ❌ Advanced Fire Weather (~8 indices)
**Missing:** Wind + fine-fuel moisture (requires RH at specific times)
- Complete Canadian FWI System (FFMC, DMC, DC, ISI, BUI, FWI)
- Keetch-Byram Drought Index (needs soil moisture or complex modeling)
- McArthur Forest Fire Danger Index

### ❌ Specialized Agricultural (~6 indices)
**Missing:** Solar radiation, specific cultivar parameters
- Huglin Index (viticulture - needs solar radiation)
- Cool Night Index (needs nightly minimums during ripening)
- Biologically Effective Degree Days (BEDD - complex cultivar-specific)
- Corn Heat Units (needs specific tmax/tmin weighting)

---

## Summary: Realistic Achievable Target

| Category | Current | Achievable | Cannot Do | Notes |
|----------|---------|------------|-----------|-------|
| **Temperature** | 33 | +1 | ~5 | Phase 7 complete, most achievable indices done |
| **Precipitation** | 13 | +9 | ~5 | Phase 6 complete, missing snow-related only |
| **Humidity/Comfort** | 11 | +1 | ~2 | Limited by no specific humidity |
| **Multivariate** | 4 | +0 | ~0 | Core compound events done |
| **Agricultural** | 5 | +0 | ~6 | Phase 8 complete, core agricultural indices done |
| **Drought** | 0 | +3 | ~2 | SPI possible, SPEI needs more data |
| **Fire/Wind/Snow** | 0 | +0 | ~40 | All require unavailable data |
| **TOTAL** | **66** | **+14** | **~60** | **Target: 80 indices** |

**Revised Goal:** 80 indices (down from 84)
- **Current:** 66 (82.5% of revised goal)
- **Achievable:** 14 more indices
- **Cannot implement:** ~4 indices from original 84-index goal

**Original 84-index goal breakdown:**
- Assumed we had all standard meteorological variables
- Reality: PRISM provides temperature, precipitation, basic humidity only
- Missing: Wind, snow, solar radiation, specific humidity

---

## Recommended Implementation Order

### ✅ Priority 1: Phase 6 - Enhanced Precipitation (COMPLETE)
**Target:** 50 → 53 indices (66.25% of 80) ✅ ACHIEVED
**Time:** ~2 hours (actual)
**Status:** COMPLETED 2025-10-10
**Outcome:**
- Implemented 3 distinct, non-redundant indices using xclim
- No baseline updates needed ✅
- CF-compliant metadata ✅
- Tested with 2023 data ✅

### ✅ Priority 2: Phase 7 - Advanced Temperature (COMPLETE)
**Target:** 53 → 61 indices (76.25% of 80) ✅ ACHIEVED
**Time:** ~3 hours (actual)
**Status:** COMPLETED 2025-10-10
**Outcome:**
- Implemented 8 indices (spell frequency, growing season timing, temperature variability)
- No baseline updates needed (all fixed thresholds) ✅
- CF-compliant metadata ✅
- Tested with 2023 data ✅
- High climate change monitoring and agricultural value achieved

### ✅ Priority 3: Phase 8 - Growing Season & Agricultural (COMPLETE)
**Target:** 61 → 66 indices (82.5% of 80) ✅ ACHIEVED
**Time:** ~2 hours (actual)
**Status:** COMPLETED 2025-10-10
**Outcome:**
- Implemented 5 agricultural indices (GSL, PET, CHU, TDD, growing season precip)
- No baseline updates needed (all fixed thresholds or simple aggregation) ✅
- CF-compliant metadata ✅
- Tested with 2023 data ✅
- High agricultural decision-making value achieved

### Priority 4: Phase 9 - Temperature Variability (1-3 indices)
**Target:** 66 → 69 indices (86.25% of 80)
**Time:** 1-2 hours
**Justification:**
- Low-medium complexity
- Climate stability metrics
- Note: `daily_temperature_range_variability` already implemented in Phase 7
- Remaining: temperature_seasonality, extreme_heat_wave_days

### Priority 5: Phase 10 - Drought Indices (up to 11 indices)
**Target:** 69 → 80 indices (100% of revised goal!)
**Time:** 4-8 hours
**Justification:**
- High complexity (statistical methods)
- High drought monitoring value
- SPI-3, SPI-6, SPI-12 are most critical
- May implement multiple SPI windows and other drought metrics

---

## Next Action: Phase 9 - Temperature Variability OR Phase 10 - Drought Indices

### Option A: Phase 9 - Temperature Variability (Quick Win)
**Estimated Implementation Time:** 1-2 hours
**Files to Modify:**
- `temperature_pipeline.py` (add 1-3 indices)
- `README.md` (update count: 66 → 69)
- `docs/ACHIEVABLE_INDICES_ROADMAP.md` (this file)
- No baseline updates needed ✅

**Indices to Implement:**
1. `temperature_seasonality` - Coefficient of variation of monthly temperatures
2. `extreme_heat_wave_days` - Days in extreme heat waves (fixed threshold)

**After Phase 9:**
- Progress: 69/80 indices (86.25%)
- Remaining: 11 indices (final push)
- Estimated time to 80 indices: 4-8 hours

### Option B: Phase 10 - Drought Indices (High-Value, Higher Complexity)
**Estimated Implementation Time:** 4-8 hours
**Files to Modify:**
- Create new `drought_pipeline.py` (add 3-11 indices)
- `README.md` (update count: 66 → 77-80)
- `docs/ACHIEVABLE_INDICES_ROADMAP.md` (this file)
- No baseline updates needed (uses internal distribution fitting) ✅

**Priority Indices:**
1. `spi_3` - 3-month Standardized Precipitation Index
2. `spi_6` - 6-month Standardized Precipitation Index
3. `spi_12` - 12-month Standardized Precipitation Index

**After Phase 10:**
- Progress: 80/80 indices (100% of achievable goal!) 🎯
- Complete achievement of data-constrained target

---

## Long-Term: Beyond 80 Indices

**If Additional Data Becomes Available:**

**Priority 1: Wind Data**
- Would unlock: ~15 fire weather + wind indices
- Sources: NCAR reanalysis, ERA5, MERRA-2
- Integration effort: ~8-10 hours

**Priority 2: Snow Data**
- Would unlock: ~12 snow indices
- Sources: SNODAS, ERA5-Land
- Integration effort: ~6-8 hours

**Priority 3: Solar Radiation**
- Would unlock: ~5 radiation + full PET indices
- Sources: NLDAS, ERA5
- Integration effort: ~4-6 hours

**Total Possible with Full Data:** ~100+ indices

---

## Conclusion

**Realistic Goals:**
- **Phase 6 Complete:** 53/80 indices (66.25%) ✅
- **Phase 7 Complete:** 61/80 indices (76.25%) ✅
- **Phase 8 Complete:** 66/80 indices (82.5%) ✅
- **Next milestone:** Phase 9 (Temperature Variability) → 69 indices (86.25%) OR Phase 10 (Drought) → 80 indices (100%)
- **End of 2025:** 80/80 indices (100% of achievable) 🎯
- **With additional data:** 100+ indices possible

**Current Limitation:** PRISM data scope (temperature + precipitation + basic humidity)
**Current Strength:** High-quality, long-term, fine-resolution data for CONUS

**Phase 8 Learnings:**
- Agricultural indices provide high practical value for decision-making
- Baier-Robertson PET method works well with temperature-only data
- Corn Heat Units valuable for North American agriculture (USDA standard)
- Growing season precipitation simple but effective for water availability assessment
- Created separate agricultural_pipeline.py for temperature + precipitation integration
- ~2 hours implementation time for 5 agricultural indices

**Phase 7 Learnings:**
- Fixed-threshold spell frequency indices (cold spell, hot spell, heat wave) complement existing duration indices
- ETCCDI-aligned growing season timing indices provide high agricultural value
- Temperature variability metrics add climate stability assessment
- All 8 indices CF-compliant without requiring baseline percentiles
- ~3 hours implementation time for 8 complex spell and timing indices

**Phase 6 Learnings:**
- Focus on distinct, non-redundant indices using xclim's validated functions
- Avoid semantic duplicates (e.g., CDD already captures max dry spell length)
- Prioritize indices with clear CF-compliant metadata
- ~2 hours implementation time for 3 well-scoped indices

This roadmap focuses on **what's achievable now** rather than aspirational goals requiring unavailable data.

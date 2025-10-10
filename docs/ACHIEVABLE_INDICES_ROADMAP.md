# Achievable Indices Roadmap (Data-Constrained)

**Last Updated:** 2025-10-10 (Post Phase 9)
**Current Progress:** 68/80 indices (85%)
**Remaining Achievable:** 12 indices

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

## Currently Implemented (68 indices)

### Temperature Pipeline (35 indices) ✅ Phase 9 Complete
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

**Advanced Phase 7 (8):**
- growing_season_start, growing_season_end, cold_spell_frequency, hot_spell_frequency, heat_wave_frequency, freezethaw_spell_frequency, last_spring_frost, daily_temperature_range_variability

**Temperature Variability Phase 9 (2):**
- temperature_seasonality, heat_wave_index

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

### ✅ Phase 9: Temperature Variability - COMPLETE
**Status:** COMPLETED 2025-10-10 | **Implemented:** 2 indices

**Completed Indices:**
1. ✅ `temperature_seasonality` - Annual temperature coefficient of variation (standard deviation as percentage of mean) - ANUCLIM/WorldClim BIO4 variable
2. ✅ `heat_wave_index` - Total days that are part of a heat wave (5+ consecutive days with tasmax > 25°C)

**Notes:**
- Both indices use fixed thresholds (no baseline percentiles required) ✅
- `daily_temperature_range_variability` was already implemented in Phase 7
- `temperature_seasonality` is in `xclim.indices` (not `atmos`)
- `heat_wave_index` counts total days (different from `heat_wave_frequency` which counts discrete events)
- All indices CF-compliant with comprehensive metadata

**Data Used:** `tas`, `tmax` ✅
**Baseline Updates:** None required ✅
**Actual Implementation Time:** ~1 hour

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
| **Temperature** | 35 | +0 | ~5 | Phase 9 complete, all achievable temperature indices done |
| **Precipitation** | 13 | +0 | ~5 | Phase 6 complete, missing snow-related only |
| **Humidity/Comfort** | 11 | +0 | ~2 | Complete with available data |
| **Multivariate** | 4 | +0 | ~0 | Core compound events done |
| **Agricultural** | 5 | +0 | ~6 | Phase 8 complete, core agricultural indices done |
| **Drought** | 0 | +12 | ~2 | SPI possible (multiple windows), SPEI needs more data |
| **Fire/Wind/Snow** | 0 | +0 | ~40 | All require unavailable data |
| **TOTAL** | **68** | **+12** | **~60** | **Target: 80 indices** |

**Revised Goal:** 80 indices (down from 84)
- **Current:** 68 (85% of revised goal)
- **Achievable:** 12 more indices (drought focus)
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

### ✅ Priority 4: Phase 9 - Temperature Variability (COMPLETE)
**Target:** 66 → 68 indices (85% of 80) ✅ ACHIEVED
**Time:** ~1 hour (actual)
**Status:** COMPLETED 2025-10-10
**Outcome:**
- Implemented 2 temperature variability indices
- No baseline updates needed (all fixed thresholds) ✅
- CF-compliant metadata ✅
- Tested with 2023 data ✅
- Climate stability and ANUCLIM BIO4 variable now available

### Priority 5: Phase 10 - Drought Indices (up to 12 indices)
**Target:** 68 → 80 indices (100% of revised goal!)
**Time:** 4-8 hours
**Justification:**
- High complexity (statistical methods)
- High drought monitoring value
- SPI-3, SPI-6, SPI-12 are most critical
- May implement multiple SPI windows (1, 3, 6, 12, 24 months)
- Possible additional drought metrics: consecutive_dry_days_variability, etc.

---

## Next Action: Phase 10 - Drought Indices (Final Push to 80!)

### Phase 10: Drought Indices (High-Value Implementation)
**Estimated Implementation Time:** 4-8 hours
**Files to Create/Modify:**
- Create new `drought_pipeline.py` (add 12+ indices)
- `README.md` (update count: 68 → 80)
- `docs/ACHIEVABLE_INDICES_ROADMAP.md` (this file)
- No baseline updates needed (uses internal distribution fitting) ✅

**Priority Indices (in order):**
1. `spi_3` - 3-month Standardized Precipitation Index
2. `spi_6` - 6-month Standardized Precipitation Index
3. `spi_12` - 12-month Standardized Precipitation Index
4. `spi_1` - 1-month SPI (optional)
5. `spi_24` - 24-month SPI (optional)
6. `consecutive_dry_days_variability` - Interannual CDD variability
7. Additional drought metrics as appropriate

**After Phase 10:**
- Progress: 80/80 indices (100% of achievable goal!) 🎯
- Complete achievement of data-constrained target
- Comprehensive drought monitoring capabilities

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
- **Phase 9 Complete:** 68/80 indices (85%) ✅
- **Next milestone:** Phase 10 (Drought Indices) → 80 indices (100%) 🎯
- **End of 2025:** 80/80 indices (100% of achievable) 🎯
- **With additional data:** 100+ indices possible

**Current Limitation:** PRISM data scope (temperature + precipitation + basic humidity)
**Current Strength:** High-quality, long-term, fine-resolution data for CONUS

**Phase 9 Learnings:**
- Temperature seasonality (coefficient of variation) is a valuable ANUCLIM BIO4 variable
- Heat wave index complements existing heat_wave_frequency (total days vs. event count)
- Both indices use fixed thresholds, maintaining simplicity
- `temperature_seasonality` found in `xclim.indices` (not `atmos`)
- ~1 hour implementation time for 2 well-scoped variability indices
- All temperature indices now complete with available PRISM data

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

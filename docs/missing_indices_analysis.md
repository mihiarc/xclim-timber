# Missing Climate Indices Analysis

**Current Progress:** 36 of 84 indices (43% complete)
**Missing:** 48 indices
**Available in xclim:** 168+ atmospheric indicators

---

## Current Implementation (36 indices)

### Temperature Indices (18/84)
✅ Basic stats: tg_mean, tx_max, tn_min
✅ Threshold counts: tropical_nights, frost_days, ice_days, summer_days, hot_days, consecutive_frost_days
✅ Degree days: growing_degree_days, heating_degree_days, cooling_degree_days
✅ Percentile extremes: tx90p, tx10p, tn90p, tn10p, warm_spell_duration_index, cold_spell_duration_index

### Precipitation Indices (10/84)
✅ Basic stats: prcptot, rx1day, rx5day, sdii
✅ Consecutive events: cdd, cwd
✅ Percentile extremes: r95p, r99p
✅ Fixed thresholds: r10mm, r20mm

### Humidity Indices (8/84)
✅ Dewpoint: dewpoint_mean, dewpoint_min, dewpoint_max, humid_days
✅ VPD: vpdmax_mean, vpdmin_mean, extreme_vpd_days, low_vpd_days

---

## Recommended Next Implementation Phases

### 🟢 PHASE 1: Quick Wins - Temperature Extensions (8 indices)
**Complexity:** LOW | **Data Required:** Only tasmax/tasmin (already available)
**Estimated Time:** 1-2 hours implementation + testing

1. **`daily_temperature_range`** - Daily tmax - tmin (basic variability metric)
2. **`extreme_temperature_range`** - Annual max(tmax) - min(tmin) (annual extremes span)
3. **`freezing_degree_days`** - Accumulated temperature below 0°C (winter severity)
4. **`frost_season_length`** - Duration from first to last frost (agricultural planning)
5. **`frost_free_season_start`** - Julian day of last spring frost (planting date)
6. **`frost_free_season_end`** - Julian day of first fall frost (harvest planning)
7. **`frost_free_season_length`** - Days between last spring and first fall frost
8. **`tropical_nights_extent`** - Maximum consecutive tropical nights (heat stress duration)

**Why These?**
- No additional data required (use existing tasmax/tasmin)
- No baseline percentiles needed (simple threshold-based)
- High agricultural/practical value
- Straightforward implementation (similar to existing indices)

**Implementation Pattern:**
```python
# In temperature_pipeline.py
indices['daily_temperature_range'] = atmos.daily_temperature_range(
    tasmin=ds.tasmin, tasmax=ds.tasmax, freq='YS'
)
indices['freezing_degree_days'] = atmos.freezing_degree_days(
    tas=ds.tas, freq='YS'
)
```

---

### 🟡 PHASE 2: Precipitation Extensions (10 indices)
**Complexity:** LOW-MEDIUM | **Data Required:** Only pr (already available)
**Estimated Time:** 2-3 hours implementation + testing

1. **`dry_days`** - Days with pr < 1mm (drought monitoring)
2. **`dry_spell_frequency`** - Number of dry spell events per year
3. **`dry_spell_max_length`** - Longest dry spell duration (drought severity)
4. **`dry_spell_total_length`** - Total days in dry spells (cumulative drought)
5. **`wet_spell_frequency`** - Number of wet spell events per year
6. **`wet_spell_max_length`** - Longest wet spell duration
7. **`daily_pr_intensity`** - Mean precipitation intensity on wet days
8. **`max_pr_intensity`** - Maximum daily precipitation intensity
9. **`precip_accumulation`** - Total annual precipitation (alternative to prcptot)
10. **`rain_season`** - Start/end dates of rainy season

**Why These?**
- Complete the dry/wet spell analysis suite
- No baseline percentiles needed
- High hydrological/agricultural value
- Natural extension of existing CDD/CWD indices

**Implementation Pattern:**
```python
# In precipitation_pipeline.py
indices['dry_spell_max_length'] = atmos.dry_spell_max_length(
    pr=ds.pr, thresh='1 mm/day', window=3, freq='YS'
)
indices['dry_spell_frequency'] = atmos.dry_spell_frequency(
    pr=ds.pr, thresh='1 mm/day', window=3, freq='YS'
)
```

---

### 🟡 PHASE 3: Advanced Temperature (8 indices)
**Complexity:** MEDIUM | **Data Required:** tasmax/tasmin + some require percentiles
**Estimated Time:** 3-4 hours (may need baseline updates)

1. **`cold_spell_days`** - Total days in cold spells (extreme cold exposure)
2. **`cold_spell_frequency`** - Number of cold spell events per year
3. **`cold_spell_max_length`** - Longest cold spell duration
4. **`cold_spell_total_length`** - Total days in cold spells
5. **`warm_spell_days`** - Total days in warm spells (not just duration index)
6. **`warm_spell_frequency`** - Number of warm spell events per year
7. **`first_day_tx_above`** - First day tmax exceeds threshold (spring warming)
8. **`first_day_tn_below`** - First day tmin drops below threshold (fall cooling)

**Why These?**
- Complete the temperature extreme analysis suite
- Some may require updating baseline percentiles
- High value for climate change monitoring
- Builds on existing WSDI/CSDI implementation

**Potential Baseline Update Required:**
May need to add more percentile thresholds to baseline file for spell definitions.

---

### 🟢 PHASE 4: Human Comfort Indices (3 indices)
**Complexity:** LOW-MEDIUM | **Data Required:** tas/tasmax + humidity (all available!)
**Estimated Time:** 2-3 hours

1. **`heat_index`** - Heat index combining temperature and humidity (apparent temperature)
2. **`humidex`** - Canadian humidex index (alternative heat stress metric)
3. **`relative_humidity_from_dewpoint`** - Calculate RH from dewpoint (validation/QC)

**Why These?**
- We already have dewpoint and temperature data!
- High practical value (health, energy demand)
- Well-established formulas
- Can leverage existing humidity pipeline

**Implementation:**
New pipeline: `human_comfort_pipeline.py` or extend humidity pipeline

```python
# Uses temperature + humidity data
indices['heat_index'] = atmos.heat_index(
    tas=temp_ds.tas,
    hurs=humid_ds.hurs,  # or calculate from dewpoint
    freq='YS'
)
indices['humidex'] = atmos.humidex(
    tas=temp_ds.tas,
    tdps=humid_ds.tdew,
    freq='YS'
)
```

---

### 🟡 PHASE 5: Multivariate Indices (4 indices)
**Complexity:** MEDIUM | **Data Required:** Temperature + Precipitation (both available)
**Estimated Time:** 2-3 hours

1. **`cold_and_dry_days`** - Days with low temp AND low precip (compound drought)
2. **`cold_and_wet_days`** - Days with low temp AND high precip (flooding risk)
3. **`warm_and_dry_days`** - Days with high temp AND low precip (drought/fire risk)
4. **`warm_and_wet_days`** - Days with high temp AND high precip (compound extremes)

**Why These?**
- Captures compound climate extremes (increasingly important)
- Uses existing data streams
- High scientific value for climate change research
- Moderate complexity (needs careful threshold definition)

**Implementation Approach:**
New pipeline: `multivariate_pipeline.py` that loads both temp and precip zarr stores

```python
# Load both datasets
temp_ds = xr.open_zarr(temp_zarr_store)
precip_ds = xr.open_zarr(precip_zarr_store)

# Combine and calculate
indices['warm_and_dry_days'] = atmos.warm_and_dry_days(
    tas=temp_ds.tas,
    pr=precip_ds.pr,
    tas_per=90,  # Use 90th percentile for "warm"
    pr_per=25,   # Use 25th percentile for "dry"
    freq='YS'
)
```

---

### 🟠 PHASE 6: Agricultural Indices (5 indices)
**Complexity:** MEDIUM-HIGH | **Some require additional data**
**Estimated Time:** 4-6 hours

**Can Implement Now (temp/precip only):**
1. **`growing_season_length`** - Length of growing season (agricultural planning)
2. **`growing_season_start`** - Start date of growing season
3. **`growing_season_end`** - End date of growing season

**Requires Additional Data/Methods:**
4. **`standardized_precipitation_index (SPI)`** - Drought monitoring (needs statistical distribution fitting)
5. **`potential_evapotranspiration`** - Water balance (Thornthwaite method, needs tas)

**Why These?**
- High agricultural value
- Growing season metrics are straightforward
- SPI/PET require more complex statistical methods

**Implementation Notes:**
- Growing season: Similar to frost-free season but uses different thresholds
- SPI: Requires scipy statistical functions, gamma distribution fitting
- PET: Can use Thornthwaite method with temperature only

---

### 🔴 PHASE 7: Advanced Agricultural (6 indices)
**Complexity:** HIGH | **May require additional data**
**Estimated Time:** 6-8 hours + research

1. **`biologically_effective_degree_days`** - Viticulture-specific GDD variant
2. **`corn_heat_units`** - Corn-specific heat accumulation
3. **`huglin_index`** - Grape variety suitability index
4. **`cool_night_index`** - Viticulture climate classification
5. **`chill_units`** - Winter chill accumulation for fruit trees
6. **`chill_portions`** - Alternative chill model

**Why Later?**
- Highly specialized (viticulture/horticulture specific)
- Complex calculation methods
- May require additional variables (latitude, solar radiation)
- Lower general applicability

---

## Indices We CANNOT Implement (Missing Required Data)

### ❌ Wind Indices (requires sfcWind)
- sfcWind_max, sfcWind_mean, sfcWind_min
- calm_days
- jetstream_metric_woollings

### ❌ Solar Radiation Indices (requires rsds/rlds)
- longwave_upwelling_radiation_from_net_downwelling
- shortwave_radiation metrics

### ❌ Snow Indices (requires prsn/snd)
- days_with_snow
- first_snowfall, last_snowfall
- snowfall_approximation
- solid_precip_accumulation

### ❌ Fire Weather Indices (requires wind, humidity, temp)
- drought_code
- duff_moisture_code
- fire_season
- keetch_byram_drought_index
- mcarthur_forest_fire_danger_index
- griffiths_drought_factor

### ❌ Complex Evapotranspiration (requires wind, solar, humidity)
- reference_evapotranspiration (FAO-56 Penman-Monteith)
- standardized_precipitation_evapotranspiration_index (full SPEI)

---

## Recommended Implementation Order for 48 Missing Indices

### Priority Tier 1: **Quick Wins (18 indices, ~6-8 hours)**
**Target: 36 → 54 indices (64% of goal)**

1. Phase 1: Temperature Extensions (8 indices) - 2 hours
2. Phase 4: Human Comfort (3 indices) - 2 hours
3. Phase 5: Multivariate (4 indices) - 3 hours
4. Quick picks from Phase 2: dry_days, dry_spell_max_length, wet_spell_max_length (3 indices) - 1 hour

**Why This Order?**
- Maximizes indices per hour of work
- Uses only existing data (no new data acquisition)
- High practical/scientific value
- Builds confidence with successful implementations

### Priority Tier 2: **Moderate Effort (20 indices, ~10-12 hours)**
**Target: 54 → 74 indices (88% of goal)**

1. Complete Phase 2: Precipitation Extensions (7 remaining) - 2 hours
2. Phase 3: Advanced Temperature (8 indices) - 4 hours
3. Phase 6: Agricultural Basics (5 indices) - 5 hours

**Why This Order?**
- Completes major index categories
- Still uses only existing data
- May require baseline percentile updates for Phase 3
- Positions project at 88% completion

### Priority Tier 3: **Advanced Work (10 indices, ~8-12 hours)**
**Target: 74 → 84 indices (100% of goal!)**

1. Phase 7: Advanced Agricultural (6 indices) - 6-8 hours
2. Additional specialty indices as needed (4 indices) - 4-6 hours

**Why Last?**
- Highly specialized use cases
- More complex implementations
- May require additional research/validation
- Nice-to-have vs. essential

---

## Summary Statistics

| Category | Implemented | Tier 1 | Tier 2 | Tier 3 | Cannot Do | Total Possible |
|----------|-------------|--------|--------|--------|-----------|----------------|
| Temperature | 18 | +8 | +8 | +2 | 0 | 36 |
| Precipitation | 10 | +3 | +7 | 0 | 5 (snow) | 25 |
| Humidity | 8 | +3 | 0 | 0 | 0 | 11 |
| Multivariate | 0 | +4 | 0 | 0 | 0 | 4 |
| Agricultural | 0 | 0 | +5 | +6 | 0 | 11 |
| Other | 0 | 0 | 0 | +2 | 13 (wind/snow/fire) | 15 |
| **TOTAL** | **36** | **+18** | **+20** | **+10** | **18** | **102** |

**Achievable Goal:** 84 indices (36 current + 48 new)
**Maximum Possible:** 84 indices (limited by available data)

---

## Next Action Recommendation

### 🎯 Start with Phase 1: Temperature Extensions

**Rationale:**
- Fastest implementation (1-2 hours)
- Zero new dependencies
- 8 indices in one go (36 → 44, pushing to 52% complete)
- High confidence success
- Natural extension of existing temperature pipeline

**Implementation Steps:**
1. Create feature branch: `feature/temperature-extensions`
2. Update `temperature_pipeline.py` with 8 new indices
3. Test with 2023 data
4. Update README
5. Merge to main

**After Phase 1 Success:**
→ Phase 4 (Human Comfort) - leverages existing humidity data
→ Phase 5 (Multivariate) - captures compound extremes
→ Continue with Tier 1 to reach 54 indices (64%)

This approach builds momentum with quick wins while working toward the 84-index goal systematically.

# Climate Tools Directory

Utility scripts and tools for climate data processing.

## Core Tools

### `build_indices_zarr.py`
**Purpose:** Build optimized Zarr stores from annual NetCDF files for fast time series extraction.

**Usage:**
```bash
# Build temperature indices Zarr store
python tools/build_indices_zarr.py \
    --input-pattern "outputs/production_v2/temperature/*.nc" \
    --output-zarr "outputs/zarr_stores/temperature_indices.zarr" \
    --pipeline temperature

# Append new year to existing store
python tools/build_indices_zarr.py \
    --append outputs/production_v2/temperature/temperature_indices_2025_2025.nc \
    --zarr-store outputs/zarr_stores/temperature_indices.zarr
```

**Key Features:**
- Optimized chunking for time series access (127x faster than NetCDF)
- Incremental append mode for new years
- Consolidated metadata for fast opening

---

### `extract_from_zarr_fast.py`
**Purpose:** Fast vectorized extraction of climate time series from Zarr stores.

**Usage:**
```bash
# Extract for Pacific Northwest parcels
python tools/extract_from_zarr_fast.py \
    --zarr-store outputs/zarr_stores/temperature_indices.zarr \
    --parcels data/parcel_coordinates.csv \
    --output outputs/extractions/temperature_pacific_northwest.csv

# Extract for Southeast parcels
python tools/extract_from_zarr_fast.py \
    --zarr-store outputs/zarr_stores/temperature_indices.zarr \
    --parcels data/parcel_coordinates_southeast.csv \
    --output outputs/extractions/temperature_southeast.csv
```

**Performance:**
- 24k parcels × 44 years × 35 indices = 57 seconds
- ~18,500 parcel-years/second extraction rate
- Direct wide-format output (no pivot table bottleneck)

---

### `calculate_baseline_percentiles.py`
**Purpose:** Calculate day-of-year percentiles for extreme climate indices (one-time setup).

**Usage:**
```bash
# Calculate baseline percentiles (1981-2000)
python tools/calculate_baseline_percentiles.py

# Output: data/baselines/baseline_percentiles_1981_2000.nc (10.7 GB)
```

**Baseline Indices Calculated:**
- Temperature: tx90p, tx10p, tn90p, tn10p thresholds (365 days × spatial grid)
- Precipitation: r95p, r99p thresholds (365 days × spatial grid)
- Multivariate: compound extremes thresholds (365 days × spatial grid)

**Important Notes:**
- Run once before processing temperature/precipitation pipelines
- Takes ~20-30 minutes to complete
- Results cached for all future pipeline runs

---

### `analyze_climate_trends.py`
**Purpose:** Comprehensive statistical analysis and visualization of climate trends.

**Usage:**
```bash
# Run full analysis (requires extracted data)
python tools/analyze_climate_trends.py
```

**Generates:**
1. **Visualizations (5 high-res PNG files):**
   - `temperature_trends.png` - 44-year temperature trends
   - `extreme_events.png` - Heat waves, frost, extremes
   - `growing_season.png` - Growing season metrics
   - `regional_comparison.png` - Box plot comparisons
   - `climate_change_indicators.png` - ETCCDI standard indices

2. **Reports:**
   - `analysis_summary.txt` - Statistical summary with significance tests
   - Regional climate characteristics
   - Temporal trends with p-values
   - Climate change signals

**Analysis Features:**
- Linear regression for trend detection
- Statistical significance testing (p-values)
- Regional comparison (Pacific Northwest vs Southeast)
- Climate change indicators
- Summary statistics

---

## Workflow

### Standard Processing Workflow

```bash
# 1. One-time setup: Calculate baseline percentiles
python tools/calculate_baseline_percentiles.py

# 2. Run climate indices pipeline (example: temperature)
python temperature_pipeline.py --start-year 1981 --end-year 2024

# 3. Build Zarr store from annual NetCDF files
python tools/build_indices_zarr.py \
    --input-pattern "outputs/production_v2/temperature/*.nc" \
    --output-zarr "outputs/zarr_stores/temperature_indices.zarr" \
    --pipeline temperature

# 4. Extract time series for parcels
python tools/extract_from_zarr_fast.py \
    --zarr-store outputs/zarr_stores/temperature_indices.zarr \
    --parcels data/parcel_coordinates.csv \
    --output outputs/extractions/temperature_pacific_northwest.csv

# 5. Analyze trends and generate visualizations
python tools/analyze_climate_trends.py
```

### Adding New Years

```bash
# 1. Process new year through pipeline
python temperature_pipeline.py --start-year 2025 --end-year 2025

# 2. Append to existing Zarr store
python tools/build_indices_zarr.py \
    --append outputs/production_v2/temperature/temperature_indices_2025_2025.nc \
    --zarr-store outputs/zarr_stores/temperature_indices.zarr

# 3. Re-extract (much faster with Zarr!)
python tools/extract_from_zarr_fast.py \
    --zarr-store outputs/zarr_stores/temperature_indices.zarr \
    --parcels data/parcel_coordinates.csv \
    --output outputs/extractions/temperature_pacific_northwest_updated.csv
```

---

## Performance Benchmarks

| Operation | Traditional (NetCDF) | Zarr-Based | Speedup |
|-----------|---------------------|------------|---------|
| **Store Creation** | N/A (44 files) | 45 seconds | N/A |
| **Extraction (24k parcels)** | ~44 minutes | 57 seconds | **46x** |
| **Extraction (36k parcels)** | ~66 minutes | 83 seconds | **48x** |
| **Annual Update** | Full reprocess | Append + extract | **~100x** |

---

## Dependencies

All tools use:
- xarray (multi-dimensional data)
- pandas (data frames)
- numpy (numerical computing)
- dask (parallel processing)
- zarr (cloud-optimized storage)
- matplotlib/seaborn (visualization - analyze script only)

---

## Output Locations

- **Zarr stores:** `outputs/zarr_stores/`
- **Extractions:** `outputs/extractions/`
- **Baselines:** `data/baselines/`
- **Analysis:** `outputs/analysis/`

---

**Last Updated:** October 13, 2025
**xclim-timber Version:** v5.2

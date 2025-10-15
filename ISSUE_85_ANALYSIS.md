# Issue #85: Deep Analysis - Multivariate Pipeline Empty Tiles

## Executive Summary

The multivariate pipeline generates valid data only for the **northwest (NW) quadrant**. The other 3 quadrants (NE, SW, SE) produce tiles with **zero-length spatial dimensions**, causing tile merge to fail.

**Root Cause**: Coordinate alignment mismatch during xarray operations in `calculate_multivariate_indices()` when working with spatially-sliced baseline percentiles.

## Symptoms

```
NW tile: {'lat': 310, 'lon': 702, 'time': 1}  ✅ Valid (275.9 KB)
NE tile: {'lat': 310, 'lon': 0, 'time': 1}    ❌ Empty lon (16.1 KB)
SW tile: {'lat': 0, 'lon': 0, 'time': 1}      ❌ Empty (13.9 KB)
SE tile: {'lat': 0, 'lon': 0, 'time': 1}      ❌ Empty (13.9 KB)
```

## Investigation Results

### 1. Baseline Array Structure ✅
```python
Dimensions: ('lat', 'lon', 'dayofyear')
Shape: (621, 1405, 366)

# All 4 baseline arrays have this structure:
- tas_25p_threshold: (621, 1405, 366)
- tas_75p_threshold: (621, 1405, 366)
- pr_25p_threshold:  (621, 1405, 366)
- pr_75p_threshold:  (621, 1405, 366)
```

### 2. Baseline Slicing Works Correctly ✅
All 4 tiles slice successfully with proper shapes:
```
NW: (310, 702, 366) ✅
NE: (310, 703, 366) ✅  # lon split: 1405 - 702 = 703
SW: (311, 702, 366) ✅  # lat split: 621 - 310 = 311
SE: (311, 703, 366) ✅
```

All tiles have valid data and correct coordinate ranges.

### 3. Input Dataset Slicing Works Correctly ✅
Temperature/precipitation datasets slice successfully:
```
NW: (365, 310, 702) ✅
NE: (365, 310, 703) ✅
SW: (365, 311, 702) ✅
SE: (365, 311, 703) ✅
```

### 4. The Problem Occurs During Index Calculation ❌

The actual output tiles show:
```
NW: Variables have data (90,054 non-null values)
    lat coords: 310 values [24.06, 36.94]
    lon coords: 702 values [-125.02, -95.81]

NE: Variables are EMPTY
    lat coords: 310 values [24.06, 36.94]  ✅ Same as NW!
    lon coords: 0 values                   ❌ EMPTY!

SW: Variables are EMPTY
    lat coords: 0 values                   ❌ EMPTY!
    lon coords: 0 values                   ❌ EMPTY!

SE: Variables are EMPTY
    lat coords: 0 values                   ❌ EMPTY!
    lon coords: 0 values                   ❌ EMPTY!
```

## SOLUTION IMPLEMENTED ✅

**Root Cause:** Thread-safety race condition in parallel tile processing

The bug was **NOT** a coordinate alignment issue. It was a **race condition** when multiple threads processed tiles in parallel.

### The Problem

In `_process_single_tile()`, the code was modifying the shared instance attribute `self.baselines`:

```python
# BUGGY CODE - causes race condition
original_baselines = self.baselines
self.baselines = tile_baselines_temp  # Thread 1 sets baselines for tile NW
                                       # Thread 2 overwrites with tile NE baselines!
try:
    tile_indices = self.calculate_multivariate_indices(tile_ds)  # Thread 1 uses wrong baselines!
finally:
    self.baselines = original_baselines
```

When 4 threads ran simultaneously:
- Thread A (NW): Set `self.baselines = NW_baselines`
- Thread B (NE): Set `self.baselines = NE_baselines` (overwrites Thread A!)
- Thread C (SW): Set `self.baselines = SW_baselines` (overwrites Thread B!)
- Thread D (SE): Set `self.baselines = SE_baselines` (overwrites Thread C!)
- Thread A: Uses SE_baselines instead of NW_baselines → coordinate mismatch → empty arrays

### The Fix

1. Modified `calculate_multivariate_indices()` to accept baselines as a parameter
2. Removed instance attribute modification
3. Pass tile-specific baselines directly to the function

```python
# FIXED CODE - thread-safe
with self.baseline_lock:
    tile_baselines = {}
    for key, baseline in self.baselines.items():
        tile_baselines[key] = baseline.isel(lat=lat_slice, lon=lon_slice)

# Pass baselines as parameter - no shared state modification
tile_indices = self.calculate_multivariate_indices(tile_ds, baselines=tile_baselines)
```

### Test Results

```
✓ Tile northeast completed successfully
✓ Tile southeast completed successfully
✓ Tile southwest completed successfully
✓ Tile northwest completed successfully
✓ Successfully merged to dimensions: {'time': 1, 'lat': 621, 'lon': 1405}
✓ Output file: 1.26 MB with 4 variables
```

**All tiles now generate valid data!**

## Root Cause Hypothesis (INCORRECT - See Solution Above)

The problem is in `multivariate_pipeline.py:236-244` during the baseline broadcasting step:

```python
def calculate_multivariate_indices(self, ds: xr.Dataset) -> dict:
    # ...

    # Cold and Wet Days calculation:
    tas_25_bcast = tas_25.sel(dayofyear=ds.time.dt.dayofyear).drop_vars('dayofyear')
    pr_75_bcast = pr_75.sel(dayofyear=ds.time.dt.dayofyear).drop_vars('dayofyear')
    cold_days = ds.tas < tas_25_bcast  # <-- COORDINATE ALIGNMENT ISSUE
    wet_days = ds.pr > pr_75_bcast     # <-- COORDINATE ALIGNMENT ISSUE
    cold_wet = (cold_days & wet_days).resample(time='YS').sum()
```

### The Bug Mechanism

1. **NW Tile (Works)**:
   - `tas_25` has coords: lat=[24.06...36.94], lon=[-125.02...-95.81]
   - `ds.tas` has coords: lat=[24.06...36.94], lon=[-125.02...-95.81]
   - Coordinates match exactly → broadcast succeeds ✅

2. **NE Tile (Fails)**:
   - `tas_25` has coords: lat=[24.06...36.94], lon=[-95.77...-66.52]
   - `ds.tas` has coords: lat=[24.06...36.94], lon=[???]
   - **Coordinates don't align properly** → broadcast produces empty result ❌

### Suspected Cause

The issue is likely one of the following:

**Option A: Coordinate Value Mismatch**
- The sliced baseline coordinates and sliced dataset coordinates don't have exact floating-point matches
- xarray's alignment requires exact coordinate matches or will produce empty results

**Option B: Coordinate Object Mismatch**
- The baseline and dataset coordinates may have different coordinate objects (different metadata/attributes)
- xarray may fail to align them during broadcasting

**Option C: Index Ordering Issue**
- The slicing operation may produce coordinates in different orders
- xarray may fail to properly align mis-ordered coordinates

## Comparison with Working Pipelines

**Why do agricultural and human_comfort pipelines work with 4-tile spatial tiling?**

These pipelines DON'T use baseline percentiles with day-of-year indexing. They either:
1. Calculate indices directly from data without baseline thresholds (agricultural)
2. Use simpler baseline percentiles without the complex dayofyear selection (human_comfort)

The multivariate pipeline is **unique** in that it:
- Loads baseline percentiles with 3 dimensions: (lat, lon, dayofyear)
- Spatially subsets the baselines for each tile
- Performs dayofyear-based selection: `.sel(dayofyear=ds.time.dt.dayofyear)`
- Broadcasts the selected baselines against the tile dataset

This complex workflow is where the coordinate alignment fails.

## Recommended Solution

### Fix: Ensure Coordinate Alignment

In `multivariate_pipeline.py:_process_single_tile()`, after slicing the baselines, **explicitly align the coordinates** with the tile dataset:

```python
def _process_single_tile(
    self,
    ds: xr.Dataset,
    lat_slice: slice,
    lon_slice: slice,
    tile_name: str
) -> Dict[str, xr.DataArray]:
    logger.info(f"  Processing tile: {tile_name}")

    # Select spatial subset
    tile_ds = ds.isel(lat=lat_slice, lon=lon_slice)

    # Subset baseline percentiles to match tile (thread-safe)
    with self.baseline_lock:
        tile_baselines_temp = {}
        for key, baseline in self.baselines.items():
            # Slice baseline spatially
            baseline_sliced = baseline.isel(lat=lat_slice, lon=lon_slice)

            # FIX: Explicitly assign tile coordinates to ensure alignment
            baseline_sliced = baseline_sliced.assign_coords({
                'lat': tile_ds.lat,
                'lon': tile_ds.lon
            })

            tile_baselines_temp[key] = baseline_sliced

    # Rest of the function remains the same...
```

This ensures the baseline coordinates **exactly match** the tile dataset coordinates, preventing xarray alignment failures.

### Alternative Fix: Use .reindex() for Explicit Alignment

```python
baseline_sliced = baseline.isel(lat=lat_slice, lon=lon_slice)
baseline_aligned = baseline_sliced.reindex(
    lat=tile_ds.lat,
    lon=tile_ds.lon,
    method='nearest'
)
```

## Testing Strategy

1. Apply the fix to `multivariate_pipeline.py:_process_single_tile()`
2. Run a test with 2023 data and 4 tiles:
   ```bash
   python multivariate_pipeline.py --start-year 2023 --end-year 2023 --n-tiles 4
   ```
3. Verify all 4 tiles have valid dimensions:
   ```bash
   ls -lh outputs/tile_*.nc
   python -c "import xarray as xr; [print(f'{name}: {dict(xr.open_dataset(f\"outputs/tile_{name}.nc\").sizes)}') for name in ['northwest', 'northeast', 'southwest', 'southeast']]"
   ```
4. Verify merged output has correct dimensions: `{lat: 621, lon: 1405, time: 1}`

## Files Involved

- `multivariate_pipeline.py:288-333` (`_process_single_tile`) **← PRIMARY FIX HERE**
- `multivariate_pipeline.py:184-286` (`calculate_multivariate_indices`)
- `core/spatial_tiling.py:63-140` (`_get_spatial_tiles`)
- `core/baseline_loader.py:171-182` (`get_multivariate_baselines`)

## Priority

**High Priority** - This completely blocks the multivariate pipeline, which is a core feature providing 4 compound extreme indices that are scientifically valuable for climate analysis.

## References

- Issue #85: https://github.com/[repo]/issues/85
- Debug scripts created:
  - `debug_issue_85.py`: Comprehensive baseline and dataset slicing tests
  - `debug_tile_contents.py`: Tile file content analysis

# Issue: Drought pipeline SPI calculation deadlocks during compute()

## Problem

The drought pipeline deadlocks during SPI (Standardized Precipitation Index) computation, even with all parallelism disabled.

## Symptoms

- Pipeline hangs indefinitely at "Computing SPI for full..."
- Process shows ~120-140% CPU usage but makes no progress
- No log output after initial "Computing SPI..." message
- Occurs with both threaded and synchronous Dask schedulers
- Occurs with 1, 2, and 4 spatial tiles

## Reproduction

```bash
./scripts/run_production.sh --pipeline drought --start-year 1981 --end-year 1981
```

**Expected**: Complete in reasonable time (similar to precipitation pipeline)
**Actual**: Deadlocks after ~30 seconds at SPI computation step

## Diagnostic Timeline

### Attempt 1: 4 tiles (default)
- **Result**: Hung indefinitely
- **CPU**: 140%
- **Duration**: 10+ minutes with no progress

### Attempt 2: 2 tiles
- **Result**: Hung indefinitely
- **CPU**: 141%
- **Duration**: 10+ minutes with no progress

### Attempt 3: 1 tile (no spatial parallelism)
- **Result**: Hung indefinitely
- **CPU**: 139-146%
- **Duration**: 6+ minutes with no progress

### Attempt 4: 1 tile + synchronous scheduler (no threading at all)
- **Result**: **Still hangs**
- **CPU**: 120%
- **Duration**: 3+ minutes with no progress
- **Code change**: `dask.config.set(scheduler='synchronous')`

## Technical Details

**Location**: `drought_pipeline.py:435-437`

```python
# This code deadlocks:
logger.info(f"    Computing SPI for {tile_name}...")
with dask.config.set(scheduler='synchronous'):  # Also tried 'threads'
    for key in spi_indices.keys():
        spi_indices[key] = spi_indices[key].compute()  # <- HANGS HERE
```

**SPI Calculation**: `drought_pipeline.py:159-167`

```python
spi = atmos.standardized_precipitation_index(
    pr=precip_ds.pr,
    freq='MS',              # Monthly frequency
    window=window,          # 1, 3, 6, 12, 24 months
    dist='gamma',           # Gamma distribution
    method='ML',            # Maximum likelihood fitting
    cal_start='1981-01-01', # 30-year calibration
    cal_end='2010-12-31'
)
```

**Data dimensions**:
- Time: 1981-2010 (30 years, daily → monthly resampling = ~360 months)
- Spatial: 621 × 1405 = 872,505 grid cells
- 5 SPI windows: 1, 3, 6, 12, 24 months

## Why Other Pipelines Work

- **Temperature pipeline**: 35 indices, 4 tiles → ✅ Works perfectly
- **Precipitation pipeline**: 13 indices, 2 tiles → ✅ Works (after fixing threading issue)
- **Drought pipeline**: 12 indices (5 SPI + 7 others), 1 tile → ❌ Deadlocks on SPI

The non-SPI drought indices (dry spell, intensity) are untested but likely work fine.

## Hypothesis

The deadlock appears to be **inside xclim's `standardized_precipitation_index()` function** during gamma distribution fitting (scipy), not in our tiling/threading code.

Possible causes:
1. **Scipy threading issue**: Maximum likelihood gamma fitting may have internal threading conflicts
2. **xclim implementation bug**: SPI computation may not be thread-safe or has circular dependencies
3. **Memory/compute intensity**: 872K pixels × 5 windows × 360 months = massive computation that may exceed some internal limit

## Impact

- **Drought pipeline completely blocked** (0/44 years completed)
- 5 SPI indices are critical for drought monitoring (McKee et al. 1993 standard)
- 7 other drought indices are available but untested

## Workarounds Attempted

1. ✅ Reduce spatial tiles: 4 → 2 → 1
2. ✅ Disable threading: `scheduler='synchronous'`
3. ✅ Remove all parallelism (1 tile + synchronous)
4. ❌ **None worked**

## Next Steps

### Short-term
- Skip SPI indices temporarily
- Ship 7 working drought indices (dry spell, intensity)
- Document known limitation

### Medium-term
- Test with smaller spatial domain to isolate issue
- Try different SPI parameters (different distribution, method)
- Profile with py-spy to identify exact bottleneck

### Long-term
- File bug report with xclim maintainers
- Consider alternative SPI implementation (manual scipy.stats.gamma)
- Investigate chunking strategies for large-scale SPI

## Environment

```bash
# Check versions:
pip show xclim scipy dask xarray
```

**Platform**: Linux 6.14.0-33-generic

## Related Files

- `drought_pipeline.py` (SPI calculation logic)
- `core/spatial_tiling.py` (tiling infrastructure)
- `scripts/lib/pipeline_utils.sh` (default tile configuration)
- Logs: `logs/drought_1981*.log`

## Related Issues

- #69 - Production orchestration system (implemented)
- Threading deadlock fix for precipitation pipeline (resolved by using 2 tiles)

---

**Priority**: High - blocks entire drought pipeline (Phase 10 of climate indices)
**Complexity**: Complex - likely upstream xclim/scipy issue
**Labels**: bug, drought, performance, blocked

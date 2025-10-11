# Temperature Pipeline Performance Anomalies Analysis

**Date:** October 11, 2025
**Pipeline Version:** xclim-timber temperature pipeline v5.0 (Parallel Spatial Tiling)
**Analysis Focus:** Performance anomalies that could indicate logic errors

## Executive Summary

Critical anomalies detected in the temperature pipeline processing that indicate **significant logic errors**:

1. **CRITICAL: Year 2003 has double latitude dimension (1242 vs 621)** - Processing error
2. **CRITICAL: File size jump from ~19MB (1981-2004) to ~30MB (2005-2024)** - Data format change
3. **Year 2005 shows anomalous size (29.58 MB) despite reprocessing** - Possible incomplete fix
4. **Memory usage not being released between years** - Memory leak pattern

## 1. Performance Metrics Summary

### Processing Configuration
- **Temporal chunking:** 1 year per chunk (memory-efficient)
- **Spatial tiling:** 4 quadrants (NW, NE, SW, SE)
- **Parallel processing:** ThreadPoolExecutor with 4 workers
- **Time period:** 1981-2024 (44 years)
- **Indices calculated:** 35 temperature indices

### Average Performance (2008-2021 sample)
- **Processing time per year:** ~2.5-3 minutes
- **Memory usage:**
  - Initial: ~340 MB
  - Final: ~8,300 MB
  - Average increase: ~8,100 MB per year
- **Output file sizes:**
  - 1981-2004: ~18-19 MB
  - 2005-2024: ~29-31 MB

## 2. Critical Anomalies Detected

### 🔴 CRITICAL: Dimensional Inconsistency in Year 2003

**Finding:** Year 2003 has double the latitude dimension (1242 points instead of 621).

```
Year | Lat Dim | Lon Dim | File Size (MB) | Status
-----|---------|---------|----------------|--------
2003 |    1242 |    1405 |          30.73 | ANOMALY
```

**Severity:** CRITICAL
**Impact:** Data corruption, incorrect spatial coverage, invalid statistics
**Likely Cause:**
- Tile merging error during parallel processing
- Duplicate concatenation of north/south tiles
- Race condition in concurrent tile processing

**Evidence of Logic Error:** YES - The doubling suggests tiles were concatenated twice or improper slicing during merge.

### 🔴 CRITICAL: File Size Discontinuity at 2005

**Finding:** Abrupt file size increase starting in 2005:

```
Year Range | Average Size | Variance
-----------|--------------|----------
1981-2004  | 18.66 MB    | ±0.15 MB
2005-2024  | 29.87 MB    | ±0.47 MB
```

**Size increase:** 60% (11.21 MB difference)

**Severity:** CRITICAL
**Possible Causes:**
1. **Data coverage expansion** - More spatial points with valid data
2. **Compression settings changed** - Different zlib compression level
3. **Additional variables** - But variable count shows 35 for all years
4. **Fill value handling** - Change in how NaN values are stored

**Evidence of Logic Error:** MODERATE - Could be legitimate data change or processing difference

### 🟡 WARNING: Year 2005 Anomalous Size Despite Reprocessing

**Finding:** Year 2005 was reprocessed (timestamp Oct 11 11:14) but still shows anomalous size pattern.

```
2004: 18.79 MB (normal)
2005: 29.58 MB (anomalous, reprocessed at 11:14)
2006: 29.69 MB (continues high pattern)
```

**Severity:** MODERATE
**Impact:** Suggests the issue is in the source data or calculation logic, not a transient processing error

### 🟡 WARNING: Memory Not Released Between Years

**Finding:** Memory usage accumulates without proper cleanup:

```
Year | Initial (MB) | Final (MB) | Increase (MB)
-----|--------------|------------|---------------
2008 |        340.6 |     7708.6 |        7368.0
2016 |        342.2 |     9076.0 |        8733.9
```

**Pattern:**
- Initial memory remains constant (~340 MB)
- Final memory increases by ~8 GB per year
- No evidence of memory being released between chunks

**Severity:** MODERATE
**Impact:** Potential for out-of-memory errors on longer runs
**Evidence of Logic Error:** YES - Memory leak or improper cleanup

## 3. Processing Time Analysis

### Consistency Check

Processing times appear relatively consistent (~2.5-3 minutes per year), with no major outliers detected. This suggests:
- The dimensional anomaly in 2003 didn't affect processing time
- The file size change at 2005 didn't impact performance
- Parallel processing is working as expected

## 4. Resource Utilization Analysis

### CPU Utilization
- **Parallel efficiency:** 4 tiles processed concurrently
- **Thread synchronization:** NetCDF write lock prevents concurrent writes
- **Pattern:** Consistent tile processing order (NW → NE → SW → SE)

### Memory Patterns
- **Spike timing:** Memory increases during tile merging phase
- **Peak usage:** ~9 GB for single year processing
- **Concerning pattern:** Memory not fully released between years

### I/O Patterns
- **Read:** Zarr store chunked reading (efficient)
- **Write:** Sequential NetCDF writes with compression (thread-safe)
- **Bottleneck:** Tile merging and final file writing

## 5. Root Cause Analysis

### Year 2003 Dimension Anomaly
**Most likely cause:** Bug in tile merging logic for that specific year
```python
# Lines 632-636 in temperature_pipeline.py
if self.n_tiles == 4:
    # NW + NE = North, SW + SE = South
    north = xr.concat([tile_datasets[0], tile_datasets[1]], dim='lon')
    south = xr.concat([tile_datasets[2], tile_datasets[3]], dim='lon')
    merged_ds = xr.concat([north, south], dim='lat')  # <- Possible duplicate concat
```

### File Size Jump at 2005
**Investigation needed:**
1. Check source data coverage changes
2. Verify NaN percentage in outputs
3. Compare compression ratios
4. Analyze variable data types

### Memory Leak Pattern
**Location:** Likely in tile processing or baseline percentile handling
- Tile datasets not properly closed
- Baseline percentiles being copied instead of referenced
- Dask compute not releasing intermediate results

## 6. Severity Assessment

| Anomaly | Severity | Logic Error? | Business Impact |
|---------|----------|--------------|-----------------|
| Year 2003 dimension doubling | CRITICAL | YES | Invalid results for 2003 |
| File size jump at 2005 | HIGH | MAYBE | Possible data inconsistency |
| Memory not released | MODERATE | YES | Scalability issues |
| Year 2005 reprocessing didn't fix | MODERATE | MAYBE | Persistent data issue |

## 7. Recommendations

### Immediate Actions (Priority 1)

1. **Fix Year 2003 Data**
   ```bash
   # Reprocess year 2003 with additional logging
   python temperature_pipeline.py --start-year 2003 --end-year 2003 --verbose
   ```

2. **Verify Tile Merging Logic**
   - Add dimension checks before concatenation
   - Log tile dimensions at each merge step
   - Implement assertion checks for expected dimensions

3. **Investigate File Size Change**
   ```python
   # Add diagnostic code to check data coverage
   # Compare NaN percentages between 2004 and 2005
   ```

### Short-term Fixes (Priority 2)

1. **Memory Management**
   - Explicitly close tile datasets after merging
   - Use context managers for NetCDF operations
   - Force garbage collection between years

2. **Add Validation Checks**
   ```python
   # After processing each year
   assert ds.dims['lat'] == 621, f"Unexpected lat dimension: {ds.dims['lat']}"
   assert ds.dims['lon'] == 1405, f"Unexpected lon dimension: {ds.dims['lon']}"
   ```

3. **Enhanced Monitoring**
   - Log tile dimensions before and after merge
   - Track memory usage at each processing stage
   - Monitor data coverage statistics

### Long-term Improvements (Priority 3)

1. **Refactor Tile Merging**
   - Use xarray's combine_by_coords for safer merging
   - Implement rollback on merge failure
   - Add comprehensive unit tests

2. **Implement Data Validation Pipeline**
   - Post-processing validation script
   - Automated anomaly detection
   - Data quality metrics tracking

3. **Optimize Memory Usage**
   - Process tiles sequentially if memory is constrained
   - Implement streaming writes for large datasets
   - Use Dask's memory management features

## 8. Validation Script

Create `validate_temperature_outputs.py`:

```python
import xarray as xr
import numpy as np
from pathlib import Path

def validate_file(filepath):
    """Validate a single temperature indices file."""
    with xr.open_dataset(filepath) as ds:
        # Check dimensions
        assert ds.dims['lat'] == 621, f"Invalid lat dimension: {ds.dims['lat']}"
        assert ds.dims['lon'] == 1405, f"Invalid lon dimension: {ds.dims['lon']}"

        # Check for excessive NaN values
        for var in ds.data_vars:
            data = ds[var].values
            nan_pct = np.isnan(data).sum() / data.size * 100
            if nan_pct > 90:
                print(f"WARNING: {var} has {nan_pct:.1f}% NaN values")

        # Check file size
        size_mb = filepath.stat().st_size / (1024 * 1024)
        year = int(filepath.stem.split('_')[2])
        expected_size = 19 if year <= 2004 else 30
        if abs(size_mb - expected_size) > 3:
            print(f"WARNING: Unexpected file size {size_mb:.1f} MB for year {year}")

        return True

# Run validation on all files
output_dir = Path('outputs/production/temperature')
for file in sorted(output_dir.glob('temperature_indices_*.nc')):
    try:
        validate_file(file)
        print(f"✓ {file.name}")
    except AssertionError as e:
        print(f"✗ {file.name}: {e}")
```

## 9. Conclusion

The temperature pipeline has **critical logic errors** that must be addressed:

1. **Year 2003's doubled latitude dimension is a clear processing bug**
2. **The file size discontinuity at 2005 requires investigation**
3. **Memory leak pattern indicates improper resource management**

These issues compromise data integrity and pipeline reliability. Immediate action is required to:
- Reprocess affected years (especially 2003)
- Fix the tile merging logic
- Implement proper validation checks
- Resolve memory management issues

The pipeline's parallel processing design is sound, but the implementation has bugs that create data anomalies. With the recommended fixes, the pipeline can achieve both performance and correctness.

## Appendix: Monitoring Commands

```bash
# Check dimension consistency
for f in outputs/production/temperature/*.nc; do
    ncdump -h "$f" | grep "lat = " | head -1
done | sort | uniq -c

# Monitor memory during processing
python temperature_pipeline.py --start-year 2003 --end-year 2003 --verbose 2>&1 | \
    grep -E "memory|Processing tile|Merging"

# Compare file contents between years
ncdump -v tg_mean outputs/production/temperature/temperature_indices_2004_2004.nc | head -100 > 2004.txt
ncdump -v tg_mean outputs/production/temperature/temperature_indices_2005_2005.nc | head -100 > 2005.txt
diff 2004.txt 2005.txt
```
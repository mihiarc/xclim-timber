# Integration Tests for v5.0 Parallel Spatial Tiling

Comprehensive integration tests for xclim-timber v5.0 parallel spatial tiling functionality.

## Test Suite Overview

### Test Files

1. **test_spatial_tiling_e2e.py** - End-to-end tiling correctness
   - Tile boundary calculation and validation
   - Spatial coverage verification (no gaps, no overlaps)
   - Coordinate correctness
   - Tile ordering for concatenation
   - Configuration validation

2. **test_tile_merge.py** - Tile merging correctness
   - Merged dimension validation
   - Coordinate uniqueness verification
   - Data continuity across tile boundaries
   - Tiling equivalence (tiled vs. non-tiled processing)
   - Edge case handling (odd dimensions, minimal datasets)

3. **test_thread_safety.py** - Concurrent processing validation
   - Parallel tile execution verification
   - NetCDF write lock correctness
   - Baseline lock prevents race conditions
   - Thread-safe file management
   - ThreadPoolExecutor integration

4. **test_error_recovery.py** - Failure scenarios and cleanup
   - Tile cleanup on processing failure
   - Partial tile failure handling
   - Disk space exhaustion handling
   - Dimension mismatch detection
   - Resource exhaustion scenarios

5. **test_temperature_pipeline.py** - Full temperature pipeline integration
   - Complete pipeline execution (35 indices)
   - Data quality validation
   - Spatial tiling integration
   - Output metadata verification
   - Memory efficiency testing

6. **test_precipitation_pipeline.py** - Full precipitation pipeline integration
   - Complete pipeline execution (13 indices)
   - Data quality validation
   - Spatial tiling integration
   - Output metadata verification
   - Data consistency checks

## Running Tests

### Run All Integration Tests
```bash
# Run all integration tests
pytest tests/integration/ -v

# Run with coverage
pytest tests/integration/ --cov=core --cov=temperature_pipeline --cov=precipitation_pipeline --cov-report=html

# Run in parallel (faster)
pytest tests/integration/ -n auto
```

### Run Specific Test Files
```bash
# Spatial tiling tests only
pytest tests/integration/test_spatial_tiling_e2e.py -v

# Tile merge tests only
pytest tests/integration/test_tile_merge.py -v

# Thread safety tests only
pytest tests/integration/test_thread_safety.py -v

# Error recovery tests only
pytest tests/integration/test_error_recovery.py -v

# Temperature pipeline tests only
pytest tests/integration/test_temperature_pipeline.py -v

# Precipitation pipeline tests only
pytest tests/integration/test_precipitation_pipeline.py -v
```

### Run Specific Test Classes
```bash
# Run only tile boundary tests
pytest tests/integration/test_spatial_tiling_e2e.py::TestSpatialTilingBoundaries -v

# Run only merge dimension tests
pytest tests/integration/test_tile_merge.py::TestTileMergeDimensions -v

# Run only thread safety tests
pytest tests/integration/test_thread_safety.py::TestConcurrentTileProcessing -v
```

### Run with Markers
```bash
# Run only slow tests
pytest tests/integration/ -m slow -v

# Skip slow tests
pytest tests/integration/ -m "not slow" -v
```

## Test Fixtures

### conftest.py Fixtures

- **test_zarr_store_temperature** - Session-scoped test Zarr store for temperature data (100x100 grid, 2 years)
- **test_zarr_store_precipitation** - Session-scoped test Zarr store for precipitation data (100x100 grid, 2 years)
- **test_baseline_percentiles** - Session-scoped test baseline percentiles file
- **tmp_output_dir** - Function-scoped temporary output directory
- **mock_pipeline_config** - Mocks PipelineConfig to use test data paths
- **small_test_dataset** - Minimal in-memory test dataset (50x50 grid, 1 year)
- **cleanup_temp_files** - Cleanup fixture for temporary tile files

## Test Data

Test data is generated programmatically with realistic patterns:

### Temperature Data
- 100x100 spatial grid (40-45°N, 120-115°W)
- 730 days (2 years)
- Seasonal temperature cycle
- Realistic tas, tasmax, tasmin relationships

### Precipitation Data
- 100x100 spatial grid (40-45°N, 120-115°W)
- 730 days (2 years)
- 70% dry days, 30% wet days
- Log-normal precipitation distribution

### Baseline Percentiles
- 365 days of year
- Temperature percentiles: tx90p, tx10p, tn90p, tn10p
- Precipitation percentiles: pr95p, pr99p
- Seasonal patterns included

## Success Criteria

All tests should pass with:
- No test failures
- No test errors
- No test warnings (except expected xclim warnings)
- Execution time < 5 minutes for full suite
- Coverage > 80% for core modules

## Expected Test Counts

- **test_spatial_tiling_e2e.py**: ~15 tests
- **test_tile_merge.py**: ~12 tests
- **test_thread_safety.py**: ~10 tests
- **test_error_recovery.py**: ~15 tests
- **test_temperature_pipeline.py**: ~15 tests
- **test_precipitation_pipeline.py**: ~15 tests

**Total: ~82 integration tests**

## Known Issues and Limitations

1. **Test Data Size**: Tests use small datasets (100x100) for speed. Production uses 621x1405.
2. **Baseline Files**: Tests use synthetic baselines, not actual 1981-2000 PRISM data.
3. **Thread Timing**: Thread safety tests may be sensitive to system load and timing.
4. **Memory Tests**: Memory efficiency tests are approximate and system-dependent.

## Troubleshooting

### Tests Fail with "No such file or directory"
- Ensure mock_pipeline_config fixture is used in tests that need test data
- Check that test_zarr_store fixtures are created properly

### Thread Safety Tests Fail Intermittently
- Thread timing is non-deterministic
- Re-run tests to confirm if failures are consistent
- Consider increasing sleep durations in instrumentation

### Memory Tests Fail
- Memory usage varies by system
- Adjust thresholds if necessary
- Run tests in isolation to avoid interference

## Contributing

When adding new integration tests:
1. Use existing fixtures from conftest.py
2. Follow naming conventions: `test_<feature>_<scenario>`
3. Add docstrings explaining what is tested
4. Group related tests in classes
5. Update this README with new test descriptions

## References

- Issue #63: Add Integration Tests for v5.0 Parallel Tiling
- Issue #32: Test Infrastructure
- v5.0 Release: Mandatory Parallel Spatial Tiling

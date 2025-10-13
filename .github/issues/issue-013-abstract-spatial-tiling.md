# Abstract Spatial Tiling to Core Module

**Priority:** Critical | **Estimate:** 3-5 days | **Labels:** refactoring, priority-critical, enhancement

## Description

Extract spatial tiling functionality from `TemperaturePipeline` into a reusable `SpatialTilingMixin` in the core module. This prevents code duplication across 5-6 pipelines that would benefit from parallel spatial processing.

## Problem Statement

The temperature pipeline includes 200+ lines of spatial tiling code (lines 466-679) that:
- Provides 3-4x performance improvement through parallel processing
- Reduces memory usage by processing data in smaller spatial chunks
- Would benefit precipitation, multivariate, drought, human_comfort, and agricultural pipelines

**Without abstraction:** Each pipeline must copy-paste 200 lines of complex tiling logic, leading to:
- Code duplication (200 lines × 5-6 pipelines = 1,000-1,200 duplicate lines)
- Divergent implementations (inconsistent behavior across pipelines)
- Difficult maintenance (bugs must be fixed in multiple places)
- Risk of errors during copy-paste

## Proposed Solution

Create `/home/mihiarc/repos/xclim-timber/core/spatial_tiling.py` with `SpatialTilingMixin` class that provides:

### Core Functionality
1. **Spatial tile calculation** - Split dataset into 2, 4, or 8 tiles
2. **Parallel processing** - ThreadPoolExecutor for concurrent tile processing
3. **Thread-safe NetCDF writes** - Locking mechanism for HDF5 limitation
4. **Tile merging** - Concatenate processed tiles with dimension validation
5. **Resource cleanup** - Automatic cleanup of temporary tile files

### Interface Design

```python
# core/spatial_tiling.py

class SpatialTilingMixin:
    """
    Mixin to add spatial tiling capabilities to climate pipelines.

    Usage:
        class MyPipeline(SpatialTilingMixin, BasePipeline):
            def __init__(self, n_tiles=4, **kwargs):
                super().__init__(**kwargs)
                self.configure_tiling(n_tiles=n_tiles)

            def process_tile(self, tile_ds, tile_context):
                # Implement tile-specific processing
                return self.calculate_indices({'data': tile_ds})

            def prepare_tile_context(self, datasets, lat_slice, lon_slice, tile_name, tile_index):
                # Optional: Add domain-specific data (e.g., baseline subsets)
                context = super().prepare_tile_context(...)
                context['additional_data']['baselines'] = self._subset_baselines(...)
                return context
    """

    def configure_tiling(self, n_tiles: int = 4, tile_mode: str = 'quadrants', ...):
        """Configure spatial tiling parameters."""

    @abstractmethod
    def process_tile(self, tile_ds: xr.Dataset, tile_context: dict) -> dict:
        """Process single tile (implemented by subclass)."""

    def prepare_tile_context(self, datasets, lat_slice, lon_slice, tile_name, tile_index) -> dict:
        """Hook for adding domain-specific data to tile context."""

    def _get_spatial_tiles(self, ds: xr.Dataset) -> list:
        """Calculate tile boundaries."""

    def _calculate_all_indices(self, datasets: dict) -> dict:
        """Override BasePipeline to use spatial tiling."""
```

### Updated Temperature Pipeline

```python
class TemperaturePipeline(SpatialTilingMixin, BasePipeline):
    def __init__(self, n_tiles=4, **kwargs):
        super().__init__(
            zarr_paths={'temperature': PipelineConfig.TEMP_ZARR},
            **kwargs
        )
        self.configure_tiling(n_tiles=n_tiles)
        self.baseline_loader = BaselineLoader()
        self.baselines = self.baseline_loader.get_temperature_baselines()

    def prepare_tile_context(self, datasets, lat_slice, lon_slice, tile_name, tile_index):
        """Add temperature baselines to tile context."""
        context = super().prepare_tile_context(...)
        context['additional_data']['baselines'] = {
            key: baseline.isel(lat=lat_slice, lon=lon_slice)
            for key, baseline in self.baselines.items()
        }
        return context

    def process_tile(self, tile_ds, tile_context):
        """Process single temperature tile."""
        baselines = tile_context['additional_data']['baselines']
        basic = self.calculate_temperature_indices(tile_ds)
        extreme = self.calculate_extreme_indices(tile_ds, baselines)
        advanced = self.calculate_advanced_temperature_indices(tile_ds)
        return {**basic, **extreme, **advanced}

    def postprocess_tile(self, tile_ds):
        """Fix count indices before saving."""
        return self.fix_count_indices(tile_ds)
```

**Result:** Temperature pipeline reduces from 792 lines → ~550 lines (eliminate 240 lines of infrastructure)

## Tasks

- [ ] Create `core/spatial_tiling.py` with `SpatialTilingMixin` class
- [ ] Implement core tiling functionality:
  - [ ] `configure_tiling()` - Configuration
  - [ ] `_get_spatial_tiles()` - Tile boundary calculation
  - [ ] `_process_and_save_tile()` - Thread-safe tile processing and saving
  - [ ] `_merge_tiles()` - Tile concatenation and validation
  - [ ] `_calculate_all_indices()` - Override BasePipeline for tiling workflow
- [ ] Add abstract method `process_tile()` for subclass implementation
- [ ] Add hook method `prepare_tile_context()` for domain-specific data
- [ ] Add optional hook `postprocess_tile()` for tile post-processing
- [ ] Refactor `TemperaturePipeline` to use `SpatialTilingMixin`
- [ ] Test refactored temperature pipeline:
  - [ ] 2-tile mode (east/west)
  - [ ] 4-tile mode (quadrants)
  - [ ] Verify output matches original
  - [ ] Benchmark performance (maintain 3-4x speedup)
  - [ ] Test thread safety (concurrent processing)
- [ ] Update documentation:
  - [ ] Add docstrings to all methods
  - [ ] Create usage examples
  - [ ] Update REFACTORING_PROGRESS.md

## Acceptance Criteria

- [ ] `SpatialTilingMixin` implemented in `core/spatial_tiling.py` (~200-250 lines)
- [ ] TemperaturePipeline refactored to use mixin (~550 lines, down from 792)
- [ ] All existing tests pass (temperature indices calculation unchanged)
- [ ] Performance maintained (3-4x speedup with spatial tiling)
- [ ] Thread safety verified (no race conditions)
- [ ] Dimension validation working correctly
- [ ] Temporary tile files cleaned up properly
- [ ] Documentation complete with usage examples

## Benefits

1. **Code Reuse**: Eliminate 1,000-1,200 lines of duplicate code across 5-6 pipelines
2. **Consistency**: All pipelines use same tiling algorithm and error handling
3. **Maintainability**: Fix bugs in one place, benefit all pipelines
4. **Flexibility**: Easy to add new tiling strategies (8-tile, adaptive)
5. **Testing**: Test tiling infrastructure once, not per pipeline
6. **Performance**: Optimize core infrastructure benefits all pipelines
7. **Scalability**: Enables efficient processing of large datasets across all pipelines

## Pipelines That Will Benefit

1. **precipitation_pipeline.py** (630 lines) - HIGH priority
   - 13 indices, similar complexity to temperature
   - Uses baseline percentiles (pr95p, pr99p)
   - Memory pressure for large time ranges

2. **multivariate_pipeline.py** (593 lines) - VERY HIGH priority
   - Processes multiple datasets simultaneously
   - Highest memory footprint of all pipelines

3. **drought_pipeline.py** (714 lines) - HIGH priority
   - Multiple variables, high memory usage
   - SPI calculations over multiple time windows

4. **human_comfort_pipeline.py** (502 lines) - HIGH priority
   - Multivariate (temperature + humidity)

5. **agricultural_pipeline.py** (505 lines) - HIGH priority
   - Multivariate (temperature + precipitation)

6. **humidity_pipeline.py** (430 lines) - MEDIUM priority
   - Simpler pipeline, tiling optional but beneficial

## Dependencies

- **Requires**: #1 (BasePipeline), #2 (Core infrastructure), #3 (Temperature refactor)
- **Blocks**: #4-9 (Remaining pipeline refactors - should use SpatialTilingMixin)

## Related Issues

- Implements recommendation from Issue #3 architectural review
- Enables efficient refactoring of Issues #4-9
- Supports Issue #10 (integration testing)

## Technical Notes

### Design Patterns Used
- **Mixin Pattern**: Add tiling capability via multiple inheritance
- **Template Method**: BasePipeline defines workflow, mixin overrides `_calculate_all_indices()`
- **Hook Methods**: `prepare_tile_context()` and `postprocess_tile()` for customization
- **Strategy Pattern** (future): Support multiple tiling strategies

### Performance Characteristics
- **Speedup**: 3-4x faster than sequential processing (verified)
- **Memory**: Processes 1/N of dataset at a time (N = number of tiles)
- **Parallelism**: Uses all available CPU cores with ThreadPoolExecutor
- **I/O**: Thread-safe NetCDF writes with lock (HDF5 limitation)

### Testing Strategy
```bash
# Test refactored temperature pipeline
python temperature_pipeline.py --start-year 2023 --end-year 2023 --n-tiles 4

# Verify output matches original
python tests/validate_refactored_pipeline.py \
    temperature_pipeline.py.backup-refactor \
    temperature_pipeline.py \
    --year 2023

# Benchmark performance
python tests/benchmark_tiling.py --pipeline temperature --tiles 2,4
```

## Risk Assessment

**Risk Level**: Medium

**Risks**:
1. **Complexity**: Multiple inheritance and mixins add cognitive load
   - *Mitigation*: Clear documentation, examples, comprehensive tests

2. **Breaking Changes**: Refactoring might introduce bugs
   - *Mitigation*: Test suite, gradual migration, backups

3. **Performance Regression**: Abstraction overhead might slow processing
   - *Mitigation*: Benchmark before/after, profile hot paths

4. **Thread Safety**: Concurrent tile processing could cause race conditions
   - *Mitigation*: Comprehensive thread safety tests, lock mechanisms

## Estimated Effort

- **Core module implementation**: 2 days (SpatialTilingMixin, ~250 lines)
- **Temperature refactor**: 1 day (update to use mixin)
- **Testing and validation**: 1 day (test suite, benchmarks)
- **Documentation**: 0.5 days (docstrings, usage examples)

**Total**: 3-5 days

## Priority Justification

**CRITICAL** priority because:
1. **Blocks all remaining pipeline refactors** (Issues #4-9)
2. **Prevents 1,000+ lines of code duplication**
3. **Must be done before refactoring other pipelines**
4. **Architectural debt if postponed** (difficult to fix after copy-paste)

## Success Metrics

- [ ] Code reduction: ~240 lines eliminated from temperature pipeline
- [ ] Performance maintained: 3-4x speedup preserved
- [ ] Test coverage: 100% of SpatialTilingMixin methods tested
- [ ] Documentation: Complete with usage examples for all pipelines
- [ ] Adoption: 5-6 pipelines successfully use SpatialTilingMixin after refactoring

---

**References:**
- Architectural review: ULTRATHINK analysis (2025-10-13)
- Original implementation: `temperature_pipeline.py` lines 466-679
- Pattern: Template Method + Mixin

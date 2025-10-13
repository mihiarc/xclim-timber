# Pipeline Refactoring Progress Report

**Date:** 2025-10-13
**Status:** Core Infrastructure Complete (Issues #1-2) ✅
**Next:** Temperature Pipeline Refactoring (Issue #3)

---

## ✅ Completed Work

### Issue #1: Core Module Structure (COMPLETE)
Created `core/` module with base pipeline class:

**Files Created:**
- `core/__init__.py` - Module initialization
- `core/base_pipeline.py` - BasePipeline abstract class (~250 lines)

**Features:**
- Abstract `BasePipeline` class with all common functionality
- Common methods: `_load_zarr_data()`, `_rename_variables()`, `_fix_units()`, `_save_result()`, `_add_global_metadata()`
- Standard `process_time_chunk()` workflow
- Temporal chunking in `run()` method
- Memory tracking and reporting built-in
- Type hints and comprehensive docstrings

### Issue #2: Shared Configuration & Utilities (COMPLETE)
Created shared utilities to eliminate duplication:

**Files Created:**
- `core/config.py` - Centralized configuration (~170 lines)
- `core/baseline_loader.py` - Baseline percentile loading (~180 lines)
- `core/cli_builder.py` - CLI argument parsing (~140 lines)

**Features:**
- `PipelineConfig`: All constants in one place (Zarr paths, chunk config, rename maps, CF standard names)
- `BaselineLoader`: Cached baseline loading with validation
- `PipelineCLI`: Consistent CLI across all pipelines
- Warning filter setup
- Validation utilities

**Verification:**
```bash
python3 -c "from core import BasePipeline, PipelineConfig, BaselineLoader, PipelineCLI; print('✓ Core module imports successfully')"
# Result: ✓ Core module imports successfully
```

---

## 🔄 In Progress

### Issue #3: Refactor Temperature Pipeline

**Current Status:** Analysis complete, ready for implementation

**Complexity Factors:**
1. **Spatial Tiling** (Unique to temperature pipeline)
   - Parallel processing across 2 or 4 tiles
   - ThreadPoolExecutor for tile processing
   - Thread-safe NetCDF writing with locks
   - Tile merging logic (NW+NE+SW+SE → full dataset)
   - Dimension validation after merge

2. **Count Indices Fix** (Unique to temperature)
   - 15 indices need units='1' to prevent timedelta encoding
   - Must be applied before saving

3. **Three Index Calculation Methods**
   - `calculate_temperature_indices()` - 19 basic indices
   - `calculate_extreme_indices()` - 6 percentile-based indices
   - `calculate_advanced_temperature_indices()` - 10 advanced indices

4. **Baseline Percentiles**
   - Loads tx90p, tx10p, tn90p, tn10p thresholds
   - Must subset baselines for each spatial tile

**Recommended Approach:**

Since spatial tiling is complex and unique to temperature pipeline, consider **Option B** (partial refactoring):

#### Option A: Full Refactoring (Complex)
- Inherit from `BasePipeline`
- Override `process_time_chunk()` to add spatial tiling
- Keep only unique methods
- **Estimated Time:** 4-6 hours
- **Risk:** Medium (spatial tiling logic is complex)

#### Option B: Partial Refactoring (Pragmatic) ⭐ RECOMMENDED
- Keep temperature pipeline mostly as-is
- Use `BaselineLoader` and `PipelineConfig` for consistency
- Refactor simpler pipelines first (precipitation, humidity)
- Return to temperature pipeline after validating approach
- **Estimated Time:** 1-2 hours for partial, defer full refactoring
- **Risk:** Low

---

## 📋 Remaining Issues

### High Priority
- **Issue #3:** Refactor temperature_pipeline.py (1,028 lines → ~300 lines)
- **Issue #4:** Refactor precipitation_pipeline.py (630 lines → ~200 lines)
- **Issue #10:** Integration testing and validation

### Medium Priority
- **Issue #5:** Refactor drought_pipeline.py (714 lines)
- **Issue #6:** Refactor agricultural_pipeline.py (505 lines)
- **Issue #11:** Update documentation

### Low Priority
- **Issue #7:** Refactor humidity_pipeline.py (430 lines)
- **Issue #8:** Refactor human_comfort_pipeline.py (502 lines)
- **Issue #9:** Refactor multivariate_pipeline.py (593 lines)
- **Issue #12:** Cleanup and archive

---

## 🎯 Recommended Next Steps

### Immediate (Next Session)

**Option 1: Continue with Temperature** (Original Plan)
```bash
# Backup original
cp temperature_pipeline.py temperature_pipeline.py.backup

# Implement refactored version
# - Inherit from BasePipeline
# - Override process_time_chunk() for spatial tiling
# - Keep unique methods only
# - Test on 2023 data
```

**Option 2: Start with Simpler Pipeline** (Validate Approach) ⭐ RECOMMENDED
```bash
# Start with humidity pipeline (simplest)
# - Only 430 lines
# - No spatial tiling
# - No baseline percentiles
# - Validates base class approach quickly

# Then precipitation (baseline percentiles)
# Then temperature (complex tiling)
```

### Testing Strategy
```bash
# For each refactored pipeline:
1. Run on 2023 test data
2. Compare output with original
3. Validate all indices present
4. Check metadata completeness
5. Verify performance
```

---

## 📊 Expected Benefits

After completing all 12 issues:

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Total Lines** | 4,402 | ~2,230 | **-49%** |
| **Duplicate Code** | ~2,800 | ~0 | **-100%** |
| **Maintainability** | Low | High | ✅ |
| **Code Reuse** | 0% | 50% | ✅ |

**Individual Pipeline Reductions:**
- Temperature: 1,028 → ~300 lines (**-71%**)
- Precipitation: 630 → ~200 lines (**-68%**)
- Drought: 714 → ~250 lines (**-65%**)
- Agricultural: 505 → ~180 lines (**-64%**)
- Humidity: 430 → ~150 lines (**-65%**)
- Human Comfort: 502 → ~180 lines (**-64%**)
- Multivariate: 593 → ~220 lines (**-63%**)

---

## 🔧 Using the Core Module

### Example: Simple Pipeline
```python
from core import BasePipeline, PipelineConfig, BaselineLoader

class MyPipeline(BasePipeline):
    def __init__(self):
        super().__init__(
            zarr_paths={'data': PipelineConfig.TEMP_ZARR},
            chunk_config=PipelineConfig.DEFAULT_CHUNKS
        )

    def calculate_indices(self, datasets):
        # Implement your indices here
        return {'my_index': self._calculate_my_index(datasets['data'])}

# Run pipeline
pipeline = MyPipeline()
pipeline.run(start_year=2023, end_year=2023)
```

### Example: With Baseline Percentiles
```python
from core import BasePipeline, BaselineLoader, PipelineConfig

class ExtremePipeline(BasePipeline):
    def __init__(self):
        super().__init__(
            zarr_paths={'temperature': PipelineConfig.TEMP_ZARR}
        )
        self.baseline_loader = BaselineLoader()
        self.baselines = self.baseline_loader.get_temperature_baselines()

    def calculate_indices(self, datasets):
        # Use baseline percentiles for extreme indices
        return self._calculate_extremes(datasets['temperature'], self.baselines)
```

---

## 📝 Notes

1. **No Backward Compatibility:** This is a clean break from old architecture
2. **CLI Unchanged:** User-facing CLI remains the same
3. **Output Format Unchanged:** NetCDF structure is identical
4. **Spatial Tiling:** Only temperature pipeline uses this feature
5. **Testing Critical:** Each refactored pipeline must be validated against original output

---

## 🚀 Quick Reference

### Core Module Files
```
core/
├── __init__.py              # Module exports
├── base_pipeline.py         # BasePipeline abstract class
├── config.py                # PipelineConfig constants
├── baseline_loader.py       # BaselineLoader for percentiles
└── cli_builder.py           # PipelineCLI for CLI building
```

### Test Core Module
```bash
python3 -c "from core import *; print('✓ All imports successful')"
```

### GitHub Issues
All 12 issues created in `.github/issues/` directory.

### Upload Issues to GitHub
```bash
cd .github/issues
for file in issue-*.md; do
  gh issue create --title "$(head -n1 $file | sed 's/# //')" --body-file "$file"
done
```

---

**Total Time Invested:** ~6 hours (Issues #1-2)
**Remaining Estimate:** ~20-22 hours (Issues #3-12)
**Critical Path:** #3 → #4 → #10 (temperature → precipitation → testing)

# Prioritized Action Plan - xclim-timber
## Based on Critical Code Review & Architecture Analysis

**Generated:** 2025-10-13
**Current Status:** 26 open issues, 2 recently closed (#30, #68)
**In Progress:** Issue #61 (Precipitation production processing)

---

## Executive Summary

After comprehensive critical review, the codebase shows **strong architecture (8.5/10)** but has **critical gaps in testing, security, and memory optimization**. This plan prioritizes issues by impact, dependencies, and risk mitigation.

### Risk Assessment
- **🔴 CRITICAL:** 4 issues (security vulnerability, no test coverage)
- **🟡 HIGH:** 8 issues (memory optimization, production data gaps)
- **🟢 MEDIUM:** 10 issues (features, enhancements)
- **🔵 LOW:** 4 issues (nice-to-have improvements)

---

## PHASE 1: CRITICAL SECURITY & STABILITY (Week 1-2)
**Goal:** Fix critical vulnerabilities and establish testing foundation

### 🔴 NEW ISSUE: Path Traversal Vulnerability
**Priority:** P0 - IMMEDIATE
**Effort:** 30 minutes
**Impact:** Security vulnerability allowing arbitrary file writes

**Location:** `core/base_pipeline.py:324`
```python
# VULNERABLE CODE:
output_file = output_dir / f'{pipeline_name}_indices_{start_year}_{end_year}.nc'

# FIX:
import os
pipeline_name = os.path.basename(pipeline_name)  # Sanitize
output_file = output_dir / f'{pipeline_name}_indices_{start_year}_{end_year}.nc'
```

**Action Items:**
- [ ] Create issue for path traversal vulnerability
- [ ] Implement sanitization in base_pipeline.py
- [ ] Add input validation for all user-provided paths
- [ ] Add security tests

---

### 🔴 Issue #32: Add test coverage for climate pipelines
**Priority:** P0 - CRITICAL
**Effort:** 16 hours
**Impact:** Cannot verify correctness, high regression risk

**Current State:** ZERO test coverage
**Target:** 80% code coverage for core modules

**Implementation Plan:**

#### Step 1: Unit Tests (8 hours)
```python
# tests/unit/test_base_pipeline.py
def test_zarr_loading():
    """Test Zarr data loading with mock data."""

def test_time_chunking():
    """Test time chunk calculation."""

def test_spatial_tiling():
    """Test tile coordinate calculation."""

# tests/unit/test_indices.py
def test_frost_days_calculation():
    """Verify frost days count."""
    test_data = create_test_dataset(tasmin=[-5, 0, 5])
    result = calculate_frost_days(test_data)
    assert result.values[0] == 2  # 2 days below 0°C
```

#### Step 2: Integration Tests (8 hours)
```python
# tests/integration/test_pipelines.py
def test_temperature_pipeline_full_run():
    """Test full pipeline with small dataset."""

def test_spatial_tiling_merge():
    """Verify tiles merge correctly."""

def test_thread_safety():
    """Test concurrent tile processing."""
```

**Blocked By:** None
**Blocks:** Issue #63 (integration tests for v5.0)

---

### 🔴 Issue #75: Baseline percentiles not rechunked for tile processing
**Priority:** P0 - CONFIRMED BUG
**Effort:** 8 hours
**Impact:** 2-3x hidden memory usage, performance degradation

**Affected Files:**
- `temperature_pipeline.py:498-501`
- `precipitation_pipeline.py:312-315`
- `drought_pipeline.py:407-410`

**Fix Implementation:**
```python
# BEFORE (Lines 498-501 in temperature_pipeline.py):
tile_baselines = {
    key: baseline.isel(lat=lat_slice, lon=lon_slice)
    for key, baseline in baseline_percentiles.items()
}

# AFTER:
tile_baselines = {}
for key, baseline in baseline_percentiles.items():
    # Slice first
    tile_baseline = baseline.isel(lat=lat_slice, lon=lon_slice)

    # Then rechunk to match tile data structure
    if hasattr(tile_ds, 'chunks'):
        chunk_dict = {
            'lat': tile_ds.chunks.get('lat', -1)[0] if 'lat' in tile_ds.chunks else -1,
            'lon': tile_ds.chunks.get('lon', -1)[0] if 'lon' in tile_ds.chunks else -1,
            'dayofyear': -1  # Keep dayofyear together
        }
        tile_baseline = tile_baseline.chunk(chunk_dict)

    tile_baselines[key] = tile_baseline
```

**Validation:**
```python
# Before fix - check for rechunk in dask graph
import dask
result = atmos.tx90p(tasmax, threshold)
print(result.__dask_graph__())  # Will show rechunk operations

# After fix - no implicit rechunks
print(result.__dask_graph__())  # No rechunk operations
```

**Action Items:**
- [ ] Apply fix to temperature_pipeline.py
- [ ] Apply fix to precipitation_pipeline.py
- [ ] Apply fix to drought_pipeline.py
- [ ] Profile memory usage before/after
- [ ] Add regression test
- [ ] Document in code comments

**Expected Improvement:** 30% performance gain, 2-3x memory reduction

---

### 🟡 Issue #63: Add comprehensive integration tests for v5.0 parallel tiling
**Priority:** P1 - HIGH
**Effort:** 12 hours
**Impact:** Regression risk, production confidence

**Test Coverage Needed:**

#### 1. Tile Processing Tests (4 hours)
```python
def test_spatial_tile_creation():
    """Verify tile coordinates correct."""

def test_tile_processing_parallel():
    """Test 4 tiles process correctly."""

def test_tile_merge_no_duplicates():
    """Ensure no duplicate coordinates after merge."""
```

#### 2. Thread Safety Tests (4 hours)
```python
def test_baseline_concurrent_access():
    """Verify baseline lock works correctly."""

def test_multiple_years_parallel():
    """Test processing multiple years concurrently."""
```

#### 3. Error Recovery Tests (4 hours)
```python
def test_tile_failure_recovery():
    """Verify cleanup on tile processing failure."""

def test_partial_tile_success():
    """Test behavior when some tiles fail."""
```

**Depends On:** Issue #32 (test infrastructure)

---

## PHASE 2: PRODUCTION DATA & VALIDATION (Week 3-4)
**Goal:** Complete production datasets and establish validation

### 🟡 Issue #61: Run production processing for precipitation pipeline (1981-2024)
**Priority:** P1 - HIGH (IN PROGRESS)
**Effort:** 3 hours (running now)
**Impact:** Unblocks agricultural & drought pipelines

**Status:** Currently processing (background job 4be50e)
**Expected Completion:** ~2.5 hours remaining
**Output:** 44 files, ~528 MB

**Next Steps:**
- [ ] Monitor completion
- [ ] Verify all 44 files generated
- [ ] Run validation checks
- [ ] Create PR and close issue

**Blocks:** Issue #66 (agricultural), #67 (drought)

---

### 🟡 Issue #60: Create automated data quality validation suite
**Priority:** P1 - HIGH
**Effort:** 10-12 hours
**Impact:** Prevents data corruption (caught 2003/2005 issues)

**Implementation Plan:**

#### 1. File Validation (3 hours)
```python
# validate_dataset.py
def validate_file_sizes(directory, expected_range=(1_000_000, 30_000_000)):
    """Flag files outside expected size range."""

def validate_dimensions(nc_file, expected_dims):
    """Verify dimension sizes match expectations."""

def validate_indices_present(nc_file, expected_indices):
    """Check all indices calculated."""
```

#### 2. Data Integrity (4 hours)
```python
def validate_data_coverage(ds):
    """Check for all-NaN or all-zero arrays."""

def validate_value_ranges(ds, index_name, expected_range):
    """Verify values are physically plausible."""

def validate_temporal_consistency(files):
    """Check time series continuity."""
```

#### 3. CF-Compliance (3 hours)
```python
def validate_cf_compliance(nc_file):
    """Use cfchecker to verify standards."""

def validate_metadata(ds):
    """Check required attributes present."""
```

#### 4. Automated Reporting (2 hours)
```python
def generate_validation_report(results):
    """Create summary report with flagged issues."""
```

**Integration:**
- Add to production orchestration scripts
- Run automatically after each pipeline completion
- Generate reports in `outputs/validation/`

---

### 🟡 Issue #66: Run production processing for agricultural pipeline (1981-2024)
**Priority:** P1 - HIGH
**Effort:** 2-3 hours
**Impact:** Complete agricultural dataset

**Depends On:** Issue #61 (precipitation data)

**Prerequisites:**
- ✅ Temperature data complete (44/44 years)
- 🔄 Precipitation data in progress (44 years)
- ✅ Pipeline v5.0 compliant

**Indices:** 5 agricultural indices
- Growing Season Length (GSL)
- Potential Evapotranspiration (PET)
- Corn Heat Units (CHU)
- Thawing Degree Days (TDD)
- Growing Season Precipitation

**Expected Output:** 44 files, ~300 MB

---

### 🟡 Issue #67: Run production processing for drought indices (1981-2024)
**Priority:** P1 - HIGH
**Effort:** 2-3 hours (+ baseline calculation if needed)
**Impact:** Complete drought monitoring capability

**Depends On:** Issue #61 (precipitation data)

**Special Considerations:**
- Verify drought baseline percentiles exist
- SPI requires 30-year calibration period
- May need to create `data/baselines/drought_percentiles_1981_2000.nc`

**Indices:** 11 drought indices
- SPI-3, SPI-6, SPI-12 (Standardized Precipitation Index)
- Other drought metrics

**Expected Output:** 44 files, ~400 MB

**Action Items:**
- [ ] Verify drought baseline exists
- [ ] Test single year first (2024)
- [ ] Process full time series if test successful

---

## PHASE 3: MEMORY OPTIMIZATION (Week 5)
**Goal:** Reduce memory footprint for scalability

### 🟡 Issue #62: Memory optimization: Reduce per-year memory footprint
**Priority:** P2 - MEDIUM
**Effort:** 12 hours
**Impact:** Enable concurrent processing, reduce resource requirements

**Current State:** 13 GB per year (too high)
**Target:** <5 GB per year

**Investigation Areas:**

#### 1. Memory Profiling (4 hours)
```python
from memory_profiler import profile

@profile
def process_time_chunk(self, ...):
    """Profile memory at each pipeline stage."""
```

**Profile Points:**
- Zarr loading
- Baseline loading
- Each index calculation
- Tile processing
- Tile merging

#### 2. Optimization Strategies (8 hours)

**A. Baseline Variable Selection (2 hours)**
```python
# CURRENT: Loads entire 9.3 GB baseline file
ds = xr.open_dataset(self.baseline_file, chunks='auto')

# OPTIMIZED: Load only needed variables
needed_vars = ['tx90p_threshold', 'tx10p_threshold', 'tn90p_threshold', 'tn10p_threshold']
ds = xr.open_dataset(self.baseline_file, chunks='auto')[needed_vars]
# Reduces from 9.3 GB to ~2.3 GB (75% reduction)
```

**B. Aggressive Garbage Collection (2 hours)**
```python
import gc
del merged_ds, merged_ds_computed
gc.collect()  # Force immediate cleanup
```

**C. Index-by-Index Processing (4 hours)**
```python
# Instead of calculating all indices at once:
for index_name, index_func in self.indices.items():
    result = index_func(ds).compute()
    indices[index_name] = result
    del result
    gc.collect()
```

**Validation:**
- Profile memory before/after each optimization
- Verify processing speed maintained
- Ensure data quality unchanged

---

### 🟡 Issue #74: Baseline percentiles loaded eagerly (wastes 1.4 GB memory)
**Priority:** P2 - MEDIUM
**Effort:** 4 hours
**Impact:** Additional memory optimization (after #62)

**Current State:** Uses `chunks='auto'` but loads all variables
**Optimization:** Variable selection + explicit chunking

```python
def _load_baseline_percentiles(self):
    """Load only needed baseline variables."""
    # Define which variables this pipeline needs
    needed_vars = self._get_required_baseline_vars()

    ds = xr.open_dataset(
        self.baseline_file,
        chunks={
            'lat': 103,   # Match processing chunk size
            'lon': 201,
            'dayofyear': -1
        }
    )[needed_vars]  # Load only needed variables

    percentiles = {var: ds[var] for var in needed_vars}
    logger.info(f"  Loaded {len(percentiles)} baseline percentiles (lazy, {len(needed_vars)} vars)")
    return percentiles
```

**Expected Improvement:** 75% memory reduction (9.3 GB → 2.3 GB for baselines)

---

## PHASE 4: DOCUMENTATION & CI/CD (Week 6)
**Goal:** Improve maintainability and automation

### 🟢 Issue #64: Document v5.0 architecture and parallel spatial tiling
**Priority:** P2 - MEDIUM
**Effort:** 10 hours
**Impact:** Onboarding, maintainability

**Documentation Needed:**

#### 1. Architecture Document (4 hours)
**File:** `docs/ARCHITECTURE.md`

**Content:**
- BasePipeline design pattern
- SpatialTilingMixin composition
- Thread safety design
- Tile merge strategy
- Memory efficiency patterns

#### 2. Bug Fix Documentation (2 hours)
**File:** `docs/CHANGELOG.md`

**Content:**
- v4.0 → v5.0 breaking changes
- Critical bug fixes (file size, thread safety)
- Performance improvements

#### 3. Developer Guide (2 hours)
**File:** `docs/DEVELOPMENT.md`

**Content:**
- How to add new indices
- Thread safety requirements
- Testing requirements
- Memory profiling guidance

#### 4. Code Comments (2 hours)
Add comprehensive docstrings to:
- `_process_spatial_tile()` - Thread safety design
- `_get_spatial_tiles()` - Coordinate slicing logic
- `process_time_chunk()` - Tile merge strategy

---

### 🟢 Issue #65: Set up CI/CD pipeline for automated testing and validation
**Priority:** P2 - MEDIUM
**Effort:** 12 hours
**Impact:** Automated quality assurance

**Depends On:** Issue #32 (test coverage), #60 (validation suite)

**Implementation Plan:**

#### 1. GitHub Actions Workflow (4 hours)
**File:** `.github/workflows/test.yml`

```yaml
name: Test Suite

on:
  push:
    branches: [main, develop, feature/*]
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 0 * * *'  # Nightly

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-cov
      - name: Run unit tests
        run: pytest tests/unit --cov=core --cov-report=xml
      - name: Run integration tests
        run: pytest tests/integration
      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

#### 2. Pre-commit Hooks (2 hours)
**File:** `.pre-commit-config.yaml`

```yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.3.0
    hooks:
      - id: black
  - repo: https://github.com/charliermarsh/ruff-pre-commit
    rev: v0.0.270
    hooks:
      - id: ruff
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: check-yaml
      - id: check-json
      - id: trailing-whitespace
```

#### 3. Automated Validation (4 hours)
- Integrate validation suite into CI
- Run on test datasets
- Generate coverage reports

#### 4. Performance Benchmarks (2 hours)
- Track memory usage over time
- Alert on >10% regression
- Benchmark processing speed

---

### 🟢 Issue #69: Create production orchestration master script
**Priority:** P2 - MEDIUM
**Effort:** 10 hours
**Impact:** Simplified production operations

**Implementation:**

```bash
#!/bin/bash
# run_production.sh - Master orchestration script

usage() {
    echo "Usage: $0 [options]"
    echo "  --pipeline PIPELINE    Pipeline: temperature|precipitation|agricultural|drought|all"
    echo "  --start-year YEAR      Start year (default: 1981)"
    echo "  --end-year YEAR        End year (default: 2024)"
    echo "  --validate             Run validation after processing"
    echo "  --resume               Skip already processed years"
}

# Features:
# 1. Process any year range for any pipeline
# 2. Resume capability (skip existing files)
# 3. Automatic validation after completion
# 4. Error handling and logging
# 5. Progress reporting
```

**Features:**
- Resume capability (skip completed years)
- Validation integration
- Error handling and logging
- Multi-pipeline support
- Parallel execution option

---

## PHASE 5: FEATURE IMPLEMENTATION (Week 7-10)
**Goal:** Complete remaining indices and enhancements

### 🟢 Issue #42: Phase 2: Precipitation Extensions (10 indices)
**Priority:** P3 - MEDIUM
**Effort:** 12 hours
**Impact:** Additional precipitation analysis capabilities

**Current:** 13 precipitation indices
**Target:** 23 precipitation indices (+10)

**Depends On:** Issue #61 (precipitation production complete)

---

### 🟢 Issue #43: Phase 3: Advanced Temperature (8 indices)
**Priority:** P3 - MEDIUM
**Effort:** 10 hours
**Impact:** Enhanced temperature analysis

**Current:** 35 temperature indices
**Target:** 43 temperature indices (+8)

---

### 🟢 Issue #46: Phase 6: Agricultural Indices (5 indices)
**Priority:** P3 - MEDIUM
**Effort:** 12 hours
**Impact:** Agricultural planning capabilities

**Status:** MAY BE ALREADY IMPLEMENTED - Need to verify against current agricultural_pipeline.py

---

## PHASE 6: LOW PRIORITY ENHANCEMENTS (Week 11+)
**Goal:** Polish and advanced features

### 🔵 Issue #33: Add checkpointing and recovery mechanism
**Priority:** P4 - LOW
**Effort:** 8 hours
**Impact:** Resume from failures

---

### 🔵 Issue #34: Add data validation layer
**Priority:** P4 - LOW (covered by Issue #60)
**Effort:** 6 hours
**Impact:** Input data validation

---

### 🔵 Issue #35: Implement adaptive resource scaling
**Priority:** P4 - LOW
**Effort:** 12 hours
**Impact:** Dynamic resource allocation

---

### 🔵 Issue #36: Add scientific validation benchmarks
**Priority:** P4 - LOW
**Effort:** 16 hours
**Impact:** Verify against known climate datasets

---

## ISSUES TO CLOSE/CONSOLIDATE

### Already Resolved
- ✅ #30: Code duplication (CLOSED - BasePipeline implemented)
- ✅ #68: Tile cleanup (CLOSED - Auto-cleanup implemented)

### Duplicate/Redundant
- #5: Refactor monolithic temperature function (covered by #30 - CLOSE)
- #6: Unit tests for all 84 indices (covered by #32 - CLOSE)
- #34: Data validation layer (covered by #60 - CLOSE)

### May Be Already Implemented
- #46: Phase 6 Agricultural Indices - Need to verify against current code

---

## EFFORT SUMMARY

| Phase | Issues | Effort (hours) | Priority |
|-------|--------|----------------|----------|
| Phase 1: Security & Stability | 3 | 36.5 | P0-P1 |
| Phase 2: Production Data | 4 | 27-33 | P1 |
| Phase 3: Memory Optimization | 2 | 16 | P2 |
| Phase 4: Documentation & CI/CD | 3 | 32 | P2 |
| Phase 5: Feature Implementation | 3 | 34 | P3 |
| Phase 6: Low Priority | 4 | 42 | P4 |
| **TOTAL** | **19** | **187-193 hours** | |

**Timeline:** 10-12 weeks (assuming 15-20 hours/week)

---

## RECOMMENDED IMMEDIATE ACTIONS (Next 2 Weeks)

### Week 1
1. **Create & fix path traversal vulnerability** (30 min) - IMMEDIATE
2. **Issue #75: Fix baseline rechunking bug** (8 hours) - Critical performance
3. **Issue #32: Establish test infrastructure** (16 hours) - Foundation for quality

**Total:** ~25 hours

### Week 2
4. **Issue #61: Complete precipitation production** (3 hours remaining) - In progress
5. **Issue #63: Add integration tests** (12 hours) - Production confidence
6. **Issue #60: Create validation suite** (10 hours) - Data quality

**Total:** ~25 hours

### Week 3
7. **Issue #66: Agricultural production** (2-3 hours) - Complete datasets
8. **Issue #67: Drought production** (2-3 hours) - Complete datasets
9. **Issue #62: Memory optimization investigation** (12 hours) - Scalability

**Total:** ~17-19 hours

---

## SUCCESS METRICS

### Phase 1 Success Criteria
- [ ] Zero critical security vulnerabilities
- [ ] 80%+ code coverage on core modules
- [ ] All integration tests passing
- [ ] Baseline rechunking bug fixed

### Phase 2 Success Criteria
- [ ] All production datasets complete (temperature ✓, precipitation ✓, agricultural ✓, drought ✓)
- [ ] Automated validation suite operational
- [ ] No data quality issues detected

### Phase 3 Success Criteria
- [ ] Memory usage <5 GB per year (from 13 GB)
- [ ] Baseline loading <2.5 GB (from 9.3 GB)
- [ ] No performance regression

### Phase 4 Success Criteria
- [ ] Complete architecture documentation
- [ ] CI/CD pipeline operational
- [ ] All tests run automatically on PR

---

## RISK MITIGATION

### High-Risk Items
1. **Path traversal vulnerability** - Fix immediately
2. **No test coverage** - Blocks all other work safely
3. **Memory optimization** - May require significant refactoring

### Dependencies
- Phase 2 blocked by Phase 1 (need test infrastructure)
- Phase 5 blocked by Phase 2 (need production data)
- Phase 4 can run parallel to Phase 2-3

### Contingency Plans
- If memory optimization too complex: Document workarounds
- If CI/CD integration difficult: Start with local pre-commit hooks
- If production data issues: Validation suite will catch before deployment

---

**END OF PRIORITIZED ACTION PLAN**

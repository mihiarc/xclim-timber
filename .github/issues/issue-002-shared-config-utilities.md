# Create shared configuration and utilities

**Priority:** High | **Estimate:** 2 hours | **Labels:** refactoring, enhancement, priority-high

## Description
Centralize all configuration constants and shared utilities to eliminate duplication.

## Tasks
- [ ] Create `core/config.py` with:
  - Zarr paths (TEMP_ZARR, PRECIP_ZARR, HUMIDITY_ZARR)
  - Default chunk config
  - Variable rename maps
  - CF standard names dictionary
  - Common warning filters
- [ ] Create `core/baseline_loader.py` with `BaselineLoader` class:
  - Load baseline percentiles with caching
  - Methods: `get_temperature_baselines()`, `get_precipitation_baselines()`, `get_multivariate_baselines()`
  - Validation and error handling
- [ ] Create `core/cli_builder.py` with `PipelineCLI` class:
  - `create_parser()` - standard argument parser
  - Common args: `--start-year`, `--end-year`, `--output-dir`, `--chunk-years`, `--verbose`, `--dashboard`
  - Logging setup helper

## Acceptance Criteria
- [ ] All constants in one place
- [ ] BaselineLoader with caching
- [ ] Consistent CLI across all pipelines
- [ ] No code duplication for common setup

## Dependencies
- Requires: #1

## Related Issues
- Blocks: #3, #4, #5, #6, #7, #8, #9

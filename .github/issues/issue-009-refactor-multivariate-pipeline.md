# Refactor multivariate_pipeline.py

**Priority:** Low | **Estimate:** 2 hours | **Labels:** refactoring, priority-low

## Description
Refactor multivariate pipeline - uses temperature + precipitation + baseline percentiles.

## Tasks
- [ ] Make `MultivariatePipeline` inherit from `BasePipeline`
- [ ] Handle loading multiple Zarr stores and baselines
- [ ] Keep only unique methods:
  - `calculate_indices()` - override
  - `calculate_compound_extreme_indices()` - 4 compound indices
- [ ] Target: reduce from 593 → ~220 lines

## Acceptance Criteria
- [ ] All 4 multivariate indices present
- [ ] Compound extremes (cold_and_dry, warm_and_wet, etc.) work
- [ ] Baseline percentiles integration works
- [ ] Test on 2023 data

## Dependencies
- Requires: #8

## Related Issues
- Blocks: #10 (integration testing)

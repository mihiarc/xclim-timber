# Refactor agricultural_pipeline.py

**Priority:** Medium | **Estimate:** 2 hours | **Labels:** refactoring, priority-medium

## Description
Refactor agricultural pipeline - unique because it loads BOTH temperature AND precipitation data.

## Tasks
- [ ] Make `AgriculturalPipeline` inherit from `BasePipeline`
- [ ] Update to load multiple Zarr stores in `__init__`
- [ ] Keep only unique methods:
  - `calculate_indices()` - override
  - `calculate_agricultural_indices()` - 5 agricultural indices
- [ ] Target: reduce from 505 → ~180 lines

## Acceptance Criteria
- [ ] All 5 agricultural indices present
- [ ] Both temperature and precipitation data loaded
- [ ] PET (Baier-Robertson) works
- [ ] Corn Heat Units calculation works
- [ ] Test on 2023 data

## Dependencies
- Requires: #5

## Related Issues
- Blocks: #10 (integration testing)

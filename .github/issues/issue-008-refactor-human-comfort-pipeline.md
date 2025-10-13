# Refactor human_comfort_pipeline.py

**Priority:** Low | **Estimate:** 1.5 hours | **Labels:** refactoring, priority-low

## Description
Refactor human comfort pipeline - uses temperature + humidity data.

## Tasks
- [ ] Make `HumanComfortPipeline` inherit from `BasePipeline`
- [ ] Handle loading both temperature and humidity Zarr stores
- [ ] Keep only unique methods:
  - `calculate_indices()` - override
  - `calculate_comfort_indices()` - 3 comfort metrics
- [ ] Target: reduce from 502 → ~180 lines

## Acceptance Criteria
- [ ] All 3 human comfort indices present
- [ ] Heat index and humidex calculations work
- [ ] Test on 2023 data

## Dependencies
- Requires: #7

## Related Issues
- Blocks: #10 (integration testing)

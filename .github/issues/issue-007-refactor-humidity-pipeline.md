# Refactor humidity_pipeline.py

**Priority:** Low | **Estimate:** 1.5 hours | **Labels:** refactoring, priority-low

## Description
Simplest pipeline - mostly standard processing.

## Tasks
- [ ] Make `HumidityPipeline` inherit from `BasePipeline`
- [ ] Keep only `calculate_indices()` override
- [ ] Target: reduce from 430 → ~150 lines

## Acceptance Criteria
- [ ] All 8 humidity indices present
- [ ] VPD calculations work
- [ ] Test on 2023 data

## Dependencies
- Requires: #6

## Related Issues
- Blocks: #10 (integration testing)

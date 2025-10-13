# Refactor precipitation_pipeline.py

**Priority:** High | **Estimate:** 2 hours | **Labels:** refactoring, priority-high

## Description
Refactor precipitation pipeline - simpler than temperature (no spatial tiling).

## Tasks
- [ ] Make `PrecipitationPipeline` inherit from `BasePipeline`
- [ ] Remove duplicate methods
- [ ] Keep only unique methods:
  - `calculate_indices()` - override with precipitation logic
  - Individual calculation methods (basic, extreme, threshold, enhanced)
- [ ] Update `main()` to use `PipelineCLI`
- [ ] Target: reduce from 630 → ~200 lines

## Acceptance Criteria
- [ ] Pipeline produces identical output
- [ ] All 13 precipitation indices present
- [ ] Baseline percentiles still work
- [ ] Test on 2023 data

## Dependencies
- Requires: #3 (validates approach)

## Related Issues
- Blocks: #10 (integration testing)

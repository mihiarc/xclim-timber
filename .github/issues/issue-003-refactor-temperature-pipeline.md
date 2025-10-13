# Refactor temperature_pipeline.py

**Priority:** High | **Estimate:** 3 hours | **Labels:** refactoring, priority-high

## Description
Refactor temperature pipeline to use base class. This is the most complex pipeline (spatial tiling) so we do it first.

## Tasks
- [ ] Make `TemperaturePipeline` inherit from `BasePipeline`
- [ ] Remove duplicate methods (use base class)
- [ ] Keep only unique methods:
  - `calculate_indices()` - override with temperature logic
  - `_process_spatial_tile()` - unique spatial tiling
  - `_get_spatial_tiles()` - tile boundary calculation
  - `fix_count_indices()` - units='1' for count indices
- [ ] Update `main()` to use `PipelineCLI`
- [ ] Remove all duplicate init/setup/save code
- [ ] Target: reduce from 1,028 → ~300 lines

## Acceptance Criteria
- [ ] Pipeline produces identical output to original
- [ ] Spatial tiling still works (4 tiles)
- [ ] All 35 temperature indices present
- [ ] CLI interface unchanged
- [ ] Test on 2023 data, compare NetCDF outputs

## Dependencies
- Requires: #1, #2

## Related Issues
- Blocks: #10 (integration testing)

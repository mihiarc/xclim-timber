# Refactor drought_pipeline.py

**Priority:** Medium | **Estimate:** 2.5 hours | **Labels:** refactoring, priority-medium

## Description
Refactor drought pipeline - includes SPI calculations requiring full calibration period.

## Tasks
- [ ] Make `DroughtPipeline` inherit from `BasePipeline`
- [ ] Handle special case: SPI needs 1981-2010 calibration period loaded
- [ ] Keep only unique methods:
  - `calculate_indices()` - override
  - `calculate_spi_indices()` - 5 SPI windows
  - `calculate_dry_spell_indices()` - 4 dry spell metrics
  - `calculate_precip_intensity_indices()` - 3 intensity metrics
- [ ] Target: reduce from 714 → ~250 lines

## Acceptance Criteria
- [ ] All 12 drought indices present
- [ ] SPI calibration period (1981-2010) handled correctly
- [ ] Manual implementations still work (dry_spell_frequency, etc.)
- [ ] Test on 2023 data

## Dependencies
- Requires: #4

## Related Issues
- Blocks: #10 (integration testing)

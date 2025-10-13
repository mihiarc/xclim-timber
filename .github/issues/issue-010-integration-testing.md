# Integration testing and validation

**Priority:** High | **Estimate:** 3 hours | **Labels:** testing, priority-high

## Description
Comprehensive testing to ensure all 80 indices produce identical outputs.

## Tasks
- [ ] Create test script: `tests/validate_refactored_pipelines.py`
- [ ] For each pipeline:
  - Run on 2023 test data
  - Compare NetCDF output with archived original output
  - Validate all expected indices present
  - Check metadata completeness
- [ ] Performance comparison (timing, memory usage)
- [ ] Create validation report

## Acceptance Criteria
- [ ] All 7 pipelines pass validation
- [ ] 80 total indices verified
- [ ] No output differences detected
- [ ] Performance is equal or better
- [ ] Validation report generated

## Dependencies
- Requires: #3, #4, #5, #6, #7, #8, #9

## Related Issues
- Blocks: #11 (documentation)

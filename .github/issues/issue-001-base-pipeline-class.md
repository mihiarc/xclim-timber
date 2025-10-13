# Create core module structure and base pipeline class

**Priority:** High | **Estimate:** 4 hours | **Labels:** refactoring, enhancement, priority-high

## Description
Create a unified base class to eliminate ~2,800 lines of duplicate code across 7 pipelines. No backward compatibility - this is a breaking change.

## Tasks
- [ ] Create `core/` directory with `__init__.py`
- [ ] Create `core/base_pipeline.py` with `BasePipeline` abstract class
- [ ] Implement common methods:
  - `__init__()` - zarr paths, chunk config, Dask setup
  - `setup_dask_client()` - threaded scheduler only
  - `close()` - resource cleanup
  - `process_time_chunk()` - load → process → save pattern
  - `run()` - temporal chunking loop with error handling
  - `_load_zarr_data()` - common Zarr loading
  - `_rename_variables()` - variable renaming
  - `_fix_units()` - CF-compliant unit fixing
  - `_save_result()` - NetCDF save with compression
  - `_add_global_metadata()` - common attributes
- [ ] Add abstract method: `calculate_indices(datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]`

## Acceptance Criteria
- [ ] `BasePipeline` class with all common functionality
- [ ] Type hints on all methods
- [ ] Docstrings with Args/Returns
- [ ] No Dask distributed client (threaded only)
- [ ] Memory tracking built-in
- [ ] File size reporting built-in

## Dependencies
None

## Related Issues
- Blocks: #2, #3

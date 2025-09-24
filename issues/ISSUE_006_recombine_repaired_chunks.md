# Issue #006: Recombine repaired chunk files into unified dataset

## Problem Description
After repairing individual chunk files to fix timedelta and temperature range issues, we need to recombine them into a single `combined_indices.nc` file for easier analysis and distribution.

## Current Situation
- 8 chunk files have been repaired (indices_2001_2003.nc through indices_2022_2024.nc)
- Each file contains valid numeric data with correct units
- Old combined file still has the original issues
- Users need a single file for analysis

## Required Actions

### 1. Create Recombination Script
```python
# Script should:
1. Load all repaired chunk files
2. Concatenate along time dimension
3. Verify temporal continuity
4. Save with optimal compression
```

### 2. Validate Combined Output
- Ensure no data gaps between chunks
- Verify all indices present
- Check file size is reasonable
- Confirm metadata is complete

### 3. Update Documentation
- Document the recombination process
- Add to pipeline workflow documentation
- Update user guide

## Implementation Approach

```python
import xarray as xr
from pathlib import Path

def recombine_chunks(chunk_dir, output_file):
    """Recombine repaired chunk files."""

    # Find all chunk files
    chunks = sorted(Path(chunk_dir).glob('indices_*_*.nc'))

    # Load all chunks
    datasets = []
    for chunk in chunks:
        ds = xr.open_dataset(chunk)
        datasets.append(ds)

    # Concatenate along time
    combined = xr.concat(datasets, dim='time')

    # Sort by time
    combined = combined.sortby('time')

    # Save with compression
    encoding = {var: {'zlib': True, 'complevel': 4}
                for var in combined.data_vars}

    combined.to_netcdf(output_file, encoding=encoding)

    return combined
```

## Success Criteria
- ✅ Single combined file created successfully
- ✅ All time periods represented (2001-2024)
- ✅ All indices have valid numeric data
- ✅ File passes QA/QC checks

## Priority
**HIGH** - Users need a single file for analysis, not multiple chunks

## Estimated Effort
1-2 hours

## Dependencies
- Issue #001 (timedelta fix) must be completed
- Issue #003 (temperature range fix) must be completed

## Related Files
- `outputs/comprehensive_2001_2024/indices_*.nc` (chunks)
- `outputs/comprehensive_2001_2024/combined_indices.nc` (output)
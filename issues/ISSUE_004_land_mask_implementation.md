# Issue #004: Implement land masking for accurate terrestrial climate analysis

## Feature Request
Add land/ocean masking capability to the climate data pipeline to improve data quality, reduce storage requirements, and provide more accurate statistics for terrestrial climate indices.

## Current Situation
- Pipeline processes entire rectangular grid including ocean areas
- Ocean points result in NaN values for terrestrial indices
- No way to distinguish between true missing data and ocean areas
- Statistics include ocean areas, diluting land-based signals

## Proposed Implementation

### 1. Land Mask Data Sources

#### Option A: Use Natural Earth Data
```python
import geopandas as gpd
from rasterio import features

# Get high-resolution land polygons
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
land = world[world.continent != 'Antarctica']

# Rasterize to PRISM grid
land_mask = features.rasterize(
    land.geometry,
    out_shape=(621, 1405),
    transform=prism_transform
)
```

#### Option B: Use PRISM Metadata
```python
# Check if PRISM includes a mask
ds = xr.open_zarr('/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature')
# Look for variables like 'mask', 'land', 'land_area_fraction'
```

#### Option C: Generate from Data Availability
```python
def create_mask_from_data(ds, min_valid_frac=0.5):
    """Create land mask based on data availability."""
    # Points with >50% valid data across time are likely land
    valid_frac = (~ds.isnull()).mean(dim='time')

    # Apply spatial smoothing to clean up edges
    from scipy.ndimage import binary_closing
    mask = binary_closing(valid_frac > min_valid_frac, iterations=2)

    return mask
```

### 2. Pipeline Integration

#### Add to Configuration (config.yaml):
```yaml
data:
  land_mask:
    enabled: true
    source: 'auto'  # 'auto', 'file', or 'generate'
    file_path: '/path/to/land_mask.nc'  # if source='file'
    include_great_lakes: false  # whether to mask large water bodies
    buffer_distance_km: 0  # coastal buffer (0 = strict coastline)
```

#### Modify Preprocessor (preprocessor.py):
```python
class ClimateDataPreprocessor:
    def __init__(self, config):
        self.config = config
        self.land_mask = None

        if config.get('data.land_mask.enabled', False):
            self.land_mask = self._load_land_mask()

    def _load_land_mask(self):
        """Load or generate land mask."""
        source = self.config.get('data.land_mask.source', 'auto')

        if source == 'file':
            mask_path = self.config.get('data.land_mask.file_path')
            return xr.open_dataset(mask_path).mask

        elif source == 'generate':
            return None  # Will generate from data

        else:  # auto
            return self._auto_detect_mask()

    def apply_mask(self, ds):
        """Apply land mask to dataset."""
        if self.land_mask is None:
            return ds

        # Apply mask to all data variables
        for var in ds.data_vars:
            ds[var] = ds[var].where(self.land_mask)

        # Add mask as coordinate for reference
        ds.coords['land_mask'] = self.land_mask

        # Add metadata
        ds.attrs['land_masked'] = True
        ds.attrs['ocean_points_masked'] = int((~self.land_mask).sum())

        return ds
```

### 3. Benefits Implementation

#### Storage Optimization:
```python
# Use sparse arrays for masked data
encoding = {
    var: {
        'zlib': True,
        'complevel': 4,
        '_FillValue': np.nan,
        'chunksizes': (1, 100, 100)
    }
    for var in ds.data_vars
}
```

#### Improved Statistics:
```python
# Calculate statistics only over land
land_only_mean = ds['tg_mean'].where(ds.land_mask).mean()
ocean_only_mean = ds['tg_mean'].where(~ds.land_mask).mean()

# Report separately
print(f"Land mean: {land_only_mean}")
print(f"Ocean mean: {ocean_only_mean}")
```

### 4. QA/QC Integration

Update qa_qc_indices.py:
```python
def check_data_completeness_with_mask(ds):
    """Check missing data separately for land and ocean."""

    if 'land_mask' in ds.coords:
        land_points = ds.land_mask.sum()
        ocean_points = (~ds.land_mask).sum()

        print(f"Grid composition:")
        print(f"  Land points: {land_points:,} ({100*land_points/ds.land_mask.size:.1f}%)")
        print(f"  Ocean points: {ocean_points:,} ({100*ocean_points/ds.land_mask.size:.1f}%)")

        # Calculate missing data for land only
        for var in ds.data_vars:
            land_data = ds[var].where(ds.land_mask)
            missing_pct = 100 * land_data.isnull().sum() / land_points

            print(f"{var}: {missing_pct:.1f}% missing over land")
```

## Implementation Phases

### Phase 1: Basic Mask (Quick Win)
- Generate mask from data availability
- Apply to existing outputs
- Update QA/QC reporting

### Phase 2: Improved Mask (Better Accuracy)
- Integrate Natural Earth or similar coastline data
- Handle Great Lakes and other water bodies
- Add coastal buffer options

### Phase 3: Full Integration (Production)
- Add to main pipeline
- Configure via YAML
- Add mask validation and visualization

## Testing Requirements
- Verify mask correctly identifies coastlines
- Check alignment with PRISM grid
- Validate statistics improvement
- Test file size reduction
- Ensure backwards compatibility

## Priority
**MEDIUM-HIGH** - Significantly improves data quality and interpretation without affecting core calculations.

## Expected Outcomes
- ✅ Accurate land-only statistics
- ✅ ~45% reduction in file size
- ✅ Clearer QA/QC reports
- ✅ Better visualization (no ocean artifacts)
- ✅ Improved scientific validity

## References
- Natural Earth: https://www.naturalearthdata.com/
- CF Conventions on masks: https://cfconventions.org/
- PRISM documentation on spatial domain
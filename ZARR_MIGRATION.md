# Zarr Migration Summary

## Overview
The xclim-timber project has been successfully streamlined to work exclusively with Zarr stores, removing legacy support for GeoTIFF and NetCDF file formats. This migration reduces technical debt and optimizes the pipeline for modern, cloud-native climate data processing.

## Key Changes

### 1. Data Loader (`src/data_loader.py`)
- **Removed**: `load_geotiff()` and `load_netcdf()` methods
- **Removed**: Dependencies on `rasterio` and `rioxarray`
- **Added**: `optimize_zarr_store()` method for rechunking and consolidation
- **Enhanced**: Zarr-specific optimizations including consolidated metadata support
- **Simplified**: From 459 lines to 339 lines of cleaner, focused code

### 2. Configuration System
- **Updated**: `src/config.py` to use `zarr_stores` instead of `file_patterns`
- **Created**: `config_zarr.yaml` as the new configuration template
- **Pattern**: Now exclusively searches for `*.zarr` stores

### 3. Pipeline (`src/pipeline.py`)
- **Updated**: Data loading phase to work exclusively with Zarr stores
- **Modified**: Variable discovery to use `data.zarr_stores` configuration

### 4. Dependencies (`requirements.txt`)
- **Removed**:
  - `rasterio` - No longer needed for GeoTIFF support
  - `rioxarray` - No longer needed for raster operations
  - `pyproj` - No longer needed for CRS transformations
  - `cartopy` - Removed optional geographic plotting library

### 5. Extraction Scripts
- **Removed**:
  - `extract_parcel_climate.py` (legacy extraction)
  - `extract_sample_points.py` (redundant functionality)
- **Renamed**: `extract_zarr_to_csv.py` → `extract_climate_indices.py`

## Benefits

### Performance
- **Faster Loading**: Consolidated metadata enables rapid dataset discovery
- **Efficient Chunking**: Native Zarr chunking optimized for parallel processing
- **Reduced Memory**: Zarr's compression and chunking reduce memory footprint

### Maintainability
- **Simpler Codebase**: ~270 lines of code removed
- **Single Format**: No need to maintain multiple data format handlers
- **Fewer Dependencies**: Reduced from 14 to 10 core dependencies

### Scalability
- **Cloud-Ready**: Zarr is designed for cloud object storage
- **Parallel I/O**: Built-in support for concurrent reads/writes
- **Incremental Updates**: Zarr stores can be updated without rewriting entire datasets

## Migration Guide

### For Existing Users

1. **Convert existing data to Zarr format**:
   ```python
   import xarray as xr

   # Load your NetCDF or GeoTIFF data
   ds = xr.open_dataset('old_data.nc')

   # Save as Zarr with optimization
   ds.to_zarr('new_data.zarr', consolidated=True)
   ```

2. **Update configuration files**:
   - Replace `file_patterns` with `zarr_stores` in your YAML configs
   - Use `config_zarr.yaml` as a template

3. **Update scripts**:
   - Replace calls to `load_file()` with `load_zarr()`
   - Remove any GeoTIFF or NetCDF-specific code

### Testing

Run the test script to verify the installation:
```bash
.venv/bin/python test_zarr_pipeline.py
```

## Data Format Specifications

### Expected Zarr Store Structure
```
data_store.zarr/
├── .zarray          # Array metadata
├── .zattrs          # Dataset attributes
├── .zgroup          # Group metadata
├── .zmetadata       # Consolidated metadata (optional)
├── time/            # Time dimension chunks
├── lat/             # Latitude dimension chunks
├── lon/             # Longitude dimension chunks
└── variable_name/   # Data variable chunks
```

### Dimension Naming
The pipeline automatically standardizes dimensions to CF conventions:
- Latitude: `lat` (accepts: latitude, Latitude, LAT, y, Y)
- Longitude: `lon` (accepts: longitude, Longitude, LON, long, x, X)
- Time: `time` (accepts: Time, TIME, t, T)

## Future Enhancements

1. **Remote Zarr Stores**: Add support for S3, GCS, and Azure blob storage
2. **Incremental Processing**: Process new time slices as they become available
3. **Zarr V3 Support**: Prepare for upcoming Zarr specification v3
4. **Multi-resolution Stores**: Support for pyramid/multi-scale Zarr stores

## Backward Compatibility

This is a breaking change. Users with existing GeoTIFF or NetCDF workflows will need to:
1. Convert their data to Zarr format
2. Update their configuration files
3. Reinstall dependencies with the new requirements.txt

## Support

For questions or issues with the Zarr migration, please:
1. Check the test script: `test_zarr_pipeline.py`
2. Review the example configuration: `config_zarr.yaml`
3. Open an issue on the project repository
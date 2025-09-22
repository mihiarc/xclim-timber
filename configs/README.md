# Configuration Files

This directory contains configuration templates and examples for the xclim-timber pipeline.

## Structure

```
configs/
├── config_template.yaml    # Main template - copy and customize this
└── examples/               # Example configurations
    ├── config_prism.yaml   # PRISM US climate data (4km daily)
    └── config_zarr.yaml    # Basic Zarr configuration
```

## Quick Start

1. **Copy the template**:
   ```bash
   cp configs/config_template.yaml my_config.yaml
   ```

2. **Edit your configuration**:
   - Set `input_path` to your Zarr store location
   - Update `zarr_stores` patterns to match your data
   - Select which indices to calculate

3. **Run the pipeline**:
   ```bash
   python src/pipeline.py --config my_config.yaml
   ```

## Configuration Sections

### Data Paths
- `input_path`: Location of your Zarr stores (e.g., external drive)
- `output_path`: Where to save results
- `zarr_stores`: Patterns to find your Zarr data

### Processing
- `chunk_size`: Memory management settings
- `temperature_units`: degC or K
- `zarr.consolidated`: Use consolidated metadata for faster loading

### Indices
- Choose from 84 available climate indices
- Set baseline period for percentile calculations
- Comment out indices you don't need

## Example Configurations

### PRISM Configuration (`examples/config_prism.yaml`)
- Optimized for PRISM 4km US climate data
- Covers temperature, precipitation, and humidity
- 1981-2024 daily data

### Basic Zarr Configuration (`examples/config_zarr.yaml`)
- Simple starting point for Zarr stores
- Essential indices only
- Minimal configuration

## Tips

1. **Memory Management**: Adjust `chunk_size` based on your system RAM
2. **Performance**: Use `consolidated: true` for faster metadata loading
3. **Indices**: Start with basic indices, add more as needed
4. **Paths**: Use absolute paths for external drives

## Migration from Legacy Configs

If you have old configuration files using GeoTIFF or NetCDF patterns:
1. Convert `file_patterns` to `zarr_stores`
2. Remove `.tif`, `.nc` extensions
3. Add `.zarr` to your patterns
4. See ZARR_MIGRATION.md for details
# xclim-timber: Climate Data Processing Pipeline

## Project Overview

**xclim-timber** is a robust Python pipeline for processing large-scale climate raster data and calculating climate indices using the [xclim](https://xclim.readthedocs.io/) library. The project bridges raw climate data stored on external drives with actionable climate metrics for research, agricultural planning, and environmental impact assessment.

## Project Architecture

### Core Components

```
xclim-timber/
├── src/                         # Core pipeline modules
│   ├── pipeline.py              # Main orchestrator & CLI (src/pipeline.py:63)
│   ├── config.py               # Configuration management
│   ├── data_loader.py          # Multi-format data loading (src/data_loader.py:21)
│   ├── preprocessor.py         # Data cleaning & standardization
│   ├── indices_calculator.py   # Climate indices calculation (src/indices_calculator.py:21)
│   └── config_noresm.py        # NorESM-specific configuration
├── efficient_extraction.py     # Vectorized point extraction
├── fast_point_extraction.py    # Alternative extraction method
├── point_extraction.py         # Legacy extraction script
├── csv_formatter.py            # CSV format converter (long ↔ wide)
├── format_csv_example.py       # CSV formatting example/demo
├── config_simple.yaml          # Configuration template
└── requirements.txt            # Python dependencies
```

### Pipeline Workflow

The pipeline follows a **5-phase processing workflow**:

#### 1. Configuration Phase
- **Location**: `src/config.py:1`
- **Purpose**: YAML-based configuration management
- **Features**:
  - Flexible file pattern matching for different climate variable naming conventions
  - Dask worker configuration (memory limits, thread counts)
  - Chunk size optimization for memory-efficient processing
  - Input/output path specification for external drives

#### 2. Data Loading Phase
- **Location**: `src/data_loader.py:39`
- **Purpose**: Multi-format climate data ingestion
- **Capabilities**:
  - Scans external drives using configurable file patterns
  - Supports GeoTIFF (`*.tif`, `*.tiff`) and NetCDF (`*.nc`, `*.nc4`) formats
  - Uses rioxarray for GeoTIFF and xarray for NetCDF loading
  - Implements intelligent chunking for datasets larger than RAM

#### 3. Preprocessing Phase
- **Location**: `src/preprocessor.py:1`
- **Purpose**: Data standardization and quality control
- **Operations**:
  - Coordinate system standardization (handles 0-360° vs -180-180° longitude)
  - Unit conversions (Kelvin to Celsius, etc.)
  - Missing value handling and outlier detection
  - Temporal resampling (daily, monthly, annual)
  - CF compliance for metadata standards

#### 4. Indices Calculation Phase
- **Location**: `src/indices_calculator.py:37`
- **Purpose**: Climate indices computation using xclim
- **Supported Indices**:
  - **Temperature**: annual_mean, frost_days, ice_days, summer_days, tropical_nights, growing_degree_days, heating/cooling_degree_days
  - **Precipitation**: prcptot, rx1day, rx5day, sdii, cdd, cwd, r10mm, r20mm, r95p, r99p
  - **Extremes**: tx90p, tn10p, warm/cold spell duration indices

#### 5. Output Phase
- **Location**: `src/pipeline.py:216`
- **Purpose**: Results persistence with compression
- **Formats**:
  - NetCDF with CF-compliant metadata
  - GeoTIFF for spatial analysis tools
  - CSV for point-based extractions

## Data Processing Capabilities

### Input Data Support
- **Formats**: GeoTIFF, NetCDF, NetCDF4
- **Variables**: Temperature (tas, tasmax, tasmin), Precipitation (pr, precip)
- **Coordinate Systems**: Geographic (lat/lon), projected coordinates
- **Temporal Resolution**: Daily, sub-daily data
- **Spatial Scales**: From local to global datasets

### Processing Optimizations
- **Parallel Computing**: Dask distributed processing across multiple cores
- **Memory Management**: Lazy evaluation with intelligent chunking
- **Monitoring**: Web dashboard at `localhost:8787` during processing
- **Scalability**: Handles datasets larger than available RAM

## Output Structure

The pipeline generates structured outputs in the `outputs/` directory:

### Directory Structure
```
outputs/
├── annual_mean_YYYY.nc          # NetCDF climate indices by year
├── climate_indices_YYYYMMDD.nc  # Combined indices file
├── parcel_indices_YYYY.csv      # Point extraction results
├── historical/                  # Time series data
│   ├── parcel_indices_1950.csv
│   ├── parcel_indices_1951.csv
│   └── parcel_indices_historical_1950_1955.csv
├── batch_test/                  # Test outputs
└── maps/                        # Visualization outputs
```

### CSV Output Formats (Point Extraction)

The pipeline now supports **both long and wide format CSV outputs** for climate indices:

#### Long Format (Traditional - Default)
Each row represents a **unique location-year combination**:
```csv
saleid,parcelid,lat,lon,year,annual_mean,annual_min,annual_max,annual_std,frost_days,ice_days,summer_days,hot_days,tropical_nights,growing_degree_days,heating_degree_days,cooling_degree_days
600007,1,39.7964,-121.445,1950,11.224,-3.771,24.343,6.895,11,0,0,0,46,1320.4,2677.7,204.5
600007,1,39.7964,-121.445,1951,10.702,-2.081,22.823,6.744,12,0,0,0,33,1228.2,2807.6,143.7
```
- **Use cases**: Time series analysis, statistical modeling, longitudinal studies
- **Structure**: 144K+ rows for 24K locations × 6 years (sample period)

#### Wide Format (Pivot Table Style)
Each row represents a **unique location** with climate indices spread across years:
```csv
saleid,parcelid,lat,lon,annual_mean_1950,annual_mean_1951,frost_days_1950,frost_days_1951,...
600007,1,39.7964,-121.445,11.224,10.702,11,12,...
```
- **Use cases**: Spatial analysis, year-over-year comparisons, GIS applications
- **Structure**: 24K rows × 76 columns (4 location cols + 12 indices × 6 years)

#### Historical Data Filtering
Both formats automatically filter for **historical years only (1950-2014)**, excluding projected scenarios (2015-2100) to maintain focus on observational data.

### NetCDF Output Format
NetCDF files contain gridded climate indices with CF-compliant metadata:

- **Variables**: Each climate index as a separate data variable
- **Dimensions**: lat, lon, time (typically annual)
- **Attributes**: Title, description, software version, processing metadata
- **Compression**: Configurable zlib compression (default: level 4)

### Metadata Standards
All outputs include comprehensive metadata following CF conventions:
- Processing software and version information
- Data source attribution
- Variable descriptions and units
- Processing timestamps and parameters

## Configuration Management

### Configuration File Structure (`config_simple.yaml`)
```yaml
data:
  input_path: /media/external_drive/climate_data
  output_path: ./outputs
  log_path: ./logs
  file_patterns:
    temperature: ['*temperature*.nc', '*tas*.nc', '*temp*.tif']

processing:
  chunk_size:
    time: 365    # Process one year at a time
    lat: 100     # Spatial chunking
    lon: 100
  dask:
    n_workers: 2
    memory_limit: '2GB'
  temperature_units: 'degC'

indices:
  temperature:
    - annual_mean
    - frost_days
    - growing_degree_days

output:
  format: 'netcdf'  # or 'geotiff'
  compression:
    complevel: 4
    engine: 'netcdf4'
```

## Usage Patterns

### 1. Basic Pipeline Execution
```bash
# Create sample configuration
python src/pipeline.py --create-config

# Edit config_sample.yaml with your paths
# Run pipeline
python src/pipeline.py --config config_simple.yaml --verbose
```

### 2. Programmatic Usage
```python
from src.pipeline import ClimateDataPipeline

# Initialize and run pipeline
pipeline = ClimateDataPipeline('config.yaml')
pipeline.run(['temperature'])  # Process specific variables

# Access results
status = pipeline.get_status()
```

### 3. Point Extraction
```bash
# Extract climate data at specific coordinates
python efficient_extraction.py nc_file.nc parcels.csv output.csv
```

## Performance Characteristics

### Memory Efficiency
- **Chunking Strategy**: Processes data in manageable chunks (configurable)
- **Lazy Evaluation**: Operations queued and executed when needed
- **Streaming**: Large datasets processed without full memory loading

### Parallel Processing
- **Dask Integration**: Automatic parallelization across CPU cores
- **Dashboard Monitoring**: Real-time performance tracking
- **Resource Management**: Configurable worker and memory limits

### Scalability Benchmarks
Based on existing benchmark results (`benchmark_results.json`):
- Supports datasets from external drives (TB-scale)
- Efficient point extraction for thousands of locations
- Reasonable processing times for annual climate indices

## Dependencies & Technical Stack

### Core Libraries
```
xclim>=0.48.0          # Climate indices calculation
xarray>=2024.1.0       # N-dimensional data handling
dask[complete]>=2024.1.0 # Parallel computing
netCDF4>=1.6.0         # NetCDF file support
rasterio>=1.3.0        # Geospatial raster I/O
rioxarray>=0.15.0      # Xarray-rasterio integration
```

### Scientific Foundation
- **xclim**: Provides scientifically-validated climate indices following WMO standards
- **CF Conventions**: Ensures interoperability with climate research tools
- **Quality Assurance**: Built-in data validation and quality control

## Development & Testing

### Test Structure
- `test_annual_temp.py`: Temperature processing validation
- `run_test.sh`: Automated testing script
- Sample data in `sample_data/` for development

### Logging & Monitoring
- **Structured Logging**: Timestamped logs in `logs/` directory
- **Progress Tracking**: tqdm progress bars for long operations
- **Error Handling**: Comprehensive exception handling with traceback

### Performance Optimization Scripts
- `efficient_extraction.py`: Vectorized operations for point extraction
- `fast_point_extraction.py`: Alternative high-performance extraction
- `visualize_temp.py`: Results visualization and validation

## Scientific Applications

### Agricultural Planning
- Growing degree days for crop development models
- Frost risk assessment for vineyard management
- Irrigation planning using precipitation indices

### Climate Research
- Extreme weather event analysis
- Climate change impact assessment
- Regional climate model validation

### Environmental Management
- Drought monitoring using consecutive dry days
- Heat stress assessment for urban planning
- Water resource management using precipitation totals

## Key Insights

1. **Modular Design**: Clean separation between data loading, processing, and calculation phases enables easy maintenance and extension

2. **Performance Optimization**: The combination of Dask parallelization and intelligent chunking allows processing of datasets that exceed system memory

3. **Scientific Rigor**: Built on xclim ensures calculated indices follow established meteorological standards and can be trusted for research applications

4. **Flexibility**: YAML configuration and multiple output formats make it adaptable to various research workflows and analysis tools

5. **Production Ready**: Comprehensive logging, error handling, and monitoring capabilities make it suitable for operational climate data processing

The pipeline represents a mature, well-architected solution for climate data processing that balances scientific accuracy with computational efficiency.
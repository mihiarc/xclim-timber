# xclim-timber: Streamlined Climate Data Processing Pipeline

## Project Overview

**xclim-timber** is a streamlined Python pipeline for processing PRISM climate data and calculating 84 comprehensive climate indices using the [xclim](https://xclim.readthedocs.io/) library. The pipeline has been simplified by 75% to work directly with clean, pre-processed PRISM Zarr stores, eliminating unnecessary preprocessing and format conversion steps.

## Project Architecture (Simplified)

### Core Components

```
xclim-timber/
├── src/                         # Core pipeline modules
│   ├── pipeline.py              # Streamlined orchestrator & CLI
│   ├── config.py               # Minimal configuration (103 lines, was 258)
│   ├── data_loader.py          # Direct PRISM Zarr loading (153 lines, was 338)
│   └── indices_calculator.py   # Climate indices calculation
├── efficient_extraction.py     # Vectorized point extraction
├── csv_formatter.py            # CSV format converter (long ↔ wide)
└── requirements.txt            # Python dependencies
```

**Major Simplification**: Removed `preprocessor.py` (433 lines) entirely - PRISM data is already clean and properly formatted.

### Pipeline Workflow

The pipeline follows a **streamlined 3-phase workflow**:

#### 1. Configuration Phase
- **Location**: `src/config.py`
- **Purpose**: Minimal configuration management
- **Features**:
  - Single PRISM Zarr store path: `/media/mihiarc/SSD4TB/data/PRISM/prism.zarr`
  - Automatic handling of external drive mount point changes
  - All 84 climate indices definitions preserved
  - Simple YAML override for custom paths

#### 2. Data Loading Phase
- **Location**: `src/data_loader.py`
- **Purpose**: Direct PRISM Zarr loading
- **Capabilities**:
  - Direct loading of three Zarr stores:
    - Temperature: tmax, tmin, tmean (already in °C)
    - Precipitation: ppt (already in mm)
    - Humidity: tdmean, vpdmax, vpdmin
  - Variable renaming for xclim compatibility (e.g., tmax → tasmax)
  - Optional time and spatial subsetting
  - **No preprocessing needed** - data is already clean!

#### 3. Indices Calculation & Output Phase
- **Location**: `src/indices_calculator.py` & `src/pipeline.py`
- **Purpose**: Direct climate indices computation
- **Features**:
  - All 84 climate indices using xclim
  - Direct calculation on clean data
  - NetCDF output with compression (zlib level 4)
  - CSV extraction for specific parcel locations

## Data Processing Capabilities

### PRISM Data Specifications
- **Format**: Zarr stores (optimized chunked arrays)
- **Variables**:
  - Temperature: tmax, tmin, tmean (units: °C)
  - Precipitation: ppt (units: mm)
  - Humidity: tdmean, vpdmax, vpdmin
- **Coordinate System**: Geographic lat/lon (EPSG:4326)
- **Temporal Coverage**: 1981-2024 (16,071 daily timesteps)
- **Spatial Coverage**: Continental US
  - Dimensions: 621 x 1405 grid points
  - Resolution: ~4km
  - Total size: ~168 GB per variable

### Streamlined Processing
- **Direct Loading**: No format conversion or preprocessing
- **Native Efficiency**: Zarr's built-in chunking and compression
- **Reduced Overhead**: 75% less code to execute
- **Simplified Pipeline**: 3 steps instead of 5

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

## Comprehensive Climate Indices

The pipeline calculates **84 comprehensive climate indices** using the scientifically-validated xclim library, organized into seven standardized categories following World Meteorological Organization (WMO) standards.

### Underlying Climate Variables

**Core Variables Required:**
- `tas`: Near-surface air temperature (daily mean)
- `tasmax`: Daily maximum near-surface air temperature
- `tasmin`: Daily minimum near-surface air temperature
- `pr`: Daily precipitation amount
- `hus`: Specific humidity (kg/kg)
- `hurs`: Relative humidity (%)

**Variable Name Flexibility:** The system automatically detects variables using multiple naming conventions:
- Temperature: 'tas', 'temperature', 'temp', 'tmean', 'tasmax', 'tmax', 'tasmin', 'tmin'
- Precipitation: 'pr', 'precipitation', 'precip', 'prcp'
- Humidity: 'hus', 'huss', 'specific_humidity', 'hurs', 'relative_humidity', 'rh'

### Temperature Indices (17 indices)

**Basic Statistics:**
- `tg_mean`: Annual mean temperature
- `tx_max`: Annual maximum temperature
- `tn_min`: Annual minimum temperature
- `daily_temperature_range`: Mean daily temperature range
- `daily_temperature_range_variability`: Temperature range variability

**Threshold-Based Counts:**
- `tropical_nights`: Number of nights with minimum temperature > 20°C
- `frost_days`: Number of days with minimum temperature < 0°C
- `ice_days`: Number of days with maximum temperature < 0°C
- `summer_days`: Number of days with maximum temperature > 25°C
- `hot_days`: Number of days with maximum temperature > 30°C
- `very_hot_days`: Number of days with maximum temperature > 35°C
- `warm_nights`: Number of nights with minimum temperature > 15°C
- `consecutive_frost_days`: Maximum consecutive frost days

**Degree Day Metrics:**
- `growing_degree_days`: Accumulated temperature above 10°C threshold (crop development)
- `heating_degree_days`: Accumulated temperature below 17°C threshold (energy demand)
- `cooling_degree_days`: Accumulated temperature above 18°C threshold (cooling energy demand)

**Extreme Temperature Events:**
- `tx90p`, `tn90p`: Warm days/nights (>90th percentile)
- `tx10p`, `tn10p`: Cool days/nights (<10th percentile)
- `warm_spell_duration_index`: Consecutive warm periods
- `cold_spell_duration_index`: Consecutive cold periods

### Precipitation Indices (10 indices)

**Basic Statistics:**
- `prcptot`: Total annual precipitation
- `rx1day`: Maximum 1-day precipitation amount
- `rx5day`: Maximum 5-day precipitation amount
- `sdii`: Simple daily intensity index (average precipitation on wet days)

**Consecutive Events:**
- `cdd`: Maximum consecutive dry days (< 1mm)
- `cwd`: Maximum consecutive wet days (≥ 1mm)

**Threshold Events:**
- `r10mm`: Number of heavy precipitation days (≥ 10mm)
- `r20mm`: Number of very heavy precipitation days (≥ 20mm)
- `r95p`: Very wet days (above 95th percentile)
- `r99p`: Extremely wet days (above 99th percentile)

### Humidity Indices (2 indices)

**Basic Humidity Calculations:**
- `dewpoint_temperature`: Dewpoint temperature from specific humidity
- `relative_humidity`: Relative humidity from specific humidity and temperature

### Human Comfort Indices (2 indices)

**Heat Stress Assessment:**
- `heat_index`: Heat index combining temperature and humidity effects
- `humidex`: Canadian humidex index for apparent temperature

### Evapotranspiration Indices (3 indices)

**Water Balance & Agricultural:**
- `potential_evapotranspiration`: Potential evapotranspiration (Thornthwaite method)
- `reference_evapotranspiration`: FAO-56 Penman-Monteith reference ET
- `spei_3`: 3-month Standardized Precipitation Evapotranspiration Index

### Multivariate Indices (4 indices)

**Combined Temperature-Precipitation Events:**
- `cold_and_dry_days`: Days with low temperature and low precipitation
- `cold_and_wet_days`: Days with low temperature and high precipitation
- `warm_and_dry_days`: Days with high temperature and low precipitation
- `warm_and_wet_days`: Days with high temperature and high precipitation

### Agricultural Indices (3 indices)

**Specialized Agricultural Applications:**
- `gsl`: Growing season length
- `spi_3`: 3-month Standardized Precipitation Index
- `spei_3`: Enhanced drought index with evapotranspiration

### Extreme Weather Indices (6 indices)

**Temperature Extremes:**
- `tx90p`: Warm days (daily maximum temperature > 90th percentile)
- `tn90p`: Warm nights (daily minimum temperature > 90th percentile)
- `tx10p`: Cool days (daily maximum temperature < 10th percentile)
- `tn10p`: Cool nights (daily minimum temperature < 10th percentile)

**Extreme Duration:**
- `wsdi`: Warm spell duration index (consecutive warm days)
- `csdi`: Cold spell duration index (consecutive cold days)

### Agricultural Indices (3 indices)

**Specialized Agricultural Metrics:**
- `gsl`: Growing season length (period suitable for plant growth)
- `spi`: Standardized Precipitation Index (drought monitoring, 3-month window)
- `spei`: Standardized Precipitation Evapotranspiration Index (requires additional variables)

### Technical Implementation Details

**Processing Architecture:**
- All indices calculated using the scientifically-validated **xclim library** (`src/indices_calculator.py`)
- **Modular design**: Separate calculation functions for each index category
- **Enhanced validation**: Comprehensive data quality checks for all input variables
- **Annual frequency**: Most indices use annual calculations (`freq='YS'`)
- **Robust error handling**: Each index calculation includes comprehensive exception handling
- **CF-compliant metadata**: All outputs follow Climate and Forecast conventions
- **WMO standards**: Thresholds and methodologies follow international meteorological standards

**Index Configuration:**
- Indices organized into 7 categories via YAML settings (`src/config.py:66-146`)
- **Temperature indices**: 17 comprehensive temperature-based calculations
- **Precipitation indices**: 10 precipitation event and intensity metrics
- **Humidity indices**: 2 fundamental humidity calculations
- **Comfort indices**: 2 human heat stress assessments
- **Evapotranspiration indices**: 3 water balance calculations
- **Multivariate indices**: 4 combined temperature-precipitation events
- **Agricultural indices**: 3 specialized agricultural applications
- Flexible threshold settings and custom percentile calculations
- Enhanced variable detection supporting multiple naming conventions

**New Capabilities:**
- **Multi-variable processing**: Integrates temperature, precipitation, and humidity data
- **Heat stress assessment**: Human comfort indices for public health applications
- **Advanced drought monitoring**: SPEI calculations with evapotranspiration
- **Agricultural optimization**: Enhanced growing season and water stress indices
- **Quality assurance**: Data validation checks for scientific integrity

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
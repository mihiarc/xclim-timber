# xclim-timber: Climate Data Processing Pipeline

A robust Python pipeline for processing climate raster data and calculating climate indices using the [xclim](https://xclim.readthedocs.io/) library. This pipeline efficiently handles large climate datasets from external drives, supporting both GeoTIFF and NetCDF formats.

## Features

- **Multi-format Support**: Load climate data from GeoTIFF and NetCDF files
- **Parallel Processing**: Leverages Dask for efficient processing of large datasets
- **Comprehensive Indices**: Calculate 30+ climate indices including:
  - Temperature indices (frost days, tropical nights, growing degree days)
  - Precipitation indices (consecutive dry/wet days, extreme precipitation)
  - Agricultural indices (growing season length, SPI)
  - Extreme event indices (heat waves, cold spells)
- **Data Quality Control**: Automatic outlier detection and missing value handling
- **CF Compliance**: Outputs follow Climate and Forecast (CF) conventions
- **Flexible Configuration**: YAML-based configuration for easy customization

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/xclim-timber.git
cd xclim-timber
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

### One-Time Setup

**Generate baseline percentiles for extreme indices** (required for temperature pipeline):
```bash
python calculate_baseline_percentiles.py
```
This is a one-time operation (~15 minutes) that calculates day-of-year percentiles from 1981-2000 baseline period. The results are cached in `data/baselines/` for all future runs.

### Running the Pipelines

1. **Run temperature pipeline** (18 indices):
```bash
python temperature_pipeline.py
```

2. **Run precipitation pipeline** (6 indices):
```bash
python precipitation_pipeline.py
```

3. **Run humidity pipeline** (8 indices):
```bash
python humidity_pipeline.py
```

All pipelines default to processing 1981-2024 data period. Use `--start-year` and `--end-year` to customize:
```bash
python temperature_pipeline.py --start-year 2000 --end-year 2020
```

## Configuration

Edit the configuration file to customize:

- **Data paths**: Location of input data on external drive
- **File patterns**: Patterns to identify climate variable files
- **Processing options**: Chunk sizes, Dask workers, resampling frequency
- **Climate indices**: Select which indices to calculate
- **Output format**: NetCDF or GeoTIFF

Example configuration snippet:
```yaml
data:
  input_path: /media/external_drive/climate_data
  output_path: ./outputs
  file_patterns:
    temperature: ['*tas*.tif', '*temp*.nc']
    precipitation: ['*pr*.tif', '*precip*.nc']

processing:
  chunk_size:
    time: 365
    lat: 100
    lon: 100
  dask:
    n_workers: 4
    memory_limit: 4GB

indices:
  temperature:
    - tg_mean
    - frost_days
    - growing_degree_days
  precipitation:
    - prcptot
    - rx1day
    - cdd
```

## Usage Examples

### Basic Pipeline Usage

```python
from src.pipeline import ClimateDataPipeline

# Initialize pipeline
pipeline = ClimateDataPipeline('config.yaml')

# Run complete pipeline
pipeline.run()
```

### Loading Data Only

```python
from src.config import Config
from src.data_loader import ClimateDataLoader

config = Config('config.yaml')
loader = ClimateDataLoader(config)

# Load temperature data
temp_data = loader.load_variable_data('temperature')
print(f"Loaded data shape: {dict(temp_data.dims)}")
```

### Calculating Specific Indices

```python
from src.indices_calculator import ClimateIndicesCalculator

calculator = ClimateIndicesCalculator(config)

# Calculate temperature indices
temp_indices = calculator.calculate_temperature_indices(temp_dataset)

# Save results
calculator.save_indices('outputs/indices.nc')
```

## Climate Indices

This pipeline currently implements **32 validated climate indices** (18 temperature + 6 precipitation + 8 humidity) with a goal of 84 total indices. All indices follow World Meteorological Organization (WMO) standards and CF (Climate and Forecast) conventions using the xclim library.

### Underlying Climate Variables

The pipeline processes these core climate variables:

**Temperature Data:**
- `tas`: Near-surface air temperature (daily mean)
- `tasmax`: Daily maximum near-surface air temperature
- `tasmin`: Daily minimum near-surface air temperature

**Precipitation Data:**
- `pr`: Daily precipitation amount

**Humidity Data:**
- `hus`: Specific humidity (kg/kg)
- `hurs`: Relative humidity (%)

**Variable Name Flexibility:** The system supports multiple naming conventions:
- Temperature: 'tas', 'temperature', 'temp', 'tmean', 'tasmax', 'tmax', 'tasmin', 'tmin'
- Precipitation: 'pr', 'precipitation', 'precip', 'prcp'
- Humidity: 'hus', 'huss', 'specific_humidity', 'hurs', 'relative_humidity', 'rh'

### Temperature Indices (18 indices - Currently Implemented)

**Basic Statistics (3):**
- `tg_mean`: Annual mean temperature
- `tx_max`: Annual maximum temperature
- `tn_min`: Annual minimum temperature

**Threshold-Based Counts (6):**
- `tropical_nights`: Number of nights with minimum temperature > 20°C
- `frost_days`: Number of days with minimum temperature < 0°C
- `ice_days`: Number of days with maximum temperature < 0°C
- `summer_days`: Number of days with maximum temperature > 25°C
- `hot_days`: Number of days with maximum temperature > 30°C
- `consecutive_frost_days`: Maximum consecutive frost days

**Degree Day Metrics (3):**
- `growing_degree_days`: Accumulated temperature above 10°C threshold (crop development)
- `heating_degree_days`: Accumulated temperature below 17°C threshold (energy demand)
- `cooling_degree_days`: Accumulated temperature above 18°C threshold (cooling energy demand)

**Extreme Percentile-Based Indices (6) - Uses 1981-2000 Baseline:**
- `tx90p`: Warm days (daily maximum temperature > 90th percentile)
- `tn90p`: Warm nights (daily minimum temperature > 90th percentile)
- `tx10p`: Cool days (daily maximum temperature < 10th percentile)
- `tn10p`: Cool nights (daily minimum temperature < 10th percentile)
- `warm_spell_duration_index`: Warm spell duration (≥6 consecutive warm days)
- `cold_spell_duration_index`: Cold spell duration (≥6 consecutive cold days)

### Precipitation Indices (6 indices - Currently Implemented)

**Basic Statistics (4):**
- `prcptot`: Total annual precipitation (wet days ≥ 1mm)
- `rx1day`: Maximum 1-day precipitation amount
- `rx5day`: Maximum 5-day precipitation amount
- `sdii`: Simple daily intensity index (average precipitation on wet days)

**Consecutive Events (2):**
- `cdd`: Maximum consecutive dry days (< 1mm)
- `cwd`: Maximum consecutive wet days (≥ 1mm)

**Note:** Percentile-based precipitation indices (r95p, r99p) and threshold indices (r10mm, r20mm) planned for future implementation.

### Humidity Indices (8 indices - Currently Implemented)

**Dewpoint Statistics (4):**
- `dewpoint_mean`: Annual mean dewpoint temperature
- `dewpoint_min`: Annual minimum dewpoint temperature
- `dewpoint_max`: Annual maximum dewpoint temperature
- `humid_days`: Days with dewpoint > 18°C (uncomfortable humidity)

**Vapor Pressure Deficit (4):**
- `vpdmax_mean`: Annual mean maximum VPD
- `extreme_vpd_days`: Days with VPD > 4 kPa (plant water stress)
- `vpdmin_mean`: Annual mean minimum VPD
- `low_vpd_days`: Days with VPD < 0.5 kPa (high moisture/fog potential)

### Human Comfort Indices (2 indices)

**Heat Stress Assessment:**
- `heat_index`: Heat index combining temperature and humidity effects
- `humidex`: Canadian humidex index for apparent temperature

### Evapotranspiration Indices (3 indices)

**Water Balance Calculations:**
- `potential_evapotranspiration`: Potential evapotranspiration (Thornthwaite method)
- `reference_evapotranspiration`: FAO-56 Penman-Monteith reference ET
- `spei_3`: 3-month Standardized Precipitation Evapotranspiration Index

### Multivariate Indices (4 indices)

**Combined Temperature-Precipitation:**
- `cold_and_dry_days`: Days with low temperature and low precipitation
- `cold_and_wet_days`: Days with low temperature and high precipitation
- `warm_and_dry_days`: Days with high temperature and low precipitation
- `warm_and_wet_days`: Days with high temperature and high precipitation

### Extreme Weather Indices (6 indices)

**Temperature Extremes:**
- `tx90p`: Warm days (days when daily maximum temperature > 90th percentile)
- `tn90p`: Warm nights (days when daily minimum temperature > 90th percentile)
- `tx10p`: Cool days (days when daily maximum temperature < 10th percentile)
- `tn10p`: Cool nights (days when daily minimum temperature < 10th percentile)

**Extreme Duration:**
- `wsdi`: Warm spell duration index (consecutive warm days)
- `csdi`: Cold spell duration index (consecutive cold days)

### Agricultural Indices (3 indices)

**Specialized Agricultural Metrics:**
- `gsl`: Growing season length (period suitable for plant growth)
- `spi`: Standardized Precipitation Index (drought monitoring, 3-month window)
- `spei`: Standardized Precipitation Evapotranspiration Index (requires additional data)

### Index Calculation Details

**Processing Architecture:**
- All indices calculated using the scientifically-validated **xclim library**
- **Annual frequency**: Most indices use annual calculations (`freq='YS'`)
- **Robust error handling**: Each index calculation includes comprehensive error handling
- **CF-compliant metadata**: All outputs follow Climate and Forecast conventions

## Pipeline Architecture

```
xclim-timber/
├── src/                    # Core pipeline modules
│   ├── config.py           # Configuration management
│   ├── data_loader.py      # Data loading from various formats
│   ├── preprocessor.py     # Data cleaning and standardization
│   ├── indices_calculator.py # Climate indices calculation
│   └── pipeline.py         # Main orchestration
├── scripts/                # Processing and analysis scripts
│   ├── csv_formatter.py    # CSV format converter (long ↔ wide)
│   ├── efficient_extraction.py # Optimized point extraction
│   ├── fast_point_extraction.py # Alternative extraction method
│   └── visualize_temp.py   # Results visualization
├── data/                   # Data files
│   ├── test_data/          # Test datasets and coordinates
│   └── sample_data/        # Sample data for development
├── outputs/                # Processed results
├── logs/                   # Processing logs
├── docs/                   # Documentation
├── benchmarks/             # Performance benchmarks
└── requirements.txt        # Dependencies
```

## Performance Optimization

- **Chunking**: Data is automatically chunked for efficient memory usage
- **Parallel Processing**: Dask enables parallel computation across multiple cores
- **Lazy Evaluation**: Operations are queued and executed efficiently
- **Memory Management**: Large datasets are processed without loading entirely into memory

## Command Line Interface

```bash
# Show help
python src/pipeline.py --help

# Run with verbose logging
python src/pipeline.py -c config.yaml --verbose

# Specify output directory
python src/pipeline.py -c config.yaml -o /path/to/output

# Process specific variables
python src/pipeline.py -c config.yaml -v temperature -v precipitation

4. **Format CSV outputs** (optional):
```bash
# Convert to both long and wide formats
python scripts/csv_formatter.py --input-dir outputs --output-dir outputs/formatted
```

## Monitoring

Access the Dask dashboard during processing to monitor:
- Worker activity
- Memory usage
- Task progress
- Performance metrics

Dashboard typically available at: http://localhost:8787

## Troubleshooting

### Memory Issues
- Reduce chunk sizes in configuration
- Decrease number of Dask workers
- Process variables separately

### Missing Data
- Check file patterns in configuration
- Verify data path accessibility
- Review logs for loading errors

### Performance
- Increase Dask workers for more parallelism
- Optimize chunk sizes for your data
- Consider temporal/spatial subsetting

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License.

## Acknowledgments

- Built on [xclim](https://xclim.readthedocs.io/) for climate index calculations
- Uses [xarray](http://xarray.pydata.org/) for N-dimensional data handling
- Powered by [Dask](https://dask.org/) for parallel computing
- Supports [rioxarray](https://corteva.github.io/rioxarray/) for geospatial operations

## Citation

If you use this pipeline in your research, please cite:
```
xclim-timber: Climate Data Processing Pipeline
https://github.com/yourusername/xclim-timber
```

And the xclim library:
```
Bourgault et al., (2023). xclim: xarray-based climate data analytics. 
Journal of Open Source Software, 8(85), 5415, https://doi.org/10.21105/joss.05415
```
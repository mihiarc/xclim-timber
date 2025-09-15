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

1. **Create a configuration file**:
```bash
python src/pipeline.py --create-config
```
This creates `config_sample.yaml`. Edit it to specify your data paths and processing options.

2. **Run the pipeline**:
```bash
python src/pipeline.py --config config_sample.yaml
```

3. **Process specific variables**:
```bash
python src/pipeline.py -c config.yaml -v temperature -v precipitation
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

### Temperature Indices
- **tg_mean**: Mean temperature
- **tx_max**: Maximum temperature
- **tn_min**: Minimum temperature
- **frost_days**: Days with minimum temperature < 0°C
- **ice_days**: Days with maximum temperature < 0°C
- **summer_days**: Days with maximum temperature > 25°C
- **tropical_nights**: Nights with minimum temperature > 20°C
- **growing_degree_days**: Accumulated temperature for crop growth
- **heating/cooling_degree_days**: Energy demand indicators

### Precipitation Indices
- **prcptot**: Total precipitation
- **rx1day/rx5day**: Maximum 1-day and 5-day precipitation
- **sdii**: Simple daily intensity index
- **cdd/cwd**: Consecutive dry/wet days
- **r10mm/r20mm**: Heavy precipitation days
- **r95p/r99p**: Very wet and extremely wet days

### Extreme Indices
- **tx90p/tn90p**: Warm days and nights
- **tx10p/tn10p**: Cool days and nights
- **wsdi/csdi**: Warm and cold spell duration indices

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
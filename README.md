# xclim-timber: Climate Data Processing Pipeline

A robust Python pipeline for processing climate raster data and calculating climate indices using the [xclim](https://xclim.readthedocs.io/) library. This pipeline efficiently handles large climate datasets from external drives, supporting both GeoTIFF and NetCDF formats.

## Features

- **Multi-format Support**: Load climate data from GeoTIFF and NetCDF files
- **Parallel Processing**: Leverages Dask for efficient processing of large datasets
- **Comprehensive Indices**: Calculate 66+ climate indices including:
  - Temperature indices (frost days, tropical nights, growing degree days)
  - Precipitation indices (consecutive dry/wet days, extreme precipitation)
  - Agricultural indices (growing season length, PET, corn heat units)
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

**Generate baseline percentiles for extreme indices** (required for temperature, precipitation, and multivariate pipelines):
```bash
python calculate_baseline_percentiles.py
```
This is a one-time operation (~20-30 minutes) that calculates day-of-year percentiles from 1981-2000 baseline period for temperature extremes, precipitation extremes, and multivariate thresholds. The results are cached as `data/baselines/baseline_percentiles_1981_2000.nc` (10.7GB) for all future runs.

### Running the Pipelines

1. **Run temperature pipeline** (33 indices - Phase 7):
```bash
python temperature_pipeline.py
```

2. **Run precipitation pipeline** (13 indices - Phase 6):
```bash
python precipitation_pipeline.py
```

3. **Run humidity pipeline** (8 indices):
```bash
python humidity_pipeline.py
```

4. **Run human comfort pipeline** (3 indices):
```bash
python human_comfort_pipeline.py
```

5. **Run multivariate pipeline** (4 indices):
```bash
python multivariate_pipeline.py
```

6. **Run agricultural pipeline** (5 indices - Phase 8):
```bash
python agricultural_pipeline.py
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

This pipeline currently implements **66 validated climate indices** (33 temperature + 13 precipitation + 8 humidity + 3 human comfort + 4 multivariate + 5 agricultural) with a goal of 80 total indices. All indices follow World Meteorological Organization (WMO) standards and CF (Climate and Forecast) conventions using the xclim library.

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

### Temperature Indices (33 indices - Currently Implemented, Phase 7 Complete)

**Basic Statistics (3):**
- `tg_mean`: Annual mean temperature
- `tx_max`: Annual maximum temperature
- `tn_min`: Annual minimum temperature

**Temperature Range Metrics (2):**
- `daily_temperature_range`: Mean daily temperature range (tmax - tmin)
- `extreme_temperature_range`: Annual max(tmax) - min(tmin) (annual extremes span)

**Threshold-Based Counts (6):**
- `tropical_nights`: Number of nights with minimum temperature > 20°C
- `frost_days`: Number of days with minimum temperature < 0°C
- `ice_days`: Number of days with maximum temperature < 0°C
- `summer_days`: Number of days with maximum temperature > 25°C
- `hot_days`: Number of days with maximum temperature > 30°C
- `consecutive_frost_days`: Maximum consecutive frost days

**Frost Season Indices (4):**
- `frost_season_length`: Duration from first to last frost (agricultural planning)
- `frost_free_season_start`: Julian day of last spring frost (planting date)
- `frost_free_season_end`: Julian day of first fall frost (harvest planning)
- `frost_free_season_length`: Days between last spring and first fall frost

**Degree Day Metrics (4):**
- `growing_degree_days`: Accumulated temperature above 10°C threshold (crop development)
- `heating_degree_days`: Accumulated temperature below 17°C threshold (energy demand)
- `cooling_degree_days`: Accumulated temperature above 18°C threshold (cooling energy demand)
- `freezing_degree_days`: Accumulated temperature below 0°C (winter severity)

**Extreme Percentile-Based Indices (6) - Uses 1981-2000 Baseline:**
- `tx90p`: Warm days (daily maximum temperature > 90th percentile)
- `tn90p`: Warm nights (daily minimum temperature > 90th percentile)
- `tx10p`: Cool days (daily maximum temperature < 10th percentile)
- `tn10p`: Cool nights (daily minimum temperature < 10th percentile)
- `warm_spell_duration_index`: Warm spell duration (≥6 consecutive warm days)
- `cold_spell_duration_index`: Cold spell duration (≥6 consecutive cold days)

**Advanced Temperature Extremes (8) - Phase 7:**
- `growing_season_start`: First day when temperature exceeds 5°C for 5+ consecutive days (ETCCDI standard)
- `growing_season_end`: First day after July 1st when temperature drops below 5°C for 5+ consecutive days
- `cold_spell_frequency`: Number of discrete cold spell events (temperature < -10°C for 5+ days)
- `hot_spell_frequency`: Number of hot spell events (tasmax > 30°C for 3+ days)
- `heat_wave_frequency`: Number of heat wave events (tasmin > 22°C AND tasmax > 30°C for 3+ days)
- `freezethaw_spell_frequency`: Number of freeze-thaw cycles (tasmax > 0°C AND tasmin ≤ 0°C on same day)
- `last_spring_frost`: Last day in spring when tasmin < 0°C (critical for agriculture)
- `daily_temperature_range_variability`: Average day-to-day variation in daily temperature range (climate stability)

### Precipitation Indices (13 indices - Currently Implemented, Phase 6 Complete)

**Basic Statistics (4):**
- `prcptot`: Total annual precipitation (wet days ≥ 1mm)
- `rx1day`: Maximum 1-day precipitation amount
- `rx5day`: Maximum 5-day precipitation amount
- `sdii`: Simple daily intensity index (average precipitation on wet days)

**Consecutive Events (2):**
- `cdd`: Maximum consecutive dry days (< 1mm)
- `cwd`: Maximum consecutive wet days (≥ 1mm)

**Extreme Percentile-Based Indices (2) - Uses 1981-2000 Baseline:**
- `r95p`: Very wet days (precipitation > 95th percentile of wet days)
- `r99p`: Extremely wet days (precipitation > 99th percentile of wet days)

**Fixed Threshold Indices (2):**
- `r10mm`: Heavy precipitation days (≥ 10mm)
- `r20mm`: Very heavy precipitation days (≥ 20mm)

**Enhanced Precipitation Analysis (3) - Phase 6:**
- `dry_days`: Total number of dry days (< 1mm)
- `wetdays`: Total number of wet days (≥ 1mm)
- `wetdays_prop`: Proportion of days that are wet

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

### Human Comfort Indices (3 indices - Currently Implemented)

**Heat Stress Assessment:**
- `heat_index`: Heat index combining temperature and humidity effects (apparent temperature)
- `humidex`: Canadian humidex index for apparent temperature

**Humidity Validation:**
- `relative_humidity`: Relative humidity calculated from dewpoint temperature (QC metric)

### Multivariate Indices (4 indices - Currently Implemented)

**Compound Climate Extremes - Uses 1981-2000 Baseline:**
- `cold_and_dry_days`: Days with temperature below 25th percentile AND precipitation below 25th percentile (compound drought conditions)
- `cold_and_wet_days`: Days with temperature below 25th percentile AND precipitation above 75th percentile (flooding risk, winter storms)
- `warm_and_dry_days`: Days with temperature above 75th percentile AND precipitation below 25th percentile (drought/fire weather)
- `warm_and_wet_days`: Days with temperature above 75th percentile AND precipitation above 75th percentile (compound extreme events)

**Scientific Context:** These multivariate indices capture compound climate extremes that result from the interaction of multiple climate variables. They are increasingly important for climate change impact assessment, as compound events often have disproportionate impacts compared to single-variable extremes.

### Agricultural Indices (5 indices - Currently Implemented, Phase 8 Complete)

**Growing Season Analysis (1):**
- `growing_season_length`: Total days between first and last occurrence of 6+ consecutive days with temperature above 5°C (ETCCDI standard)

**Water Balance (1):**
- `potential_evapotranspiration`: Annual potential evapotranspiration using Baier-Robertson 1965 method (temperature-only, suitable for regions without wind/radiation data)

**Crop-Specific Indices (1):**
- `corn_heat_units`: Annual accumulated corn heat units for crop development and maturity prediction (USDA standard, widely used in North American agriculture)

**Spring Thaw Monitoring (1):**
- `thawing_degree_days`: Sum of degree-days above 0°C (permafrost monitoring, spring melt timing, critical for northern latitudes)

**Growing Season Water Availability (1):**
- `growing_season_precipitation`: Total precipitation during growing season (April-October, northern hemisphere)

**Agricultural Value:** These indices support agricultural decision-making including crop variety selection, planting timing, irrigation scheduling, and harvest planning. They are particularly valuable for adapting to climate change impacts on agriculture.

---

## Planned Future Indices (14 additional indices toward 80 goal)

The following index categories are planned for future implementation:

### Drought & Water Balance Indices (3 indices - Planned)

**Advanced Water Balance:**
- `reference_evapotranspiration`: FAO-56 Penman-Monteith reference ET (requires wind and solar radiation data)
- `spi_3`: 3-month Standardized Precipitation Index (drought monitoring)
- `spei_3`: 3-month Standardized Precipitation Evapotranspiration Index

**Note:** SPI and SPEI require statistical distribution fitting and longer processing time

---

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
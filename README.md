# xclim-timber

A streamlined climate data extraction tool for timber economics research. Efficiently extracts climate indices at geographic point locations from NetCDF files.

## Features

- **Fast extraction**: Vectorized operations process thousands of parcels in seconds
- **Essential climate indices**: Temperature statistics, growing degree days, frost days, and more
- **Batch processing**: Process multiple years of climate data efficiently
- **Simple interface**: Clear command-line interface with sensible defaults

## Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd xclim-timber

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Single Year Processing

Extract temperature indices:
```bash
python xclim_timber.py --input temperature_data.nc --parcels coordinates.csv --output temp_results.csv
```

Extract precipitation indices:
```bash
python xclim_timber.py --input precipitation_data.nc --parcels coordinates.csv \
    --output precip_results.csv --variable-type precipitation
```

### Multi-Year Batch Processing

Process multiple years of climate data:

```bash
python xclim_timber.py \
    --parcels coordinates.csv \
    --data-dir /path/to/climate/data \
    --start-year 2020 \
    --end-year 2023 \
    --scenario historical \
    --output-dir outputs
```

### CSV Output Formatting (Optional)

Convert climate indices to different formats for analysis:

```bash
# Convert to both long and wide formats
python csv_formatter.py --input-dir outputs --output-dir formatted

# Specify historical period
python csv_formatter.py --start-year 1950 --end-year 2014 --input-dir outputs
```

**Output Formats:**
- **Long format**: Each row = location-year (time series analysis)
- **Wide format**: Each row = location, columns = index_year (spatial analysis)

## Input Format

### Parcel Coordinates CSV

Your parcels CSV should have the following columns:
- `saleid`: Unique identifier for the sale
- `parcelid`: Unique identifier for the parcel
- `parcel_level_latitude`: Latitude in decimal degrees
- `parcel_level_longitude`: Longitude in decimal degrees

Example:
```csv
saleid,parcelid,parcel_level_latitude,parcel_level_longitude
1,P001,45.5231,-122.6765
2,P002,45.5152,-122.6544
```

## Output

The tool calculates 60+ comprehensive climate indices for each parcel location.

### Temperature Indices (when processing temperature data)

#### Basic Statistics
- `annual_mean`, `annual_min`, `annual_max`, `annual_std`, `annual_range`
- Temperature percentiles: `temp_p5`, `temp_p10`, `temp_p25`, `temp_p50`, `temp_p75`, `temp_p90`, `temp_p95`

#### Threshold-Based Counts
- **Cold extremes**: `frost_days` (<0°C), `ice_days` (<-10°C), `deep_freeze_days` (<-20°C)
- **Warm extremes**: `summer_days` (>25°C), `hot_days` (>30°C), `very_hot_days` (>35°C), `extreme_heat_days` (>40°C)
- **Tropical indices**: `tropical_nights` (>20°C), `warm_nights` (>15°C)

#### Degree Days (Multiple Base Temperatures)
- **Growing Degree Days**: `gdd_base0`, `gdd_base5`, `gdd_base10`, `gdd_base15`
- **Heating Degree Days**: `hdd_base15`, `hdd_base18`, `hdd_base20`
- **Cooling Degree Days**: `cdd_base18`, `cdd_base20`, `cdd_base22`

#### Agricultural & Forestry Indices
- `corn_gdd`: Corn-specific GDD (base 10°C, capped at 30°C)
- `killing_degree_days`: Heat stress indicator
- `chill_days`: Days below 7°C (fruit tree dormancy)
- `vernalization_days`: Days between 0-10°C (winter wheat)
- `optimal_growth_days`: Days in 15-25°C range
- `drought_stress_days`: Days above 30°C
- `freeze_thaw_cycles`: Number of 0°C crossings

#### Extreme Event Indices
- `max_consecutive_frost`: Longest frost period
- `max_consecutive_summer`: Longest warm period
- `max_consecutive_hot`: Longest heat wave
- `cold_spell_days`: Days below 10th percentile
- `warm_spell_days`: Days above 90th percentile

#### Bioclimatic Variables
- `bio4_temp_seasonality`: Temperature variability
- `bio5_max_temp_warmest_month`: Warmest period average
- `bio6_min_temp_coldest_month`: Coldest period average

### Precipitation Indices (when processing precipitation data)

#### Basic Statistics
- `total_precip`, `mean_precip`, `max_precip`, `precip_std`
- Precipitation percentiles: `precip_p5` through `precip_p99`

#### Precipitation Events
- `wet_days` (≥1mm), `heavy_precip_days` (≥10mm), `very_heavy_precip_days` (≥20mm), `extreme_precip_days` (≥50mm)
- `dry_days` (<1mm), `very_dry_days` (<0.1mm)

#### Consecutive Events
- `max_consecutive_dry`: Longest dry spell
- `max_consecutive_wet`: Longest wet spell
- `max_5day_precip`: Maximum 5-day precipitation total
- `simple_daily_intensity`: Average precipitation on wet days

## Performance

- Processes 1,000 parcels in ~0.2 seconds
- Processes 3,000 parcels in ~0.6 seconds
- Scales efficiently with O(1) complexity after initial data load

## Requirements

- Python 3.8+
- xarray
- pandas
- numpy
- netCDF4

## License

[Your License]

## Citation

If you use this tool in your research, please cite:
[Your citation information]
# Baseline Period Configuration for Climate Indices

## Overview

The xclim-timber pipeline (now Zarr-exclusive) supports configurable baseline periods for percentile-based climate indices, following World Meteorological Organization (WMO) standards. This feature enables proper climate change signal detection by comparing current conditions against a historical baseline.

**Note**: As of v2.0, the pipeline works exclusively with Zarr stores. All examples use Zarr format.

## Configuration

### Default Settings

The pipeline uses a configurable baseline period (default: 1981-2000 for PRISM data compatibility):

```yaml
indices:
  # Baseline period for percentile calculations
  baseline_period:
    start: 1981  # Configurable - using 1981-2000 for PRISM
    end: 2000
  use_baseline_for_percentiles: true
```

**Note**: The baseline period should match your data availability. PRISM data starts in 1981, so we use 1981-2000 as our 20-year baseline.

### Affected Indices

When `use_baseline_for_percentiles` is enabled, the following indices use the baseline period for percentile calculation:

#### Temperature Percentile Indices
- **tx90p**: Warm days (daily maximum temperature > 90th percentile)
- **tn90p**: Warm nights (daily minimum temperature > 90th percentile)
- **tx10p**: Cool days (daily maximum temperature < 10th percentile)
- **tn10p**: Cool nights (daily minimum temperature < 10th percentile)

#### Precipitation Percentile Indices
- **r95p**: Very wet days (precipitation > 95th percentile)
- **r99p**: Extremely wet days (precipitation > 99th percentile)

## How It Works

### 1. Pre-calculation of Baseline Percentiles
**IMPORTANT**: Percentile indices require the full baseline period (20+ years) to calculate proper day-of-year thresholds. This must be done BEFORE processing data in chunks:

```bash
# Run once to generate baseline percentiles
python src/calculate_baseline_percentiles.py
# Creates: data/baselines/baseline_percentiles_1981_2000.nc
```

### 2. Streaming Pipeline Integration
The pre-calculated percentiles can then be used when processing data in yearly chunks:
- Load the baseline percentiles file
- Apply thresholds to each year's data independently
- Count exceedances for that year

### 3. Index Calculation
The percentile thresholds from the baseline period are applied to any time period:
- **Baseline period (1981-2000)**: Should show ~10% exceedance for 90th percentile indices
- **Recent period (2001-present)**: May show higher exceedance due to climate change

### 4. Climate Change Signal Detection
This approach enables detection of climate change signals:
- If recent periods show >10% exceedance of the baseline 90th percentile, it indicates warming
- If recent periods show <10% exceedance of the baseline 10th percentile, it indicates fewer cold extremes

## Example Usage

### Basic Configuration

```python
from src.config import Config
from src.indices_calculator import ClimateIndicesCalculator

# Create configuration with baseline period
config = Config('configs/config_template.yaml')
config.set('indices.baseline_period.start', 1981)
config.set('indices.baseline_period.end', 2000)
config.set('indices.use_baseline_for_percentiles', True)

# Initialize calculator with Zarr data
calculator = ClimateIndicesCalculator(config)

# Load temperature data from Zarr store
loader = ClimateDataLoader(config)
temperature_dataset = loader.load_zarr('path/to/temperature.zarr')

# Calculate indices (will use baseline for percentiles)
indices = calculator.calculate_temperature_indices(temperature_dataset)
```

### YAML Configuration

```yaml
# config.yaml - Zarr-exclusive configuration
data:
  input_path: /path/to/your/zarr/stores  # Path to Zarr data
  output_path: ./outputs
  log_path: ./logs

  # Zarr store patterns
  zarr_stores:
    temperature:
      - '*temperature*.zarr'
      - '*tas*.zarr'

indices:
  # Baseline period (using 1981-2000 for PRISM)
  baseline_period:
    start: 1981  # 20-year baseline
    end: 2000
  use_baseline_for_percentiles: true

  temperature:
    - tx90p  # Will use 1981-2000 for percentile calculation
    - tn10p  # Will use 1981-2000 for percentile calculation
    - tg_mean  # Not affected (not a percentile index)
```

## Validation and Quality Control

### Data Coverage Requirements
- Minimum 80% data coverage in baseline period required
- Warning issued if coverage is below threshold
- Calculation proceeds but results may not be representative

### Baseline Period Validation
- Start year must be before end year
- Minimum 10-year baseline period (30 years recommended by WMO)
- Validation occurs during calculator initialization

## Scientific Background

### WMO Standards
The World Meteorological Organization recommends:
- 30-year baseline periods for climate normals
- Standard periods: 1961-1990, 1971-2000, 1981-2010, 1991-2020 (current)
- Consistent baseline use across institutions for comparability

### Climate Change Detection
Using a fixed baseline period enables:
- Detection of trends in extreme events
- Quantification of changes in climate variability
- Comparison across different time periods and regions

## Implementation Details

### Pre-calculated Baseline Percentiles

The pipeline now uses **pre-calculated day-of-year percentiles** following WMO guidelines:

1. **One-time Calculation**: Run `calculate_baseline_percentiles.py` to generate baseline thresholds
2. **Day-of-Year Percentiles**: Calculates separate thresholds for each day of the year (366 values)
3. **Proper Seasonal Adjustment**: Accounts for seasonal temperature variations
4. **Efficient Processing**: Pre-calculated thresholds enable fast processing of any time period

### Calculation Process

```bash
# Step 1: Calculate baseline percentiles (one-time, ~10-20 minutes)
python src/calculate_baseline_percentiles.py

# This creates: data/baselines/baseline_percentiles_1981_2000.nc
# Contains 3D arrays (lat × lon × dayofyear) for:
#   - tx90p_threshold: 90th percentile of daily max temperature
#   - tx10p_threshold: 10th percentile of daily max temperature
#   - tn90p_threshold: 90th percentile of daily min temperature
#   - tn10p_threshold: 10th percentile of daily min temperature
```

### Key Features

1. **Day-of-Year Percentiles** ✅:
   - Implements proper WMO methodology
   - 5-day window around each calendar day for robust estimates
   - Accounts for seasonal temperature cycles

2. **Memory Efficient**:
   - Pre-calculation allows processing data in small chunks
   - Streaming pipeline can process year-by-year using pre-calculated thresholds

3. **Quality Assured**:
   - Validated temperature ranges for all percentiles
   - Ensures 90th > 10th percentiles everywhere
   - ~45% missing data matches PRISM ocean mask

## Testing

A test script is provided to verify baseline functionality:

```bash
# First ensure you have test data in Zarr format
python scripts/tests/test_baseline_simple.py
```

**Note**: Test scripts may need updating to use Zarr stores instead of NetCDF files.

This test:
1. Creates synthetic data with a warming trend
2. Calculates tx90p using configurable baseline period
3. Verifies that recent period shows increased warm days
4. Demonstrates climate change signal detection

### Expected Results
- Baseline period (1981-2000): ~10% exceedance
- Recent period (2001-present): Higher exceedance indicates warming
- Magnitude depends on location and data source

## References

1. WMO Guidelines on the Calculation of Climate Normals (WMO-No. 1203)
2. Zhang, X., et al. (2005). "Avoiding inhomogeneity in percentile-based indices of temperature extremes." Journal of Climate, 18(11), 1641-1651.
3. ETCCDI Climate Change Indices: http://etccdi.pacificclimate.org/

## Support

For questions or issues related to baseline period configuration:
1. Check the configuration validates correctly during initialization
2. Ensure your data covers the baseline period adequately
3. Review log messages for coverage warnings
4. Refer to WMO guidelines for standard baseline periods
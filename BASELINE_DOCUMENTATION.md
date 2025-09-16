# Baseline Period Configuration for Climate Indices

## Overview

The xclim-timber pipeline now supports configurable baseline periods for percentile-based climate indices, following World Meteorological Organization (WMO) standards. This feature enables proper climate change signal detection by comparing current conditions against a historical baseline.

## Configuration

### Default Settings

The pipeline uses the WMO standard baseline period by default:

```yaml
indices:
  baseline_period:
    start: 1971
    end: 2000
  use_baseline_for_percentiles: true
```

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

### 1. Baseline Period Extraction
When calculating percentile indices, the system:
1. Extracts data for the baseline period (1971-2000)
2. Validates data coverage (warns if <80% complete)
3. Calculates percentile thresholds from baseline data only

### 2. Index Calculation
The percentile thresholds from the baseline period are then applied to the entire time series:
- **Baseline period (1971-2000)**: Should show ~10% exceedance for 90th percentile indices
- **Recent period (2001-2015)**: May show higher exceedance due to climate change

### 3. Climate Change Signal Detection
This approach enables detection of climate change signals:
- If recent periods show >10% exceedance of the baseline 90th percentile, it indicates warming
- If recent periods show <10% exceedance of the baseline 10th percentile, it indicates fewer cold extremes

## Example Usage

### Basic Configuration

```python
from src.config import Config
from src.indices_calculator import ClimateIndicesCalculator

# Create configuration with baseline period
config = Config()
config.set('indices.baseline_period.start', 1971)
config.set('indices.baseline_period.end', 2000)
config.set('indices.use_baseline_for_percentiles', True)

# Initialize calculator
calculator = ClimateIndicesCalculator(config)

# Calculate indices (will use baseline for percentiles)
indices = calculator.calculate_temperature_indices(temperature_dataset)
```

### YAML Configuration

```yaml
# config.yaml
data:
  input_path: /path/to/climate/data
  output_path: ./outputs

indices:
  # WMO standard baseline period
  baseline_period:
    start: 1971
    end: 2000
  use_baseline_for_percentiles: true

  temperature:
    - tx90p  # Will use 1971-2000 for percentile calculation
    - tn10p  # Will use 1971-2000 for percentile calculation
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
- Standard periods: 1961-1990, 1971-2000, 1981-2010, 1991-2020
- Consistent baseline use across institutions for comparability

### Climate Change Detection
Using a fixed baseline period enables:
- Detection of trends in extreme events
- Quantification of changes in climate variability
- Comparison across different time periods and regions

## Known Limitations

### Current Implementation
1. **Simplified Percentile Calculation**:
   - Uses simple quantile calculation over entire baseline period
   - Does not implement bootstrap or day-of-year percentiles as per full WMO guidelines

2. **No Seasonal Adjustment**:
   - Percentiles calculated across all days in baseline period
   - Does not account for seasonal variations in percentile thresholds

3. **Fixed Percentile Values**:
   - Uses standard percentiles (10th, 90th, 95th, 99th)
   - Cannot configure custom percentile values

### Future Enhancements
- Implement bootstrap methodology for robust percentile estimation
- Add day-of-year percentile calculation for seasonal adjustment
- Support multiple baseline periods for comparison studies
- Add percentile interpolation methods

## Testing

A test script is provided to verify baseline functionality:

```bash
python test_baseline_simple.py
```

This test:
1. Creates synthetic data with a warming trend
2. Calculates tx90p using 1971-2000 baseline
3. Verifies that recent period (2001-2015) shows increased warm days
4. Demonstrates climate change signal detection

### Expected Results
- Baseline period (1971-2000): ~10% exceedance
- Recent period (2001-2015): ~18% exceedance
- Indicates significant warming signal

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
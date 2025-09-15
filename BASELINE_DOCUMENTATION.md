# Baseline Period for Percentile Calculations

## Overview

The xclim-timber pipeline now supports configurable baseline periods for percentile-based climate indices, ensuring compliance with WMO (World Meteorological Organization) standards and enabling accurate climate change signal detection.

## Background

According to WMO guidelines and ETCCDI (Expert Team on Climate Change Detection and Indices) standards, percentile-based climate indices should be calculated relative to a reference baseline period, typically:
- 1971-2000 (current default)
- 1961-1990 (older standard)
- 1981-2010 (alternative modern baseline)
- 1991-2020 (newest WMO baseline)

## Configuration

### YAML Configuration

Add the following to your configuration file:

```yaml
indices:
  # Baseline period for percentile calculations
  baseline_period:
    start: 1971      # Start year of baseline period
    end: 2000        # End year of baseline period
  use_baseline_for_percentiles: true  # Set to false to use entire period
```

### Programmatic Configuration

```python
from config import Config

config = Config()
config.config['indices']['baseline_period'] = {
    'start': 1971,
    'end': 2000
}
config.config['indices']['use_baseline_for_percentiles'] = True
```

## Affected Climate Indices

The following percentile-based indices now use the baseline period:

### Temperature Extremes
- **tx90p**: Warm days (days when Tmax > 90th percentile)
- **tn90p**: Warm nights (days when Tmin > 90th percentile)
- **tx10p**: Cool days (days when Tmax < 10th percentile)
- **tn10p**: Cool nights (days when Tmin < 10th percentile)

### Precipitation Extremes
- **r95p**: Very wet days (precipitation > 95th percentile)
- **r99p**: Extremely wet days (precipitation > 99th percentile)

## Implementation Details

### How It Works

1. **Baseline Selection**: When calculating percentiles, the pipeline extracts data from the specified baseline period (e.g., 1971-2000)

2. **Percentile Calculation**: Percentiles are computed using only the baseline period data:
   ```python
   baseline_data = data.sel(time=slice('1971', '2000'))
   threshold = baseline_data.quantile(0.9, dim='time')
   ```

3. **Index Calculation**: The calculated threshold is then applied to the entire time series to determine exceedances

### Edge Case Handling

- **Missing Baseline Data**: If the baseline period is not available in the dataset, the pipeline falls back to using the entire time series with a warning
- **Partial Baseline**: If only partial baseline data exists, it uses what's available
- **Future Projections**: Works correctly with climate model projections that may not include historical baseline periods

## Scientific Impact

### With Baseline Period (Recommended)
- **Accurate Climate Signal**: Better captures warming trends by comparing recent extremes to historical baseline
- **WMO Compliance**: Results are comparable with international climate studies
- **Clear Trends**: Shows increasing frequency of extremes in recent decades relative to baseline

### Without Baseline Period
- **Underestimated Changes**: May underestimate climate change impacts as percentiles include recent warming
- **Limited Comparability**: Results may not be comparable with standard climate assessments
- **Masked Signals**: Climate change signal may be partially masked

## Example Results

Using the test script with 1971-2000 baseline:
- **Baseline period (1971-2000)**: ~10% of days exceed 90th percentile (by definition)
- **Target period (2001-2015)**: ~18% of days exceed 90th percentile
- **Interpretation**: Nearly double the frequency of extreme warm days in recent period

## Testing

Run the test script to verify baseline functionality:

```bash
# Simple test focusing on baseline calculations
python test_baseline_simple.py

# Comprehensive test with multiple indices
python test_baseline_percentiles.py
```

## Best Practices

1. **Choose Appropriate Baseline**: Select a baseline period with good data coverage that represents pre-industrial or early industrial climate

2. **Document Your Choice**: Always document which baseline period was used in your analysis

3. **Consider Data Availability**: Ensure your chosen baseline period has sufficient data coverage

4. **Validate Results**: Compare results with and without baseline to understand the impact

## References

- WMO Guidelines on the Calculation of Climate Normals (WMO-No. 1203)
- Zhang, X., et al. (2011): Indices for monitoring changes in extremes based on daily temperature and precipitation data
- IPCC Sixth Assessment Report (AR6) methodology for climate extremes

## Migration Guide

For existing pipelines:

1. **Default Behavior**: The pipeline now defaults to using 1971-2000 baseline
2. **Disable Baseline**: Set `use_baseline_for_percentiles: false` to maintain old behavior
3. **Custom Period**: Modify `baseline_period` to use alternative years
4. **Validation**: Re-run analyses to ensure results meet expectations

## Troubleshooting

### Common Issues

1. **No data for baseline period**
   - Check your input data temporal coverage
   - Consider using a different baseline period
   - Set `use_baseline_for_percentiles: false` as fallback

2. **Unexpected percentile values**
   - Verify baseline period configuration
   - Check data units and preprocessing
   - Review log messages for warnings

3. **Performance concerns**
   - Baseline extraction is optimized and adds minimal overhead
   - Uses lazy evaluation with xarray/dask

## Future Enhancements

Potential improvements under consideration:
- Support for multiple baseline periods for comparison
- Automatic baseline period detection based on data availability
- Integration with CMIP6 standard baseline periods
- Baseline period validation and quality checks
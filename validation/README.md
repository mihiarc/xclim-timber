# xclim-timber Data Quality Validation Suite

## Overview

The xclim-timber validation suite provides comprehensive automated data quality checks for climate indices pipeline outputs. It was developed in response to discovering corrupted data in years 2003/2005, ensuring such issues are caught automatically in the future.

## Features

### 🔍 Comprehensive Validation Checks

1. **File Validation**
   - File size validation (detects corrupted/incomplete files)
   - File completeness (ensures all years are present)
   - File permissions and accessibility
   - Directory structure validation

2. **Dimension Validation**
   - Verifies expected dimensions (time, lat, lon)
   - Validates coordinate ranges (CONUS domain)
   - Checks grid consistency across files
   - Ensures coordinate monotonicity

3. **Data Integrity Validation**
   - Validates all expected indices are present
   - Checks data coverage (NaN fractions)
   - Verifies physically plausible value ranges
   - Detects all-zero arrays (calculation errors)
   - Statistical property validation

4. **CF-Compliance Validation**
   - Checks CF conventions compliance
   - Validates required/recommended attributes
   - Verifies units and metadata
   - Encoding and compression validation

5. **Cross-Year Consistency**
   - Temporal continuity checking
   - Spatial coverage comparison
   - Value distribution analysis
   - Multi-year trend validation
   - Index relationship validation

## Installation

The validation suite is included with xclim-timber. No additional installation required.

```bash
# Ensure dependencies are installed
pip install xarray numpy
```

## Usage

### Command Line Interface

#### Basic Validation

```bash
# Validate temperature pipeline output
python validation/validate_dataset.py outputs/production/temperature/ --pipeline temperature

# Quick validation (sample files only)
python validation/validate_dataset.py outputs/production/precipitation/ --pipeline precipitation --quick

# Generate HTML report
python validation/validate_dataset.py outputs/production/drought/ --pipeline drought --report

# Save results to JSON
python validation/validate_dataset.py outputs/production/agricultural/ --pipeline agricultural --json results.json

# Strict mode (fail on warnings)
python validation/validate_dataset.py outputs/production/humidity/ --pipeline humidity --fail-on-warning
```

#### Validate All Pipelines

```bash
# Validate entire production run
python validation/validate_dataset.py outputs/production/ --pipeline all --report
```

### Shell Script Integration

```bash
# Use the provided shell script
./validation/run_validation.sh -p temperature -d outputs/production/temperature/ -r

# In production scripts
./run_temperature_pipeline.sh
./validation/run_validation.sh -p temperature -d outputs/production/temperature/ -w
```

### Python Integration

```python
from validation.integrate_validation import validate_after_pipeline

# After pipeline completion
success = validate_after_pipeline(
    'temperature',
    Path('outputs/production/temperature/')
)

if not success:
    raise ValueError("Validation failed!")
```

### Batch Validation

```python
from validation.integrate_validation import validate_production_batch

# Validate all pipelines in production
results = validate_production_batch(Path('outputs/production/'))
```

## Pipeline Configurations

The validation suite has pre-configured expectations for each pipeline:

| Pipeline | Expected Indices | Dimensions | Years |
|----------|-----------------|------------|-------|
| Temperature | 35 | time=1, lat=621, lon=1405 | 1981-2024 |
| Precipitation | 13 | time=1, lat=621, lon=1405 | 1981-2024 |
| Drought | 6 | time=1, lat=621, lon=1405 | 1981-2024 |
| Agricultural | 11 | time=1, lat=621, lon=1405 | 1981-2024 |
| Multivariate | 1 | time=1, lat=621, lon=1405 | 1981-2024 |
| Humidity | 14 | time=1, lat=621, lon=1405 | 1981-2024 |
| Human Comfort | 16 | time=1, lat=621, lon=1405 | 1981-2024 |

## Validation Results

### Status Codes

- **PASS** ✅: All checks passed
- **WARNING** ⚠️: Non-critical issues found
- **FAIL** ❌: Critical issues detected
- **ERROR** 🔥: Validation error occurred

### Output Formats

#### Console Output
```
============================================================
VALIDATION SUMMARY
============================================================
Pipeline: temperature
Directory: outputs/production/temperature
Timestamp: 2025-10-13T19:00:00

Overall Status: PASS
Message: ✅ All validation checks passed (5/5 checks)

Checks Summary:
  Total Checks: 5
  Passed: 5
  Failed: 0
  Warnings: 0
  Errors: 0
```

#### JSON Output
```json
{
  "pipeline": "temperature",
  "directory": "outputs/production/temperature",
  "timestamp": "2025-10-13T19:00:00",
  "overall_status": "PASS",
  "summary": {
    "total_checks": 5,
    "passed": 5,
    "failed": 0,
    "warnings": 0
  },
  "validations": {
    "file_validation": {...},
    "dimension_validation": {...},
    "data_integrity": {...},
    "cf_compliance": {...},
    "consistency": {...}
  }
}
```

#### HTML Report

A comprehensive HTML report is generated with:
- Visual status indicators
- Detailed check results
- Error and warning lists
- Interactive sections
- Summary statistics

## Integration with Production

### Automatic Validation After Pipeline

Add to your production scripts:

```bash
#!/bin/bash
# run_temperature_production.sh

# Run pipeline
python temperature_pipeline.py \
    --start-year 1981 \
    --end-year 2024 \
    --output-dir outputs/production/temperature/

# Automatic validation
python validation/validate_dataset.py \
    outputs/production/temperature/ \
    --pipeline temperature \
    --report \
    --fail-on-warning

if [ $? -ne 0 ]; then
    echo "❌ Validation failed - aborting deployment"
    exit 1
fi

echo "✅ Pipeline and validation complete"
```

### GitHub Actions Integration

```yaml
name: Validate Production Data

on:
  workflow_dispatch:
  schedule:
    - cron: '0 0 * * SUN'  # Weekly validation

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run validation suite
        run: |
          python validation/validate_dataset.py \
            outputs/production/ \
            --pipeline all \
            --report \
            --json validation_results.json \
            --fail-on-warning

      - name: Upload reports
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: validation-reports
          path: |
            outputs/production/validation_report_*.html
            validation_results.json
```

## Common Issues and Solutions

### Issue: Missing Years Detected
**Solution**: Check if pipeline completed successfully for all years. Re-run missing years.

### Issue: Excessive NaN Values
**Solution**: Check input data quality and mask application. May indicate spatial coverage issues.

### Issue: Dimension Mismatch
**Solution**: Verify input data resolution and domain. Check regridding operations.

### Issue: All-Zero Arrays
**Solution**: Indicates calculation failure. Check index computation logic.

### Issue: CF-Compliance Warnings
**Solution**: Add missing metadata attributes. See CF conventions documentation.

## Advanced Usage

### Custom Validation Thresholds

```python
from validation.validators import DataValidator

validator = DataValidator()
results = validator.validate_data_coverage(
    nc_file,
    max_nan_fraction=0.3,  # Custom threshold
    warn_nan_fraction=0.2
)
```

### Adding Custom Validators

```python
class CustomValidator:
    def validate_custom_check(self, nc_file):
        # Custom validation logic
        return {
            'status': 'PASS',
            'message': 'Custom check passed'
        }
```

### Parallel Validation

```bash
# Validate multiple pipelines in parallel
parallel -j 4 python validation/validate_dataset.py {} \
    --pipeline {/} --report ::: outputs/production/*/
```

## Performance Considerations

- **Quick mode**: Samples files for faster validation (~5-10 seconds)
- **Full mode**: Validates all files thoroughly (~1-5 minutes per pipeline)
- **Memory usage**: ~500MB for typical validation run
- **Disk I/O**: Optimized to minimize file reads

## Troubleshooting

### Debug Mode

```bash
# Enable verbose logging
python validation/validate_dataset.py \
    outputs/production/temperature/ \
    --pipeline temperature \
    --verbose
```

### Check Individual Validators

```python
from validation.validators import FileValidator

validator = FileValidator()
results = validator.validate_file_sizes(
    Path('outputs/production/temperature/')
)
print(results)
```

## Contributing

To add new validation checks:

1. Add validator class to `validators/` directory
2. Implement validation methods returning standardized results
3. Integrate into main validation script
4. Add tests and documentation

## Success Metrics

The validation suite successfully:
- ✅ Detects corrupted files (like 2003/2005 issue)
- ✅ Validates dimensional consistency
- ✅ Ensures data completeness
- ✅ Checks physical plausibility
- ✅ Verifies CF-compliance
- ✅ Integrates with production workflows
- ✅ Generates actionable reports
- ✅ Provides programmatic interfaces

## License

Part of xclim-timber project. See main LICENSE file.

## Support

For issues or questions:
- Check this documentation
- Review validation reports
- Examine JSON output for details
- Enable verbose mode for debugging
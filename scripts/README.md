# Production Orchestration Scripts

This directory contains the master orchestration system for xclim-timber production processing.

## Overview

The production orchestration system provides a unified, robust way to process all climate indices pipelines with advanced features like resume capability, validation, error handling, and progress tracking.

## Key Features

- **Unified Interface**: Single script for all 7 pipelines
- **Resume Capability**: Skip already-processed years
- **Validation**: Automatic output file validation
- **Error Handling**: Continue processing even if individual years fail
- **Progress Tracking**: Real-time progress bars and summaries
- **Logging**: Comprehensive timestamped logs
- **Dry Run Mode**: Preview what will be processed without running

## Quick Start

### Process All Missing Years (Resume Mode)

```bash
# Process only missing years for a specific pipeline
./scripts/run_production.sh --pipeline precipitation --resume

# Process all pipelines with resume
./scripts/run_production.sh --pipeline all --resume
```

### Process Specific Year Range

```bash
# Process years 2020-2024 for temperature
./scripts/run_production.sh --pipeline temperature --start-year 2020 --end-year 2024

# Process with validation
./scripts/run_production.sh --pipeline drought --start-year 2020 --end-year 2024 --validate
```

### Dry Run (See What Would Be Processed)

```bash
# Check what years are missing without processing
./scripts/run_production.sh --pipeline precipitation --resume --dry-run
```

## Main Script: run_production.sh

### Usage

```bash
./scripts/run_production.sh [OPTIONS]
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `-p, --pipeline PIPELINE` | Pipeline to run (temperature, precipitation, drought, agricultural, humidity, human_comfort, multivariate, all) | all |
| `-s, --start-year YEAR` | Start year | 1981 |
| `-e, --end-year YEAR` | End year | 2024 |
| `-r, --resume` | Skip already-processed years | false |
| `-v, --validate` | Validate output files | false |
| `-f, --fail-fast` | Stop on first error | false |
| `--dry-run` | Show what would be processed | false |
| `--n-tiles N` | Number of spatial tiles (2, 4, 8) | 4 |
| `--chunk-years N` | Years per chunk | 1 |
| `-h, --help` | Show help message | - |

### Examples

#### 1. Complete Full Production Dataset

```bash
# Process all pipelines for all years (1981-2024)
# Skip any years that already exist
./scripts/run_production.sh --pipeline all --resume --validate
```

**Expected Output:**
- Processes 7 pipelines sequentially
- Skips completed years
- Validates all output files
- ~6-10 hours for complete dataset (depending on missing years)

#### 2. Process Single Pipeline

```bash
# Temperature indices only
./scripts/run_production.sh --pipeline temperature --resume

# Precipitation with validation
./scripts/run_production.sh --pipeline precipitation --resume --validate
```

#### 3. Reprocess Specific Years

```bash
# Reprocess 2020-2024 without resume (overwrites existing files)
./scripts/run_production.sh --pipeline temperature --start-year 2020 --end-year 2024
```

#### 4. Check Status Before Processing

```bash
# See what years are missing for each pipeline
./scripts/run_production.sh --pipeline all --resume --dry-run
```

#### 5. Fail-Fast Mode (Stop on First Error)

```bash
# Stop immediately if any year fails
./scripts/run_production.sh --pipeline drought --resume --fail-fast
```

## Helper Utilities

### logging_utils.sh

Provides consistent logging functions with colored output and timestamps.

**Functions:**
- `init_logging LOG_FILE` - Initialize logging to file
- `log_info MESSAGE` - Log informational message
- `log_success MESSAGE` - Log success with ✓
- `log_warn MESSAGE` - Log warning
- `log_error MESSAGE` - Log error
- `log_section TITLE` - Log section header
- `progress_bar CURRENT TOTAL DESCRIPTION` - Show progress bar

### pipeline_utils.sh

Provides pipeline management and validation functions.

**Functions:**
- `validate_pipeline PIPELINE` - Check if pipeline exists
- `output_exists PIPELINE YEAR` - Check if output file exists
- `get_missing_years PIPELINE START END` - Get list of missing years
- `count_completed_years PIPELINE START END` - Count completed files
- `run_pipeline_year PIPELINE YEAR [ARGS]` - Run pipeline for one year
- `validate_output PIPELINE YEAR` - Validate output file
- `pipeline_summary PIPELINE START END` - Show pipeline statistics

## Output Structure

All pipelines write to `outputs/production/`:

```
outputs/production/
├── temperature/
│   ├── temperature_indices_1981_1981.nc
│   ├── temperature_indices_1982_1982.nc
│   └── ...
├── precipitation/
│   ├── precipitation_indices_1981_1981.nc
│   └── ...
├── drought/
├── agricultural/
├── humidity/
├── human_comfort/
└── multivariate/
```

## Logs

Logs are stored in `logs/`:

```
logs/
├── production_20251014_080000.log  # Main orchestration log
├── temperature_2023.log             # Per-year pipeline logs
├── precipitation_2023.log
└── ...
```

**Log Format:**
```
[2025-10-14 08:00:00] INFO: Pipeline: temperature
[2025-10-14 08:00:05] INFO: Running temperature pipeline for year 2023
[2025-10-14 08:01:23] ✓ Output validation passed for 2023
```

## Validation

When `--validate` is enabled, each output file is checked for:

1. **File Existence**: Output file was created
2. **File Size**: File is > 1 MB (reasonable data)
3. **NetCDF Validity**: File is readable by ncdump
4. **Index Count**: Correct number of indices present

**Expected Indices:**
- Temperature: 35 indices
- Precipitation: 13 indices
- Drought: 12 indices
- Agricultural: 5 indices
- Humidity: 8 indices
- Human Comfort: 3 indices
- Multivariate: 4 indices

## Error Handling

### Default Behavior (Continue on Error)

By default, if a year fails, the script:
1. Logs the error
2. Continues processing remaining years
3. Reports all failures at the end

### Fail-Fast Mode

With `--fail-fast`, the script:
1. Stops immediately on first error
2. Exits with error code 1
3. Useful for debugging

## Resume Mode Details

Resume mode (`--resume`) is the **recommended** way to run production processing.

**How it works:**
1. Checks `outputs/production/{pipeline}/` for existing files
2. Identifies missing years
3. Processes only missing years

**Benefits:**
- Safe to re-run after interruption
- No wasted computation
- Automatic progress tracking

**Example:**
```bash
# First run: Processes years 1981-2024
./scripts/run_production.sh --pipeline temperature --resume

# Gets interrupted after processing 1981-2000

# Second run: Automatically resumes from 2001
./scripts/run_production.sh --pipeline temperature --resume
# Output: "Resume mode: 20/44 years complete, 24 remaining"
```

## Performance Tips

### Memory Optimization

Use fewer spatial tiles if memory is limited:
```bash
# Use 2 tiles (saves more memory, slightly slower)
./scripts/run_production.sh --pipeline temperature --resume --n-tiles 2
```

### Parallel Processing (Experimental)

Process multiple pipelines simultaneously (requires sufficient RAM):
```bash
# Process temperature and precipitation in parallel
./scripts/run_production.sh --parallel --pipeline temperature,precipitation
```

**Warning:** This is experimental and may cause memory issues on systems with <64GB RAM.

## Troubleshooting

### "Pipeline script not found"

**Cause:** Running from wrong directory

**Solution:** Always run from project root:
```bash
cd /path/to/xclim-timber
./scripts/run_production.sh --pipeline temperature
```

### "Validation failed: Expected X indices, found Y"

**Cause:** Pipeline crashed or produced incomplete output

**Solution:**
1. Check year-specific log: `logs/{pipeline}_{year}.log`
2. Re-run without resume to force regeneration:
   ```bash
   ./scripts/run_production.sh --pipeline temperature --start-year 2023 --end-year 2023
   ```

### "Output file suspiciously small"

**Cause:** Pipeline produced invalid output

**Solution:**
1. Check for errors in pipeline log
2. May indicate data issues (e.g., 2003, 2005 data corruption)
3. Re-run with verbose logging

### Out of Memory

**Cause:** Too many spatial tiles or insufficient RAM

**Solution:**
```bash
# Reduce spatial tiles
./scripts/run_production.sh --pipeline temperature --resume --n-tiles 2

# Or process in smaller batches
./scripts/run_production.sh --pipeline temperature --start-year 2020 --end-year 2024
```

## Migration from Legacy Scripts

Old scripts are deprecated but preserved for reference:

| Old Script | New Command |
|-----------|-------------|
| `run_early_temp_years.sh` | `./scripts/run_production.sh --pipeline temperature --start-year 1981 --end-year 1999` |
| `run_remaining_temp_years.sh` | `./scripts/run_production.sh --pipeline temperature --start-year 2000 --end-year 2024` |
| `run_precipitation_production.sh` | `./scripts/run_production.sh --pipeline precipitation --resume` |

**Benefits of new system:**
- Resume capability (no wasted reprocessing)
- Validation built-in
- Better error handling
- Consistent interface across all pipelines

## Complete Production Workflow

### Initial Setup (One-Time)

```bash
# 1. Calculate baseline percentiles (if not already done)
python calculate_baseline_percentiles.py

# 2. Verify baseline file exists
ls -lh data/baselines/baseline_percentiles_1981_2000.nc
```

### Process All Pipelines

```bash
# Process complete dataset (1981-2024) for all pipelines
./scripts/run_production.sh --pipeline all --resume --validate
```

**This will:**
1. Process temperature (35 indices) - ~2-3 hours for 44 years
2. Process precipitation (13 indices) - ~1-2 hours for 44 years
3. Process drought (12 indices) - ~3-4 hours for 44 years
4. Process agricultural (5 indices) - ~15 minutes for 44 years
5. Process humidity (8 indices) - ~15 minutes for 44 years
6. Process human comfort (3 indices) - ~10 minutes for 44 years
7. Process multivariate (4 indices) - ~2-3 hours for 44 years

**Total time:** ~6-10 hours for complete dataset

### Monitor Progress

Open a second terminal and watch logs:
```bash
# Monitor main log
tail -f logs/production_*.log

# Or check specific pipeline
tail -f logs/temperature_*.log
```

### Verify Completion

```bash
# Check completion status for all pipelines
./scripts/run_production.sh --pipeline all --resume --dry-run
```

## Support

For issues or questions:
- Check logs in `logs/` directory
- Review [PRODUCTION_GUIDE.md](../PRODUCTION_GUIDE.md)
- Open issue on GitHub with relevant log excerpts

## Related Documentation

- [PRODUCTION_GUIDE.md](../PRODUCTION_GUIDE.md) - Overall production processing guide
- [DATA_DICTIONARY.md](../docs/DATA_DICTIONARY.md) - Climate indices reference
- [ARCHITECTURAL_REVIEW.md](../docs/ARCHITECTURAL_REVIEW.md) - System architecture
- Issue #69: Production orchestration implementation

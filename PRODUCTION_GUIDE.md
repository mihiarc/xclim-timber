# Production Pipeline Orchestration Guide

This guide covers running the full production dataset generation for all 80 climate indices over the complete PRISM time series (1981-2024).

## Overview

The xclim-timber project includes orchestration scripts to automate the generation of comprehensive climate indices across the full 44-year PRISM record. These scripts manage memory constraints, handle pipeline failures gracefully, and provide comprehensive logging.

## Architecture

### Pipeline Structure

**7 Independent Pipelines:**
1. **Temperature** (35 indices) - Temperature extremes, degree days, frost metrics, spell frequency
2. **Precipitation** (13 indices) - Precipitation totals, extremes, consecutive events, ETCCDI standards
3. **Humidity** (8 indices) - Dewpoint, relative humidity, vapor pressure deficit
4. **Human Comfort** (3 indices) - Heat index, humidex for public health applications
5. **Multivariate** (4 indices) - Compound climate extremes (hot/cold × dry/wet)
6. **Agricultural** (5 indices) - Growing season length, PET, corn heat units, thawing degree days
7. **Drought** (12 indices) - SPI (5 time windows), dry spell analysis, precipitation intensity

**Total: 80 climate indices**

### Memory Optimization

The pipelines use different chunking strategies based on memory requirements:

- **1-year chunks**: Temperature, Precipitation, Multivariate, Drought
  - These pipelines compare against baseline percentiles (1981-2000)
  - Require loading both current data and baseline thresholds
  - Generate 44 output files per pipeline (one per year)

- **4-year chunks**: Humidity, Human Comfort, Agricultural
  - Simple aggregations without baseline comparisons
  - More efficient processing
  - Generate 11 output files per pipeline

### Orchestration Scripts

#### 1. `run_all_pipelines.py` - Full Production Run

**Purpose**: Run all 7 pipelines sequentially from scratch

**Usage:**
```bash
python run_all_pipelines.py \
    --start-year 1981 \
    --end-year 2024 \
    --output-dir outputs/production
```

**Features:**
- Runs all 7 pipelines sequentially
- 2-hour timeout per pipeline (configurable)
- Comprehensive logging to `logs/full_production_run_*.log`
- JSON results output with timing and file counts
- Automatic error handling and reporting

**Best for:** Initial full dataset generation

#### 2. `run_failed_pipelines.py` - Retry Failed Pipelines

**Purpose**: Re-run only failed pipelines with optimized settings, keeping successful results

**Usage:**
```bash
python run_failed_pipelines.py \
    --start-year 1981 \
    --end-year 2024 \
    --output-dir outputs/production
```

**Features:**
- Only processes failed pipelines (saves time)
- Extended timeouts (3-4 hours per pipeline)
- Preserves successful results from previous runs
- 1-year chunk processing for maximum memory efficiency
- Detailed progress tracking

**Best for:** Recovering from OOM errors or timeouts

#### 3. `optimize_all_pipelines.py` - Batch Memory Optimization

**Purpose**: Apply memory optimizations across all pipeline files

**Usage:**
```bash
python optimize_all_pipelines.py
```

**Optimizations Applied:**
- Reduces temporal chunks (12 → 4 → 1 years)
- Reduces spatial chunks (smaller lat/lon divisions)
- Switches Dask scheduler (distributed → threaded)
- Updates default parameters across all pipelines

**Best for:** Applying consistent memory optimizations before production runs

## Production Workflow

### Initial Run

```bash
# 1. Ensure baseline percentiles are calculated (done once)
python calculate_baseline_percentiles.py

# 2. Run full production dataset generation
python run_all_pipelines.py --start-year 1981 --end-year 2024 --output-dir outputs/production

# Monitor logs in real-time
tail -f logs/full_production_run*.log
```

### Handling Failures

If some pipelines fail due to memory constraints:

```bash
# Re-run only failed pipelines with 1-year chunks
python run_failed_pipelines.py --start-year 1981 --end-year 2024 --output-dir outputs/production
```

The script automatically detects which pipelines failed and re-processes them while preserving successful results.

## Output Structure

```
outputs/production/
├── temperature/
│   ├── temperature_indices_1981_1981.nc
│   ├── temperature_indices_1982_1982.nc
│   └── ... (44 files, ~20 MB each, 35 indices)
├── precipitation/
│   ├── precipitation_indices_1981_1981.nc
│   └── ... (44 files, 13 indices)
├── humidity/
│   ├── humidity_indices_1981_1984.nc
│   └── ... (11 files, ~30 MB each, 8 indices)
├── human_comfort/
│   ├── human_comfort_indices_1981_1984.nc
│   └── ... (11 files, ~14 MB each, 3 indices)
├── multivariate/
│   ├── multivariate_indices_1981_1981.nc
│   └── ... (44 files, 4 indices)
├── agricultural/
│   ├── agricultural_indices_1981_1984.nc
│   └── ... (11 files, ~28 MB each, 5 indices)
└── drought/
    ├── drought_indices_1981_1981.nc
    └── ... (44 files, 12 indices)
```

**Total Expected Output:**
- ~220-250 NetCDF files
- ~8-10 GB total size
- All 80 climate indices
- 1981-2024 coverage (44 years)

## Performance Expectations

### Processing Times (per pipeline)

- **Temperature** (35 indices, 1-year chunks): ~2-4 minutes/year → 2-3 hours total
- **Precipitation** (13 indices, 1-year chunks): ~1-2 minutes/year → 1-2 hours total
- **Humidity** (8 indices, 4-year chunks): ~10-15 minutes total
- **Human Comfort** (3 indices, 4-year chunks): ~5-10 minutes total
- **Multivariate** (4 indices, 1-year chunks): ~2-3 minutes/year → 2-3 hours total
- **Agricultural** (5 indices, 4-year chunks): ~10-15 minutes total
- **Drought** (12 indices, 1-year chunks): ~3-5 minutes/year → 3-4 hours total

**Total Estimated Time:** 6-10 hours for complete dataset generation

### Memory Requirements

- **System RAM**: 32-64 GB recommended
- **Peak Usage**: ~18-22 GB per pipeline
- **Storage**: 10-15 GB free space for outputs

### Monitoring Progress

```bash
# Check number of output files generated
find outputs/production -name "*.nc" | wc -l

# Check total size
du -sh outputs/production/*

# Monitor current pipeline
tail -f logs/failed_pipelines_run.log

# Check memory usage
htop  # or: free -h
```

## Troubleshooting

### Out of Memory Errors

**Symptoms:** Pipeline killed with return code -9

**Solutions:**
1. Use `run_failed_pipelines.py` to retry with 1-year chunks
2. Close other memory-intensive applications
3. Consider reducing spatial chunks further in pipeline files
4. Process pipelines individually with longer breaks between runs

### Timeout Errors

**Symptoms:** Pipeline timeout message after 2-4 hours

**Solutions:**
1. Edit timeout values in orchestration script
2. Check system load (other processes competing for CPU)
3. For Drought pipeline: SPI calculations are intensive, 4-hour timeout is normal

### Baseline Percentiles Missing

**Symptoms:** "Baseline percentiles not found" error

**Solution:**
```bash
# Generate baseline percentiles first (one-time operation)
python calculate_baseline_percentiles.py
```

This creates:
- `data/baselines/temperature_percentiles_1981_2000.nc`
- `data/baselines/precipitation_percentiles_1981_2000.nc`

## Maintenance

### Log Management

Logs are automatically organized:
- Active logs: `logs/*.log`
- Archived logs: `logs/archive/*.log`

Clean up old logs periodically:
```bash
# Archive logs older than 30 days
find logs/ -name "*.log" -mtime +30 -exec mv {} logs/archive/ \;
```

### Test Outputs

Test directories (phase*_test) are excluded from version control via `.gitignore`. Remove manually if needed:
```bash
rm -rf outputs/phase*_test
```

### Python Cache

Remove compiled Python files:
```bash
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
```

## Data Products

### NetCDF Metadata

All output files include CF-compliant metadata:
- **Title**: Climate Indices from PRISM Data
- **Source**: PRISM 4km daily data (1981-2024)
- **Variables**: Each climate index as separate variable
- **Dimensions**: lat (621), lon (1405), time (variable by chunk)
- **Compression**: zlib level 4
- **Processing**: Software version, calculation date, xclim version

### Index Availability

All 80 indices are scientifically validated following:
- **ETCCDI/WMO Standards**: Temperature and precipitation extremes
- **USDA Standards**: Agricultural indices (CHU, GSL)
- **Research Standards**: Drought indices (SPI), human comfort metrics

## Advanced Usage

### Custom Year Range

Process a subset of years:
```bash
python run_all_pipelines.py --start-year 2000 --end-year 2020 --output-dir outputs/subset
```

### Individual Pipeline Execution

Run a single pipeline directly:
```bash
python temperature_pipeline.py --start-year 1981 --end-year 2024 --output-dir outputs/temp_only --verbose
```

### Memory Profiling

Monitor memory usage during execution:
```bash
# Terminal 1: Run pipeline
python run_failed_pipelines.py

# Terminal 2: Monitor memory
watch -n 5 'ps aux | grep python | grep pipeline'
```

## Support

For issues or questions:
1. Check logs in `logs/` directory
2. Review pipeline-specific documentation in source files
3. Consult `docs/ACHIEVABLE_INDICES_ROADMAP.md` for index details
4. Check `README.md` for general project information

## Version History

- **2025-10-11**: Added production orchestration scripts with memory optimizations
- **2025-10-10**: Implemented 1-year chunking for memory-intensive pipelines
- **2025-10-09**: Completed Phase 10 (80/80 indices achieved)

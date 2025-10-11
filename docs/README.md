# xclim-timber Documentation

**Climate Indices Processing Pipeline for PRISM Data**

Welcome to the xclim-timber documentation. This guide will help you navigate the comprehensive documentation for this climate indices calculation system.

## 🎯 Quick Start

**New to xclim-timber?** Start here:
1. Read [../README.md](../README.md) for project overview
2. Check [ACHIEVABLE_INDICES_ROADMAP.md](ACHIEVABLE_INDICES_ROADMAP.md) for index catalog (80 indices)
3. Review [PRODUCTION_GUIDE.md](../PRODUCTION_GUIDE.md) for running production workflows

## 📚 Current Documentation (Updated 2025-10-11)

### Core Documentation

| Document | Purpose | Status | Last Updated |
|----------|---------|--------|--------------|
| [ACHIEVABLE_INDICES_ROADMAP.md](ACHIEVABLE_INDICES_ROADMAP.md) | **Primary Index Reference** - Complete catalog of all 80 implemented indices | ✅ Current | 2025-10-10 |
| [BASELINE_DOCUMENTATION.md](BASELINE_DOCUMENTATION.md) | Baseline percentile calculation methodology (1981-2000) | ✅ Current | 2025-10-09 |
| [CLAUDE.md](CLAUDE.md) | Comprehensive system architecture and project overview | ⚠️ Updating | 2025-10-09 |
| [../PRODUCTION_GUIDE.md](../PRODUCTION_GUIDE.md) | Production orchestration and memory optimization guide | ✅ Current | 2025-10-11 |

### Specialized Guides

| Document | Purpose | Status |
|----------|---------|--------|
| [POINT_EXTRACTION.md](POINT_EXTRACTION.md) | Extract indices at specific geographic locations | ⚠️ Basic |
| [DATA_DICTIONARY.md](DATA_DICTIONARY.md) | Variable definitions and metadata reference | ⚠️ Partial |

### Historical Documentation (Archive)

| Document | Purpose | Notes |
|----------|---------|-------|
| [archive/](archive/) | Historical planning documents and implementation notes | See archive/README.md |

## 🔍 Documentation by Task

### I want to understand the project
→ Read [CLAUDE.md](CLAUDE.md) - Comprehensive overview
→ See [ACHIEVABLE_INDICES_ROADMAP.md](ACHIEVABLE_INDICES_ROADMAP.md) - What indices are calculated

### I want to run a production dataset
→ Follow [../PRODUCTION_GUIDE.md](../PRODUCTION_GUIDE.md) - Complete workflow
→ Check [BASELINE_DOCUMENTATION.md](BASELINE_DOCUMENTATION.md) - One-time baseline setup

### I want to understand baseline percentiles
→ Read [BASELINE_DOCUMENTATION.md](BASELINE_DOCUMENTATION.md) - Percentile methodology
→ Check [ACHIEVABLE_INDICES_ROADMAP.md](ACHIEVABLE_INDICES_ROADMAP.md) - Which indices use baselines

### I want to extract data at specific locations
→ Follow [POINT_EXTRACTION.md](POINT_EXTRACTION.md) - Point extraction guide

### I want to know what indices are available
→ See [ACHIEVABLE_INDICES_ROADMAP.md](ACHIEVABLE_INDICES_ROADMAP.md) - Complete catalog
→ Reference [DATA_DICTIONARY.md](DATA_DICTIONARY.md) - Detailed definitions

### I encountered an error
→ Check [../PRODUCTION_GUIDE.md](../PRODUCTION_GUIDE.md#troubleshooting) - Common issues
→ Review logs in `logs/` directory
→ See memory optimization notes in pipeline files

## 📊 Project Status (2025-10-11)

**Indices Implemented:** 80/80 (100% of achievable goal ✅)

**By Category:**
- Temperature: 35 indices ✅
- Precipitation: 13 indices ✅
- Humidity: 8 indices ✅
- Human Comfort: 3 indices ✅
- Multivariate: 4 indices ✅
- Agricultural: 5 indices ✅
- Drought: 12 indices ✅

**Data Coverage:** 1981-2024 (44 years, PRISM CONUS)

**Production Status:** ✅ Full production orchestration implemented with memory optimizations

## 🎓 Understanding Index Categories

### Temperature Indices (35)
- **Basic stats**: Annual mean, max, min, temperature range
- **Threshold counts**: Frost days, ice days, summer days, hot days, tropical nights
- **Frost season**: Growing season timing, frost-free period
- **Degree days**: Growing, heating, cooling, freezing, thawing
- **Extremes**: Percentile-based warm/cool days and nights (tx90p, tn90p, tx10p, tn10p)
- **Spell analysis**: Warm/cold spell duration, heat waves, cold spells
- **Variability**: Temperature seasonality, daily range variability

### Precipitation Indices (13)
- **Totals**: Annual precipitation, maximum 1-day and 5-day amounts
- **Intensity**: Simple daily intensity index
- **Consecutive events**: Maximum dry/wet spell lengths
- **Extremes**: Heavy precipitation days (≥10mm, ≥20mm), very wet days (95th/99th percentile)
- **Frequency**: Dry days, wet days, wet day proportion

### Humidity Indices (8)
- **Dewpoint**: Mean, min, max dewpoint temperature, humid days
- **VPD**: Vapor pressure deficit statistics, extreme/low VPD days

### Human Comfort Indices (3)
- **Heat stress**: Heat index, humidex
- **Derived**: Relative humidity

### Multivariate Indices (4)
- **Compound extremes**: Cold+dry, cold+wet, warm+dry, warm+wet days

### Agricultural Indices (5)
- **Growing season**: Length, growing season precipitation
- **Water balance**: Potential evapotranspiration (Baier-Robertson)
- **Crop-specific**: Corn heat units
- **Thaw metrics**: Thawing degree days

### Drought Indices (12)
- **SPI**: 1, 3, 6, 12, 24-month Standardized Precipitation Index
- **Dry spell analysis**: Max consecutive dry days, frequency, total length, dry day counts
- **Intensity**: Simple daily intensity, max 7-day precipitation, heavy precipitation fraction

## 🔧 Data Requirements

**Available with PRISM Data:**
- ✅ Temperature: tmax, tmin, tmean (°C)
- ✅ Precipitation: ppt (mm/day)
- ✅ Humidity: tdmean (dewpoint, °C), vpdmax, vpdmin (hPa)

**Not Available (Cannot Implement):**
- ❌ Wind: Surface wind speed (needed for ~15 fire weather indices)
- ❌ Snow: Snow depth/snowfall (needed for ~12 snow indices)
- ❌ Solar: Solar radiation (needed for ~5 radiation indices)

## 📖 Key Concepts

### Baseline Percentiles (1981-2000)
Many indices use **day-of-year percentile thresholds** calculated from a 20-year baseline period (1981-2000) following WMO standards. This enables detection of climate change signals by comparing current conditions to historical normals.

**See:** [BASELINE_DOCUMENTATION.md](BASELINE_DOCUMENTATION.md)

### Memory-Optimized Processing
The production pipeline processes data in **temporal chunks** (1-4 years) to prevent out-of-memory errors on systems with limited RAM. Different pipelines use different strategies based on computational requirements.

**See:** [../PRODUCTION_GUIDE.md](../PRODUCTION_GUIDE.md)

### CF-Compliant Metadata
All outputs follow **Climate and Forecast (CF) conventions** with comprehensive metadata, ensuring interoperability with climate research tools and standards compliance.

### ETCCDI/WMO Standards
Temperature and precipitation extreme indices follow **Expert Team on Climate Change Detection and Indices (ETCCDI)** and **World Meteorological Organization (WMO)** standards for scientific validity and international comparability.

## 🚀 Quick Reference Commands

```bash
# Calculate baseline percentiles (one-time, ~20-30 minutes)
python calculate_baseline_percentiles.py

# Run individual pipeline
python temperature_pipeline.py --start-year 1981 --end-year 2024 --verbose

# Run all pipelines (production dataset)
python run_all_pipelines.py --start-year 1981 --end-year 2024 --output-dir outputs/production

# Re-run only failed pipelines with memory optimization
python run_failed_pipelines.py --start-year 1981 --end-year 2024 --output-dir outputs/production

# Extract indices at specific locations
python extract_points.py temperature_indices_2023_2023.nc parcels.csv output.csv
```

## 🐛 Troubleshooting

**Out of Memory (OOM) Errors:**
- Use `run_failed_pipelines.py` to retry with 1-year chunks
- Close other memory-intensive applications
- See [PRODUCTION_GUIDE.md - Troubleshooting](../PRODUCTION_GUIDE.md#troubleshooting)

**Baseline Percentiles Missing:**
- Run `python calculate_baseline_percentiles.py` once
- Check that `data/baselines/baseline_percentiles_1981_2000.nc` exists
- See [BASELINE_DOCUMENTATION.md](BASELINE_DOCUMENTATION.md)

**Pipeline Timeout:**
- Increase timeout values in orchestration scripts
- Check system load (other processes competing for CPU)
- Drought pipeline: 4-hour timeout is normal for SPI calculations

## 📞 Getting Help

1. **Check documentation**: Start with this README and follow links to specific guides
2. **Review logs**: Check `logs/` directory for detailed error messages
3. **Examine outputs**: Use `ncdump -h` to inspect NetCDF metadata
4. **Validation**: Compare results against known climate patterns

## 🔄 Documentation Updates

This documentation is actively maintained. Last major update: **2025-10-11** (production orchestration and 80/80 index completion).

**See [archive/](archive/)** for historical planning documents and implementation notes from Phases 1-10.

---

**Navigation:**
- [Back to Project Root](../)
- [View Source Code](../src/)
- [Production Guide](../PRODUCTION_GUIDE.md)
- [Index Roadmap](ACHIEVABLE_INDICES_ROADMAP.md)

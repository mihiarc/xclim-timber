#!/usr/bin/env python
"""
Quality Assurance/Quality Control for Climate Indices
Performs comprehensive validation of calculated climate indices.
"""

import sys
from pathlib import Path
import numpy as np
import xarray as xr
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def check_data_completeness(ds):
    """Check for missing values and data coverage."""
    print("\n" + "="*80)
    print("1. DATA COMPLETENESS CHECK")
    print("="*80)

    total_points = ds.dims['time'] * ds.dims['lat'] * ds.dims['lon']
    print(f"\nDataset dimensions:")
    print(f"  Time steps: {ds.dims['time']}")
    print(f"  Spatial points: {ds.dims['lat']} × {ds.dims['lon']} = {ds.dims['lat'] * ds.dims['lon']:,}")
    print(f"  Total data points: {total_points:,}")

    print("\nMissing data analysis:")
    print("-" * 50)

    issues = []
    for var in sorted(ds.data_vars):
        data = ds[var].values
        nan_count = np.isnan(data).sum()
        nan_percent = 100 * nan_count / data.size

        # Check different types of missing data
        inf_count = np.isinf(data[~np.isnan(data)]).sum() if nan_count < data.size else 0

        status = "✓" if nan_percent < 5 else "⚠" if nan_percent < 20 else "✗"

        print(f"{status} {var:35s}: {nan_percent:5.1f}% missing")

        if inf_count > 0:
            print(f"    WARNING: {inf_count} infinite values detected!")
            issues.append(f"{var}: {inf_count} infinite values")

        if nan_percent > 50:
            issues.append(f"{var}: {nan_percent:.1f}% missing data")

    return issues

def validate_value_ranges(ds):
    """Validate that indices are within physically plausible ranges."""
    print("\n" + "="*80)
    print("2. VALUE RANGE VALIDATION")
    print("="*80)

    # Define expected ranges for each index
    expected_ranges = {
        'tg_mean': (-60, 50, 'degC'),  # Mean temperature
        'tx_max': (-50, 60, 'degC'),    # Max temperature
        'tn_min': (-70, 40, 'degC'),    # Min temperature
        'frost_days': (0, 365, 'days'),
        'ice_days': (0, 365, 'days'),
        'tropical_nights': (0, 365, 'days'),
        'summer_days': (0, 365, 'days'),
        'consecutive_frost_days': (0, 365, 'days'),
        'growing_degree_days': (0, 10000, 'degree-days'),
        'heating_degree_days': (0, 10000, 'degree-days'),
        'cooling_degree_days': (0, 5000, 'degree-days'),
        'daily_temperature_range': (0, 50, 'degC'),
        'daily_temperature_range_variability': (0, 20, 'degC'),
        'gsl': (0, 365, 'days'),  # Growing season length
    }

    print(f"\n{'Index':<35} {'Min':>10} {'Max':>10} {'Mean':>10} {'Status':<10}")
    print("-" * 80)

    issues = []
    for var in sorted(ds.data_vars):
        data = ds[var].values[~np.isnan(ds[var].values)]

        if len(data) == 0:
            print(f"{var:<35} {'No data':<10}")
            continue

        vmin, vmax = float(data.min()), float(data.max())
        vmean = float(data.mean())

        # Check against expected ranges
        if var in expected_ranges:
            exp_min, exp_max, unit = expected_ranges[var]

            if vmin < exp_min or vmax > exp_max:
                status = "✗ FAIL"
                issues.append(f"{var}: values outside expected range [{exp_min}, {exp_max}]")
            elif vmin < exp_min * 1.1 or vmax > exp_max * 0.9:
                status = "⚠ CHECK"
            else:
                status = "✓ OK"
        else:
            status = "? Unknown"

        print(f"{var:<35} {vmin:>10.1f} {vmax:>10.1f} {vmean:>10.1f} {status:<10}")

    return issues

def check_temporal_consistency(ds):
    """Check for temporal consistency and anomalies."""
    print("\n" + "="*80)
    print("3. TEMPORAL CONSISTENCY CHECK")
    print("="*80)

    issues = []

    print("\nYear-to-year variability analysis:")
    print("-" * 50)

    for var in sorted(ds.data_vars):
        # Calculate spatial mean for each time step
        spatial_mean = ds[var].mean(dim=['lat', 'lon'], skipna=True)

        if len(spatial_mean) < 2:
            continue

        # Check for sudden jumps
        diffs = np.diff(spatial_mean.values)
        diffs_clean = diffs[~np.isnan(diffs)]

        if len(diffs_clean) > 0:
            max_jump = np.abs(diffs_clean).max()
            std_jump = np.std(diffs_clean)

            # Flag if maximum jump is > 5 standard deviations
            if max_jump > 5 * std_jump and std_jump > 0:
                status = "⚠ Large jump"
                jump_year = np.where(np.abs(diffs) == max_jump)[0][0] + 1
                jump_year_actual = ds.time.values[jump_year]
                issues.append(f"{var}: Large jump at {jump_year_actual}")
            else:
                status = "✓ Stable"

            print(f"{var:<35} Max change: {max_jump:>8.1f}  Std: {std_jump:>8.2f}  {status}")

    return issues

def check_spatial_patterns(ds):
    """Check for spatial anomalies and patterns."""
    print("\n" + "="*80)
    print("4. SPATIAL PATTERN CHECK")
    print("="*80)

    issues = []

    print("\nSpatial coherence analysis (2024 or latest year):")
    print("-" * 50)

    # Use the last time step for spatial analysis
    last_time = ds.time[-1]

    for var in sorted(ds.data_vars):
        data_2d = ds[var].sel(time=last_time).values

        # Skip if too many missing values
        if np.isnan(data_2d).sum() > 0.5 * data_2d.size:
            print(f"{var:<35} Too many missing values for spatial analysis")
            continue

        # Check for spatial outliers (isolated extreme values)
        data_clean = data_2d[~np.isnan(data_2d)]
        if len(data_clean) > 0:
            q25, q75 = np.percentile(data_clean, [25, 75])
            iqr = q75 - q25

            # Count extreme outliers (beyond 3*IQR)
            outliers = np.sum((data_clean < q25 - 3*iqr) | (data_clean > q75 + 3*iqr))
            outlier_pct = 100 * outliers / len(data_clean)

            if outlier_pct > 5:
                status = "⚠ Many outliers"
                issues.append(f"{var}: {outlier_pct:.1f}% spatial outliers")
            elif outlier_pct > 1:
                status = "⚠ Some outliers"
            else:
                status = "✓ Normal"

            # Calculate spatial autocorrelation (simplified - just check variance)
            spatial_std = np.nanstd(data_clean)
            spatial_mean = np.nanmean(data_clean)
            cv = spatial_std / abs(spatial_mean) if spatial_mean != 0 else 0

            print(f"{var:<35} Outliers: {outlier_pct:>5.1f}%  CV: {cv:>6.2f}  {status}")

    return issues

def check_index_relationships(ds):
    """Check relationships between related indices."""
    print("\n" + "="*80)
    print("5. INTER-INDEX RELATIONSHIP CHECK")
    print("="*80)

    issues = []

    print("\nLogical consistency checks:")
    print("-" * 50)

    # Check 1: tn_min <= tg_mean <= tx_max
    if all(v in ds.data_vars for v in ['tn_min', 'tg_mean', 'tx_max']):
        tn = ds['tn_min'].values
        tg = ds['tg_mean'].values
        tx = ds['tx_max'].values

        # Check where all three have valid values
        valid_mask = ~(np.isnan(tn) | np.isnan(tg) | np.isnan(tx))

        violations1 = np.sum((tn > tg)[valid_mask])
        violations2 = np.sum((tg > tx)[valid_mask])

        if violations1 > 0 or violations2 > 0:
            status = "✗ FAIL"
            issues.append(f"Temperature ordering violated: {violations1 + violations2} points")
        else:
            status = "✓ PASS"

        print(f"Temperature ordering (min≤mean≤max):        {status}")

    # Check 2: frost_days >= ice_days
    if all(v in ds.data_vars for v in ['frost_days', 'ice_days']):
        frost = ds['frost_days'].values
        ice = ds['ice_days'].values

        valid_mask = ~(np.isnan(frost) | np.isnan(ice))
        violations = np.sum((frost < ice)[valid_mask])

        if violations > 0:
            status = "✗ FAIL"
            issues.append(f"Frost/ice days relationship violated: {violations} points")
        else:
            status = "✓ PASS"

        print(f"Frost days ≥ Ice days:                       {status}")

    # Check 3: Growing degree days should be 0 when tg_mean < 10°C
    if all(v in ds.data_vars for v in ['tg_mean', 'growing_degree_days']):
        tg = ds['tg_mean'].values
        gdd = ds['growing_degree_days'].values

        # Check points where mean temp < 5°C but GDD > 100
        cold_mask = (tg < 5) & ~np.isnan(tg) & ~np.isnan(gdd)
        suspicious = np.sum((gdd > 100)[cold_mask])

        if suspicious > 0:
            status = "⚠ CHECK"
            issues.append(f"GDD suspicious in cold regions: {suspicious} points")
        else:
            status = "✓ PASS"

        print(f"Growing degree days vs temperature:          {status}")

    return issues

def generate_summary_statistics(ds):
    """Generate summary statistics for the dataset."""
    print("\n" + "="*80)
    print("6. SUMMARY STATISTICS")
    print("="*80)

    # Temporal coverage
    time_range = f"{ds.time.values[0]} to {ds.time.values[-1]}"
    print(f"\nTemporal coverage: {time_range}")
    print(f"Number of years: {len(ds.time)}")

    # Spatial coverage
    lat_range = f"{ds.lat.values.min():.2f}°N to {ds.lat.values.max():.2f}°N"
    lon_range = f"{ds.lon.values.min():.2f}°W to {abs(ds.lon.values.max()):.2f}°W"
    print(f"\nSpatial coverage:")
    print(f"  Latitude:  {lat_range}")
    print(f"  Longitude: {lon_range}")
    print(f"  Grid size: {ds.dims['lat']} × {ds.dims['lon']}")

    # Calculate approximate area
    lat_res = abs(ds.lat.values[1] - ds.lat.values[0])
    lon_res = abs(ds.lon.values[1] - ds.lon.values[0])
    print(f"  Resolution: ~{lat_res:.4f}° × {lon_res:.4f}°")

    # File size
    file_path = Path("outputs/comprehensive_2001_2024/combined_indices.nc")
    if file_path.exists():
        size_mb = file_path.stat().st_size / (1024 * 1024)
        print(f"\nFile size: {size_mb:.1f} MB")

def main():
    """Run QA/QC checks on climate indices."""

    print("\n" + "="*80)
    print("CLIMATE INDICES QA/QC REPORT")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load the combined dataset
    file_path = "outputs/comprehensive_2001_2024/combined_indices.nc"

    print(f"\nAnalyzing: {file_path}")

    try:
        ds = xr.open_dataset(file_path)
    except Exception as e:
        print(f"\n✗ ERROR: Could not load file: {e}")
        return 1

    all_issues = []

    # Run all checks
    issues1 = check_data_completeness(ds)
    all_issues.extend(issues1)

    issues2 = validate_value_ranges(ds)
    all_issues.extend(issues2)

    issues3 = check_temporal_consistency(ds)
    all_issues.extend(issues3)

    issues4 = check_spatial_patterns(ds)
    all_issues.extend(issues4)

    issues5 = check_index_relationships(ds)
    all_issues.extend(issues5)

    generate_summary_statistics(ds)

    # Final summary
    print("\n" + "="*80)
    print("QA/QC SUMMARY")
    print("="*80)

    if len(all_issues) == 0:
        print("\n✓ All QA/QC checks PASSED!")
        print("The climate indices appear to be of good quality.")
    else:
        print(f"\n⚠ Found {len(all_issues)} potential issues:")
        for i, issue in enumerate(all_issues, 1):
            print(f"  {i}. {issue}")

        print("\nRecommendations:")
        print("  1. Review areas with high missing data percentages")
        print("  2. Investigate spatial outliers for data quality issues")
        print("  3. Check temporal jumps for processing artifacts")

    ds.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
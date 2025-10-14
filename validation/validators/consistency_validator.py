"""
Cross-year and inter-file consistency validation module for xclim-timber outputs.

Validates temporal consistency, spatial coverage stability, and data patterns across years.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
import numpy as np
import xarray as xr
from collections import defaultdict
import re

logger = logging.getLogger(__name__)


class ConsistencyValidator:
    """Validate consistency across multiple years and files."""

    def __init__(self):
        """Initialize ConsistencyValidator with consistency thresholds."""
        # Thresholds for detecting anomalies
        self.coverage_std_threshold = 3.0  # Standard deviations for coverage anomaly
        self.value_std_threshold = 4.0  # Standard deviations for value anomaly
        self.min_files_for_comparison = 3  # Minimum files needed for consistency check

    def validate_temporal_consistency(self,
                                     files: List[Path],
                                     expected_start_year: Optional[int] = None,
                                     expected_end_year: Optional[int] = None) -> Dict:
        """
        Check time series continuity and detect gaps.

        Args:
            files: List of NetCDF file paths
            expected_start_year: Expected first year
            expected_end_year: Expected last year

        Returns:
            dict: Temporal consistency validation results
        """
        results = {
            'status': 'PASS',
            'errors': [],
            'warnings': [],
            'years_found': [],
            'missing_years': [],
            'duplicate_years': [],
            'year_coverage': {}
        }

        if not files:
            results['status'] = 'ERROR'
            results['errors'].append('No files provided for validation')
            return results

        # Extract years from filenames
        year_files = defaultdict(list)
        for f in files:
            year = self._extract_year_from_filename(f.name)
            if year:
                year_files[year].append(f.name)
                results['years_found'].append(year)

        results['years_found'] = sorted(set(results['years_found']))

        # Check for duplicates
        for year, file_list in year_files.items():
            if len(file_list) > 1:
                results['duplicate_years'].append(year)
                results['warnings'].append(
                    f'Year {year} has {len(file_list)} files: {", ".join(file_list[:3])}'
                )

        # Check for gaps if expected range provided
        if expected_start_year and expected_end_year:
            expected_years = list(range(expected_start_year, expected_end_year + 1))
            found_years = set(results['years_found'])
            missing = set(expected_years) - found_years

            if missing:
                results['missing_years'] = sorted(missing)
                results['status'] = 'FAIL'
                results['errors'].append(
                    f'Missing {len(missing)} years: {sorted(missing)[:10]}{"..." if len(missing) > 10 else ""}'
                )

            # Check for years outside expected range
            extra_years = found_years - set(expected_years)
            if extra_years:
                results['warnings'].append(
                    f'Found {len(extra_years)} years outside expected range: {sorted(extra_years)[:5]}'
                )

        # Check for continuity (no gaps in sequence)
        if len(results['years_found']) > 1:
            years = results['years_found']
            gaps = []
            for i in range(1, len(years)):
                if years[i] - years[i-1] > 1:
                    gap_years = list(range(years[i-1] + 1, years[i]))
                    gaps.extend(gap_years)

            if gaps:
                results['warnings'].append(f'Gaps in temporal sequence: {gaps[:10]}{"..." if len(gaps) > 10 else ""}')

        # Calculate coverage statistics
        results['year_coverage'] = {
            'total_years': len(results['years_found']),
            'year_range': (min(results['years_found']), max(results['years_found']))
                         if results['years_found'] else (None, None),
            'completeness': len(results['years_found']) / len(expected_years) * 100
                          if expected_start_year and expected_end_year else None
        }

        # Set summary message
        if results['status'] == 'PASS':
            results['message'] = f'Temporal consistency verified for {len(results["years_found"])} years'
        else:
            results['message'] = f'Temporal inconsistencies found: {len(results["missing_years"])} missing years'

        return results

    def compare_spatial_coverage(self,
                                files: List[Path],
                                sample_size: Optional[int] = None,
                                variables_to_check: Optional[List[str]] = None) -> Dict:
        """
        Compare spatial coverage across years to detect anomalies.

        Args:
            files: List of NetCDF file paths
            sample_size: Number of files to sample (None = all files)
            variables_to_check: Specific variables to check (None = all)

        Returns:
            dict: Spatial coverage comparison results
        """
        results = {
            'status': 'PASS',
            'anomalies': [],
            'warnings': [],
            'coverage_stats': {},
            'anomalous_files': []
        }

        if len(files) < self.min_files_for_comparison:
            results['warnings'].append(
                f'Only {len(files)} files available, need at least {self.min_files_for_comparison} for comparison'
            )
            return results

        # Sample files if requested
        files_to_check = files[:sample_size] if sample_size else files

        # Collect coverage statistics for each file
        coverage_data = defaultdict(list)
        file_years = {}

        for f in files_to_check:
            try:
                year = self._extract_year_from_filename(f.name)
                file_years[f.name] = year

                ds = xr.open_dataset(f, decode_timedelta=False)

                # Determine which variables to check
                if variables_to_check:
                    vars_to_analyze = [v for v in variables_to_check if v in ds.data_vars]
                else:
                    vars_to_analyze = list(ds.data_vars)[:5]  # Check first 5 variables

                for var_name in vars_to_analyze:
                    data = ds[var_name].values
                    nan_fraction = np.isnan(data).sum() / data.size
                    coverage_fraction = 1.0 - nan_fraction

                    coverage_data[var_name].append({
                        'file': f.name,
                        'year': year,
                        'coverage': coverage_fraction,
                        'nan_fraction': nan_fraction
                    })

                ds.close()

            except Exception as e:
                results['warnings'].append(f'Error reading {f.name}: {str(e)}')
                continue

        # Analyze coverage patterns for anomalies
        for var_name, coverage_list in coverage_data.items():
            if len(coverage_list) < self.min_files_for_comparison:
                continue

            coverages = [item['coverage'] for item in coverage_list]
            mean_coverage = np.mean(coverages)
            std_coverage = np.std(coverages)

            var_stats = {
                'mean_coverage': float(mean_coverage),
                'std_coverage': float(std_coverage),
                'min_coverage': float(np.min(coverages)),
                'max_coverage': float(np.max(coverages)),
                'anomalous_years': []
            }

            # Detect anomalies (coverage significantly different from mean)
            if std_coverage > 0:
                for item in coverage_list:
                    z_score = abs(item['coverage'] - mean_coverage) / std_coverage
                    if z_score > self.coverage_std_threshold:
                        anomaly_info = {
                            'year': item['year'],
                            'file': item['file'],
                            'coverage': item['coverage'],
                            'z_score': z_score,
                            'deviation': item['coverage'] - mean_coverage
                        }
                        var_stats['anomalous_years'].append(anomaly_info)
                        results['anomalies'].append(
                            f'{var_name} in year {item["year"]}: unusual coverage '
                            f'({item["coverage"]:.2%} vs mean {mean_coverage:.2%})'
                        )
                        if item['file'] not in results['anomalous_files']:
                            results['anomalous_files'].append(item['file'])

            results['coverage_stats'][var_name] = var_stats

        # Update status based on findings
        if results['anomalies']:
            results['status'] = 'WARNING'
            results['message'] = f'Found {len(results["anomalies"])} coverage anomalies across {len(results["anomalous_files"])} files'
        else:
            results['message'] = f'Spatial coverage consistent across {len(files_to_check)} files'

        return results

    def compare_value_distributions(self,
                                  files: List[Path],
                                  variables_to_check: Optional[List[str]] = None,
                                  sample_size: int = 10) -> Dict:
        """
        Compare statistical distributions of values across files.

        Args:
            files: List of NetCDF file paths
            variables_to_check: Specific variables to check
            sample_size: Number of files to sample

        Returns:
            dict: Value distribution comparison results
        """
        results = {
            'status': 'PASS',
            'warnings': [],
            'anomalies': [],
            'distributions': {}
        }

        if len(files) < self.min_files_for_comparison:
            results['warnings'].append(f'Insufficient files for comparison ({len(files)} < {self.min_files_for_comparison})')
            return results

        # Sample files
        files_to_check = files[:sample_size] if len(files) > sample_size else files

        # Collect statistics for each variable across files
        var_statistics = defaultdict(list)

        for f in files_to_check:
            try:
                year = self._extract_year_from_filename(f.name)
                ds = xr.open_dataset(f, decode_timedelta=False)

                # Determine variables to analyze
                if variables_to_check:
                    vars_to_analyze = [v for v in variables_to_check if v in ds.data_vars]
                else:
                    vars_to_analyze = list(ds.data_vars)[:5]

                for var_name in vars_to_analyze:
                    data = ds[var_name].values
                    valid_data = data[~np.isnan(data)]

                    if len(valid_data) > 0:
                        stats = {
                            'file': f.name,
                            'year': year,
                            'mean': float(np.mean(valid_data)),
                            'std': float(np.std(valid_data)),
                            'min': float(np.min(valid_data)),
                            'max': float(np.max(valid_data)),
                            'median': float(np.median(valid_data)),
                            'q25': float(np.percentile(valid_data, 25)),
                            'q75': float(np.percentile(valid_data, 75))
                        }
                        var_statistics[var_name].append(stats)

                ds.close()

            except Exception as e:
                results['warnings'].append(f'Error reading {f.name}: {str(e)}')
                continue

        # Analyze distribution consistency
        for var_name, stats_list in var_statistics.items():
            if len(stats_list) < self.min_files_for_comparison:
                continue

            # Calculate reference statistics (mean of means, etc.)
            means = [s['mean'] for s in stats_list]
            stds = [s['std'] for s in stats_list]

            ref_mean = np.mean(means)
            ref_std = np.std(means)

            var_analysis = {
                'reference_mean': float(ref_mean),
                'reference_std': float(ref_std),
                'mean_range': (float(np.min(means)), float(np.max(means))),
                'std_range': (float(np.min(stds)), float(np.max(stds))),
                'anomalous_years': []
            }

            # Detect anomalous distributions
            if ref_std > 0:
                for stats in stats_list:
                    z_score = abs(stats['mean'] - ref_mean) / ref_std
                    if z_score > self.value_std_threshold:
                        var_analysis['anomalous_years'].append({
                            'year': stats['year'],
                            'file': stats['file'],
                            'mean': stats['mean'],
                            'z_score': float(z_score)
                        })
                        results['anomalies'].append(
                            f'{var_name} in year {stats["year"]}: anomalous mean value '
                            f'({stats["mean"]:.2f} vs reference {ref_mean:.2f}, z={z_score:.2f})'
                        )

            results['distributions'][var_name] = var_analysis

        # Update status
        if results['anomalies']:
            results['status'] = 'WARNING'
            results['message'] = f'Found {len(results["anomalies"])} distribution anomalies'
        else:
            results['message'] = f'Value distributions consistent across {len(files_to_check)} files'

        return results

    def validate_index_relationships(self,
                                    files: List[Path],
                                    relationships: Optional[Dict] = None) -> Dict:
        """
        Validate known relationships between indices.

        Args:
            files: List of NetCDF files to check
            relationships: Dict of index relationships to validate

        Returns:
            dict: Relationship validation results
        """
        results = {
            'status': 'PASS',
            'violations': [],
            'warnings': [],
            'checks_performed': []
        }

        # Default relationships if none provided
        if relationships is None:
            relationships = {
                'tx_tn_order': {
                    'check': lambda ds: np.all(ds['tx_mean'].values >= ds['tn_mean'].values),
                    'description': 'Maximum temperature should be >= minimum temperature',
                    'variables': ['tx_mean', 'tn_mean']
                },
                'frost_ice_order': {
                    'check': lambda ds: np.all(ds['frost_days'].values >= ds['ice_days'].values),
                    'description': 'Frost days should be >= ice days',
                    'variables': ['frost_days', 'ice_days']
                },
                'precipitation_components': {
                    'check': lambda ds: np.all(ds['r99p'].values <= ds['r95p'].values),
                    'description': 'R99p should be <= R95p',
                    'variables': ['r95p', 'r99p']
                }
            }

        files_checked = 0
        for f in files[:10]:  # Check up to 10 files
            try:
                ds = xr.open_dataset(f, decode_timedelta=False)
                year = self._extract_year_from_filename(f.name)

                for rel_name, rel_info in relationships.items():
                    # Check if required variables are present
                    vars_present = all(v in ds.data_vars for v in rel_info['variables'])

                    if vars_present:
                        try:
                            # Remove NaN values for comparison
                            check_passed = rel_info['check'](ds)

                            if not check_passed:
                                results['violations'].append(
                                    f'{f.name} (year {year}): {rel_info["description"]} violated'
                                )
                                results['status'] = 'FAIL'

                            results['checks_performed'].append({
                                'file': f.name,
                                'year': year,
                                'relationship': rel_name,
                                'passed': bool(check_passed)
                            })

                        except Exception as e:
                            results['warnings'].append(
                                f'Could not check {rel_name} in {f.name}: {str(e)}'
                            )

                files_checked += 1
                ds.close()

            except Exception as e:
                results['warnings'].append(f'Error reading {f.name}: {str(e)}')
                continue

        # Set summary message
        if results['status'] == 'PASS':
            results['message'] = f'All index relationships valid across {files_checked} files'
        else:
            results['message'] = f'Found {len(results["violations"])} relationship violations'

        return results

    def compare_with_baseline(self,
                            files: List[Path],
                            baseline_file: Path,
                            tolerance: float = 0.1) -> Dict:
        """
        Compare files against a known good baseline.

        Args:
            files: List of files to validate
            baseline_file: Path to baseline file
            tolerance: Acceptable deviation from baseline (fraction)

        Returns:
            dict: Baseline comparison results
        """
        results = {
            'status': 'PASS',
            'baseline': str(baseline_file),
            'deviations': [],
            'warnings': []
        }

        try:
            # Load baseline
            baseline_ds = xr.open_dataset(baseline_file)
            baseline_stats = {}

            # Calculate baseline statistics
            for var_name in baseline_ds.data_vars:
                data = baseline_ds[var_name].values
                valid_data = data[~np.isnan(data)]
                if len(valid_data) > 0:
                    baseline_stats[var_name] = {
                        'mean': float(np.mean(valid_data)),
                        'std': float(np.std(valid_data))
                    }

            baseline_ds.close()

            # Compare each file against baseline
            for f in files[:10]:  # Check up to 10 files
                try:
                    ds = xr.open_dataset(f, decode_timedelta=False)
                    year = self._extract_year_from_filename(f.name)

                    for var_name in ds.data_vars:
                        if var_name not in baseline_stats:
                            continue

                        data = ds[var_name].values
                        valid_data = data[~np.isnan(data)]

                        if len(valid_data) > 0:
                            current_mean = float(np.mean(valid_data))
                            baseline_mean = baseline_stats[var_name]['mean']

                            if baseline_mean != 0:
                                deviation = abs(current_mean - baseline_mean) / abs(baseline_mean)
                                if deviation > tolerance:
                                    results['deviations'].append({
                                        'file': f.name,
                                        'year': year,
                                        'variable': var_name,
                                        'current_mean': current_mean,
                                        'baseline_mean': baseline_mean,
                                        'deviation_percent': deviation * 100
                                    })
                                    results['status'] = 'WARNING'

                    ds.close()

                except Exception as e:
                    results['warnings'].append(f'Error comparing {f.name}: {str(e)}')

        except Exception as e:
            results['status'] = 'ERROR'
            results['warnings'].append(f'Error reading baseline: {str(e)}')

        # Set summary message
        if results['status'] == 'PASS':
            results['message'] = 'All files consistent with baseline'
        elif results['deviations']:
            results['message'] = f'Found {len(results["deviations"])} deviations from baseline'
        else:
            results['message'] = 'Baseline comparison completed with warnings'

        return results

    def _extract_year_from_filename(self, filename: str) -> Optional[int]:
        """
        Extract year from filename.

        Args:
            filename: Name of the file

        Returns:
            int: Year extracted from filename, or None
        """
        # Try to match pattern like: temperature_indices_2023_2023.nc
        pattern = r'_(\d{4})_(\d{4})\.nc'
        match = re.search(pattern, filename)

        if match:
            start_year = int(match.group(1))
            return start_year

        # Try alternative pattern: _2023.nc
        pattern = r'_(\d{4})\.nc'
        match = re.search(pattern, filename)

        if match:
            return int(match.group(1))

        return None

    def validate_multi_year_trends(self,
                                  files: List[Path],
                                  min_years: int = 5) -> Dict:
        """
        Validate that multi-year trends are reasonable.

        Args:
            files: List of files spanning multiple years
            min_years: Minimum years needed for trend analysis

        Returns:
            dict: Trend validation results
        """
        results = {
            'status': 'PASS',
            'warnings': [],
            'trends': {}
        }

        if len(files) < min_years:
            results['warnings'].append(
                f'Insufficient files for trend analysis ({len(files)} < {min_years})'
            )
            return results

        # Collect time series data
        time_series = defaultdict(lambda: {'years': [], 'values': []})

        for f in sorted(files):
            try:
                year = self._extract_year_from_filename(f.name)
                if not year:
                    continue

                ds = xr.open_dataset(f, decode_timedelta=False)

                # Sample a few key variables
                for var_name in list(ds.data_vars)[:3]:
                    data = ds[var_name].values
                    valid_data = data[~np.isnan(data)]

                    if len(valid_data) > 0:
                        mean_val = float(np.mean(valid_data))
                        time_series[var_name]['years'].append(year)
                        time_series[var_name]['values'].append(mean_val)

                ds.close()

            except Exception as e:
                results['warnings'].append(f'Error reading {f.name}: {str(e)}')

        # Analyze trends
        for var_name, series in time_series.items():
            if len(series['years']) >= min_years:
                years = np.array(series['years'])
                values = np.array(series['values'])

                # Simple linear trend
                coeffs = np.polyfit(years, values, 1)
                trend = coeffs[0]
                intercept = coeffs[1]

                # Calculate R-squared
                predicted = trend * years + intercept
                ss_res = np.sum((values - predicted) ** 2)
                ss_tot = np.sum((values - np.mean(values)) ** 2)
                r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

                results['trends'][var_name] = {
                    'trend_per_year': float(trend),
                    'r_squared': float(r_squared),
                    'n_years': len(years),
                    'suspicious': abs(trend) > abs(np.mean(values)) * 0.1  # >10% change per year is suspicious
                }

                if results['trends'][var_name]['suspicious']:
                    results['warnings'].append(
                        f'{var_name}: Suspicious trend detected ({trend:.3f} per year)'
                    )

        return results
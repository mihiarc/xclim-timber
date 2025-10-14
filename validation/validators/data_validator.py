"""
Data integrity validation module for xclim-timber outputs.

Validates data quality, completeness, and physical plausibility of climate indices.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate data quality and integrity."""

    def __init__(self):
        """Initialize DataValidator with expected indices and value ranges."""
        # Expected indices by pipeline type
        self.expected_indices = {
            'temperature': [
                'tg_mean', 'tg10p', 'tg90p', 'tx_mean', 'tx10p', 'tx90p',
                'tn_mean', 'tn10p', 'tn90p', 'dtr', 'etr', 'frost_days',
                'ice_days', 'summer_days', 'tropical_nights', 'tx_days_above',
                'heat_wave_frequency', 'heat_wave_duration', 'heat_wave_total_length',
                'warm_spell_duration_index', 'gsl', 'spr32', 'tg_max', 'tg_min',
                'tnlt2', 'tnltm2', 'tnltm20', 'tx_tn_days_above', 'degree_days_below_0',
                'degree_days_below_10', 'degree_days_below_18', 'growing_degree_days',
                'heating_degree_days', 'cooling_degree_days', 'freshet_start'
            ],
            'precipitation': [
                'prcptot', 'rx1day', 'rx5day', 'cdd', 'cwd', 'sdii',
                'r95p', 'r99p', 'r10mm', 'r20mm', 'r75ptot', 'r95ptot', 'r99ptot'
            ],
            'drought': [
                'spi3', 'spi6', 'spi12', 'spei3', 'spei6', 'spei12'
            ],
            'multivariate': [
                'daily_freezethaw_cycles'
            ],
            'agricultural': [
                'gdd_base_5', 'gdd_base_10', 'biologically_effective_degree_days',
                'huglin_index', 'cool_night_index', 'latitude_temperature_index',
                'corn_heat_units', 'crop_water_stress_days', 'dry_spell_frequency',
                'dry_spell_total_length', 'dry_spell_max_length'
            ],
            'humidity': [
                'hurs_mean', 'hurs_min', 'hurs_max', 'hurs_10p', 'hurs_90p',
                'dry_days', 'humid_days', 'vpd_mean', 'vpd_max', 'vpd_min',
                'dewpoint_mean', 'dewpoint_min', 'dewpoint_max', 'vpd_90p'
            ],
            'human_comfort': [
                'humidex_mean', 'heat_index_mean', 'humidex_max', 'heat_index_max',
                'heat_stress_days', 'extreme_heat_stress_days',
                'dangerous_heat_days', 'humidex_days_above_35', 'humidex_days_above_40',
                'heat_index_days_above_90', 'heat_index_days_above_100',
                'windchill_mean', 'windchill_min', 'cold_stress_days', 'extreme_cold_days',
                'thermo_hygrometric_index'
            ]
        }

        # Physical value ranges for validation
        self.value_ranges = {
            # Temperature indices (°C or days)
            'tg_mean': (-50, 50),
            'tx_mean': (-50, 60),
            'tn_mean': (-60, 40),
            'frost_days': (0, 365),
            'ice_days': (0, 365),
            'summer_days': (0, 365),
            'tropical_nights': (0, 365),
            'heat_wave_frequency': (0, 50),
            'gsl': (0, 365),
            'growing_degree_days': (0, 10000),
            'heating_degree_days': (0, 15000),
            'cooling_degree_days': (0, 10000),

            # Precipitation indices (mm or days)
            'prcptot': (0, 10000),
            'rx1day': (0, 1000),
            'rx5day': (0, 2000),
            'cdd': (0, 365),
            'cwd': (0, 365),
            'sdii': (0, 200),
            'r10mm': (0, 365),
            'r20mm': (0, 365),

            # Drought indices (standardized)
            'spi3': (-5, 5),
            'spi6': (-5, 5),
            'spi12': (-5, 5),
            'spei3': (-5, 5),
            'spei6': (-5, 5),
            'spei12': (-5, 5),

            # Humidity indices
            'hurs_mean': (0, 100),
            'hurs_min': (0, 100),
            'hurs_max': (0, 100),
            'dry_days': (0, 365),
            'humid_days': (0, 365),

            # Human comfort indices
            'humidex_mean': (-50, 60),
            'heat_index_mean': (-50, 70),
            'windchill_mean': (-80, 50),
            'heat_stress_days': (0, 365),
            'cold_stress_days': (0, 365)
        }

    def validate_indices_present(self,
                                nc_file: Path,
                                pipeline_type: Optional[str] = None,
                                expected_indices: Optional[List[str]] = None) -> Dict:
        """
        Check all expected indices are calculated.

        Args:
            nc_file: Path to NetCDF file
            pipeline_type: Type of pipeline to determine expected indices
            expected_indices: Optional custom list of expected indices

        Returns:
            dict: Validation results with missing/extra indices
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'errors': [],
            'warnings': []
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            # Determine expected indices
            if expected_indices is None and pipeline_type:
                expected_indices = self.expected_indices.get(pipeline_type, [])
            elif expected_indices is None:
                # Try to infer from filename
                for ptype, indices in self.expected_indices.items():
                    if ptype in nc_file.name.lower():
                        expected_indices = indices
                        pipeline_type = ptype
                        break

            if not expected_indices:
                results['warnings'].append('Could not determine expected indices for validation')
                expected_indices = []

            found_indices = set(ds.data_vars)
            expected_set = set(expected_indices)

            missing = expected_set - found_indices
            extra = found_indices - expected_set

            results['found_indices'] = sorted(found_indices)
            results['expected_indices'] = sorted(expected_indices)
            results['missing_indices'] = sorted(missing)
            results['extra_indices'] = sorted(extra)
            results['found_count'] = len(found_indices)
            results['expected_count'] = len(expected_indices)
            results['completeness_percent'] = (
                (len(found_indices & expected_set) / len(expected_set) * 100)
                if expected_set else 100
            )

            if missing:
                results['status'] = 'FAIL'
                results['errors'].append(f'Missing {len(missing)} indices: {", ".join(list(missing)[:5])}{"..." if len(missing) > 5 else ""}')

            if extra and len(extra) > 5:  # Allow some extra indices
                results['warnings'].append(f'Found {len(extra)} unexpected indices')

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['errors'].append(f'Error reading file: {str(e)}')

        # Set summary message
        if results['status'] == 'PASS':
            results['message'] = f'All {len(expected_indices)} expected indices present'
        elif results['status'] == 'FAIL':
            results['message'] = f'Missing {len(results.get("missing_indices", []))} of {len(expected_indices)} expected indices'
        else:
            results['message'] = 'Error validating indices'

        return results

    def validate_data_coverage(self,
                              nc_file: Path,
                              max_nan_fraction: float = 0.5,
                              warn_nan_fraction: float = 0.3) -> Dict:
        """
        Check for excessive NaN values in data.

        Args:
            nc_file: Path to NetCDF file
            max_nan_fraction: Maximum acceptable fraction of NaN values (triggers FAIL)
            warn_nan_fraction: Warning threshold for NaN fraction

        Returns:
            dict: Coverage validation results for each variable
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'variables': {},
            'summary': {}
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            total_vars = len(ds.data_vars)
            failed_vars = []
            warned_vars = []

            for var_name in ds.data_vars:
                data_array = ds[var_name]
                total_values = data_array.size

                # Count NaN values
                nan_count = int(data_array.isnull().sum().values)
                nan_fraction = nan_count / total_values if total_values > 0 else 0

                # Determine status
                if nan_fraction > max_nan_fraction:
                    status = 'FAIL'
                    failed_vars.append(var_name)
                elif nan_fraction > warn_nan_fraction:
                    status = 'WARNING'
                    warned_vars.append(var_name)
                else:
                    status = 'PASS'

                results['variables'][var_name] = {
                    'nan_fraction': float(nan_fraction),
                    'nan_percent': float(nan_fraction * 100),
                    'nan_count': nan_count,
                    'total_values': total_values,
                    'valid_count': total_values - nan_count,
                    'status': status
                }

            # Update overall status
            if failed_vars:
                results['status'] = 'FAIL'
                results['message'] = f'{len(failed_vars)} variable(s) exceed {max_nan_fraction*100:.0f}% NaN threshold'
            elif warned_vars:
                results['status'] = 'WARNING'
                results['message'] = f'{len(warned_vars)} variable(s) exceed {warn_nan_fraction*100:.0f}% NaN warning threshold'
            else:
                results['message'] = f'All {total_vars} variables have acceptable data coverage'

            results['summary'] = {
                'total_variables': total_vars,
                'failed_variables': failed_vars,
                'warned_variables': warned_vars,
                'passed_variables': total_vars - len(failed_vars) - len(warned_vars)
            }

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['message'] = f'Error analyzing data coverage: {str(e)}'

        return results

    def validate_value_ranges(self,
                             nc_file: Path,
                             custom_ranges: Optional[Dict] = None,
                             check_physical_limits: bool = True) -> Dict:
        """
        Verify values are within physically plausible ranges.

        Args:
            nc_file: Path to NetCDF file
            custom_ranges: Optional custom value ranges
            check_physical_limits: Whether to check against physical limits

        Returns:
            dict: Value range validation results
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'variables': {},
            'errors': [],
            'warnings': []
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            # Merge custom ranges with defaults
            ranges_to_check = self.value_ranges.copy()
            if custom_ranges:
                ranges_to_check.update(custom_ranges)

            for var_name in ds.data_vars:
                data = ds[var_name].values
                # Remove NaN values for range checking
                valid_data = data[~np.isnan(data)]

                if len(valid_data) == 0:
                    results['variables'][var_name] = {
                        'status': 'SKIP',
                        'message': 'No valid data'
                    }
                    continue

                actual_min = float(np.min(valid_data))
                actual_max = float(np.max(valid_data))
                actual_mean = float(np.mean(valid_data))
                actual_std = float(np.std(valid_data))

                var_result = {
                    'actual_range': (actual_min, actual_max),
                    'actual_mean': actual_mean,
                    'actual_std': actual_std,
                    'status': 'PASS',
                    'errors': [],
                    'warnings': []
                }

                # Check against expected ranges if available
                if var_name in ranges_to_check:
                    min_val, max_val = ranges_to_check[var_name]
                    var_result['expected_range'] = (min_val, max_val)

                    if actual_min < min_val:
                        var_result['status'] = 'FAIL'
                        var_result['errors'].append(
                            f'Minimum {actual_min:.2f} below expected {min_val:.2f}'
                        )
                        results['errors'].append(f'{var_name}: min value out of range')

                    if actual_max > max_val:
                        var_result['status'] = 'FAIL'
                        var_result['errors'].append(
                            f'Maximum {actual_max:.2f} above expected {max_val:.2f}'
                        )
                        results['errors'].append(f'{var_name}: max value out of range')

                # Check for suspicious patterns
                if check_physical_limits:
                    # Check for constant values
                    if actual_std < 1e-10:
                        var_result['warnings'].append('Constant or near-constant values')
                        results['warnings'].append(f'{var_name}: constant values detected')

                    # Check for extreme outliers (beyond 6 sigma)
                    if actual_std > 0:
                        z_min = abs(actual_min - actual_mean) / actual_std
                        z_max = abs(actual_max - actual_mean) / actual_std
                        if z_min > 6 or z_max > 6:
                            var_result['warnings'].append('Extreme outliers detected (>6σ)')
                            results['warnings'].append(f'{var_name}: extreme outliers')

                results['variables'][var_name] = var_result

                if var_result['status'] == 'FAIL':
                    results['status'] = 'FAIL'

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['errors'].append(f'Error validating ranges: {str(e)}')

        # Set summary message
        if results['status'] == 'PASS':
            results['message'] = 'All variables within expected ranges'
        elif results['status'] == 'FAIL':
            results['message'] = f'{len(results["errors"])} range violation(s) detected'
        else:
            results['message'] = 'Error validating value ranges'

        return results

    def detect_all_zero_arrays(self, nc_file: Path, threshold: float = 0.99) -> Dict:
        """
        Flag variables that are all or mostly zero (likely calculation error).

        Args:
            nc_file: Path to NetCDF file
            threshold: Fraction of zeros to trigger detection (default 0.99)

        Returns:
            dict: Detection results for zero arrays
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'variables': {},
            'suspicious_variables': []
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            for var_name in ds.data_vars:
                data = ds[var_name].values
                non_nan_data = data[~np.isnan(data)]

                if len(non_nan_data) == 0:
                    results['variables'][var_name] = {
                        'all_zero': None,
                        'zero_fraction': None,
                        'status': 'SKIP',
                        'message': 'No valid data'
                    }
                    continue

                # Count zeros
                zero_count = np.sum(non_nan_data == 0)
                zero_fraction = zero_count / len(non_nan_data)
                all_zero = np.all(non_nan_data == 0)

                # Determine if suspicious
                is_suspicious = all_zero or zero_fraction > threshold

                # Some indices can legitimately be mostly zero
                legitimate_zero_indices = [
                    'frost_days', 'ice_days', 'tropical_nights',
                    'r99p', 'r95p', 'heat_wave_frequency'
                ]

                if is_suspicious and var_name not in legitimate_zero_indices:
                    status = 'FAIL'
                    results['suspicious_variables'].append(var_name)
                    results['status'] = 'FAIL'
                elif is_suspicious:
                    status = 'WARNING'
                else:
                    status = 'PASS'

                results['variables'][var_name] = {
                    'all_zero': bool(all_zero),
                    'zero_fraction': float(zero_fraction),
                    'zero_percent': float(zero_fraction * 100),
                    'zero_count': int(zero_count),
                    'non_zero_count': len(non_nan_data) - zero_count,
                    'status': status
                }

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['message'] = f'Error detecting zero arrays: {str(e)}'
            return results

        # Set summary message
        if results['suspicious_variables']:
            results['message'] = f'{len(results["suspicious_variables"])} variable(s) are suspiciously all/mostly zero'
        else:
            results['message'] = 'No suspicious zero arrays detected'

        return results

    def validate_statistical_properties(self, nc_file: Path) -> Dict:
        """
        Validate statistical properties of the data.

        Args:
            nc_file: Path to NetCDF file

        Returns:
            dict: Statistical validation results
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'variables': {},
            'warnings': []
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            for var_name in ds.data_vars:
                data = ds[var_name].values
                non_nan_data = data[~np.isnan(data)]

                if len(non_nan_data) < 2:
                    continue

                stats = {
                    'mean': float(np.mean(non_nan_data)),
                    'std': float(np.std(non_nan_data)),
                    'min': float(np.min(non_nan_data)),
                    'max': float(np.max(non_nan_data)),
                    'median': float(np.median(non_nan_data)),
                    'q25': float(np.percentile(non_nan_data, 25)),
                    'q75': float(np.percentile(non_nan_data, 75))
                }

                # Check for statistical anomalies
                warnings = []

                # Check for highly skewed distributions
                if stats['std'] > 0:
                    skewness = (stats['mean'] - stats['median']) / stats['std']
                    if abs(skewness) > 2:
                        warnings.append(f'Highly skewed distribution (skewness={skewness:.2f})')

                # Check for bimodal distributions (simplified check)
                iqr = stats['q75'] - stats['q25']
                if iqr > 0 and (stats['max'] - stats['min']) / iqr > 10:
                    warnings.append('Possible multimodal distribution')

                results['variables'][var_name] = {
                    'statistics': stats,
                    'warnings': warnings
                }

                if warnings:
                    results['warnings'].extend([f'{var_name}: {w}' for w in warnings])

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['message'] = f'Error validating statistics: {str(e)}'

        return results
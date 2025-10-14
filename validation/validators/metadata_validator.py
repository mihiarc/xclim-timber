"""
Metadata and CF-compliance validation module for xclim-timber outputs.

Validates NetCDF metadata, attributes, and CF conventions compliance.
"""

from pathlib import Path
from typing import Dict, List, Optional, Set
import logging
import xarray as xr
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)


class MetadataValidator:
    """Validate CF-compliance and metadata quality."""

    def __init__(self):
        """Initialize MetadataValidator with CF conventions requirements."""
        # Required global attributes for CF compliance
        self.required_global_attrs = {
            'creation_date',
            'software'
        }

        # Recommended global attributes
        self.recommended_global_attrs = {
            'Conventions',  # Should be "CF-1.8" or similar
            'title',
            'institution',
            'source',
            'history',
            'references',
            'comment'
        }

        # Required variable attributes
        self.required_var_attrs = {
            'units',  # Required for all data variables
        }

        # Recommended variable attributes
        self.recommended_var_attrs = {
            'long_name',
            'standard_name',  # Where applicable
            '_FillValue',
            'valid_min',
            'valid_max',
            'description'
        }

        # Required coordinate attributes
        self.required_coord_attrs = {
            'lat': {'units', 'standard_name'},
            'lon': {'units', 'standard_name'},
            'time': {'units', 'calendar'}
        }

        # Standard units for common variables
        self.standard_units = {
            # Temperature indices
            'tg_mean': '°C',
            'tx_mean': '°C',
            'tn_mean': '°C',
            'frost_days': 'days',
            'ice_days': 'days',
            'summer_days': 'days',
            'growing_degree_days': '°C·days',
            'heating_degree_days': '°C·days',

            # Precipitation indices
            'prcptot': 'mm',
            'rx1day': 'mm/day',
            'rx5day': 'mm',
            'cdd': 'days',
            'cwd': 'days',
            'sdii': 'mm d-1',

            # Coordinates
            'lat': 'degrees_north',
            'lon': 'degrees_east',
            'time': 'days since 1900-01-01'  # Common CF time reference
        }

    def validate_cf_compliance(self,
                              nc_file: Path,
                              strict: bool = False) -> Dict:
        """
        Check CF conventions compliance.

        Args:
            nc_file: Path to NetCDF file
            strict: If True, treat all issues as errors; if False, some are warnings

        Returns:
            dict: CF compliance validation results
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'errors': [],
            'warnings': [],
            'global_attrs': {},
            'variable_attrs': {}
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            # Check global attributes
            global_results = self._validate_global_attributes(ds, strict)
            results['global_attrs'] = global_results
            if global_results['errors']:
                results['errors'].extend(global_results['errors'])
                results['status'] = 'FAIL'
            if global_results['warnings']:
                results['warnings'].extend(global_results['warnings'])

            # Check variable attributes
            for var_name in ds.data_vars:
                var_results = self._validate_variable_attributes(
                    ds[var_name], var_name, strict
                )
                results['variable_attrs'][var_name] = var_results

                if var_results['errors']:
                    results['errors'].extend([f'{var_name}: {e}' for e in var_results['errors']])
                    results['status'] = 'FAIL'
                if var_results['warnings']:
                    results['warnings'].extend([f'{var_name}: {w}' for w in var_results['warnings']])

            # Check coordinate attributes
            coord_results = self._validate_coordinate_attributes(ds, strict)
            results['coordinate_attrs'] = coord_results
            if coord_results['errors']:
                results['errors'].extend(coord_results['errors'])
                results['status'] = 'FAIL'
            if coord_results['warnings']:
                results['warnings'].extend(coord_results['warnings'])

            # Check CF version if specified
            if 'Conventions' in ds.attrs:
                conventions = ds.attrs['Conventions']
                results['cf_version'] = conventions
                if not conventions.startswith('CF-'):
                    results['warnings'].append(
                        f'Conventions attribute "{conventions}" does not follow CF format'
                    )

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['errors'].append(f'Error reading file: {str(e)}')

        # Set summary message
        if results['status'] == 'PASS':
            results['message'] = 'File is CF-compliant'
        elif results['status'] == 'FAIL':
            results['message'] = f'CF compliance issues: {len(results["errors"])} error(s)'
        else:
            results['message'] = 'Error checking CF compliance'

        return results

    def _validate_global_attributes(self, ds: xr.Dataset, strict: bool) -> Dict:
        """
        Validate global attributes.

        Args:
            ds: xarray Dataset
            strict: Whether to treat missing recommended attrs as errors

        Returns:
            dict: Global attribute validation results
        """
        results = {
            'errors': [],
            'warnings': [],
            'present': [],
            'missing_required': [],
            'missing_recommended': []
        }

        # Check required attributes
        for attr in self.required_global_attrs:
            if attr not in ds.attrs:
                results['missing_required'].append(attr)
                results['errors'].append(f'Missing required global attribute: {attr}')
            else:
                results['present'].append(attr)

        # Check recommended attributes
        for attr in self.recommended_global_attrs:
            if attr not in ds.attrs:
                results['missing_recommended'].append(attr)
                if strict:
                    results['errors'].append(f'Missing recommended global attribute: {attr}')
                else:
                    results['warnings'].append(f'Missing recommended global attribute: {attr}')
            else:
                results['present'].append(attr)

        # Validate specific attribute formats
        if 'creation_date' in ds.attrs:
            try:
                # Try to parse the creation date
                date_str = ds.attrs['creation_date']
                if isinstance(date_str, str):
                    # Try common date formats
                    for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                        try:
                            datetime.strptime(date_str.split('.')[0], fmt)
                            break
                        except:
                            continue
                    else:
                        results['warnings'].append(
                            f'creation_date format may not be standard: {date_str}'
                        )
            except Exception as e:
                results['warnings'].append(f'Could not validate creation_date format: {str(e)}')

        return results

    def _validate_variable_attributes(self,
                                     var: xr.DataArray,
                                     var_name: str,
                                     strict: bool) -> Dict:
        """
        Validate variable attributes.

        Args:
            var: xarray DataArray
            var_name: Name of the variable
            strict: Whether to treat missing recommended attrs as errors

        Returns:
            dict: Variable attribute validation results
        """
        results = {
            'errors': [],
            'warnings': [],
            'present': [],
            'missing': []
        }

        # Check required attributes
        for attr in self.required_var_attrs:
            if attr not in var.attrs:
                results['missing'].append(attr)
                results['errors'].append(f'Missing required attribute: {attr}')
            else:
                results['present'].append(attr)

        # Check for long_name or standard_name (at least one should be present)
        if 'long_name' not in var.attrs and 'standard_name' not in var.attrs:
            if strict:
                results['errors'].append('Missing both long_name and standard_name')
            else:
                results['warnings'].append('Missing descriptive name (long_name or standard_name)')

        # Validate units if present
        if 'units' in var.attrs:
            units = var.attrs['units']
            # Check against known standard units
            if var_name in self.standard_units:
                expected_units = self.standard_units[var_name]
                # Allow some flexibility in units representation
                if not self._units_compatible(units, expected_units):
                    results['warnings'].append(
                        f'Units "{units}" may not match expected "{expected_units}"'
                    )

        # Check for _FillValue consistency
        if '_FillValue' in var.attrs:
            fill_value = var.attrs['_FillValue']
            # Check if it's a valid number
            if not isinstance(fill_value, (int, float, np.number)):
                results['warnings'].append(f'_FillValue is not numeric: {fill_value}')

        # Check valid_range if present
        if 'valid_min' in var.attrs and 'valid_max' in var.attrs:
            valid_min = var.attrs['valid_min']
            valid_max = var.attrs['valid_max']
            if valid_min >= valid_max:
                results['errors'].append(f'Invalid range: valid_min ({valid_min}) >= valid_max ({valid_max})')

        return results

    def _validate_coordinate_attributes(self, ds: xr.Dataset, strict: bool) -> Dict:
        """
        Validate coordinate attributes.

        Args:
            ds: xarray Dataset
            strict: Whether to treat warnings as errors

        Returns:
            dict: Coordinate attribute validation results
        """
        results = {
            'errors': [],
            'warnings': [],
            'coordinates': {}
        }

        for coord_name, required_attrs in self.required_coord_attrs.items():
            if coord_name not in ds.coords:
                continue

            coord = ds.coords[coord_name]
            coord_results = {
                'present': [],
                'missing': []
            }

            for attr in required_attrs:
                if attr not in coord.attrs:
                    coord_results['missing'].append(attr)
                    if strict:
                        results['errors'].append(f'{coord_name}: Missing required attribute "{attr}"')
                    else:
                        results['warnings'].append(f'{coord_name}: Missing required attribute "{attr}"')
                else:
                    coord_results['present'].append(attr)

            # Specific validation for coordinate attributes
            if coord_name == 'lat' and 'units' in coord.attrs:
                if coord.attrs['units'] not in ['degrees_north', 'degree_north', 'degrees_N', 'degree_N']:
                    results['warnings'].append(f'lat: Non-standard units "{coord.attrs["units"]}"')

            if coord_name == 'lon' and 'units' in coord.attrs:
                if coord.attrs['units'] not in ['degrees_east', 'degree_east', 'degrees_E', 'degree_E']:
                    results['warnings'].append(f'lon: Non-standard units "{coord.attrs["units"]}"')

            if coord_name == 'time':
                if 'calendar' in coord.attrs:
                    valid_calendars = ['standard', 'gregorian', 'proleptic_gregorian',
                                     'noleap', '365_day', '360_day', 'julian', 'all_leap', '366_day']
                    if coord.attrs['calendar'] not in valid_calendars:
                        results['errors'].append(f'time: Invalid calendar "{coord.attrs["calendar"]}"')

                if 'units' in coord.attrs:
                    # Check if units follow CF time format
                    units = coord.attrs['units']
                    if not any(units.startswith(prefix) for prefix in
                             ['days since', 'hours since', 'minutes since', 'seconds since']):
                        results['warnings'].append(f'time: Non-standard units format "{units}"')

            results['coordinates'][coord_name] = coord_results

        return results

    def _units_compatible(self, units1: str, units2: str) -> bool:
        """
        Check if two unit strings are compatible.

        Args:
            units1: First units string
            units2: Second units string

        Returns:
            bool: True if units are likely compatible
        """
        # Normalize units for comparison
        units1_norm = units1.lower().replace(' ', '').replace('_', '').replace('-', '')
        units2_norm = units2.lower().replace(' ', '').replace('_', '').replace('-', '')

        # Direct match
        if units1_norm == units2_norm:
            return True

        # Common equivalencies
        equivalencies = [
            {'celsius', '°c', 'degc', 'c'},
            {'days', 'day', 'd'},
            {'mm', 'millimeter', 'millimeters'},
            {'mmd1', 'mm/d', 'mm/day', 'mmday1'},
            {'degrees_north', 'degreesnorth', 'degreen', 'degrees_n'},
            {'degrees_east', 'degreeseast', 'degreee', 'degrees_e'}
        ]

        for equiv_set in equivalencies:
            if units1_norm in equiv_set and units2_norm in equiv_set:
                return True

        return False

    def validate_encoding(self, nc_file: Path) -> Dict:
        """
        Validate NetCDF encoding and compression settings.

        Args:
            nc_file: Path to NetCDF file

        Returns:
            dict: Encoding validation results
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'warnings': [],
            'variables': {}
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            for var_name in ds.data_vars:
                var = ds[var_name]
                var_encoding = var.encoding

                var_info = {
                    'dtype': str(var.dtype),
                    'compression': None,
                    'chunking': None
                }

                # Check for compression
                if 'complevel' in var_encoding:
                    var_info['compression'] = var_encoding['complevel']
                elif 'compression' in var_encoding:
                    var_info['compression'] = var_encoding['compression']

                # Check for chunking
                if 'chunks' in var_encoding:
                    var_info['chunking'] = var_encoding['chunks']

                # Check for potential issues
                if var.dtype == 'float64':
                    results['warnings'].append(
                        f'{var_name}: Using float64 (consider float32 for space savings)'
                    )

                if var_info['compression'] is None:
                    results['warnings'].append(
                        f'{var_name}: No compression detected (consider enabling for space savings)'
                    )

                results['variables'][var_name] = var_info

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['warnings'].append(f'Error checking encoding: {str(e)}')

        return results

    def validate_time_metadata(self, nc_file: Path) -> Dict:
        """
        Specifically validate time-related metadata.

        Args:
            nc_file: Path to NetCDF file

        Returns:
            dict: Time metadata validation results
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'errors': [],
            'warnings': []
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            if 'time' in ds.coords:
                time_coord = ds.coords['time']

                # Check time attributes
                time_info = {
                    'units': time_coord.attrs.get('units', 'MISSING'),
                    'calendar': time_coord.attrs.get('calendar', 'MISSING'),
                    'size': len(time_coord)
                }

                # Validate time bounds if present
                if 'bounds' in time_coord.attrs:
                    bounds_name = time_coord.attrs['bounds']
                    if bounds_name not in ds.variables:
                        results['errors'].append(
                            f'Time bounds variable "{bounds_name}" referenced but not found'
                        )
                        results['status'] = 'FAIL'

                # Check for climatology attributes if relevant
                if 'climatology' in time_coord.attrs:
                    clim_name = time_coord.attrs['climatology']
                    if clim_name not in ds.variables:
                        results['errors'].append(
                            f'Climatology variable "{clim_name}" referenced but not found'
                        )
                        results['status'] = 'FAIL'

                results['time_metadata'] = time_info

            # Check for time_range in global attributes
            if 'time_range' in ds.attrs:
                results['time_range'] = ds.attrs['time_range']

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['errors'].append(f'Error validating time metadata: {str(e)}')

        return results
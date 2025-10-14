"""
Dimension validation module for xclim-timber outputs.

Validates dataset dimensions, coordinates, and spatial/temporal consistency.
"""

from pathlib import Path
from typing import Dict, Optional, List, Tuple
import logging
import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


class DimensionValidator:
    """Validate dataset dimensions match expectations."""

    def __init__(self):
        """Initialize DimensionValidator with expected dimensions."""
        # Standard dimensions for CONUS domain
        self.expected_dimensions = {
            'default': {
                'time': 1,  # Annual indices
                'lat': 621,  # CONUS latitude points
                'lon': 1405  # CONUS longitude points
            },
            'monthly': {
                'time': 12,  # Monthly data
                'lat': 621,
                'lon': 1405
            },
            'multi_year': {
                'time': None,  # Variable based on year range
                'lat': 621,
                'lon': 1405
            }
        }

        # Expected coordinate ranges for CONUS
        self.coord_ranges = {
            'lat': (24.0, 50.0),  # Approximate CONUS latitude range
            'lon': (-125.0, -66.0)  # Approximate CONUS longitude range
        }

    def validate_dimensions(self,
                           nc_file: Path,
                           expected_dims: Optional[Dict] = None,
                           dimension_type: str = 'default') -> Dict:
        """
        Verify dimension sizes match expectations.

        Args:
            nc_file: Path to NetCDF file
            expected_dims: Optional custom expected dimensions
            dimension_type: Type of dimensions to expect ('default', 'monthly', 'multi_year')

        Returns:
            dict: Validation results with pass/fail status
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'errors': [],
            'warnings': [],
            'dimensions': {}
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            # Use provided dimensions or defaults
            if expected_dims is None:
                expected_dims = self.expected_dimensions.get(
                    dimension_type,
                    self.expected_dimensions['default']
                )

            # Check each expected dimension
            for dim_name, expected_size in expected_dims.items():
                if expected_size is None:
                    # Skip variable-size dimensions
                    continue

                if dim_name not in ds.dims:
                    results['status'] = 'FAIL'
                    results['errors'].append(f'Missing dimension: {dim_name}')
                    results['dimensions'][dim_name] = {
                        'expected': expected_size,
                        'actual': None,
                        'status': 'MISSING'
                    }
                    continue

                actual_size = ds.sizes[dim_name]
                dimension_info = {
                    'expected': expected_size,
                    'actual': actual_size,
                    'status': 'PASS'
                }

                if actual_size != expected_size:
                    # Check if it's a minor difference (within 5%)
                    if abs(actual_size - expected_size) / expected_size < 0.05:
                        results['warnings'].append(
                            f'{dim_name}: slight size mismatch (expected {expected_size}, got {actual_size})'
                        )
                        dimension_info['status'] = 'WARNING'
                    else:
                        results['status'] = 'FAIL'
                        results['errors'].append(
                            f'{dim_name}: size mismatch (expected {expected_size}, got {actual_size})'
                        )
                        dimension_info['status'] = 'FAIL'

                results['dimensions'][dim_name] = dimension_info

            # Check for unexpected dimensions
            for dim_name in ds.dims:
                if dim_name not in expected_dims:
                    results['warnings'].append(f'Unexpected dimension: {dim_name} (size={ds.sizes[dim_name]})')
                    results['dimensions'][dim_name] = {
                        'expected': None,
                        'actual': ds.sizes[dim_name],
                        'status': 'UNEXPECTED'
                    }

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['errors'].append(f'Error reading file: {str(e)}')

        # Set summary message
        if results['status'] == 'PASS':
            results['message'] = 'All dimensions match expected values'
        elif results['status'] == 'FAIL':
            results['message'] = f"Dimension errors: {'; '.join(results['errors'][:3])}"
        else:
            results['message'] = f"Error validating dimensions: {results['errors'][0] if results['errors'] else 'Unknown error'}"

        return results

    def validate_coordinates(self,
                           nc_file: Path,
                           check_monotonic: bool = True,
                           check_ranges: bool = True) -> Dict:
        """
        Check coordinate values are monotonic and within expected ranges.

        Args:
            nc_file: Path to NetCDF file
            check_monotonic: Check if coordinates are monotonically increasing
            check_ranges: Check if coordinates are within expected ranges

        Returns:
            dict: Coordinate validation results
        """
        results = {
            'file': nc_file.name,
            'status': 'PASS',
            'errors': [],
            'warnings': [],
            'coordinates': {}
        }

        try:
            ds = xr.open_dataset(nc_file, decode_timedelta=False)

            # Check latitude coordinates
            if 'lat' in ds.coords:
                lat_results = self._validate_coordinate_array(
                    ds.lat.values,
                    'lat',
                    self.coord_ranges.get('lat'),
                    check_monotonic,
                    check_ranges
                )
                results['coordinates']['lat'] = lat_results

                if lat_results['status'] == 'FAIL':
                    results['status'] = 'FAIL'
                    results['errors'].extend(lat_results['errors'])
                elif lat_results['status'] == 'WARNING':
                    results['warnings'].extend(lat_results['warnings'])

            # Check longitude coordinates
            if 'lon' in ds.coords:
                lon_results = self._validate_coordinate_array(
                    ds.lon.values,
                    'lon',
                    self.coord_ranges.get('lon'),
                    check_monotonic,
                    check_ranges
                )
                results['coordinates']['lon'] = lon_results

                if lon_results['status'] == 'FAIL':
                    results['status'] = 'FAIL'
                    results['errors'].extend(lon_results['errors'])
                elif lon_results['status'] == 'WARNING':
                    results['warnings'].extend(lon_results['warnings'])

            # Check time coordinates
            if 'time' in ds.coords:
                time_results = self._validate_time_coordinate(ds.time)
                results['coordinates']['time'] = time_results

                if time_results['status'] == 'FAIL':
                    results['status'] = 'FAIL'
                    results['errors'].extend(time_results.get('errors', []))

            ds.close()

        except Exception as e:
            results['status'] = 'ERROR'
            results['errors'].append(f'Error reading file: {str(e)}')

        # Set summary message
        if results['status'] == 'PASS':
            results['message'] = 'All coordinates valid'
        elif results['status'] == 'FAIL':
            results['message'] = f"Coordinate errors: {'; '.join(results['errors'][:2])}"
        else:
            results['message'] = f"Error validating coordinates"

        return results

    def _validate_coordinate_array(self,
                                  coord_array: np.ndarray,
                                  coord_name: str,
                                  expected_range: Optional[Tuple[float, float]],
                                  check_monotonic: bool,
                                  check_ranges: bool) -> Dict:
        """
        Validate a single coordinate array.

        Args:
            coord_array: Numpy array of coordinate values
            coord_name: Name of the coordinate
            expected_range: Expected min/max range
            check_monotonic: Check if monotonically increasing
            check_ranges: Check if within expected range

        Returns:
            dict: Validation results for this coordinate
        """
        result = {
            'status': 'PASS',
            'errors': [],
            'warnings': [],
            'min': float(coord_array.min()),
            'max': float(coord_array.max()),
            'size': len(coord_array)
        }

        # Check for NaN values
        if np.any(np.isnan(coord_array)):
            result['status'] = 'FAIL'
            result['errors'].append(f'{coord_name}: Contains NaN values')

        # Check monotonicity
        if check_monotonic:
            diffs = np.diff(coord_array)
            if not np.all(diffs > 0):
                # Check if it's monotonically decreasing instead
                if not np.all(diffs < 0):
                    result['status'] = 'FAIL'
                    result['errors'].append(f'{coord_name}: Not monotonic')
                else:
                    result['warnings'].append(f'{coord_name}: Monotonically decreasing (expected increasing)')

        # Check ranges
        if check_ranges and expected_range:
            min_val, max_val = expected_range

            # Check minimum
            if result['min'] < min_val - 1.0:  # Allow 1 degree tolerance
                result['status'] = 'FAIL'
                result['errors'].append(
                    f'{coord_name}: Minimum {result["min"]:.2f} below expected range [{min_val:.2f}, {max_val:.2f}]'
                )
            elif result['min'] < min_val:
                result['warnings'].append(
                    f'{coord_name}: Minimum {result["min"]:.2f} slightly below expected {min_val:.2f}'
                )

            # Check maximum
            if result['max'] > max_val + 1.0:  # Allow 1 degree tolerance
                result['status'] = 'FAIL'
                result['errors'].append(
                    f'{coord_name}: Maximum {result["max"]:.2f} above expected range [{min_val:.2f}, {max_val:.2f}]'
                )
            elif result['max'] > max_val:
                result['warnings'].append(
                    f'{coord_name}: Maximum {result["max"]:.2f} slightly above expected {max_val:.2f}'
                )

        # Check for regular spacing
        if len(coord_array) > 1:
            spacings = np.diff(coord_array)
            spacing_std = np.std(spacings)
            spacing_mean = np.mean(spacings)

            if spacing_std > 0.01 * abs(spacing_mean):  # More than 1% variation
                result['warnings'].append(f'{coord_name}: Irregular spacing detected')
                result['spacing'] = {
                    'mean': float(spacing_mean),
                    'std': float(spacing_std),
                    'regular': False
                }
            else:
                result['spacing'] = {
                    'mean': float(spacing_mean),
                    'std': float(spacing_std),
                    'regular': True
                }

        return result

    def _validate_time_coordinate(self, time_coord: xr.DataArray) -> Dict:
        """
        Validate time coordinate specifically.

        Args:
            time_coord: xarray DataArray for time coordinate

        Returns:
            dict: Time coordinate validation results
        """
        result = {
            'status': 'PASS',
            'errors': [],
            'warnings': [],
            'size': len(time_coord)
        }

        try:
            # Get time values
            time_values = time_coord.values

            # Check for valid datetime
            if len(time_values) > 0:
                result['first'] = str(time_values[0])
                result['last'] = str(time_values[-1])

                # Check if time is sorted
                if len(time_values) > 1:
                    if not np.all(time_values[1:] > time_values[:-1]):
                        result['status'] = 'FAIL'
                        result['errors'].append('Time coordinate not monotonically increasing')

            # Check for required attributes
            if 'units' not in time_coord.attrs:
                result['warnings'].append('Missing "units" attribute')
            else:
                result['units'] = time_coord.attrs['units']

            if 'calendar' not in time_coord.attrs:
                result['warnings'].append('Missing "calendar" attribute')
            else:
                result['calendar'] = time_coord.attrs['calendar']

        except Exception as e:
            result['status'] = 'ERROR'
            result['errors'].append(f'Error validating time: {str(e)}')

        return result

    def validate_grid_consistency(self, nc_files: List[Path]) -> Dict:
        """
        Validate that grid dimensions are consistent across multiple files.

        Args:
            nc_files: List of NetCDF file paths to compare

        Returns:
            dict: Grid consistency validation results
        """
        results = {
            'status': 'PASS',
            'errors': [],
            'warnings': [],
            'files_checked': len(nc_files),
            'grids': {}
        }

        if len(nc_files) < 2:
            results['warnings'].append('Need at least 2 files to check consistency')
            return results

        reference_grid = None
        reference_file = None

        for nc_file in nc_files[:10]:  # Check up to 10 files
            try:
                ds = xr.open_dataset(nc_file)

                current_grid = {
                    'lat_size': ds.sizes.get('lat'),
                    'lon_size': ds.sizes.get('lon'),
                    'lat_min': float(ds.lat.min().values) if 'lat' in ds else None,
                    'lat_max': float(ds.lat.max().values) if 'lat' in ds else None,
                    'lon_min': float(ds.lon.min().values) if 'lon' in ds else None,
                    'lon_max': float(ds.lon.max().values) if 'lon' in ds else None
                }

                results['grids'][nc_file.name] = current_grid

                if reference_grid is None:
                    reference_grid = current_grid
                    reference_file = nc_file.name
                else:
                    # Compare with reference
                    for key, ref_val in reference_grid.items():
                        curr_val = current_grid[key]
                        if ref_val != curr_val:
                            # Allow small floating point differences for coordinates
                            if key in ['lat_min', 'lat_max', 'lon_min', 'lon_max']:
                                if ref_val is not None and curr_val is not None:
                                    if abs(ref_val - curr_val) > 0.01:
                                        results['status'] = 'FAIL'
                                        results['errors'].append(
                                            f'{nc_file.name}: {key} mismatch ({curr_val:.3f} vs reference {ref_val:.3f})'
                                        )
                            else:
                                results['status'] = 'FAIL'
                                results['errors'].append(
                                    f'{nc_file.name}: {key} mismatch ({curr_val} vs reference {ref_val})'
                                )

                ds.close()

            except Exception as e:
                results['warnings'].append(f'Error reading {nc_file.name}: {str(e)}')

        # Set summary message
        if results['status'] == 'PASS':
            results['message'] = f'Grid consistent across {len(nc_files)} files'
        else:
            results['message'] = f'Grid inconsistencies found: {len(results["errors"])} error(s)'

        return results
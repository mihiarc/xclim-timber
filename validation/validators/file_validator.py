"""
File validation module for xclim-timber outputs.

Validates file existence, size, and completeness of dataset outputs.
"""

import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class FileValidator:
    """Validate file existence, size, and accessibility."""

    def __init__(self):
        """Initialize FileValidator with default settings."""
        self.default_size_ranges = {
            'temperature': (5_000_000, 30_000_000),  # 5-30 MB expected
            'precipitation': (5_000_000, 30_000_000),  # 5-30 MB expected
            'drought': (3_000_000, 25_000_000),  # 3-25 MB expected
            'agricultural': (10_000_000, 40_000_000),  # 10-40 MB expected
            'multivariate': (1_000_000, 10_000_000),  # 1-10 MB expected
            'humidity': (5_000_000, 25_000_000),  # 5-25 MB expected
            'human_comfort': (3_000_000, 20_000_000),  # 3-20 MB expected
        }

    def validate_file_sizes(self,
                           directory: Path,
                           pipeline_type: str = None,
                           expected_range: Tuple[int, int] = None) -> Dict:
        """
        Check all output files are within expected size ranges.

        Args:
            directory: Path to directory containing output files
            pipeline_type: Type of pipeline (temperature, precipitation, etc.)
            expected_range: Optional custom size range in bytes

        Returns:
            dict: {filename: {'size_mb': float, 'status': str, 'message': str}}
        """
        results = {}

        # Determine size range
        if expected_range is None and pipeline_type:
            expected_range = self.default_size_ranges.get(
                pipeline_type,
                (1_000_000, 50_000_000)  # Default fallback
            )
        elif expected_range is None:
            expected_range = (1_000_000, 50_000_000)  # Generic default

        # Check each NetCDF file
        nc_files = list(directory.glob('*.nc'))

        if not nc_files:
            logger.warning(f"No NetCDF files found in {directory}")
            return {'_directory': {
                'status': 'WARNING',
                'message': 'No NetCDF files found in directory'
            }}

        for nc_file in nc_files:
            try:
                size_bytes = nc_file.stat().st_size
                size_mb = size_bytes / (1024 * 1024)

                min_mb = expected_range[0] / (1024 * 1024)
                max_mb = expected_range[1] / (1024 * 1024)

                if size_bytes == 0:
                    status = 'FAIL'
                    message = f'File is empty (0 bytes)'
                elif size_mb < min_mb:
                    status = 'FAIL'
                    message = f'File too small ({size_mb:.2f} MB), likely corrupted or incomplete'
                elif size_mb > max_mb:
                    status = 'WARNING'
                    message = f'File larger than expected ({size_mb:.2f} MB), may need investigation'
                else:
                    status = 'PASS'
                    message = f'File size OK ({size_mb:.2f} MB)'

                results[nc_file.name] = {
                    'size_mb': size_mb,
                    'size_bytes': size_bytes,
                    'status': status,
                    'message': message,
                    'expected_range_mb': (min_mb, max_mb)
                }

            except Exception as e:
                results[nc_file.name] = {
                    'size_mb': 0,
                    'size_bytes': 0,
                    'status': 'ERROR',
                    'message': f'Error accessing file: {str(e)}'
                }

        return results

    def extract_year_from_filename(self, filename: str) -> Optional[int]:
        """
        Extract year from standard filename format.

        Expected formats:
        - {pipeline}_indices_{year}_{year}.nc (single year)
        - {pipeline}_indices_{start_year}_{end_year}.nc (year range)

        Args:
            filename: Filename to parse

        Returns:
            int: Year extracted from filename, or None if not found
        """
        # Try to match pattern like: temperature_indices_2023_2023.nc
        pattern = r'_(\d{4})_(\d{4})\.nc'
        match = re.search(pattern, filename)

        if match:
            start_year = int(match.group(1))
            end_year = int(match.group(2))
            # For single year files, both should be the same
            # For range files, return the start year
            return start_year

        return None

    def validate_file_completeness(self,
                                 directory: Path,
                                 start_year: int,
                                 end_year: int,
                                 pattern: str = '*_indices_*.nc') -> Dict:
        """
        Check all expected years have output files.

        Args:
            directory: Directory containing output files
            start_year: First year expected
            end_year: Last year expected
            pattern: Glob pattern for finding files

        Returns:
            dict: Validation results including missing/extra years
        """
        expected_years = set(range(start_year, end_year + 1))
        found_files = {}
        found_years = set()

        # Scan all matching files
        for nc_file in directory.glob(pattern):
            year = self.extract_year_from_filename(nc_file.name)
            if year:
                found_years.add(year)
                if year not in found_files:
                    found_files[year] = []
                found_files[year].append(nc_file.name)

        missing_years = expected_years - found_years
        extra_years = found_years - expected_years

        # Check for duplicate files per year
        duplicates = {}
        for year, files in found_files.items():
            if len(files) > 1:
                duplicates[year] = files

        results = {
            'expected_years': sorted(expected_years),
            'found_years': sorted(found_years),
            'missing_years': sorted(missing_years),
            'extra_years': sorted(extra_years),
            'duplicate_files': duplicates,
            'complete': len(missing_years) == 0,
            'status': 'PASS' if len(missing_years) == 0 else 'FAIL',
            'total_expected': len(expected_years),
            'total_found': len(found_years),
            'completeness_percent': (len(found_years) / len(expected_years) * 100) if expected_years else 0
        }

        # Add detailed status message
        if results['complete']:
            results['message'] = f'All {len(expected_years)} years present'
        else:
            missing_count = len(missing_years)
            results['message'] = f'Missing {missing_count} year(s): {sorted(missing_years)[:5]}{"..." if missing_count > 5 else ""}'

        if duplicates:
            results['status'] = 'WARNING'
            results['message'] += f' | Duplicate files found for {len(duplicates)} year(s)'

        return results

    def validate_file_permissions(self, directory: Path) -> Dict:
        """
        Check file permissions and accessibility.

        Args:
            directory: Directory to check

        Returns:
            dict: Permission validation results
        """
        results = {
            'readable_files': [],
            'unreadable_files': [],
            'status': 'PASS'
        }

        for nc_file in directory.glob('*.nc'):
            try:
                # Try to open file for reading
                with open(nc_file, 'rb') as f:
                    # Read first byte to ensure file is accessible
                    f.read(1)
                results['readable_files'].append(nc_file.name)
            except Exception as e:
                results['unreadable_files'].append({
                    'file': nc_file.name,
                    'error': str(e)
                })
                results['status'] = 'FAIL'

        results['total_files'] = len(results['readable_files']) + len(results['unreadable_files'])
        results['readable_count'] = len(results['readable_files'])
        results['unreadable_count'] = len(results['unreadable_files'])

        if results['unreadable_files']:
            results['message'] = f"{results['unreadable_count']} file(s) not readable"
        else:
            results['message'] = f"All {results['total_files']} files are readable"

        return results

    def validate_directory_structure(self, base_dir: Path, expected_subdirs: List[str] = None) -> Dict:
        """
        Validate expected directory structure exists.

        Args:
            base_dir: Base directory to check
            expected_subdirs: List of expected subdirectory names

        Returns:
            dict: Directory structure validation results
        """
        if expected_subdirs is None:
            expected_subdirs = [
                'temperature',
                'precipitation',
                'drought',
                'agricultural',
                'multivariate',
                'humidity',
                'human_comfort'
            ]

        results = {
            'base_dir': str(base_dir),
            'exists': base_dir.exists(),
            'subdirs': {},
            'status': 'PASS'
        }

        if not base_dir.exists():
            results['status'] = 'FAIL'
            results['message'] = f'Base directory does not exist: {base_dir}'
            return results

        missing_dirs = []
        for subdir_name in expected_subdirs:
            subdir_path = base_dir / subdir_name
            exists = subdir_path.exists()
            file_count = len(list(subdir_path.glob('*.nc'))) if exists else 0

            results['subdirs'][subdir_name] = {
                'exists': exists,
                'path': str(subdir_path),
                'file_count': file_count
            }

            if not exists:
                missing_dirs.append(subdir_name)

        if missing_dirs:
            results['status'] = 'WARNING'
            results['message'] = f'Missing subdirectories: {", ".join(missing_dirs)}'
        else:
            results['message'] = f'All {len(expected_subdirs)} expected subdirectories present'

        return results
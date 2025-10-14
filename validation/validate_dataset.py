#!/usr/bin/env python3
"""
Automated data quality validation for xclim-timber outputs.

This script runs comprehensive validation checks on climate indices pipeline outputs
to ensure data quality, integrity, and consistency.

Usage:
    python validate_dataset.py outputs/production/temperature/ --pipeline temperature
    python validate_dataset.py outputs/production/precipitation/ --pipeline precipitation --report
    python validate_dataset.py outputs/production/ --pipeline all --json validation_results.json
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Import validators
try:
    from .validators import (
        FileValidator,
        DimensionValidator,
        DataValidator,
        MetadataValidator,
        ConsistencyValidator
    )
except ImportError:
    # For direct script execution
    from validation.validators import (
        FileValidator,
        DimensionValidator,
        DataValidator,
        MetadataValidator,
        ConsistencyValidator
    )

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatasetValidator:
    """Main validation orchestrator for xclim-timber datasets."""

    def __init__(self):
        """Initialize the DatasetValidator with all validator components."""
        self.file_validator = FileValidator()
        self.dimension_validator = DimensionValidator()
        self.data_validator = DataValidator()
        self.metadata_validator = MetadataValidator()
        self.consistency_validator = ConsistencyValidator()

        # Pipeline configurations
        self.pipeline_configs = {
            'temperature': {
                'expected_indices': 35,
                'expected_dims': {'time': 1, 'lat': 621, 'lon': 1405},
                'start_year': 1981,
                'end_year': 2024,
                'file_pattern': 'temperature_indices_*.nc'
            },
            'precipitation': {
                'expected_indices': 13,
                'expected_dims': {'time': 1, 'lat': 621, 'lon': 1405},
                'start_year': 1981,
                'end_year': 2024,
                'file_pattern': 'precipitation_indices_*.nc'
            },
            'drought': {
                'expected_indices': 6,
                'expected_dims': {'time': 1, 'lat': 621, 'lon': 1405},
                'start_year': 1981,
                'end_year': 2024,
                'file_pattern': 'drought_indices_*.nc'
            },
            'agricultural': {
                'expected_indices': 11,
                'expected_dims': {'time': 1, 'lat': 621, 'lon': 1405},
                'start_year': 1981,
                'end_year': 2024,
                'file_pattern': 'agricultural_indices_*.nc'
            },
            'multivariate': {
                'expected_indices': 1,
                'expected_dims': {'time': 1, 'lat': 621, 'lon': 1405},
                'start_year': 1981,
                'end_year': 2024,
                'file_pattern': 'multivariate_indices_*.nc'
            },
            'humidity': {
                'expected_indices': 14,
                'expected_dims': {'time': 1, 'lat': 621, 'lon': 1405},
                'start_year': 1981,
                'end_year': 2024,
                'file_pattern': 'humidity_indices_*.nc'
            },
            'human_comfort': {
                'expected_indices': 16,
                'expected_dims': {'time': 1, 'lat': 621, 'lon': 1405},
                'start_year': 1981,
                'end_year': 2024,
                'file_pattern': 'comfort_indices_*.nc'
            }
        }

    def validate_pipeline_output(self,
                                directory: Path,
                                pipeline_type: str,
                                quick: bool = False) -> Dict:
        """
        Run all validators on pipeline output.

        Args:
            directory: Directory containing pipeline output files
            pipeline_type: Type of pipeline to validate
            quick: If True, run quick validation (sample files only)

        Returns:
            dict: Complete validation results
        """
        # Initialize results
        results = {
            'pipeline': pipeline_type,
            'directory': str(directory),
            'timestamp': datetime.now().isoformat(),
            'validation_mode': 'quick' if quick else 'full',
            'overall_status': 'PASS',
            'summary': {
                'total_checks': 0,
                'passed': 0,
                'failed': 0,
                'warnings': 0,
                'errors': 0
            },
            'validations': {}
        }

        # Get pipeline configuration
        if pipeline_type not in self.pipeline_configs:
            results['overall_status'] = 'ERROR'
            results['error'] = f'Unknown pipeline type: {pipeline_type}'
            return results

        config = self.pipeline_configs[pipeline_type]

        # Get all NetCDF files
        nc_files = sorted(directory.glob(config['file_pattern']))

        if not nc_files:
            results['overall_status'] = 'ERROR'
            results['error'] = f'No files found matching pattern: {config["file_pattern"]}'
            return results

        print(f"\n{'='*60}")
        print(f"Validating {pipeline_type} pipeline output")
        print(f"Directory: {directory}")
        print(f"Files found: {len(nc_files)}")
        print(f"Mode: {'Quick' if quick else 'Full'} validation")
        print(f"{'='*60}\n")

        # 1. FILE VALIDATION
        print("1. File Validation...")
        file_results = self._run_file_validation(directory, nc_files, config)
        results['validations']['file_validation'] = file_results
        self._update_summary(results['summary'], file_results)

        # 2. DIMENSION VALIDATION
        print("2. Dimension Validation...")
        sample_files = nc_files[:5] if quick else nc_files[:20]
        dimension_results = self._run_dimension_validation(sample_files, config)
        results['validations']['dimension_validation'] = dimension_results
        self._update_summary(results['summary'], dimension_results)

        # 3. DATA INTEGRITY VALIDATION
        print("3. Data Integrity Validation...")
        data_files = nc_files[:10] if quick else nc_files
        data_results = self._run_data_validation(data_files, pipeline_type, config)
        results['validations']['data_integrity'] = data_results
        self._update_summary(results['summary'], data_results)

        # 4. CF-COMPLIANCE VALIDATION
        print("4. CF-Compliance Validation...")
        cf_files = nc_files[:5] if quick else nc_files[:10]
        cf_results = self._run_cf_validation(cf_files)
        results['validations']['cf_compliance'] = cf_results
        self._update_summary(results['summary'], cf_results)

        # 5. CROSS-YEAR CONSISTENCY
        if not quick and len(nc_files) > 2:
            print("5. Cross-Year Consistency Validation...")
            consistency_results = self._run_consistency_validation(nc_files, config)
            results['validations']['consistency'] = consistency_results
            self._update_summary(results['summary'], consistency_results)

        # Determine overall status
        if results['summary']['failed'] > 0:
            results['overall_status'] = 'FAIL'
        elif results['summary']['warnings'] > 0:
            results['overall_status'] = 'WARNING'
        else:
            results['overall_status'] = 'PASS'

        # Add overall message
        results['message'] = self._generate_overall_message(results)

        return results

    def _run_file_validation(self, directory: Path, nc_files: List[Path], config: Dict) -> Dict:
        """Run file-level validation checks."""
        results = {
            'checks': {},
            'status': 'PASS',
            'errors': 0,
            'warnings': 0
        }

        # Check file sizes
        size_results = self.file_validator.validate_file_sizes(
            directory,
            pipeline_type=config.get('pipeline_type')
        )
        results['checks']['file_sizes'] = size_results
        self._count_issues(size_results, results)

        # Check file completeness
        completeness_results = self.file_validator.validate_file_completeness(
            directory,
            config['start_year'],
            config['end_year'],
            config['file_pattern']
        )
        results['checks']['completeness'] = completeness_results
        if completeness_results.get('status') == 'FAIL':
            results['status'] = 'FAIL'
            results['errors'] += 1

        # Check file permissions
        permission_results = self.file_validator.validate_file_permissions(directory)
        results['checks']['permissions'] = permission_results
        if permission_results.get('status') == 'FAIL':
            results['status'] = 'FAIL'
            results['errors'] += 1

        return results

    def _run_dimension_validation(self, nc_files: List[Path], config: Dict) -> Dict:
        """Run dimension validation checks."""
        results = {
            'checks': {},
            'status': 'PASS',
            'errors': 0,
            'warnings': 0,
            'files_checked': len(nc_files)
        }

        # Check dimensions for each file
        for nc_file in nc_files:
            dim_result = self.dimension_validator.validate_dimensions(
                nc_file,
                config['expected_dims']
            )
            coord_result = self.dimension_validator.validate_coordinates(nc_file)

            file_results = {
                'dimensions': dim_result,
                'coordinates': coord_result
            }

            # Update overall status
            if dim_result['status'] == 'FAIL' or coord_result['status'] == 'FAIL':
                results['status'] = 'FAIL'
                results['errors'] += 1
            elif dim_result['status'] == 'WARNING' or coord_result['status'] == 'WARNING':
                if results['status'] != 'FAIL':
                    results['status'] = 'WARNING'
                results['warnings'] += 1

            results['checks'][nc_file.name] = file_results

        # Check grid consistency if multiple files
        if len(nc_files) > 1:
            grid_consistency = self.dimension_validator.validate_grid_consistency(nc_files)
            results['checks']['grid_consistency'] = grid_consistency
            if grid_consistency['status'] == 'FAIL':
                results['status'] = 'FAIL'
                results['errors'] += 1

        return results

    def _run_data_validation(self, nc_files: List[Path], pipeline_type: str, config: Dict) -> Dict:
        """Run data integrity validation checks."""
        results = {
            'checks': {},
            'status': 'PASS',
            'errors': 0,
            'warnings': 0,
            'files_checked': len(nc_files)
        }

        for nc_file in nc_files:
            file_checks = {}

            # Check indices present
            indices_result = self.data_validator.validate_indices_present(
                nc_file,
                pipeline_type=pipeline_type
            )
            file_checks['indices'] = indices_result

            # Check data coverage
            coverage_result = self.data_validator.validate_data_coverage(nc_file)
            file_checks['coverage'] = coverage_result

            # Check value ranges
            ranges_result = self.data_validator.validate_value_ranges(nc_file)
            file_checks['value_ranges'] = ranges_result

            # Check for all-zero arrays
            zeros_result = self.data_validator.detect_all_zero_arrays(nc_file)
            file_checks['zero_arrays'] = zeros_result

            # Update overall status
            for check_result in file_checks.values():
                if check_result.get('status') == 'FAIL':
                    results['status'] = 'FAIL'
                    results['errors'] += 1
                elif check_result.get('status') == 'WARNING':
                    if results['status'] != 'FAIL':
                        results['status'] = 'WARNING'
                    results['warnings'] += 1

            results['checks'][nc_file.name] = file_checks

        return results

    def _run_cf_validation(self, nc_files: List[Path]) -> Dict:
        """Run CF-compliance validation checks."""
        results = {
            'checks': {},
            'status': 'PASS',
            'errors': 0,
            'warnings': 0,
            'files_checked': len(nc_files)
        }

        for nc_file in nc_files:
            cf_result = self.metadata_validator.validate_cf_compliance(nc_file)
            encoding_result = self.metadata_validator.validate_encoding(nc_file)

            file_results = {
                'cf_compliance': cf_result,
                'encoding': encoding_result
            }

            # Update overall status
            if cf_result['status'] == 'FAIL':
                results['status'] = 'FAIL'
                results['errors'] += 1
            elif cf_result['status'] == 'WARNING' or len(cf_result.get('warnings', [])) > 0:
                if results['status'] != 'FAIL':
                    results['status'] = 'WARNING'
                results['warnings'] += 1

            results['checks'][nc_file.name] = file_results

        return results

    def _run_consistency_validation(self, nc_files: List[Path], config: Dict) -> Dict:
        """Run cross-year consistency validation checks."""
        results = {
            'checks': {},
            'status': 'PASS',
            'errors': 0,
            'warnings': 0
        }

        # Temporal consistency
        temporal_result = self.consistency_validator.validate_temporal_consistency(
            nc_files,
            config['start_year'],
            config['end_year']
        )
        results['checks']['temporal_consistency'] = temporal_result

        # Spatial coverage comparison
        coverage_result = self.consistency_validator.compare_spatial_coverage(
            nc_files,
            sample_size=20
        )
        results['checks']['spatial_coverage'] = coverage_result

        # Value distribution comparison
        distribution_result = self.consistency_validator.compare_value_distributions(
            nc_files,
            sample_size=15
        )
        results['checks']['value_distributions'] = distribution_result

        # Update overall status
        for check_result in results['checks'].values():
            if check_result.get('status') == 'FAIL':
                results['status'] = 'FAIL'
                results['errors'] += 1
            elif check_result.get('status') == 'WARNING':
                if results['status'] != 'FAIL':
                    results['status'] = 'WARNING'
                results['warnings'] += 1

        return results

    def _count_issues(self, check_results: Dict, results: Dict):
        """Count errors and warnings from check results."""
        if isinstance(check_results, dict):
            for key, value in check_results.items():
                if isinstance(value, dict) and 'status' in value:
                    if value['status'] == 'FAIL':
                        results['errors'] += 1
                        results['status'] = 'FAIL'
                    elif value['status'] == 'WARNING':
                        results['warnings'] += 1
                        if results['status'] != 'FAIL':
                            results['status'] = 'WARNING'

    def _update_summary(self, summary: Dict, validation_results: Dict):
        """Update summary statistics."""
        summary['total_checks'] += 1

        status = validation_results.get('status', 'UNKNOWN')
        if status == 'PASS':
            summary['passed'] += 1
        elif status == 'FAIL':
            summary['failed'] += 1
        elif status == 'WARNING':
            summary['warnings'] += 1
        elif status == 'ERROR':
            summary['errors'] += 1

        # Count individual errors and warnings
        summary['errors'] += validation_results.get('errors', 0)
        summary['warnings'] += validation_results.get('warnings', 0)

    def _generate_overall_message(self, results: Dict) -> str:
        """Generate a summary message for the validation results."""
        summary = results['summary']

        if results['overall_status'] == 'PASS':
            return f"✅ All validation checks passed ({summary['passed']}/{summary['total_checks']} checks)"
        elif results['overall_status'] == 'WARNING':
            return (f"⚠️  Validation completed with warnings: "
                   f"{summary['warnings']} warning(s) in {summary['total_checks']} checks")
        elif results['overall_status'] == 'FAIL':
            return (f"❌ Validation failed: "
                   f"{summary['failed']} failure(s), {summary['errors']} error(s) in {summary['total_checks']} checks")
        else:
            return "❓ Validation status unknown"


def print_validation_summary(results: Dict):
    """Print a formatted summary of validation results."""
    print(f"\n{'='*60}")
    print("VALIDATION SUMMARY")
    print(f"{'='*60}")

    print(f"Pipeline: {results['pipeline']}")
    print(f"Directory: {results['directory']}")
    print(f"Timestamp: {results['timestamp']}")
    print(f"Mode: {results['validation_mode']}")

    print(f"\nOverall Status: {results['overall_status']}")
    print(f"Message: {results['message']}")

    summary = results['summary']
    print(f"\nChecks Summary:")
    print(f"  Total Checks: {summary['total_checks']}")
    print(f"  Passed: {summary['passed']}")
    print(f"  Failed: {summary['failed']}")
    print(f"  Warnings: {summary['warnings']}")
    print(f"  Errors: {summary['errors']}")

    # Print detailed results for each validation category
    for category, category_results in results['validations'].items():
        status = category_results.get('status', 'UNKNOWN')
        status_symbol = {
            'PASS': '✅',
            'WARNING': '⚠️ ',
            'FAIL': '❌',
            'ERROR': '🔥'
        }.get(status, '❓')

        print(f"\n{status_symbol} {category.replace('_', ' ').title()}:")

        if 'message' in category_results:
            print(f"    {category_results['message']}")

        if category_results.get('errors', 0) > 0:
            print(f"    Errors: {category_results['errors']}")

        if category_results.get('warnings', 0) > 0:
            print(f"    Warnings: {category_results['warnings']}")


def check_for_failures(results: Dict) -> bool:
    """Check if validation has any failures."""
    return results['overall_status'] == 'FAIL' or results['summary']['failed'] > 0


def check_for_warnings(results: Dict) -> bool:
    """Check if validation has any warnings."""
    return results['overall_status'] == 'WARNING' or results['summary']['warnings'] > 0


def main():
    """Main entry point for validation script."""
    parser = argparse.ArgumentParser(
        description='Validate xclim-timber pipeline outputs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate temperature pipeline output
  python validate_dataset.py outputs/production/temperature/ --pipeline temperature

  # Quick validation with HTML report
  python validate_dataset.py outputs/production/precipitation/ --pipeline precipitation --quick --report

  # Full validation with JSON output
  python validate_dataset.py outputs/production/ --pipeline all --json results.json

  # Strict validation (fail on warnings)
  python validate_dataset.py outputs/production/drought/ --pipeline drought --fail-on-warning
        """
    )

    parser.add_argument(
        'directory',
        type=Path,
        help='Directory containing pipeline outputs'
    )

    parser.add_argument(
        '--pipeline',
        required=True,
        choices=['temperature', 'precipitation', 'drought', 'agricultural',
                'multivariate', 'humidity', 'human_comfort', 'all'],
        help='Pipeline type to validate'
    )

    parser.add_argument(
        '--quick',
        action='store_true',
        help='Run quick validation (sample files only)'
    )

    parser.add_argument(
        '--report',
        action='store_true',
        help='Generate HTML validation report'
    )

    parser.add_argument(
        '--json',
        type=Path,
        help='Save results to JSON file'
    )

    parser.add_argument(
        '--fail-on-warning',
        action='store_true',
        help='Exit with error code on warnings'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Check directory exists
    if not args.directory.exists():
        print(f"Error: Directory does not exist: {args.directory}")
        sys.exit(1)

    # Initialize validator
    validator = DatasetValidator()

    # Handle 'all' pipeline option
    if args.pipeline == 'all':
        all_results = {}
        overall_status = 'PASS'

        for pipeline_type in validator.pipeline_configs.keys():
            pipeline_dir = args.directory / pipeline_type
            if pipeline_dir.exists():
                print(f"\nValidating {pipeline_type} pipeline...")
                results = validator.validate_pipeline_output(
                    pipeline_dir,
                    pipeline_type,
                    quick=args.quick
                )
                all_results[pipeline_type] = results

                if results['overall_status'] == 'FAIL':
                    overall_status = 'FAIL'
                elif results['overall_status'] == 'WARNING' and overall_status != 'FAIL':
                    overall_status = 'WARNING'

        # Combine results
        results = {
            'pipeline': 'all',
            'directory': str(args.directory),
            'timestamp': datetime.now().isoformat(),
            'overall_status': overall_status,
            'pipelines': all_results
        }
    else:
        # Validate single pipeline
        results = validator.validate_pipeline_output(
            args.directory,
            args.pipeline,
            quick=args.quick
        )

    # Print summary
    print_validation_summary(results)

    # Save JSON if requested
    if args.json:
        with open(args.json, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to {args.json}")

    # Generate HTML report if requested
    if args.report:
        try:
            from .report_generator import generate_html_report
        except ImportError:
            from validation.report_generator import generate_html_report
        report_file = args.directory / f'validation_report_{args.pipeline}_{datetime.now():%Y%m%d_%H%M%S}.html'
        generate_html_report(results, report_file)
        print(f"\nHTML report generated: {report_file}")

    # Determine exit code
    has_failures = check_for_failures(results)
    has_warnings = check_for_warnings(results)

    if has_failures:
        print("\n❌ VALIDATION FAILED")
        sys.exit(1)
    elif has_warnings and args.fail_on_warning:
        print("\n⚠️  VALIDATION HAS WARNINGS")
        sys.exit(1)
    else:
        print("\n✅ VALIDATION PASSED")
        sys.exit(0)


if __name__ == '__main__':
    main()
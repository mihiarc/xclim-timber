#!/usr/bin/env python3
"""
Integration helper for adding validation to production pipelines.

This module provides functions to easily integrate validation into existing
production scripts and workflows.
"""

import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional, List
import json
import logging

logger = logging.getLogger(__name__)


class ValidationIntegration:
    """Helper class for integrating validation into production pipelines."""

    def __init__(self, fail_fast: bool = True):
        """
        Initialize ValidationIntegration.

        Args:
            fail_fast: If True, stop on first validation failure
        """
        self.fail_fast = fail_fast
        self.validation_results = {}

    def validate_pipeline_output(self,
                                pipeline_type: str,
                                output_dir: Path,
                                quick: bool = False,
                                generate_report: bool = True,
                                save_json: bool = True) -> bool:
        """
        Validate pipeline output with automatic reporting.

        Args:
            pipeline_type: Type of pipeline (temperature, precipitation, etc.)
            output_dir: Directory containing pipeline outputs
            quick: Run quick validation
            generate_report: Generate HTML report
            save_json: Save JSON results

        Returns:
            bool: True if validation passed, False otherwise
        """
        print(f"\n{'='*60}")
        print(f"Validating {pipeline_type} pipeline output")
        print(f"Directory: {output_dir}")
        print(f"{'='*60}\n")

        # Build validation command
        cmd = [
            sys.executable,
            str(Path(__file__).parent / 'validate_dataset.py'),
            str(output_dir),
            '--pipeline', pipeline_type
        ]

        if quick:
            cmd.append('--quick')

        if generate_report:
            cmd.append('--report')

        json_file = None
        if save_json:
            json_file = output_dir / f'validation_{pipeline_type}.json'
            cmd.extend(['--json', str(json_file)])

        # Run validation
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )

            # Parse results if JSON was saved
            if json_file and json_file.exists():
                with open(json_file, 'r') as f:
                    validation_data = json.load(f)
                    self.validation_results[pipeline_type] = validation_data

            # Check result
            if result.returncode == 0:
                print(f"✅ {pipeline_type} validation PASSED")
                return True
            else:
                print(f"❌ {pipeline_type} validation FAILED")
                print(f"Error output: {result.stderr}")
                if self.fail_fast:
                    sys.exit(1)
                return False

        except Exception as e:
            print(f"❌ Error running validation: {str(e)}")
            if self.fail_fast:
                sys.exit(1)
            return False

    def validate_production_run(self,
                               base_dir: Path,
                               pipelines: Optional[List[str]] = None) -> Dict:
        """
        Validate a complete production run with multiple pipelines.

        Args:
            base_dir: Base directory containing pipeline subdirectories
            pipelines: List of pipelines to validate (None = all)

        Returns:
            dict: Summary of validation results
        """
        if pipelines is None:
            pipelines = [
                'temperature', 'precipitation', 'drought',
                'agricultural', 'multivariate', 'humidity', 'human_comfort'
            ]

        results = {
            'passed': [],
            'failed': [],
            'skipped': []
        }

        for pipeline in pipelines:
            pipeline_dir = base_dir / pipeline

            if not pipeline_dir.exists():
                print(f"⚠️  Skipping {pipeline} - directory not found")
                results['skipped'].append(pipeline)
                continue

            success = self.validate_pipeline_output(
                pipeline,
                pipeline_dir,
                quick=False,
                generate_report=True,
                save_json=True
            )

            if success:
                results['passed'].append(pipeline)
            else:
                results['failed'].append(pipeline)

        # Generate summary report
        self._generate_summary_report(base_dir, results)

        return results

    def _generate_summary_report(self, base_dir: Path, results: Dict):
        """Generate a summary report of all validations."""
        print(f"\n{'='*60}")
        print("PRODUCTION VALIDATION SUMMARY")
        print(f"{'='*60}")

        total = len(results['passed']) + len(results['failed']) + len(results['skipped'])

        print(f"\nTotal pipelines: {total}")
        print(f"✅ Passed: {len(results['passed'])} - {results['passed']}")
        print(f"❌ Failed: {len(results['failed'])} - {results['failed']}")
        print(f"⚠️  Skipped: {len(results['skipped'])} - {results['skipped']}")

        # Save summary to file
        summary_file = base_dir / 'validation_summary.json'
        summary_data = {
            'timestamp': str(Path.ctime(Path.cwd())),
            'results': results,
            'detailed_results': self.validation_results
        }

        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2)

        print(f"\nSummary saved to: {summary_file}")

        # Overall status
        if results['failed']:
            print(f"\n❌ PRODUCTION VALIDATION FAILED")
            if self.fail_fast:
                sys.exit(1)
        else:
            print(f"\n✅ PRODUCTION VALIDATION PASSED")

    def add_to_pipeline_script(self, script_path: Path, pipeline_type: str):
        """
        Add validation call to existing pipeline script.

        Args:
            script_path: Path to pipeline script
            pipeline_type: Type of pipeline
        """
        validation_code = f"""

# Automated validation
echo "Running automated validation..."
python3 {Path(__file__).parent}/validate_dataset.py \\
    outputs/production/{pipeline_type}/ \\
    --pipeline {pipeline_type} \\
    --report \\
    --json outputs/production/{pipeline_type}/validation.json \\
    --fail-on-warning

if [ $? -eq 0 ]; then
    echo "✅ Validation passed"
else
    echo "❌ Validation failed"
    exit 1
fi
"""

        # Read existing script
        with open(script_path, 'r') as f:
            content = f.read()

        # Check if validation already added
        if 'automated validation' in content.lower():
            print(f"Validation already present in {script_path}")
            return

        # Add validation before final exit
        if content.strip().endswith('exit 0'):
            content = content.rsplit('exit 0', 1)[0] + validation_code + '\nexit 0'
        else:
            content += validation_code

        # Write updated script
        with open(script_path, 'w') as f:
            f.write(content)

        print(f"✅ Added validation to {script_path}")


def validate_after_pipeline(pipeline_type: str,
                           output_dir: Path,
                           exit_on_failure: bool = True) -> bool:
    """
    Simple function to validate after pipeline completion.

    Args:
        pipeline_type: Type of pipeline that was run
        output_dir: Directory containing outputs
        exit_on_failure: Exit with error code if validation fails

    Returns:
        bool: True if validation passed

    Example:
        # In your pipeline script
        from validation.integrate_validation import validate_after_pipeline

        # After pipeline completes
        validate_after_pipeline('temperature', Path('outputs/production/temperature/'))
    """
    integrator = ValidationIntegration(fail_fast=exit_on_failure)
    return integrator.validate_pipeline_output(
        pipeline_type,
        output_dir,
        quick=False,
        generate_report=True,
        save_json=True
    )


def validate_production_batch(base_dir: Path,
                             pipelines: Optional[List[str]] = None) -> Dict:
    """
    Validate a batch of production pipelines.

    Args:
        base_dir: Base directory containing pipeline outputs
        pipelines: List of pipelines to validate

    Returns:
        dict: Validation results summary

    Example:
        from validation.integrate_validation import validate_production_batch

        results = validate_production_batch(Path('outputs/production/'))
    """
    integrator = ValidationIntegration(fail_fast=False)
    return integrator.validate_production_run(base_dir, pipelines)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Integration helper for pipeline validation'
    )

    parser.add_argument(
        'action',
        choices=['validate', 'batch', 'add'],
        help='Action to perform'
    )

    parser.add_argument(
        '--pipeline',
        help='Pipeline type for single validation'
    )

    parser.add_argument(
        '--directory',
        type=Path,
        help='Directory containing outputs'
    )

    parser.add_argument(
        '--script',
        type=Path,
        help='Script to add validation to'
    )

    args = parser.parse_args()

    if args.action == 'validate':
        if not args.pipeline or not args.directory:
            print("Error: --pipeline and --directory required for validate action")
            sys.exit(1)

        success = validate_after_pipeline(args.pipeline, args.directory)
        sys.exit(0 if success else 1)

    elif args.action == 'batch':
        if not args.directory:
            print("Error: --directory required for batch action")
            sys.exit(1)

        results = validate_production_batch(args.directory)
        sys.exit(0 if not results['failed'] else 1)

    elif args.action == 'add':
        if not args.script or not args.pipeline:
            print("Error: --script and --pipeline required for add action")
            sys.exit(1)

        integrator = ValidationIntegration()
        integrator.add_to_pipeline_script(args.script, args.pipeline)
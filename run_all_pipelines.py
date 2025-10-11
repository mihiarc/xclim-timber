#!/usr/bin/env python3
"""
Master orchestration script for running all climate indices pipelines.
Generates complete production dataset (1981-2024) for all 80 climate indices.

This script runs all 7 pipelines sequentially:
1. Temperature (35 indices)
2. Precipitation (13 indices)
3. Humidity (8 indices)
4. Human Comfort (3 indices)
5. Multivariate (4 indices)
6. Agricultural (5 indices)
7. Drought (12 indices)

Total: 80 climate indices
"""

import subprocess
import logging
import sys
from pathlib import Path
from datetime import datetime
import time
import json

# Setup logging
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = log_dir / f'full_production_run_{timestamp}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates execution of all climate indices pipelines."""

    def __init__(self, start_year: int = 1981, end_year: int = 2024, output_dir: str = './outputs/production'):
        """
        Initialize orchestrator.

        Args:
            start_year: Start year for processing
            end_year: End year for processing
            output_dir: Base output directory
        """
        self.start_year = start_year
        self.end_year = end_year
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Pipeline definitions
        self.pipelines = [
            {
                'name': 'Temperature',
                'script': 'temperature_pipeline.py',
                'indices': 35,
                'output_subdir': 'temperature',
                'description': 'Temperature extremes, degree days, frost metrics, spell frequency'
            },
            {
                'name': 'Precipitation',
                'script': 'precipitation_pipeline.py',
                'indices': 13,
                'output_subdir': 'precipitation',
                'description': 'Precipitation totals, extremes, consecutive events, ETCCDI standards'
            },
            {
                'name': 'Humidity',
                'script': 'humidity_pipeline.py',
                'indices': 8,
                'output_subdir': 'humidity',
                'description': 'Dewpoint statistics, vapor pressure deficit extremes'
            },
            {
                'name': 'Human Comfort',
                'script': 'human_comfort_pipeline.py',
                'indices': 3,
                'output_subdir': 'human_comfort',
                'description': 'Heat index, humidex, relative humidity'
            },
            {
                'name': 'Multivariate',
                'script': 'multivariate_pipeline.py',
                'indices': 4,
                'output_subdir': 'multivariate',
                'description': 'Compound climate extremes (hot/cold × dry/wet)'
            },
            {
                'name': 'Agricultural',
                'script': 'agricultural_pipeline.py',
                'indices': 5,
                'output_subdir': 'agricultural',
                'description': 'Growing season, PET, corn heat units, thawing degree days'
            },
            {
                'name': 'Drought',
                'script': 'drought_pipeline.py',
                'indices': 12,
                'output_subdir': 'drought',
                'description': 'SPI (5 windows), dry spell analysis, precipitation intensity'
            }
        ]

        self.results = []

    def run_pipeline(self, pipeline: dict) -> dict:
        """
        Run a single pipeline.

        Args:
            pipeline: Pipeline configuration dictionary

        Returns:
            Dictionary with execution results
        """
        logger.info("=" * 80)
        logger.info(f"STARTING PIPELINE: {pipeline['name']}")
        logger.info("=" * 80)
        logger.info(f"Description: {pipeline['description']}")
        logger.info(f"Indices: {pipeline['indices']}")
        logger.info(f"Period: {self.start_year}-{self.end_year}")
        logger.info(f"Script: {pipeline['script']}")

        # Create pipeline-specific output directory
        pipeline_output = self.output_dir / pipeline['output_subdir']
        pipeline_output.mkdir(parents=True, exist_ok=True)

        # Build command
        cmd = [
            'python',
            pipeline['script'],
            '--start-year', str(self.start_year),
            '--end-year', str(self.end_year),
            '--output-dir', str(pipeline_output),
            '--verbose'
        ]

        # Execute pipeline
        start_time = time.time()
        logger.info(f"Command: {' '.join(cmd)}")
        logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=7200  # 2 hour timeout per pipeline
            )

            end_time = time.time()
            duration = end_time - start_time

            # Check for success
            success = result.returncode == 0

            if success:
                logger.info(f"✓ Pipeline completed successfully")
            else:
                logger.error(f"✗ Pipeline failed with return code {result.returncode}")
                logger.error(f"STDERR: {result.stderr[-1000:]}")  # Last 1000 chars

            # Get output files
            output_files = list(pipeline_output.glob('*.nc'))
            total_size_mb = sum(f.stat().st_size for f in output_files) / (1024 * 1024)

            # Log results
            logger.info(f"Duration: {duration/60:.1f} minutes")
            logger.info(f"Output files: {len(output_files)}")
            logger.info(f"Total size: {total_size_mb:.2f} MB")

            if output_files:
                logger.info("Generated files:")
                for f in sorted(output_files):
                    size_mb = f.stat().st_size / (1024 * 1024)
                    logger.info(f"  - {f.name} ({size_mb:.2f} MB)")

            return {
                'pipeline': pipeline['name'],
                'success': success,
                'duration_minutes': duration / 60,
                'output_files': len(output_files),
                'total_size_mb': total_size_mb,
                'return_code': result.returncode,
                'indices_count': pipeline['indices']
            }

        except subprocess.TimeoutExpired:
            end_time = time.time()
            duration = end_time - start_time
            logger.error(f"✗ Pipeline timed out after {duration/60:.1f} minutes")
            return {
                'pipeline': pipeline['name'],
                'success': False,
                'duration_minutes': duration / 60,
                'output_files': 0,
                'total_size_mb': 0,
                'return_code': -1,
                'indices_count': pipeline['indices'],
                'error': 'timeout'
            }

        except Exception as e:
            logger.error(f"✗ Pipeline failed with exception: {e}")
            return {
                'pipeline': pipeline['name'],
                'success': False,
                'duration_minutes': 0,
                'output_files': 0,
                'total_size_mb': 0,
                'return_code': -1,
                'indices_count': pipeline['indices'],
                'error': str(e)
            }

    def run_all(self):
        """Run all pipelines in sequence."""
        logger.info("=" * 80)
        logger.info("XCLIM-TIMBER FULL PRODUCTION RUN")
        logger.info("=" * 80)
        logger.info(f"Period: {self.start_year}-{self.end_year}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Total pipelines: {len(self.pipelines)}")
        logger.info(f"Total indices: {sum(p['indices'] for p in self.pipelines)}")
        logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")

        overall_start = time.time()

        # Run each pipeline
        for i, pipeline in enumerate(self.pipelines, 1):
            logger.info(f"\nPIPELINE {i}/{len(self.pipelines)}")
            result = self.run_pipeline(pipeline)
            self.results.append(result)
            logger.info("")

        overall_end = time.time()
        total_duration = overall_end - overall_start

        # Generate summary
        self.generate_summary(total_duration)

        # Save results to JSON
        self.save_results()

        return self.results

    def generate_summary(self, total_duration: float):
        """Generate and log execution summary."""
        logger.info("=" * 80)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 80)

        successful = sum(1 for r in self.results if r['success'])
        failed = len(self.results) - successful

        total_indices = sum(r['indices_count'] for r in self.results if r['success'])
        total_files = sum(r['output_files'] for r in self.results)
        total_size_mb = sum(r['total_size_mb'] for r in self.results)

        logger.info(f"Total duration: {total_duration/3600:.2f} hours ({total_duration/60:.1f} minutes)")
        logger.info(f"Pipelines successful: {successful}/{len(self.pipelines)}")
        logger.info(f"Pipelines failed: {failed}/{len(self.pipelines)}")
        logger.info(f"Indices generated: {total_indices}/80")
        logger.info(f"Output files created: {total_files}")
        logger.info(f"Total data size: {total_size_mb:.2f} MB ({total_size_mb/1024:.2f} GB)")
        logger.info("")

        logger.info("PIPELINE DETAILS:")
        logger.info(f"{'Pipeline':<20} {'Status':<10} {'Duration':<12} {'Files':<8} {'Size (MB)':<12} {'Indices':<10}")
        logger.info("-" * 80)

        for result in self.results:
            status = "✓ SUCCESS" if result['success'] else "✗ FAILED"
            logger.info(
                f"{result['pipeline']:<20} {status:<10} "
                f"{result['duration_minutes']:>10.1f}m "
                f"{result['output_files']:>6} "
                f"{result['total_size_mb']:>10.2f} "
                f"{result['indices_count']:>8}"
            )

        logger.info("")
        logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Log file: {log_file}")

        if failed > 0:
            logger.warning(f"\n⚠ {failed} pipeline(s) failed. Check log for details.")
        else:
            logger.info("\n✓ All pipelines completed successfully! 🎉")

    def save_results(self):
        """Save execution results to JSON file."""
        results_file = self.output_dir / f'execution_results_{timestamp}.json'

        results_data = {
            'timestamp': timestamp,
            'start_year': self.start_year,
            'end_year': self.end_year,
            'output_directory': str(self.output_dir),
            'pipelines': self.results,
            'summary': {
                'total_pipelines': len(self.pipelines),
                'successful': sum(1 for r in self.results if r['success']),
                'failed': sum(1 for r in self.results if not r['success']),
                'total_indices': sum(r['indices_count'] for r in self.results if r['success']),
                'total_files': sum(r['output_files'] for r in self.results),
                'total_size_mb': sum(r['total_size_mb'] for r in self.results)
            }
        }

        with open(results_file, 'w') as f:
            json.dump(results_data, f, indent=2)

        logger.info(f"\nResults saved to: {results_file}")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run all climate indices pipelines over full time series",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This orchestration script runs all 7 climate indices pipelines:
  1. Temperature (35 indices)
  2. Precipitation (13 indices)
  3. Humidity (8 indices)
  4. Human Comfort (3 indices)
  5. Multivariate (4 indices)
  6. Agricultural (5 indices)
  7. Drought (12 indices)

Total: 80 climate indices

Expected runtime: 4-8 hours for full 1981-2024 period
Expected output size: 5-15 GB

Examples:
  # Full production run (1981-2024)
  python run_all_pipelines.py

  # Custom period
  python run_all_pipelines.py --start-year 2000 --end-year 2020

  # Custom output directory
  python run_all_pipelines.py --output-dir /path/to/output
        """
    )

    parser.add_argument(
        '--start-year',
        type=int,
        default=1981,
        help='Start year for processing (default: 1981)'
    )

    parser.add_argument(
        '--end-year',
        type=int,
        default=2024,
        help='End year for processing (default: 2024)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='./outputs/production',
        help='Base output directory (default: ./outputs/production)'
    )

    args = parser.parse_args()

    # Create orchestrator
    orchestrator = PipelineOrchestrator(
        start_year=args.start_year,
        end_year=args.end_year,
        output_dir=args.output_dir
    )

    # Run all pipelines
    try:
        results = orchestrator.run_all()

        # Exit with error code if any pipeline failed
        if any(not r['success'] for r in results):
            sys.exit(1)
        else:
            sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("\n\nExecution interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

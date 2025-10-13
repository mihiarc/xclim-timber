#!/usr/bin/env python3
"""
Re-run only the failed pipelines with 1-year chunks for maximum memory efficiency.

Failed pipelines to re-process:
1. Temperature (35 indices) - OOM after 1st chunk
2. Precipitation (13 indices) - OOM after 2nd chunk
3. Multivariate (4 indices) - OOM after 2nd chunk
4. Drought (12 indices) - Timed out after 2 hours

Successful pipelines (keeping existing results):
- Humidity (8 indices) - 11 files, 327 MB ✅
- Human Comfort (3 indices) - 11 files, 154 MB ✅
- Agricultural (5 indices) - 11 files, 312 MB ✅
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
log_file = log_dir / f'failed_pipelines_run_{timestamp}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class FailedPipelinesRunner:
    """Run only the failed pipelines with optimized settings."""

    def __init__(self, start_year: int = 1981, end_year: int = 2024, output_dir: str = './outputs/production'):
        self.start_year = start_year
        self.end_year = end_year
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Only failed pipelines
        self.pipelines = [
            {
                'name': 'Temperature',
                'script': 'temperature_pipeline.py',
                'indices': 35,
                'output_subdir': 'temperature',
                'description': 'Temperature extremes, degree days, frost metrics, spell frequency',
                'timeout': 10800  # 3 hours (44 years × ~4 min/year)
            },
            {
                'name': 'Precipitation',
                'script': 'precipitation_pipeline.py',
                'indices': 13,
                'output_subdir': 'precipitation',
                'description': 'Precipitation totals, extremes, consecutive events, ETCCDI standards',
                'timeout': 7200  # 2 hours
            },
            {
                'name': 'Multivariate',
                'script': 'multivariate_pipeline.py',
                'indices': 4,
                'output_subdir': 'multivariate',
                'description': 'Compound climate extremes (hot/cold × dry/wet)',
                'timeout': 10800  # 3 hours (needs baseline comparisons)
            },
            {
                'name': 'Drought',
                'script': 'drought_pipeline.py',
                'indices': 12,
                'output_subdir': 'drought',
                'description': 'SPI (5 windows), dry spell analysis, precipitation intensity',
                'timeout': 14400  # 4 hours (SPI gamma fitting is intensive)
            }
        ]

        self.results = []

    def run_pipeline(self, pipeline: dict) -> dict:
        """Run a single pipeline."""
        logger.info("=" * 80)
        logger.info(f"STARTING PIPELINE: {pipeline['name']}")
        logger.info("=" * 80)
        logger.info(f"Description: {pipeline['description']}")
        logger.info(f"Indices: {pipeline['indices']}")
        logger.info(f"Period: {self.start_year}-{self.end_year}")
        logger.info(f"Timeout: {pipeline['timeout']/60:.0f} minutes")
        logger.info(f"Chunk size: 1 year (memory optimized)")

        pipeline_output = self.output_dir / pipeline['output_subdir']
        pipeline_output.mkdir(parents=True, exist_ok=True)

        cmd = [
            'python',
            pipeline['script'],
            '--start-year', str(self.start_year),
            '--end-year', str(self.end_year),
            '--output-dir', str(pipeline_output),
            '--verbose'
        ]

        start_time = time.time()
        logger.info(f"Command: {' '.join(cmd)}")
        logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=pipeline['timeout']
            )

            end_time = time.time()
            duration = end_time - start_time
            success = result.returncode == 0

            if success:
                logger.info(f"✓ Pipeline completed successfully")
            else:
                logger.error(f"✗ Pipeline failed with return code {result.returncode}")
                if result.stderr:
                    logger.error(f"STDERR: {result.stderr[-1000:]}")

            output_files = list(pipeline_output.glob('*.nc'))
            total_size_mb = sum(f.stat().st_size for f in output_files) / (1024 * 1024)

            logger.info(f"Duration: {duration/60:.1f} minutes")
            logger.info(f"Output files: {len(output_files)}")
            logger.info(f"Total size: {total_size_mb:.2f} MB")

            if output_files:
                logger.info("Generated files (latest 10):")
                for f in sorted(output_files)[-10:]:
                    size_mb = f.stat().st_size / (1024 * 1024)
                    logger.info(f"  - {f.name} ({size_mb:.2f} MB)")
                if len(output_files) > 10:
                    logger.info(f"  ... and {len(output_files)-10} more files")

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
        """Run all failed pipelines sequentially."""
        logger.info("=" * 80)
        logger.info("RE-RUNNING FAILED PIPELINES WITH 1-YEAR CHUNKS")
        logger.info("=" * 80)
        logger.info(f"Period: {self.start_year}-{self.end_year} (44 years)")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Pipelines to re-run: {len(self.pipelines)}")
        logger.info(f"Total indices: {sum(p['indices'] for p in self.pipelines)}")
        logger.info(f"Chunk size: 1 year per file (44 files per pipeline)")
        logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")
        logger.info("Note: Keeping existing successful results:")
        logger.info("  ✓ Humidity (8 indices)")
        logger.info("  ✓ Human Comfort (3 indices)")
        logger.info("  ✓ Agricultural (5 indices)")
        logger.info("")

        overall_start = time.time()

        for i, pipeline in enumerate(self.pipelines, 1):
            logger.info(f"\nPIPELINE {i}/{len(self.pipelines)}")
            result = self.run_pipeline(pipeline)
            self.results.append(result)
            logger.info("")

        overall_end = time.time()
        total_duration = overall_end - overall_start

        self.generate_summary(total_duration)
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

        # Add successful pipelines that were skipped
        kept_indices = 8 + 3 + 5  # Humidity + Human Comfort + Agricultural
        kept_files = 11 + 11 + 11  # 33 files
        kept_size_mb = 327.48 + 153.90 + 312.25  # ~793 MB

        logger.info(f"Total duration: {total_duration/3600:.2f} hours ({total_duration/60:.1f} minutes)")
        logger.info(f"Failed pipelines re-run: {successful}/{len(self.pipelines)} successful")
        logger.info(f"Failed pipelines still failing: {failed}/{len(self.pipelines)}")
        logger.info("")
        logger.info(f"New indices generated: {total_indices}/64")
        logger.info(f"Kept from previous run: {kept_indices}/16")
        logger.info(f"TOTAL INDICES: {total_indices + kept_indices}/80")
        logger.info("")
        logger.info(f"New output files: {total_files}")
        logger.info(f"Kept from previous run: {kept_files}")
        logger.info(f"TOTAL FILES: {total_files + kept_files}")
        logger.info("")
        logger.info(f"New data size: {total_size_mb:.2f} MB")
        logger.info(f"Kept from previous run: {kept_size_mb:.2f} MB")
        logger.info(f"TOTAL SIZE: {(total_size_mb + kept_size_mb)/1024:.2f} GB")
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
            logger.warning(f"\n⚠ {failed} pipeline(s) still failed. Check log for details.")
        else:
            logger.info("\n✓ All failed pipelines now completed successfully! 🎉")
            logger.info(f"✓ Full dataset complete: 80/80 indices across {total_files + kept_files} files")

    def save_results(self):
        """Save execution results to JSON file."""
        results_file = self.output_dir / f'failed_pipelines_results_{timestamp}.json'

        results_data = {
            'timestamp': timestamp,
            'start_year': self.start_year,
            'end_year': self.end_year,
            'output_directory': str(self.output_dir),
            'pipelines_rerun': self.results,
            'summary': {
                'failed_pipelines_attempted': len(self.pipelines),
                'now_successful': sum(1 for r in self.results if r['success']),
                'still_failed': sum(1 for r in self.results if not r['success']),
                'total_new_files': sum(r['output_files'] for r in self.results),
                'total_new_size_mb': sum(r['total_size_mb'] for r in self.results)
            }
        }

        with open(results_file, 'w') as f:
            json.dump(results_data, f, indent=2)

        logger.info(f"\nResults saved to: {results_file}")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Re-run failed pipelines with 1-year chunks for memory efficiency"
    )

    parser.add_argument('--start-year', type=int, default=1981)
    parser.add_argument('--end-year', type=int, default=2024)
    parser.add_argument('--output-dir', type=str, default='./outputs/production')

    args = parser.parse_args()

    runner = FailedPipelinesRunner(
        start_year=args.start_year,
        end_year=args.end_year,
        output_dir=args.output_dir
    )

    try:
        results = runner.run_all()

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

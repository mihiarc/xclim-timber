#!/usr/bin/env python3
"""
Simple pipeline runner for xclim-timber.
A clean entry point for processing PRISM climate data and calculating indices.
"""

import argparse
import logging
import sys
import yaml
from pathlib import Path
from typing import Optional, List
from datetime import datetime

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from pipeline_streaming import StreamingClimatePipeline


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def process_climate_data(
    config_path: Optional[str] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    variables: Optional[List[str]] = None,
    output_dir: Optional[str] = None,
    verbose: bool = False
) -> None:
    """
    Main processing function for climate data.

    Args:
        config_path: Path to configuration file
        start_year: Start year for processing
        end_year: End year for processing
        variables: List of variables to process (tas, pr, etc.)
        output_dir: Output directory for results
        verbose: Enable verbose logging
    """
    setup_logging(verbose)
    logger = logging.getLogger(__name__)

    logger.info("Starting xclim-timber pipeline")
    logger.info(f"Processing years: {start_year or 'all'} to {end_year or 'all'}")

    # Set output directory
    if output_dir:
        output_path = Path(output_dir)
    else:
        output_path = Path('./outputs')
    output_path.mkdir(parents=True, exist_ok=True)

    # Fixed PRISM data configuration - no need for config files
    prism_config = {
        'zarr_stores': {
            'temperature': '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature',
            'precipitation': '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation',
            'humidity': '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/humidity'
        },
        'baseline_path': '/home/mihiarc/xclim-timber/data/baselines/baseline_percentiles_1981_2000.nc',
        'output_path': str(output_path),
        'dask': {
            'n_workers': 4,
            'memory_limit': '4GB',
            'dashboard': True
        }
    }

    # Initialize streaming pipeline
    logger.info("Initializing streaming pipeline...")
    logger.info(f"Output directory: {output_path}")

    # If user provided config, use it; otherwise use hardcoded config
    if config_path:
        logger.info(f"Using provided config: {config_path}")
        pipeline = StreamingClimatePipeline(
            config_path=config_path,
            chunk_years=10,
            enable_dashboard=True
        )
    else:
        # Create temporary config file with our fixed settings
        # (needed because StreamingClimatePipeline expects a file)
        temp_config_path = Path.home() / '.xclim-timber' / 'runtime_config.yaml'
        temp_config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(temp_config_path, 'w') as f:
            yaml.dump({
                'data': {
                    'input_path': '/media/mihiarc/SSD4TB/data/PRISM',
                    'output_path': prism_config['output_path'],
                    'baseline_path': prism_config['baseline_path'],
                    'zarr_stores': prism_config['zarr_stores']
                },
                'processing': {
                    'dask': prism_config['dask'],
                    'baseline': {
                        'period': [1981, 2000],
                        'percentiles': [10, 90, 95, 99]
                    }
                }
            }, f)

        pipeline = StreamingClimatePipeline(
            config_path=str(temp_config_path),
            chunk_years=10,
            enable_dashboard=True
        )

    # Log dashboard URL for monitoring
    logger.info("=" * 60)
    logger.info("Dask Dashboard available at: http://localhost:8787")
    logger.info("Open this URL in your browser to monitor pipeline progress")
    logger.info("=" * 60)

    try:
        # Use year chunking for memory efficiency
        if not start_year:
            start_year = 2001  # Default start for analysis period
        if not end_year:
            end_year = 2024  # Current PRISM end

        # Determine which variables to process
        pipeline_variables = variables or ['temperature', 'precipitation']

        logger.info(f"Processing years {start_year} to {end_year}")
        logger.info(f"Variables: {pipeline_variables}")

        # Process using the streaming pipeline
        results = pipeline.run_streaming(
            variables=pipeline_variables,
            start_year=start_year,
            end_year=end_year
        )

        logger.info(f"✓ Processing complete!")
        logger.info(f"  Status: {results.get('status', 'unknown')}")
        logger.info(f"  Chunks processed: {results.get('chunks_processed', 0)}")
        if 'output_path' in results:
            logger.info(f"  Output: {results['output_path']}")

    except Exception as e:
        logger.error(f"Error in processing: {str(e)}")
        if verbose:
            logger.exception("Full traceback:")
        raise
    finally:
        # Clean up pipeline resources
        pipeline.close()

    logger.info("\n✓ Pipeline complete!")


def main():
    """Main entry point with command-line interface."""
    parser = argparse.ArgumentParser(
        description="xclim-timber: Streamlined Climate Data Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process default variables (tas, pr) for all years
  python run_pipeline.py

  # Process specific years
  python run_pipeline.py --start-year 2000 --end-year 2010

  # Process specific variables
  python run_pipeline.py --variables tas tasmax pr

  # Use custom config and output directory
  python run_pipeline.py --config myconfig.yaml --output-dir ./results

  # Enable verbose logging
  python run_pipeline.py --verbose
        """
    )

    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration file (defaults to configs/config_comprehensive_2001_2024.yaml)'
    )

    parser.add_argument(
        '--start-year',
        type=int,
        help='Start year for processing'
    )

    parser.add_argument(
        '--end-year',
        type=int,
        help='End year for processing'
    )

    parser.add_argument(
        '--variables',
        nargs='+',
        help='Variables to process (tas, tasmax, tasmin, pr, etc.)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory for results'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='xclim-timber 1.0.0'
    )

    args = parser.parse_args()

    try:
        process_climate_data(
            config_path=args.config,
            start_year=args.start_year,
            end_year=args.end_year,
            variables=args.variables,
            output_dir=args.output_dir,
            verbose=args.verbose
        )
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
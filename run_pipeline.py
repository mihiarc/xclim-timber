#!/usr/bin/env python3
"""
Simple pipeline runner for xclim-timber.
A clean entry point for processing PRISM climate data and calculating indices.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from config import Config
from pipeline_streaming import StreamingClimatePipeline
import xarray as xr


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

    # Load configuration
    config = Config(config_path)

    # Override output directory if specified
    if output_dir:
        # Don't modify config directly, pass to pipeline
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
    else:
        output_path = config.output_path

    # Initialize streaming pipeline
    logger.info("Initializing streaming pipeline...")

    # Create temporary config if none provided
    if not config_path:
        # Create a minimal config for the streaming pipeline
        config_path = Path.home() / '.xclim-timber' / 'temp_config.yaml'
        config_path.parent.mkdir(parents=True, exist_ok=True)

        # Write minimal config
        with open(config_path, 'w') as f:
            f.write(f"""
data:
  output_path: {output_path}

processing:
  dask:
    n_workers: 4
    memory_limit: '4GB'
    dashboard: false
""")

    pipeline = StreamingClimatePipeline(
        config_path=str(config_path),
        chunk_years=1,  # Process 1 year at a time for memory efficiency
        enable_dashboard=False  # Disable for cleaner output
    )

    try:
        # Use year chunking for memory efficiency
        if not start_year:
            start_year = 1981  # PRISM default start
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
        help='Path to configuration file (optional)'
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
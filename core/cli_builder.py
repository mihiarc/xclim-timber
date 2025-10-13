#!/usr/bin/env python3
"""
CLI builder for xclim-timber pipelines.

Provides consistent command-line interfaces across all pipelines.
"""

import argparse
import logging
import warnings
from typing import Optional

from core.config import PipelineConfig

logger = logging.getLogger(__name__)


class PipelineCLI:
    """
    Build consistent CLI interfaces for all climate index pipelines.

    Provides:
    - Standard argument parsing
    - Common arguments (years, output, verbosity)
    - Logging configuration
    - Warning filter setup
    """

    @staticmethod
    def create_parser(
        pipeline_name: str,
        description: str,
        indices_list: str,
        examples: Optional[str] = None
    ) -> argparse.ArgumentParser:
        """
        Create standardized argument parser for a pipeline.

        Args:
            pipeline_name: Name of the pipeline (e.g., "Temperature Indices")
            description: Brief description of what the pipeline does
            indices_list: Formatted list of indices calculated
            examples: Optional usage examples

        Returns:
            Configured ArgumentParser
        """
        epilog = f"""
Indices calculated:
{indices_list}

Examples:
  # Process default period ({PipelineConfig.DEFAULT_START_YEAR}-{PipelineConfig.DEFAULT_END_YEAR})
  python {pipeline_name.lower().replace(' ', '_')}_pipeline.py

  # Process single year
  python {pipeline_name.lower().replace(' ', '_')}_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python {pipeline_name.lower().replace(' ', '_')}_pipeline.py --output-dir ./results
"""
        if examples:
            epilog += f"\n{examples}"

        parser = argparse.ArgumentParser(
            description=f"{pipeline_name} Pipeline: {description}",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog=epilog
        )

        PipelineCLI.add_common_arguments(parser)

        return parser

    @staticmethod
    def add_common_arguments(parser: argparse.ArgumentParser):
        """
        Add standard arguments to parser.

        Args:
            parser: ArgumentParser to add arguments to
        """
        parser.add_argument(
            '--start-year',
            type=int,
            default=PipelineConfig.DEFAULT_START_YEAR,
            help=f'Start year for processing (default: {PipelineConfig.DEFAULT_START_YEAR})'
        )

        parser.add_argument(
            '--end-year',
            type=int,
            default=PipelineConfig.DEFAULT_END_YEAR,
            help=f'End year for processing (default: {PipelineConfig.DEFAULT_END_YEAR})'
        )

        parser.add_argument(
            '--output-dir',
            type=str,
            default=PipelineConfig.DEFAULT_OUTPUT_DIR,
            help=f'Output directory for results (default: {PipelineConfig.DEFAULT_OUTPUT_DIR})'
        )

        parser.add_argument(
            '--chunk-years',
            type=int,
            default=PipelineConfig.DEFAULT_CHUNK_YEARS,
            help=f'Number of years to process per chunk (default: {PipelineConfig.DEFAULT_CHUNK_YEARS} for memory efficiency)'
        )

        parser.add_argument(
            '--dashboard',
            action='store_true',
            help='Enable Dask dashboard on port 8787 (currently unused, threaded scheduler only)'
        )

        parser.add_argument(
            '--verbose', '-v',
            action='store_true',
            help='Enable verbose logging'
        )

        parser.add_argument(
            '--show-warnings',
            action='store_true',
            help='Show all warnings (default: suppressed)'
        )

    @staticmethod
    def setup_logging(verbose: bool = False):
        """
        Configure logging with standard format.

        Args:
            verbose: If True, set logging level to DEBUG
        """
        level = logging.DEBUG if verbose else logging.INFO

        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    @staticmethod
    def setup_warnings(show_warnings: bool = False):
        """
        Configure warning filters.

        Args:
            show_warnings: If True, show all warnings; otherwise apply filters
        """
        if show_warnings:
            warnings.resetwarnings()
            logger.info("Warnings enabled")
        else:
            PipelineConfig.setup_warning_filters()

    @staticmethod
    def handle_common_setup(args: argparse.Namespace):
        """
        Handle common setup based on parsed arguments.

        Args:
            args: Parsed command-line arguments
        """
        PipelineCLI.setup_logging(args.verbose)
        PipelineCLI.setup_warnings(args.show_warnings)

    @staticmethod
    def validate_years(start_year: int, end_year: int):
        """
        Validate year range.

        Args:
            start_year: Start year
            end_year: End year

        Raises:
            ValueError: If years are invalid
        """
        if start_year > end_year:
            raise ValueError(f"Start year ({start_year}) must be <= end year ({end_year})")

        if start_year < 1981:
            logger.warning(f"Start year {start_year} is before PRISM data availability (1981)")

        if end_year > 2024:
            logger.warning(f"End year {end_year} may be beyond current PRISM data availability")

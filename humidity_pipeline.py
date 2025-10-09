#!/usr/bin/env python3
"""
Humidity indices pipeline for xclim-timber.
Efficiently processes humidity-based climate indices using Zarr streaming.
Calculates 8 humidity indices including vapor pressure deficit, dewpoint, and moisture stress metrics.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional
import warnings
import xarray as xr
import xclim.indicators.atmos as atmos
import xclim.indices as indices
from dask.distributed import Client
import dask
import psutil
import os
from datetime import datetime
import numpy as np

# Suppress common warnings that don't affect functionality
warnings.filterwarnings('ignore', category=UserWarning, message='.*cell_methods.*')
warnings.filterwarnings('ignore', category=UserWarning, message='.*specified chunks.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*All-NaN slice.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*divide.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*invalid value.*')
warnings.filterwarnings('ignore', category=FutureWarning, message='.*return type of.*Dataset.dims.*')

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class HumidityPipeline:
    """
    Memory-efficient humidity indices pipeline using Zarr streaming.
    Processes 8 humidity indices without loading full dataset into memory.
    """

    def __init__(self, chunk_years: int = 12, enable_dashboard: bool = False):
        """
        Initialize the pipeline.

        Args:
            chunk_years: Number of years to process in each temporal chunk
            enable_dashboard: Whether to enable Dask dashboard
        """
        self.chunk_years = chunk_years
        self.enable_dashboard = enable_dashboard
        self.client = None

        # Zarr store path for humidity data
        self.zarr_store = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/humidity'

        # Optimal chunk configuration (aligned with dimensions)
        self.chunk_config = {
            'time': 365,  # One year of daily data
            'lat': 69,    # 621 / 69 = 9 even chunks
            'lon': 281    # 1405 / 281 = 5 even chunks
        }

    def setup_dask_client(self):
        """Initialize Dask client with memory limits."""
        if self.client is None:
            dashboard_address = ':8787' if self.enable_dashboard else None
            self.client = Client(
                n_workers=2,
                threads_per_worker=1,
                memory_limit='2GB',  # Strict memory limit per worker
                dashboard_address=dashboard_address,
                silence_logs=logging.ERROR
            )
            if self.enable_dashboard:
                logger.info(f"Dask dashboard available at: http://localhost:8787")
            logger.info(f"Dask client initialized with 2 workers, 2GB each")

    def close(self):
        """Clean up resources."""
        if self.client:
            self.client.close()
            self.client = None

    def calculate_humidity_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate humidity-based climate indices.

        Args:
            ds: Dataset with humidity variables (tdew, vpdmax, vpdmin)

        Returns:
            Dictionary of calculated indices
        """
        indices_dict = {}

        # Dewpoint temperature statistics
        if 'tdew' in ds:
            logger.info("  - Calculating annual mean dewpoint temperature...")
            indices_dict['dewpoint_mean'] = ds.tdew.groupby('time.year').mean(dim='time')

            logger.info("  - Calculating annual minimum dewpoint temperature...")
            indices_dict['dewpoint_min'] = ds.tdew.groupby('time.year').min(dim='time')

            logger.info("  - Calculating annual maximum dewpoint temperature...")
            indices_dict['dewpoint_max'] = ds.tdew.groupby('time.year').max(dim='time')

            # Days with high humidity (dewpoint > 18°C indicates uncomfortable humidity)
            logger.info("  - Calculating humid days (dewpoint > 18°C)...")
            humid_threshold = 18.0  # degrees C
            humid_days = (ds.tdew > humid_threshold).groupby('time.year').sum(dim='time')
            indices_dict['humid_days'] = humid_days

        # Vapor pressure deficit statistics
        if 'vpdmax' in ds:
            logger.info("  - Calculating annual mean maximum VPD...")
            indices_dict['vpdmax_mean'] = ds.vpdmax.groupby('time.year').mean(dim='time')

            logger.info("  - Calculating extreme VPD days (>4 kPa)...")
            # High VPD indicates water stress for plants
            extreme_vpd_threshold = 4.0  # kPa
            extreme_vpd_days = (ds.vpdmax > extreme_vpd_threshold).groupby('time.year').sum(dim='time')
            indices_dict['extreme_vpd_days'] = extreme_vpd_days

        if 'vpdmin' in ds:
            logger.info("  - Calculating annual mean minimum VPD...")
            indices_dict['vpdmin_mean'] = ds.vpdmin.groupby('time.year').mean(dim='time')

            # Low VPD days (vpdmin < 0.5 kPa indicates high moisture/fog potential)
            logger.info("  - Calculating low VPD days (<0.5 kPa)...")
            low_vpd_threshold = 0.5  # kPa
            low_vpd_days = (ds.vpdmin < low_vpd_threshold).groupby('time.year').sum(dim='time')
            indices_dict['low_vpd_days'] = low_vpd_days

        # Add proper metadata to all indices
        for key, data_array in indices_dict.items():
            if 'dewpoint' in key:
                data_array.attrs['units'] = 'degC'
                data_array.attrs['standard_name'] = 'dew_point_temperature'
            elif 'vpd' in key:
                if 'days' in key:
                    data_array.attrs['units'] = 'days'
                    data_array.attrs['standard_name'] = 'number_of_days'
                else:
                    data_array.attrs['units'] = 'kPa'
                    data_array.attrs['standard_name'] = 'vapor_pressure_deficit'
            elif 'humid_days' in key:
                data_array.attrs['units'] = 'days'
                data_array.attrs['standard_name'] = 'number_of_days_with_high_humidity'

        return indices_dict


    def process_time_chunk(
        self,
        start_year: int,
        end_year: int,
        output_dir: Path
    ) -> Path:
        """
        Process a single time chunk.

        Args:
            start_year: Start year for this chunk
            end_year: End year for this chunk
            output_dir: Output directory

        Returns:
            Path to output file
        """
        logger.info(f"\nProcessing chunk: {start_year}-{end_year}")

        # Track memory
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"Initial memory: {initial_memory:.1f} MB")

        # Load humidity data
        logger.info("Loading humidity data...")
        ds = xr.open_zarr(self.zarr_store, chunks=self.chunk_config)

        # Select time range
        combined_ds = ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

        # Rename humidity variables for consistency (if needed)
        rename_map = {
            'tdmean': 'tdew',  # PRISM uses 'tdmean' for dewpoint temperature
            'vpdmax': 'vpdmax',  # Already correct
            'vpdmin': 'vpdmin'   # Already correct
        }

        for old_name, new_name in rename_map.items():
            if old_name in combined_ds and old_name != new_name:
                combined_ds = combined_ds.rename({old_name: new_name})
                logger.debug(f"Renamed {old_name} to {new_name}")

        # Fix units for humidity variables
        unit_fixes = {
            'tdew': 'degC',     # Dewpoint temperature in Celsius
            'vpdmax': 'kPa',    # Vapor pressure deficit in kilopascals
            'vpdmin': 'kPa'     # Vapor pressure deficit in kilopascals
        }

        for var_name, unit in unit_fixes.items():
            if var_name in combined_ds:
                combined_ds[var_name].attrs['units'] = unit
                combined_ds[var_name].attrs['standard_name'] = self._get_standard_name(var_name)

        # Calculate humidity indices
        logger.info("Calculating humidity indices...")
        all_indices = self.calculate_humidity_indices(combined_ds)
        logger.info(f"  Calculated {len(all_indices)} humidity indices")

        if not all_indices:
            logger.warning("No indices calculated")
            return None

        # Combine indices into dataset
        logger.info(f"Combining {len(all_indices)} indices into dataset...")
        result_ds = xr.Dataset(all_indices)

        # Add metadata
        result_ds.attrs['creation_date'] = datetime.now().isoformat()
        result_ds.attrs['software'] = 'xclim-timber humidity pipeline v1.0'
        result_ds.attrs['time_range'] = f'{start_year}-{end_year}'
        result_ds.attrs['indices_count'] = len(all_indices)

        # Save output
        output_file = output_dir / f'humidity_indices_{start_year}_{end_year}.nc'
        logger.info(f"Saving to {output_file}...")

        with dask.config.set(scheduler='threads'):
            encoding = {}
            for var_name in result_ds.data_vars:
                encoding[var_name] = {
                    'zlib': True,
                    'complevel': 4,
                    'chunksizes': (1, 69, 281)  # Aligned chunks for storage
                }

            result_ds.to_netcdf(
                output_file,
                engine='netcdf4',
                encoding=encoding
            )

        # Report memory usage
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"Final memory: {final_memory:.1f} MB (increase: {final_memory - initial_memory:.1f} MB)")

        # Report file size
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        logger.info(f"Output file size: {file_size_mb:.2f} MB")

        return output_file

    def _get_standard_name(self, var_name: str) -> str:
        """Get CF-compliant standard name for variable."""
        standard_names = {
            'tdew': 'dew_point_temperature',
            'vpdmax': 'vapor_pressure_deficit',
            'vpdmin': 'vapor_pressure_deficit'
        }
        return standard_names.get(var_name, '')

    def run(
        self,
        start_year: int,
        end_year: int,
        output_dir: str = './outputs'
    ) -> List[Path]:
        """
        Run the pipeline for specified years.

        Args:
            start_year: Start year
            end_year: End year
            output_dir: Output directory path

        Returns:
            List of output file paths
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info("=" * 60)
        logger.info("HUMIDITY INDICES PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Period: {start_year}-{end_year}")
        logger.info(f"Output: {output_path}")
        logger.info(f"Chunk size: {self.chunk_years} years")

        # Setup Dask
        self.setup_dask_client()

        output_files = []

        try:
            # Process in temporal chunks
            current_year = start_year
            while current_year <= end_year:
                chunk_end = min(current_year + self.chunk_years - 1, end_year)

                output_file = self.process_time_chunk(
                    current_year,
                    chunk_end,
                    output_path
                )

                if output_file:
                    output_files.append(output_file)

                current_year = chunk_end + 1

            logger.info("=" * 60)
            logger.info(f"✓ Pipeline complete! Generated {len(output_files)} files")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.close()

        return output_files


def main():
    """Main entry point with command-line interface."""
    parser = argparse.ArgumentParser(
        description="Humidity Indices Pipeline: Calculate 8 humidity-based climate indices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process default period (1981-2024)
  python humidity_pipeline.py

  # Process single year
  python humidity_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python humidity_pipeline.py --output-dir ./results
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
        default='./outputs',
        help='Output directory for results (default: ./outputs)'
    )

    parser.add_argument(
        '--chunk-years',
        type=int,
        default=12,
        help='Number of years to process per chunk (default: 12)'
    )

    parser.add_argument(
        '--dashboard',
        action='store_true',
        help='Enable Dask dashboard on port 8787'
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

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Re-enable warnings if requested
    if args.show_warnings:
        warnings.resetwarnings()
        logger.info("Warnings enabled")

    # Create and run pipeline
    pipeline = HumidityPipeline(
        chunk_years=args.chunk_years,
        enable_dashboard=args.dashboard
    )

    try:
        output_files = pipeline.run(
            start_year=args.start_year,
            end_year=args.end_year,
            output_dir=args.output_dir
        )

        if output_files:
            print(f"\n✓ Successfully generated {len(output_files)} output files:")
            for f in output_files:
                print(f"  - {f}")
        else:
            print("\n✗ No output files generated")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
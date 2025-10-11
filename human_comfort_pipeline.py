#!/usr/bin/env python3
"""
Human comfort indices pipeline for xclim-timber.
Efficiently processes human comfort climate indices using Zarr streaming.
Calculates 3 comfort indices combining temperature and humidity data.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional
import warnings
import xarray as xr
import xclim.indicators.atmos as atmos
from dask.distributed import Client
import dask
import psutil
import os
from datetime import datetime

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


class HumanComfortPipeline:
    """
    Memory-efficient human comfort indices pipeline using Zarr streaming.
    Processes 3 comfort indices combining temperature and humidity without loading full dataset into memory.
    """

    def __init__(self, chunk_years: int = 4, enable_dashboard: bool = False):
        """
        Initialize the pipeline.

        Args:
            chunk_years: Number of years to process in each temporal chunk
            enable_dashboard: Whether to enable Dask dashboard
        """
        self.chunk_years = chunk_years
        self.enable_dashboard = enable_dashboard
        self.client = None

        # Zarr store paths
        self.temp_zarr_store = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'
        self.humid_zarr_store = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/humidity'

        # Optimal chunk configuration (aligned with dimensions)
        self.chunk_config = {
            'time': 365,  # One year of daily data
            'lat': 103,   # 621 / 103 = 6 chunks (smaller for less memory)
            'lon': 201    # 1405 / 201 = 7 chunks (smaller for less memory)
        }

    def setup_dask_client(self):
        """Initialize Dask client with memory limits."""
        # Use threaded scheduler instead of distributed for lower memory overhead
        logger.info("Using Dask threaded scheduler (no distributed client for memory efficiency)")

    def close(self):
        """Clean up resources."""
        if self.client:
            self.client.close()
            self.client = None

    def calculate_comfort_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate human comfort climate indices.

        Args:
            ds: Dataset with temperature and humidity variables (tas, tdew)

        Returns:
            Dictionary of calculated indices

        Note:
            Heat stress indices use annual MAXIMUM (not mean) to capture
            worst-case conditions following WMO standards for heat stress assessment.
        """
        indices = {}

        # Calculate relative humidity from dewpoint (needed for heat_index)
        if 'tas' in ds and 'tdew' in ds:
            logger.info("  - Calculating relative humidity from dewpoint...")
            # relative_humidity_from_dewpoint returns instantaneous values
            # We need to calculate it first, then aggregate to annual
            rh = atmos.relative_humidity_from_dewpoint(
                tas=ds.tas,
                tdps=ds.tdew
            )

            # Add to dataset for use in other indices
            ds['hurs'] = rh

            # Calculate annual maximum for output (extreme conditions)
            # Note: Using max instead of mean for heat stress assessment
            indices['relative_humidity'] = rh.resample(time='YS').max()

        # Heat index (requires temperature and relative humidity)
        if 'tas' in ds and 'hurs' in ds:
            logger.info("  - Calculating heat index...")
            # heat_index returns instantaneous values
            # Resample to annual MAXIMUM (worst-case heat stress per year)
            # This follows WMO standards for heat stress indices
            heat_idx = atmos.heat_index(
                tas=ds.tas,
                hurs=ds.hurs
            )
            indices['heat_index'] = heat_idx.resample(time='YS').max()

        # Humidex (Canadian index, requires temperature and dewpoint)
        if 'tas' in ds and 'tdew' in ds:
            logger.info("  - Calculating humidex...")
            # humidex returns instantaneous values
            # Resample to annual MAXIMUM (worst-case heat stress per year)
            # This follows Canadian heat stress standards
            hmidx = atmos.humidex(
                tas=ds.tas,
                tdps=ds.tdew
            )
            indices['humidex'] = hmidx.resample(time='YS').max()

        return indices

    def _validate_coordinates(self, ds1: xr.Dataset, ds2: xr.Dataset, coord_names: list):
        """
        Validate that two datasets have matching coordinates.

        Args:
            ds1: First dataset
            ds2: Second dataset
            coord_names: List of coordinate names to validate

        Raises:
            ValueError: If coordinates don't match
        """
        import numpy as np

        for coord in coord_names:
            if coord not in ds1.coords:
                raise ValueError(f"Coordinate '{coord}' missing in temperature dataset")
            if coord not in ds2.coords:
                raise ValueError(f"Coordinate '{coord}' missing in humidity dataset")

            # Check shape
            if ds1[coord].shape != ds2[coord].shape:
                raise ValueError(
                    f"Coordinate '{coord}' shape mismatch: "
                    f"temperature {ds1[coord].shape} vs humidity {ds2[coord].shape}"
                )

            # Check values match (with floating point tolerance for spatial coords)
            if coord in ['lat', 'lon']:
                if not np.allclose(ds1[coord].values, ds2[coord].values, rtol=1e-6):
                    max_diff = float(np.max(np.abs(ds1[coord].values - ds2[coord].values)))
                    raise ValueError(
                        f"Coordinate '{coord}' values mismatch. Max difference: {max_diff}"
                    )
            else:  # time coordinate - must match exactly
                if not ds1[coord].equals(ds2[coord]):
                    raise ValueError(f"Time coordinates don't match between datasets")

        logger.debug(f"Coordinate validation passed for: {coord_names}")

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

        try:
            # Load temperature data with error handling
            logger.info("Loading temperature data...")
            try:
                temp_ds = xr.open_zarr(self.temp_zarr_store, chunks=self.chunk_config)
            except Exception as e:
                logger.error(f"Failed to open temperature zarr store: {self.temp_zarr_store}")
                raise FileNotFoundError(f"Temperature data not accessible: {e}") from e

            # Load humidity data with error handling
            logger.info("Loading humidity data...")
            try:
                humid_ds = xr.open_zarr(self.humid_zarr_store, chunks=self.chunk_config)
            except Exception as e:
                logger.error(f"Failed to open humidity zarr store: {self.humid_zarr_store}")
                raise FileNotFoundError(f"Humidity data not accessible: {e}") from e

            # Validate required variables exist
            if 'tmean' not in temp_ds.data_vars:
                raise ValueError(
                    f"Expected 'tmean' not found in temperature store. "
                    f"Available: {list(temp_ds.data_vars)}"
                )
            if 'tdmean' not in humid_ds.data_vars:
                raise ValueError(
                    f"Expected 'tdmean' not found in humidity store. "
                    f"Available: {list(humid_ds.data_vars)}"
                )

            # Select ONLY needed variables (memory efficiency)
            temp_ds = temp_ds[['tmean']]
            humid_ds = humid_ds[['tdmean']]

            # Select time range for both
            temp_subset = temp_ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))
            humid_subset = humid_ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

            # Validate time ranges exist
            if len(temp_subset.time) == 0:
                raise ValueError(f"No temperature data found for period {start_year}-{end_year}")
            if len(humid_subset.time) == 0:
                raise ValueError(f"No humidity data found for period {start_year}-{end_year}")

            logger.info(f"  Temperature: {len(temp_subset.time)} timesteps")
            logger.info(f"  Humidity: {len(humid_subset.time)} timesteps")

            # Rename variables for xclim compatibility
            temp_subset = temp_subset.rename({'tmean': 'tas'})
            humid_subset = humid_subset.rename({'tdmean': 'tdew'})
            logger.debug("Renamed variables: tmean→tas, tdmean→tdew")

            # Validate coordinate alignment before merge
            logger.info("Validating coordinate alignment...")
            self._validate_coordinates(temp_subset, humid_subset, ['time', 'lat', 'lon'])

            # Merge datasets (simplified - variables exist after validation)
            logger.info("Merging temperature and humidity datasets...")
            combined_ds = xr.merge([temp_subset, humid_subset])

        except Exception as e:
            logger.error(f"Failed during data loading/merging: {e}")
            raise

        # Fix units
        unit_fixes = {
            'tas': 'degC',
            'tdew': 'degC'
        }

        for var_name, unit in unit_fixes.items():
            if var_name in combined_ds:
                combined_ds[var_name].attrs['units'] = unit
                combined_ds[var_name].attrs['standard_name'] = self._get_standard_name(var_name)

        # Calculate comfort indices
        logger.info("Calculating human comfort indices...")
        all_indices = self.calculate_comfort_indices(combined_ds)
        logger.info(f"  Calculated {len(all_indices)} comfort indices")

        if not all_indices:
            logger.warning("No indices calculated")
            return None

        # Combine indices into dataset
        logger.info(f"Combining {len(all_indices)} indices into dataset...")
        result_ds = xr.Dataset(all_indices)

        # Add metadata
        result_ds.attrs['creation_date'] = datetime.now().isoformat()
        result_ds.attrs['software'] = 'xclim-timber human comfort pipeline v1.0'
        result_ds.attrs['time_range'] = f'{start_year}-{end_year}'
        result_ds.attrs['indices_count'] = len(all_indices)

        # Save output
        output_file = output_dir / f'comfort_indices_{start_year}_{end_year}.nc'
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
            'tas': 'air_temperature',
            'tdew': 'dew_point_temperature'
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
        logger.info("HUMAN COMFORT INDICES PIPELINE")
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
        description="Human Comfort Indices Pipeline: Calculate 3 comfort indices combining temperature and humidity",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process default period (1981-2024)
  python human_comfort_pipeline.py

  # Process single year
  python human_comfort_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python human_comfort_pipeline.py --output-dir ./results
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
        default=4,
        help='Number of years to process per chunk (default: 4 for memory efficiency)'
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
    pipeline = HumanComfortPipeline(
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

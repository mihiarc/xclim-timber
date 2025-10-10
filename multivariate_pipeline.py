#!/usr/bin/env python3
"""
Multivariate climate indices pipeline for xclim-timber.
Efficiently processes compound climate extreme indices using Zarr streaming.
Calculates 4 multivariate indices combining temperature and precipitation data.
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


class MultivariatePipeline:
    """
    Memory-efficient multivariate climate indices pipeline using Zarr streaming.
    Processes 4 compound extreme indices combining temperature and precipitation without loading full dataset into memory.
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

        # Zarr store paths
        self.temp_zarr_store = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'
        self.precip_zarr_store = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation'

        # Baseline percentiles path
        self.baseline_file = Path('data/baselines/baseline_percentiles_1981_2000.nc')

        # Load baseline percentiles for multivariate indices
        self.baseline_percentiles = self._load_baseline_percentiles()

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

    def _load_baseline_percentiles(self):
        """
        Load pre-calculated baseline percentiles for multivariate indices.

        Returns:
            dict: Dictionary of baseline percentile DataArrays

        Raises:
            FileNotFoundError: If baseline file doesn't exist with helpful message
        """
        if not self.baseline_file.exists():
            error_msg = f"""
ERROR: Baseline percentiles file not found at {self.baseline_file}

Please generate baseline percentiles first:
  python calculate_baseline_percentiles.py

This is a one-time operation that takes ~20-30 minutes.
The file should include multivariate percentiles: tas_25p, tas_75p, pr_25p, pr_75p
            """
            raise FileNotFoundError(error_msg)

        logger.info(f"Loading baseline percentiles from {self.baseline_file}")
        ds = xr.open_dataset(self.baseline_file)

        # Check if multivariate percentiles are present
        required_percentiles = ['tas_25p_threshold', 'tas_75p_threshold', 'pr_25p_threshold', 'pr_75p_threshold']
        missing = [p for p in required_percentiles if p not in ds.data_vars]

        if missing:
            error_msg = f"""
ERROR: Baseline file is missing multivariate percentiles: {', '.join(missing)}

The baseline file needs to be regenerated with multivariate percentiles.
Please run:
  python calculate_baseline_percentiles.py

This will generate a new baseline file with all required percentiles.
            """
            raise ValueError(error_msg)

        percentiles = {
            'tas_25p_threshold': ds['tas_25p_threshold'],
            'tas_75p_threshold': ds['tas_75p_threshold'],
            'pr_25p_threshold': ds['pr_25p_threshold'],
            'pr_75p_threshold': ds['pr_75p_threshold']
        }

        logger.info(f"  Loaded {len(percentiles)} baseline percentile thresholds for multivariate indices")
        return percentiles

    def calculate_multivariate_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate multivariate compound climate extreme indices.

        Args:
            ds: Dataset with temperature and precipitation variables (tas, pr)

        Returns:
            Dictionary of calculated indices

        Note:
            These indices capture compound climate extremes using percentile thresholds
            from 1981-2000 baseline period:
            - "warm/cold" = 75th/25th percentile of temperature (day-of-year)
            - "wet/dry" = 75th/25th percentile of precipitation (day-of-year, wet days only)
        """
        indices = {}

        # All four indices require both temperature and precipitation
        if 'tas' not in ds or 'pr' not in ds:
            logger.warning("Missing required variables (tas or pr) for multivariate indices")
            return indices

        # Use pre-calculated baseline percentiles
        tas_25 = self.baseline_percentiles['tas_25p_threshold']
        tas_75 = self.baseline_percentiles['tas_75p_threshold']
        pr_25 = self.baseline_percentiles['pr_25p_threshold']
        pr_75 = self.baseline_percentiles['pr_75p_threshold']

        logger.info("  Calculating 4 compound extreme indices using baseline percentiles...")

        # 1. Cold and Dry Days (compound drought)
        logger.info("  - Calculating cold_and_dry_days...")
        cold_dry = atmos.cold_and_dry_days(
            tas=ds.tas,
            pr=ds.pr,
            tas_per=tas_25,  # Cold threshold (25th percentile by day-of-year)
            pr_per=pr_25,    # Dry threshold (25th percentile of wet days by day-of-year)
            freq='YS'
        )
        # Drop quantile coordinate if present
        if 'quantile' in cold_dry.coords:
            cold_dry = cold_dry.drop_vars('quantile')
        indices['cold_and_dry_days'] = cold_dry

        # 2. Cold and Wet Days (flooding risk)
        logger.info("  - Calculating cold_and_wet_days...")
        # Manual calculation: compare each day to its day-of-year percentile
        # Broadcast percentiles to match time dimension
        tas_25_bcast = tas_25.sel(dayofyear=ds.time.dt.dayofyear).drop_vars('dayofyear')
        pr_75_bcast = pr_75.sel(dayofyear=ds.time.dt.dayofyear).drop_vars('dayofyear')
        cold_days = ds.tas < tas_25_bcast
        wet_days = ds.pr > pr_75_bcast
        cold_wet = (cold_days & wet_days).resample(time='YS').sum()
        cold_wet.attrs['units'] = 'days'
        cold_wet.attrs['long_name'] = 'Cold and wet days'
        cold_wet.attrs['description'] = 'Days with temperature below 25th percentile and precipitation above 75th percentile'
        # Drop quantile coordinate if present
        if 'quantile' in cold_wet.coords:
            cold_wet = cold_wet.drop_vars('quantile')
        indices['cold_and_wet_days'] = cold_wet

        # 3. Warm and Dry Days (drought/fire risk)
        logger.info("  - Calculating warm_and_dry_days...")
        warm_dry = atmos.warm_and_dry_days(
            tas=ds.tas,
            pr=ds.pr,
            tas_per=tas_75,  # Warm threshold (75th percentile by day-of-year)
            pr_per=pr_25,    # Dry threshold (25th percentile of wet days by day-of-year)
            freq='YS'
        )
        # Drop quantile coordinate if present
        if 'quantile' in warm_dry.coords:
            warm_dry = warm_dry.drop_vars('quantile')
        indices['warm_and_dry_days'] = warm_dry

        # 4. Warm and Wet Days (compound extremes)
        logger.info("  - Calculating warm_and_wet_days...")
        warm_wet = atmos.warm_and_wet_days(
            tas=ds.tas,
            pr=ds.pr,
            tas_per=tas_75,  # Warm threshold (75th percentile by day-of-year)
            pr_per=pr_75,    # Wet threshold (75th percentile of wet days by day-of-year)
            freq='YS'
        )
        # Drop quantile coordinate if present
        if 'quantile' in warm_wet.coords:
            warm_wet = warm_wet.drop_vars('quantile')
        indices['warm_and_wet_days'] = warm_wet

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
                raise ValueError(f"Coordinate '{coord}' missing in precipitation dataset")

            # Check shape
            if ds1[coord].shape != ds2[coord].shape:
                raise ValueError(
                    f"Coordinate '{coord}' shape mismatch: "
                    f"temperature {ds1[coord].shape} vs precipitation {ds2[coord].shape}"
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

            # Load precipitation data with error handling
            logger.info("Loading precipitation data...")
            try:
                precip_ds = xr.open_zarr(self.precip_zarr_store, chunks=self.chunk_config)
            except Exception as e:
                logger.error(f"Failed to open precipitation zarr store: {self.precip_zarr_store}")
                raise FileNotFoundError(f"Precipitation data not accessible: {e}") from e

            # Validate required variables exist
            if 'tmean' not in temp_ds.data_vars:
                raise ValueError(
                    f"Expected 'tmean' not found in temperature store. "
                    f"Available: {list(temp_ds.data_vars)}"
                )
            if 'ppt' not in precip_ds.data_vars:
                raise ValueError(
                    f"Expected 'ppt' not found in precipitation store. "
                    f"Available: {list(precip_ds.data_vars)}"
                )

            # Select ONLY needed variables (memory efficiency)
            temp_ds = temp_ds[['tmean']]
            precip_ds = precip_ds[['ppt']]

            # Select time range for both
            temp_subset = temp_ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))
            precip_subset = precip_ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

            # Validate time ranges exist
            if len(temp_subset.time) == 0:
                raise ValueError(f"No temperature data found for period {start_year}-{end_year}")
            if len(precip_subset.time) == 0:
                raise ValueError(f"No precipitation data found for period {start_year}-{end_year}")

            logger.info(f"  Temperature: {len(temp_subset.time)} timesteps")
            logger.info(f"  Precipitation: {len(precip_subset.time)} timesteps")

            # Rename variables for xclim compatibility
            temp_subset = temp_subset.rename({'tmean': 'tas'})
            precip_subset = precip_subset.rename({'ppt': 'pr'})
            logger.debug("Renamed variables: tmean→tas, ppt→pr")

            # Validate coordinate alignment before merge
            logger.info("Validating coordinate alignment...")
            self._validate_coordinates(temp_subset, precip_subset, ['time', 'lat', 'lon'])

            # Merge datasets (simplified - variables exist after validation)
            logger.info("Merging temperature and precipitation datasets...")
            combined_ds = xr.merge([temp_subset, precip_subset])

        except Exception as e:
            logger.error(f"Failed during data loading/merging: {e}")
            raise

        # Fix units
        unit_fixes = {
            'tas': 'degC',
            'pr': 'mm/day'
        }

        for var_name, unit in unit_fixes.items():
            if var_name in combined_ds:
                combined_ds[var_name].attrs['units'] = unit
                combined_ds[var_name].attrs['standard_name'] = self._get_standard_name(var_name)

        # Calculate multivariate indices
        logger.info("Calculating multivariate compound extreme indices...")
        all_indices = self.calculate_multivariate_indices(combined_ds)
        logger.info(f"  Calculated {len(all_indices)} multivariate indices")

        if not all_indices:
            logger.warning("No indices calculated")
            return None

        # Combine indices into dataset
        logger.info(f"Combining {len(all_indices)} indices into dataset...")
        result_ds = xr.Dataset(all_indices)

        # Add metadata
        result_ds.attrs['creation_date'] = datetime.now().isoformat()
        result_ds.attrs['software'] = 'xclim-timber multivariate pipeline v1.0'
        result_ds.attrs['time_range'] = f'{start_year}-{end_year}'
        result_ds.attrs['indices_count'] = len(all_indices)
        result_ds.attrs['description'] = 'Compound climate extreme indices combining temperature and precipitation'

        # Save output
        output_file = output_dir / f'multivariate_indices_{start_year}_{end_year}.nc'
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
            'pr': 'precipitation_flux'
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
        logger.info("MULTIVARIATE CLIMATE INDICES PIPELINE")
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
        description="Multivariate Climate Indices Pipeline: Calculate 4 compound extreme indices combining temperature and precipitation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process default period (1981-2024)
  python multivariate_pipeline.py

  # Process single year
  python multivariate_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python multivariate_pipeline.py --output-dir ./results
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
    pipeline = MultivariatePipeline(
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

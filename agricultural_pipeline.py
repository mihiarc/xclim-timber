#!/usr/bin/env python3
"""
Agricultural indices pipeline for xclim-timber.
Efficiently processes agricultural and growing season indices using Zarr streaming.
Calculates 5 agricultural indices (Phase 8).
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


class AgriculturalPipeline:
    """
    Memory-efficient agricultural indices pipeline using Zarr streaming.
    Processes 5 agricultural indices without loading full dataset into memory.

    Indices:
    - Growing Season Length: Total days suitable for plant growth (ETCCDI)
    - Potential Evapotranspiration: Water demand (Baier-Robertson 1965 method)
    - Corn Heat Units: Crop-specific temperature index (USDA standard)
    - Thawing Degree Days: Spring warming accumulation (permafrost monitoring)
    - Growing Season Precipitation: Water availability during growing season
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
        self.temp_zarr = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'
        self.precip_zarr = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation'

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

    def calculate_agricultural_indices(self, temp_ds: xr.Dataset, precip_ds: xr.Dataset) -> dict:
        """
        Calculate agricultural and growing season indices (Phase 8).

        Implements 5 high-value agricultural indices:
        - Growing season analysis
        - Evapotranspiration
        - Crop-specific heat accumulation
        - Spring thaw timing
        - Soil moisture proxy

        All indices use fixed thresholds (no baseline percentiles required).

        Args:
            temp_ds: Dataset with temperature variables (tas, tasmax, tasmin)
            precip_ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated agricultural indices
        """
        indices = {}

        # 1. Growing Season Length (ETCCDI standard)
        if 'tas' in temp_ds:
            try:
                logger.info("  - Calculating growing season length...")
                indices['growing_season_length'] = atmos.growing_season_length(
                    tas=temp_ds.tas,
                    thresh='5.0 degC',
                    window=6,
                    mid_date='07-01',
                    freq='YS'
                )
                # Fix units metadata for CF-compliance
                indices['growing_season_length'].attrs['units'] = 'days'
                indices['growing_season_length'].attrs['long_name'] = 'Growing Season Length'
                indices['growing_season_length'].attrs['description'] = 'Number of days between first and last occurrence of 6+ consecutive days with temperature above 5°C'
            except Exception as e:
                logger.error(f"Failed to calculate growing_season_length: {e}")

        # 2. Potential Evapotranspiration (Baier-Robertson 1965 method)
        if 'tasmin' in temp_ds and 'tasmax' in temp_ds:
            try:
                logger.info("  - Calculating potential evapotranspiration (BR65 method)...")

                # Extract latitude from coordinates
                lat = temp_ds.lat

                # Calculate daily PET using Baier-Robertson method (temperature-only)
                from xclim.indices import potential_evapotranspiration as pet_index
                pet_daily = pet_index(
                    tasmin=temp_ds.tasmin,
                    tasmax=temp_ds.tasmax,
                    lat=lat,
                    method='BR65'
                )

                # Aggregate to annual sum
                indices['potential_evapotranspiration'] = pet_daily.resample(time='YS').sum()

                # Fix metadata for CF-compliance
                indices['potential_evapotranspiration'].attrs['long_name'] = 'Annual Potential Evapotranspiration (BR65)'
                indices['potential_evapotranspiration'].attrs['description'] = 'Annual sum of potential evapotranspiration using Baier-Robertson 1965 method (temperature-only)'
                indices['potential_evapotranspiration'].attrs['standard_name'] = 'water_evapotranspiration_amount'
            except Exception as e:
                logger.error(f"Failed to calculate potential_evapotranspiration: {e}")

        # 3. Corn Heat Units (crop-specific)
        if 'tasmin' in temp_ds and 'tasmax' in temp_ds:
            try:
                logger.info("  - Calculating corn heat units...")

                # Calculate daily CHU
                from xclim.indices import corn_heat_units as chu_index
                chu_daily = chu_index(
                    tasmin=temp_ds.tasmin,
                    tasmax=temp_ds.tasmax,
                    thresh_tasmin='4.44 degC',
                    thresh_tasmax='10 degC'
                )

                # Aggregate to annual sum
                indices['corn_heat_units'] = chu_daily.resample(time='YS').sum()

                # Fix units metadata for CF-compliance
                indices['corn_heat_units'].attrs['units'] = '1'  # Dimensionless index
                indices['corn_heat_units'].attrs['long_name'] = 'Annual Corn Heat Units'
                indices['corn_heat_units'].attrs['description'] = 'Annual sum of corn heat units for crop development and maturity prediction (USDA standard)'
            except Exception as e:
                logger.error(f"Failed to calculate corn_heat_units: {e}")

        # 4. Thawing Degree Days (permafrost monitoring)
        if 'tas' in temp_ds:
            try:
                logger.info("  - Calculating thawing degree days...")
                indices['thawing_degree_days'] = atmos.growing_degree_days(
                    tas=temp_ds.tas,
                    thresh='0 degC',
                    freq='YS'
                )
                # Update metadata to reflect thawing focus
                indices['thawing_degree_days'].attrs['long_name'] = 'Thawing Degree Days'
                indices['thawing_degree_days'].attrs['description'] = 'Sum of degree-days above 0°C (permafrost monitoring, spring melt timing)'
                indices['thawing_degree_days'].attrs['standard_name'] = 'integral_of_air_temperature_excess_wrt_time'
            except Exception as e:
                logger.error(f"Failed to calculate thawing_degree_days: {e}")

        # 5. Growing Season Precipitation (water availability during growing season)
        if 'pr' in precip_ds and 'growing_season_length' in indices:
            try:
                logger.info("  - Calculating growing season precipitation...")

                # Note: This would require growing_season_start and growing_season_end
                # which we computed in Phase 7. For simplicity, calculate total growing
                # season precipitation using a fixed April-October window for now.

                # Select growing season months (April-October, typical for northern hemisphere)
                pr_growing = precip_ds.pr.where(
                    (precip_ds.pr.time.dt.month >= 4) & (precip_ds.pr.time.dt.month <= 10)
                )

                # Sum precipitation during growing season
                indices['growing_season_precipitation'] = pr_growing.resample(time='YS').sum()

                # Fix units metadata
                indices['growing_season_precipitation'].attrs['units'] = 'mm'
                indices['growing_season_precipitation'].attrs['long_name'] = 'Growing Season Precipitation'
                indices['growing_season_precipitation'].attrs['description'] = 'Total precipitation during growing season (April-October)'
                indices['growing_season_precipitation'].attrs['standard_name'] = 'precipitation_amount'
            except Exception as e:
                logger.error(f"Failed to calculate growing_season_precipitation: {e}")

        return indices

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

        # Load temperature data
        logger.info("Loading temperature data...")
        temp_ds = xr.open_zarr(self.temp_zarr, chunks=self.chunk_config)
        temp_ds = temp_ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

        # Rename temperature variables for xclim compatibility
        rename_map = {
            'tmean': 'tas',
            'tmax': 'tasmax',
            'tmin': 'tasmin'
        }

        for old_name, new_name in rename_map.items():
            if old_name in temp_ds:
                temp_ds = temp_ds.rename({old_name: new_name})
                logger.debug(f"Renamed {old_name} to {new_name}")

        # Fix units for temperature variables
        for var_name in ['tas', 'tasmax', 'tasmin']:
            if var_name in temp_ds:
                temp_ds[var_name].attrs['units'] = 'degC'

        # Load precipitation data
        logger.info("Loading precipitation data...")
        precip_ds = xr.open_zarr(self.precip_zarr, chunks=self.chunk_config)
        precip_ds = precip_ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

        # Rename precipitation variable for xclim compatibility
        if 'ppt' in precip_ds:
            precip_ds = precip_ds.rename({'ppt': 'pr'})
            logger.debug("Renamed ppt to pr")
            precip_ds['pr'].attrs['units'] = 'mm'

        # Calculate agricultural indices
        logger.info("Calculating agricultural indices (Phase 8)...")
        agricultural_indices = self.calculate_agricultural_indices(temp_ds, precip_ds)
        logger.info(f"  Calculated {len(agricultural_indices)} agricultural indices")

        if not agricultural_indices:
            logger.warning("No indices calculated")
            return None

        # Combine indices into dataset
        logger.info(f"Combining {len(agricultural_indices)} indices into dataset...")
        result_ds = xr.Dataset(agricultural_indices)

        # Add global metadata
        result_ds.attrs['creation_date'] = datetime.now().isoformat()
        result_ds.attrs['software'] = 'xclim-timber agricultural pipeline v1.0 (Phase 8)'
        result_ds.attrs['time_range'] = f'{start_year}-{end_year}'
        result_ds.attrs['indices_count'] = len(agricultural_indices)
        result_ds.attrs['phase'] = 'Phase 8: Agricultural & Growing Season Indices (+5 indices)'
        result_ds.attrs['note'] = 'Agricultural indices using temperature and precipitation data. PET uses Baier-Robertson (1965) method (temperature-only). No baseline percentiles required.'

        # Save output
        output_file = output_dir / f'agricultural_indices_{start_year}_{end_year}.nc'
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
        logger.info("AGRICULTURAL INDICES PIPELINE")
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
        description="Agricultural Indices Pipeline: Calculate 5 agricultural and growing season indices (Phase 8)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Indices calculated:
  1. Growing Season Length - Days suitable for plant growth (ETCCDI standard)
  2. Potential Evapotranspiration - Annual water demand (Baier-Robertson 1965 method)
  3. Corn Heat Units - Crop-specific temperature index (USDA standard)
  4. Thawing Degree Days - Spring warming accumulation (permafrost monitoring)
  5. Growing Season Precipitation - Water availability during growing season (April-October)

Examples:
  # Process default period (1981-2024)
  python agricultural_pipeline.py

  # Process single year
  python agricultural_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python agricultural_pipeline.py --output-dir ./results
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
    pipeline = AgriculturalPipeline(
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

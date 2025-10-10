#!/usr/bin/env python3
"""
Temperature indices pipeline for xclim-timber.
Efficiently processes temperature-based climate indices using Zarr streaming.
Calculates 33 temperature indices (19 basic + 6 extreme percentile-based + 8 advanced Phase 7).
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


class TemperaturePipeline:
    """
    Memory-efficient temperature indices pipeline using Zarr streaming.
    Processes 33 temperature indices without loading full dataset into memory.

    Indices:
    - Basic (19): Core temperature statistics, thresholds, degree days, frost season
    - Extreme (6): Percentile-based warm/cool days/nights, spell duration
    - Advanced Phase 7 (8): Spell frequency, growing season timing, variability
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

        # Zarr store path for temperature data
        self.zarr_store = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'

        # Baseline percentiles path
        self.baseline_file = Path('data/baselines/baseline_percentiles_1981_2000.nc')

        # Load baseline percentiles for extreme indices
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
        Load pre-calculated baseline percentiles for extreme indices.

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

This is a one-time operation that takes ~15 minutes.
See docs/BASELINE_DOCUMENTATION.md for more information.
            """
            raise FileNotFoundError(error_msg)

        logger.info(f"Loading baseline percentiles from {self.baseline_file}")
        ds = xr.open_dataset(self.baseline_file)

        percentiles = {
            'tx90p_threshold': ds['tx90p_threshold'],
            'tx10p_threshold': ds['tx10p_threshold'],
            'tn90p_threshold': ds['tn90p_threshold'],
            'tn10p_threshold': ds['tn10p_threshold']
        }

        logger.info(f"  Loaded {len(percentiles)} baseline percentile thresholds")
        return percentiles

    def calculate_temperature_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate temperature-based climate indices.

        Args:
            ds: Dataset with temperature variables (tas, tasmax, tasmin)

        Returns:
            Dictionary of calculated indices
        """
        indices = {}

        # Basic temperature statistics
        if 'tas' in ds:
            logger.info("  - Calculating annual mean temperature...")
            indices['tg_mean'] = atmos.tg_mean(ds.tas, freq='YS')

        if 'tasmax' in ds:
            logger.info("  - Calculating annual maximum temperature...")
            indices['tx_max'] = atmos.tx_max(ds.tasmax, freq='YS')
            logger.info("  - Calculating summer days (>25°C)...")
            indices['summer_days'] = atmos.tx_days_above(ds.tasmax, thresh='25 degC', freq='YS')
            logger.info("  - Calculating hot days (>30°C)...")
            indices['hot_days'] = atmos.tx_days_above(ds.tasmax, thresh='30 degC', freq='YS')
            logger.info("  - Calculating ice days (<0°C)...")
            indices['ice_days'] = atmos.ice_days(ds.tasmax, freq='YS')

        if 'tasmin' in ds:
            logger.info("  - Calculating annual minimum temperature...")
            indices['tn_min'] = atmos.tn_min(ds.tasmin, freq='YS')
            logger.info("  - Calculating frost days...")
            indices['frost_days'] = atmos.frost_days(ds.tasmin, freq='YS')
            logger.info("  - Calculating tropical nights (>20°C)...")
            indices['tropical_nights'] = atmos.tropical_nights(ds.tasmin, freq='YS')
            logger.info("  - Calculating consecutive frost days...")
            indices['consecutive_frost_days'] = atmos.consecutive_frost_days(
                ds.tasmin, freq='YS'
            )

        if 'tas' in ds:
            logger.info("  - Calculating growing degree days...")
            indices['growing_degree_days'] = atmos.growing_degree_days(
                ds.tas, thresh='10 degC', freq='YS'
            )
            logger.info("  - Calculating heating degree days...")
            indices['heating_degree_days'] = atmos.heating_degree_days(
                ds.tas, thresh='17 degC', freq='YS'
            )
            logger.info("  - Calculating cooling degree days...")
            indices['cooling_degree_days'] = atmos.cooling_degree_days(
                ds.tas, thresh='18 degC', freq='YS'
            )
            logger.info("  - Calculating freezing degree days...")
            indices['freezing_degree_days'] = atmos.freezing_degree_days(
                ds.tas, freq='YS'
            )

        # Temperature range indices (require both tasmax and tasmin)
        if 'tasmax' in ds and 'tasmin' in ds:
            logger.info("  - Calculating daily temperature range...")
            indices['daily_temperature_range'] = atmos.daily_temperature_range(
                ds.tasmin, ds.tasmax, freq='YS'
            )
            logger.info("  - Calculating extreme temperature range...")
            indices['extreme_temperature_range'] = atmos.extreme_temperature_range(
                ds.tasmin, ds.tasmax, freq='YS'
            )

        # Frost season indices (require tasmin)
        if 'tasmin' in ds:
            logger.info("  - Calculating frost season length...")
            indices['frost_season_length'] = atmos.frost_season_length(
                ds.tasmin, freq='YS'
            )
            logger.info("  - Calculating frost-free season start...")
            indices['frost_free_season_start'] = atmos.frost_free_season_start(
                ds.tasmin, freq='YS'
            )
            logger.info("  - Calculating frost-free season end...")
            indices['frost_free_season_end'] = atmos.frost_free_season_end(
                ds.tasmin, freq='YS'
            )
            logger.info("  - Calculating frost-free season length...")
            indices['frost_free_season_length'] = atmos.frost_free_season_length(
                ds.tasmin, freq='YS'
            )

        return indices

    def calculate_extreme_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate percentile-based extreme temperature indices using pre-calculated baseline.

        Args:
            ds: Dataset with temperature variables (tasmax, tasmin)

        Returns:
            Dictionary of calculated extreme indices
        """
        indices = {}

        # Warm/cool day indices (based on tasmax)
        if 'tasmax' in ds:
            logger.info("  - Calculating warm days (tx90p)...")
            indices['tx90p'] = atmos.tx90p(
                tasmax=ds.tasmax,
                tasmax_per=self.baseline_percentiles['tx90p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating cool days (tx10p)...")
            indices['tx10p'] = atmos.tx10p(
                tasmax=ds.tasmax,
                tasmax_per=self.baseline_percentiles['tx10p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating warm spell duration (WSDI)...")
            indices['warm_spell_duration_index'] = atmos.warm_spell_duration_index(
                tasmax=ds.tasmax,
                tasmax_per=self.baseline_percentiles['tx90p_threshold'],
                window=6,
                freq='YS'
            )

        # Warm/cool night indices (based on tasmin)
        if 'tasmin' in ds:
            logger.info("  - Calculating warm nights (tn90p)...")
            indices['tn90p'] = atmos.tn90p(
                tasmin=ds.tasmin,
                tasmin_per=self.baseline_percentiles['tn90p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating cool nights (tn10p)...")
            indices['tn10p'] = atmos.tn10p(
                tasmin=ds.tasmin,
                tasmin_per=self.baseline_percentiles['tn10p_threshold'],
                freq='YS'
            )

            logger.info("  - Calculating cold spell duration (CSDI)...")
            indices['cold_spell_duration_index'] = atmos.cold_spell_duration_index(
                tasmin=ds.tasmin,
                tasmin_per=self.baseline_percentiles['tn10p_threshold'],
                window=6,
                freq='YS'
            )

        return indices

    def calculate_advanced_temperature_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate advanced temperature extreme indices (Phase 7).

        Adds 8 new indices focused on:
        - Spell frequency (counting discrete events)
        - Seasonal timing (growing season, last frost)
        - Temperature variability

        All indices use fixed thresholds (no baseline percentiles required).

        Args:
            ds: Dataset with temperature variables (tas, tasmax, tasmin)

        Returns:
            Dictionary of calculated advanced temperature indices
        """
        indices = {}

        # Growing season timing indices (ETCCDI standard)
        if 'tas' in ds:
            logger.info("  - Calculating growing season start...")
            indices['growing_season_start'] = atmos.growing_season_start(
                tas=ds.tas,
                thresh='5 degC',
                window=5,
                freq='YS'
            )

            logger.info("  - Calculating growing season end...")
            indices['growing_season_end'] = atmos.growing_season_end(
                tas=ds.tas,
                thresh='5 degC',
                window=5,
                freq='YS'
            )

        # Spell frequency indices (event counting)
        if 'tas' in ds:
            logger.info("  - Calculating cold spell frequency...")
            indices['cold_spell_frequency'] = atmos.cold_spell_frequency(
                tas=ds.tas,
                thresh='-10 degC',
                window=5,
                freq='YS'
            )

        if 'tasmax' in ds:
            logger.info("  - Calculating hot spell frequency...")
            indices['hot_spell_frequency'] = atmos.hot_spell_frequency(
                tasmax=ds.tasmax,
                thresh='30 degC',
                window=3,
                freq='YS'
            )

        if 'tasmin' in ds and 'tasmax' in ds:
            logger.info("  - Calculating heat wave frequency...")
            indices['heat_wave_frequency'] = atmos.heat_wave_frequency(
                tasmin=ds.tasmin,
                tasmax=ds.tasmax,
                thresh_tasmin='22 degC',
                thresh_tasmax='30 degC',
                window=3,
                freq='YS'
            )

            logger.info("  - Calculating freeze-thaw spell frequency...")
            indices['freezethaw_spell_frequency'] = atmos.freezethaw_spell_frequency(
                tasmin=ds.tasmin,
                tasmax=ds.tasmax,
                freq='YS'
            )

        # Seasonal timing - last spring frost
        if 'tasmin' in ds:
            logger.info("  - Calculating last spring frost...")
            indices['last_spring_frost'] = atmos.last_spring_frost(
                tasmin=ds.tasmin,
                thresh='0 degC',
                freq='YS'
            )

        # Temperature variability index
        if 'tasmin' in ds and 'tasmax' in ds:
            logger.info("  - Calculating daily temperature range variability...")
            indices['daily_temperature_range_variability'] = atmos.daily_temperature_range_variability(
                tasmin=ds.tasmin,
                tasmax=ds.tasmax,
                freq='YS'
            )

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
        ds = xr.open_zarr(self.zarr_store, chunks=self.chunk_config)

        # Select time range
        combined_ds = ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

        # Rename temperature variables for xclim compatibility
        rename_map = {
            'tmean': 'tas',
            'tmax': 'tasmax',
            'tmin': 'tasmin'
        }

        for old_name, new_name in rename_map.items():
            if old_name in combined_ds:
                combined_ds = combined_ds.rename({old_name: new_name})
                logger.debug(f"Renamed {old_name} to {new_name}")

        # Fix units for temperature variables
        unit_fixes = {
            'tas': 'degC',
            'tasmax': 'degC',
            'tasmin': 'degC'
        }

        for var_name, unit in unit_fixes.items():
            if var_name in combined_ds:
                combined_ds[var_name].attrs['units'] = unit
                combined_ds[var_name].attrs['standard_name'] = self._get_standard_name(var_name)

        # Calculate basic temperature indices
        logger.info("Calculating basic temperature indices...")
        basic_indices = self.calculate_temperature_indices(combined_ds)
        logger.info(f"  Calculated {len(basic_indices)} basic indices")

        # Calculate extreme temperature indices
        logger.info("Calculating extreme temperature indices...")
        extreme_indices = self.calculate_extreme_indices(combined_ds)
        logger.info(f"  Calculated {len(extreme_indices)} extreme indices")

        # Calculate Phase 7 advanced temperature indices
        logger.info("Calculating advanced temperature indices (Phase 7)...")
        advanced_indices = self.calculate_advanced_temperature_indices(combined_ds)
        logger.info(f"  Calculated {len(advanced_indices)} advanced indices")

        # Merge all indices
        all_indices = {**basic_indices, **extreme_indices, **advanced_indices}
        logger.info(f"  Total: {len(all_indices)} temperature indices")
        logger.info(f"    Basic: {len(basic_indices)}, Extreme: {len(extreme_indices)}, Advanced (Phase 7): {len(advanced_indices)}")

        if not all_indices:
            logger.warning("No indices calculated")
            return None

        # Combine indices into dataset
        logger.info(f"Combining {len(all_indices)} indices into dataset...")
        result_ds = xr.Dataset(all_indices)

        # Add metadata
        result_ds.attrs['creation_date'] = datetime.now().isoformat()
        result_ds.attrs['software'] = 'xclim-timber temperature pipeline v3.0 (Phase 7)'
        result_ds.attrs['time_range'] = f'{start_year}-{end_year}'
        result_ds.attrs['indices_count'] = len(all_indices)
        result_ds.attrs['phase'] = 'Phase 7: Advanced Temperature Extremes (+8 indices)'
        result_ds.attrs['baseline_period'] = '1981-2000'
        result_ds.attrs['note'] = 'Extreme indices (tx90p, tx10p, tn90p, tn10p, WSDI, CSDI) use baseline percentiles. Phase 7 adds spell frequency, growing season timing, and variability indices.'

        # Save output
        output_file = output_dir / f'temperature_indices_{start_year}_{end_year}.nc'
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
        """Get CF-compliant standard name for temperature variable."""
        standard_names = {
            'tas': 'air_temperature',
            'tasmax': 'air_temperature',
            'tasmin': 'air_temperature'
        }
        return standard_names.get(var_name, 'air_temperature')

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
        logger.info("TEMPERATURE INDICES PIPELINE")
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
        description="Temperature Indices Pipeline: Calculate 33 temperature-based climate indices (Phase 7)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Indices calculated:
  Basic (19): Temperature statistics, thresholds, degree days, frost season
  Extreme (6): Percentile-based warm/cool days/nights, spell duration (uses 1981-2000 baseline)
  Advanced Phase 7 (8): Spell frequency, growing season timing, variability

Examples:
  # Process default period (1981-2024)
  python temperature_pipeline.py

  # Process single year
  python temperature_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python temperature_pipeline.py --output-dir ./results
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
    pipeline = TemperaturePipeline(
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
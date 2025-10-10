#!/usr/bin/env python3
"""
Precipitation indices pipeline for xclim-timber.
Efficiently processes precipitation-based climate indices using Zarr streaming.
Calculates 13 precipitation indices including extremes, consecutive events, intensity, and enhanced analysis (Phase 6).
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


class PrecipitationPipeline:
    """
    Memory-efficient precipitation indices pipeline using Zarr streaming.
    Processes 13 precipitation indices without loading full dataset into memory.

    Indices:
    - Basic: prcptot, rx1day, rx5day, sdii, cdd, cwd (6 indices)
    - Extreme (percentile-based): r95p, r99p (2 indices)
    - Threshold: r10mm, r20mm (2 indices)
    - Enhanced (Phase 6): dry_days, wetdays, wetdays_prop (3 indices)
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

        # Zarr store path for precipitation data
        self.zarr_store = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation'

        # Optimal chunk configuration (aligned with dimensions)
        self.chunk_config = {
            'time': 365,  # One year of daily data
            'lat': 69,    # 621 / 69 = 9 even chunks
            'lon': 281    # 1405 / 281 = 5 even chunks
        }

        # Load baseline percentiles for extreme indices
        self.baseline_file = Path('data/baselines/baseline_percentiles_1981_2000.nc')
        self.baseline_percentiles = self._load_baseline_percentiles()

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
            Dict of baseline percentile DataArrays

        Raises:
            FileNotFoundError: If baseline file doesn't exist
            RuntimeError: If baseline file is corrupted or invalid
        """
        if not self.baseline_file.exists():
            error_msg = f"""
ERROR: Baseline percentiles file not found at {self.baseline_file}

Please generate baseline percentiles first:
  python calculate_baseline_percentiles.py

This is a one-time operation that takes ~15-25 minutes.
The file should contain both temperature and precipitation percentiles.
"""
            raise FileNotFoundError(error_msg)

        try:
            logger.info(f"Loading baseline percentiles from {self.baseline_file}")
            ds = xr.open_dataset(self.baseline_file)

            # Validate baseline period
            baseline_period = ds.attrs.get('baseline_period')
            if baseline_period != '1981-2000':
                logger.warning(
                    f"Baseline period mismatch: expected '1981-2000', got '{baseline_period}'. "
                    f"Results may not be comparable to standard climate indices."
                )

            # Extract and validate precipitation percentiles
            percentiles = {}
            required_vars = ['pr95p_threshold', 'pr99p_threshold']

            for var in required_vars:
                if var not in ds:
                    logger.warning(f"Missing expected variable: {var}")
                    continue

                data = ds[var]

                # Validate dimensions
                if 'dayofyear' not in data.dims:
                    raise ValueError(f"{var} missing required 'dayofyear' dimension")

                # Validate data integrity
                if data.isnull().all():
                    raise ValueError(f"{var} contains all NaN values - baseline may be corrupted")

                percentiles[var] = data
                logger.debug(f"  Loaded {var}: shape={data.shape}")

            if not percentiles:
                logger.warning("No precipitation percentiles found in baseline file")
                logger.warning("Extreme indices (r95p, r99p) will not be calculated")

            return percentiles

        except (OSError, IOError) as e:
            raise RuntimeError(f"Failed to read baseline file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading baseline percentiles: {e}")

    def calculate_precipitation_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate precipitation-based climate indices.

        Args:
            ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated indices
        """
        indices = {}

        if 'pr' in ds:
            logger.info("  - Calculating total precipitation...")
            indices['prcptot'] = atmos.wet_precip_accumulation(
                ds.pr, thresh='1 mm/day', freq='YS'
            )
            logger.info("  - Calculating max 1-day precipitation...")
            indices['rx1day'] = atmos.max_1day_precipitation_amount(ds.pr, freq='YS')
            logger.info("  - Calculating max 5-day precipitation...")
            indices['rx5day'] = atmos.max_n_day_precipitation_amount(
                ds.pr, window=5, freq='YS'
            )
            logger.info("  - Calculating consecutive dry days...")
            indices['cdd'] = atmos.maximum_consecutive_dry_days(
                ds.pr, thresh='1 mm/day', freq='YS'
            )
            logger.info("  - Calculating consecutive wet days...")
            indices['cwd'] = atmos.maximum_consecutive_wet_days(
                ds.pr, thresh='1 mm/day', freq='YS'
            )
            logger.info("  - Calculating daily precipitation intensity...")
            indices['sdii'] = atmos.daily_pr_intensity(
                ds.pr, thresh='1 mm/day', freq='YS'
            )

        return indices

    def calculate_extreme_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate percentile-based extreme precipitation indices.

        Uses pre-calculated baseline percentiles (1981-2000) on wet days.

        Args:
            ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated extreme indices
        """
        indices = {}

        if 'pr' not in ds:
            logger.warning("No precipitation variable found for extreme indices")
            return indices

        if not self.baseline_percentiles:
            logger.warning("No baseline percentiles loaded, skipping extreme indices")
            return indices

        # r95p: Very wet days (days above 95th percentile of wet days)
        if 'pr95p_threshold' in self.baseline_percentiles:
            logger.info("  - Calculating r95p (very wet days)...")
            indices['r95p'] = atmos.days_over_precip_doy_thresh(
                pr=ds.pr,
                pr_per=self.baseline_percentiles['pr95p_threshold'],
                thresh='1 mm/day',
                freq='YS'
            )

        # r99p: Extremely wet days (days above 99th percentile of wet days)
        if 'pr99p_threshold' in self.baseline_percentiles:
            logger.info("  - Calculating r99p (extremely wet days)...")
            indices['r99p'] = atmos.days_over_precip_doy_thresh(
                pr=ds.pr,
                pr_per=self.baseline_percentiles['pr99p_threshold'],
                thresh='1 mm/day',
                freq='YS'
            )

        return indices

    def calculate_threshold_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate fixed-threshold precipitation indices.

        Args:
            ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated threshold indices
        """
        indices = {}

        if 'pr' not in ds:
            logger.warning("No precipitation variable found for threshold indices")
            return indices

        # r10mm: Heavy precipitation days (>= 10mm)
        logger.info("  - Calculating r10mm (heavy precipitation days)...")
        indices['r10mm'] = atmos.wetdays(
            pr=ds.pr,
            thresh='10 mm/day',
            freq='YS'
        )

        # r20mm: Very heavy precipitation days (>= 20mm)
        logger.info("  - Calculating r20mm (very heavy precipitation days)...")
        indices['r20mm'] = atmos.wetdays(
            pr=ds.pr,
            thresh='20 mm/day',
            freq='YS'
        )

        return indices

    def calculate_enhanced_precipitation_indices(self, ds: xr.Dataset) -> dict:
        """
        Calculate enhanced precipitation analysis indices (Phase 6).

        Adds 3 new distinct indices complementing the existing 10:
        - dry_days: Total number of dry days (< 1mm)
        - wetdays: Total number of wet days (>= 1mm)
        - wetdays_prop: Proportion of days that are wet

        Note: Spell frequency indices would require custom temporal logic
        beyond standard xclim functions and are deferred to future phases.

        Args:
            ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated enhanced precipitation indices
        """
        indices = {}

        if 'pr' not in ds:
            logger.warning("No precipitation variable found for enhanced precipitation indices")
            return indices

        # dry_days: Total number of dry days (pr < 1mm)
        logger.info("  - Calculating dry_days (total dry days < 1mm)...")
        indices['dry_days'] = atmos.dry_days(
            pr=ds.pr,
            thresh='1 mm/day',
            freq='YS'
        )

        # wetdays: Total number of wet days (pr >= 1mm)
        logger.info("  - Calculating wetdays (total wet days >= 1mm)...")
        indices['wetdays'] = atmos.wetdays(
            pr=ds.pr,
            thresh='1 mm/day',
            freq='YS'
        )

        # wetdays_prop: Proportion of wet days
        logger.info("  - Calculating wetdays_prop (proportion of wet days)...")
        indices['wetdays_prop'] = atmos.wetdays_prop(
            pr=ds.pr,
            thresh='1 mm/day',
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

        # Load precipitation data
        logger.info("Loading precipitation data...")
        ds = xr.open_zarr(self.zarr_store, chunks=self.chunk_config)

        # Select time range
        combined_ds = ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

        # Rename precipitation variable for xclim compatibility
        rename_map = {
            'ppt': 'pr'  # PRISM uses 'ppt' for precipitation
        }

        for old_name, new_name in rename_map.items():
            if old_name in combined_ds:
                combined_ds = combined_ds.rename({old_name: new_name})
                logger.debug(f"Renamed {old_name} to {new_name}")

        # Fix units for precipitation variable
        unit_fixes = {
            'pr': 'mm/day'  # Daily precipitation total
        }

        for var_name, unit in unit_fixes.items():
            if var_name in combined_ds:
                combined_ds[var_name].attrs['units'] = unit
                combined_ds[var_name].attrs['standard_name'] = self._get_standard_name(var_name)

        # Calculate precipitation indices
        logger.info("Calculating precipitation indices...")

        # Calculate basic indices
        basic_indices = self.calculate_precipitation_indices(combined_ds)

        # Calculate extreme indices (percentile-based)
        extreme_indices = self.calculate_extreme_indices(combined_ds)

        # Calculate threshold indices
        threshold_indices = self.calculate_threshold_indices(combined_ds)

        # Calculate Phase 6 indices - Enhanced Precipitation Analysis
        enhanced_indices = self.calculate_enhanced_precipitation_indices(combined_ds)

        # Merge all indices
        all_indices = {
            **basic_indices,
            **extreme_indices,
            **threshold_indices,
            **enhanced_indices
        }
        logger.info(f"  Calculated {len(all_indices)} precipitation indices total")
        logger.info(f"    Basic: {len(basic_indices)}, Extreme: {len(extreme_indices)}, Threshold: {len(threshold_indices)}, Enhanced (Phase 6): {len(enhanced_indices)}")

        if not all_indices:
            logger.warning("No indices calculated")
            return None

        # Combine indices into dataset
        logger.info(f"Combining {len(all_indices)} indices into dataset...")
        result_ds = xr.Dataset(all_indices)

        # Add metadata
        result_ds.attrs['creation_date'] = datetime.now().isoformat()
        result_ds.attrs['software'] = 'xclim-timber precipitation pipeline v4.0 (Phase 6)'
        result_ds.attrs['time_range'] = f'{start_year}-{end_year}'
        result_ds.attrs['indices_count'] = len(all_indices)
        result_ds.attrs['baseline_period'] = '1981-2000'
        result_ds.attrs['phase'] = 'Phase 6: Enhanced Precipitation Analysis (+3 indices)'
        result_ds.attrs['note'] = 'Extreme indices (r95p, r99p) use wet-day percentiles from baseline period. Phase 6 adds dry_days, wetdays, wetdays_prop.'

        # Save output
        output_file = output_dir / f'precipitation_indices_{start_year}_{end_year}.nc'
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
        logger.info("PRECIPITATION INDICES PIPELINE")
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
        description="Precipitation Indices Pipeline: Calculate 13 precipitation-based climate indices (Phase 6)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Indices calculated:
  Basic (6): prcptot, rx1day, rx5day, sdii, cdd, cwd
  Extreme (2): r95p, r99p (percentile-based using 1981-2000 baseline)
  Threshold (2): r10mm, r20mm (fixed thresholds)
  Enhanced Phase 6 (3): dry_days, wetdays, wetdays_prop

Examples:
  # Process default period (1981-2024)
  python precipitation_pipeline.py

  # Process single year
  python precipitation_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python precipitation_pipeline.py --output-dir ./results

Note: Requires baseline percentiles file. Run first if missing:
  python calculate_baseline_percentiles.py
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
    pipeline = PrecipitationPipeline(
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
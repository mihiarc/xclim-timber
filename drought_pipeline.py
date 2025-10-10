#!/usr/bin/env python3
"""
Drought indices pipeline for xclim-timber.
Efficiently processes drought and water deficit indices using Zarr streaming.
Calculates 12 drought indices (Phase 10).
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


class DroughtPipeline:
    """
    Memory-efficient drought indices pipeline using Zarr streaming.
    Processes 12 drought indices without loading full dataset into memory.

    Indices:
    - SPI (5 windows): Standardized Precipitation Index for multi-scale drought monitoring
    - Dry Spell (2 indices): Maximum consecutive dry days, total dry days count
    - Precipitation Intensity (2 indices): Daily intensity, heavy precipitation fraction
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

        # Zarr store path for precipitation
        self.precip_zarr = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation'

        # Baseline percentiles path
        self.baseline_file = Path('data/baselines/baseline_percentiles_1981_2000.nc')

        # Load baseline percentiles for fraction_over_precip_thresh
        self.baseline_percentiles = self._load_baseline_percentiles()

        # Optimal chunk configuration (aligned with dimensions)
        self.chunk_config = {
            'time': 365,  # One year of daily data
            'lat': 69,    # 621 / 69 = 9 even chunks
            'lon': 281    # 1405 / 281 = 5 even chunks
        }

    def setup_dask_client(self):
        """Initialize Dask client with memory limits."""
        # Note: Not using distributed client for drought pipeline due to SPI serialization issues
        # Using threaded scheduler directly instead
        logger.info("Using Dask threaded scheduler (no distributed client)")

    def close(self):
        """Clean up resources."""
        if self.client:
            self.client.close()
            self.client = None

    def _load_baseline_percentiles(self):
        """
        Load pre-calculated baseline percentiles for precipitation indices.

        Returns:
            dict: Dictionary of baseline percentile DataArrays

        Raises:
            FileNotFoundError: If baseline file doesn't exist with helpful message
        """
        if not self.baseline_file.exists():
            logger.warning(f"Baseline percentiles file not found at {self.baseline_file}")
            logger.warning("Index #12 (fraction_over_precip_thresh) will be skipped")
            return {}

        logger.info(f"Loading baseline percentiles from {self.baseline_file}")
        ds = xr.open_dataset(self.baseline_file)

        percentiles = {}
        if 'pr_75p_threshold' in ds:
            percentiles['pr_75p_threshold'] = ds['pr_75p_threshold']
            logger.info(f"  Loaded pr_75p_threshold for heavy precipitation fraction")
        else:
            logger.warning("pr_75p_threshold not found in baseline file")

        return percentiles

    def calculate_spi_indices(self, precip_ds: xr.Dataset) -> dict:
        """
        Calculate Standardized Precipitation Index (SPI) at multiple time windows.

        Implements McKee et al. (1993) standard methodology using gamma distribution fitting.

        Args:
            precip_ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated SPI indices (5 windows)
        """
        indices = {}

        if 'pr' not in precip_ds:
            logger.warning("Precipitation variable 'pr' not found, skipping SPI calculation")
            return indices

        # Define SPI windows (in months)
        spi_windows = {
            1: 'spi_1month',
            3: 'spi_3month',
            6: 'spi_6month',
            12: 'spi_12month',
            24: 'spi_24month'
        }

        # Calibration period (WMO recommends 30 years)
        cal_start = '1981-01-01'
        cal_end = '2010-12-31'

        for window, var_name in spi_windows.items():
            try:
                logger.info(f"  - Calculating {var_name} (SPI-{window})...")

                # Calculate SPI using gamma distribution (McKee et al. 1993)
                spi = atmos.standardized_precipitation_index(
                    pr=precip_ds.pr,
                    freq='MS',              # Monthly frequency (required for SPI)
                    window=window,          # N-month window
                    dist='gamma',           # Gamma distribution (standard)
                    method='ML',            # Maximum likelihood fitting
                    cal_start=cal_start,    # 30-year calibration period
                    cal_end=cal_end
                )

                indices[var_name] = spi

                # Enhance metadata for CF-compliance
                indices[var_name].attrs['units'] = '1'  # Dimensionless
                indices[var_name].attrs['long_name'] = f'{window}-Month Standardized Precipitation Index'
                indices[var_name].attrs['description'] = f'Standardized precipitation index over {window}-month window using gamma distribution (McKee et al. 1993)'
                indices[var_name].attrs['calibration_period'] = f'{cal_start} to {cal_end}'
                indices[var_name].attrs['distribution'] = 'gamma'
                indices[var_name].attrs['method'] = 'ML'
                indices[var_name].attrs['interpretation'] = 'SPI < -2.0: Extreme drought, -1.5 to -1.0: Moderate drought, -1.0 to 1.0: Near normal, > 2.0: Extremely wet'

            except Exception as e:
                logger.error(f"Failed to calculate {var_name}: {e}")
                import traceback
                logger.error(traceback.format_exc())

        return indices

    def calculate_dry_spell_indices(self, precip_ds: xr.Dataset) -> dict:
        """
        Calculate dry spell and consecutive dry days indices.

        Args:
            precip_ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated dry spell indices (4 indices)
        """
        indices = {}

        if 'pr' not in precip_ds:
            logger.warning("Precipitation variable 'pr' not found, skipping dry spell indices")
            return indices

        # 1. Maximum Consecutive Dry Days (ETCCDI standard)
        try:
            logger.info("  - Calculating maximum consecutive dry days (CDD)...")
            indices['cdd'] = atmos.maximum_consecutive_dry_days(
                pr=precip_ds.pr,
                thresh='1.0 mm/day',
                freq='YS'
            )
            # Metadata already CF-compliant from xclim
        except Exception as e:
            logger.error(f"Failed to calculate cdd: {e}")

        # Note: dry_spell_frequency and dry_spell_total_length have unit compatibility issues
        # Skipped for now - can be added in future updates

        # 4. Dry Days (simple count)
        try:
            logger.info("  - Calculating dry days...")
            indices['dry_days'] = atmos.dry_days(
                pr=precip_ds.pr,
                thresh='1.0 mm d-1',  # CF-compliant units
                freq='YS'
            )
            # Metadata already CF-compliant from xclim
        except Exception as e:
            logger.error(f"Failed to calculate dry_days: {e}")

        return indices

    def calculate_precip_intensity_indices(self, precip_ds: xr.Dataset) -> dict:
        """
        Calculate precipitation intensity and distribution indices.

        Args:
            precip_ds: Dataset with precipitation variable (pr)

        Returns:
            Dictionary of calculated precipitation intensity indices (3 indices)
        """
        indices = {}

        if 'pr' not in precip_ds:
            logger.warning("Precipitation variable 'pr' not found, skipping intensity indices")
            return indices

        # 1. Simple Daily Intensity Index (ETCCDI standard)
        try:
            logger.info("  - Calculating simple daily intensity index (SDII)...")
            indices['sdii'] = atmos.daily_pr_intensity(
                pr=precip_ds.pr,
                thresh='1.0 mm d-1',  # CF-compliant units
                freq='YS'
            )
            # Metadata already CF-compliant from xclim
        except Exception as e:
            logger.error(f"Failed to calculate sdii: {e}")

        # Note: max_7day_pr_intensity has unit conversion issues in xclim
        # Skipped for now - can be added in future updates

        # 3. Fraction of Heavy Precipitation (requires baseline percentiles)
        if 'pr_75p_threshold' in self.baseline_percentiles:
            try:
                logger.info("  - Calculating fraction of heavy precipitation...")
                indices['fraction_heavy_precip'] = atmos.fraction_over_precip_thresh(
                    pr=precip_ds.pr,
                    pr_per=self.baseline_percentiles['pr_75p_threshold'],
                    freq='YS'
                )
                indices['fraction_heavy_precip'].attrs['long_name'] = 'Fraction of Heavy Precipitation'
                indices['fraction_heavy_precip'].attrs['description'] = 'Fraction of annual precipitation from heavy events (>75th percentile)'
                indices['fraction_heavy_precip'].attrs['baseline_period'] = '1981-2000'
            except Exception as e:
                logger.error(f"Failed to calculate fraction_heavy_precip: {e}")
        else:
            logger.warning("Skipping fraction_heavy_precip (baseline pr_75p_threshold not available)")

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
        precip_ds = xr.open_zarr(self.precip_zarr, chunks=self.chunk_config)

        # For SPI calculation, need to load full calibration period (1981-2010) plus target years
        # This ensures proper gamma distribution fitting
        spi_start_year = min(start_year, 1981)  # Always include calibration period
        logger.info(f"  Loading {spi_start_year}-{end_year} for SPI calibration...")
        precip_ds_full = precip_ds.sel(time=slice(f'{spi_start_year}-01-01', f'{end_year}-12-31'))

        # Also keep just target years for annual indices
        precip_ds = precip_ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

        # Rename precipitation variable for xclim compatibility
        if 'ppt' in precip_ds:
            precip_ds = precip_ds.rename({'ppt': 'pr'})
            logger.debug("Renamed ppt to pr")
            precip_ds['pr'].attrs['units'] = 'mm d-1'  # Use CF-compliant format
            precip_ds['pr'].attrs['standard_name'] = 'precipitation_flux'

        # Also prepare full dataset for SPI
        if 'ppt' in precip_ds_full:
            precip_ds_full = precip_ds_full.rename({'ppt': 'pr'})
            precip_ds_full['pr'].attrs['units'] = 'mm d-1'
            precip_ds_full['pr'].attrs['standard_name'] = 'precipitation_flux'

        # Calculate SPI indices (5 indices) - uses full calibration period
        logger.info("Calculating SPI indices (5 windows)...")
        spi_indices = self.calculate_spi_indices(precip_ds_full)

        # Filter SPI results to only target years
        if start_year > 1981:
            logger.info(f"  Filtering SPI results to {start_year}-{end_year}...")
            for key, value in spi_indices.items():
                spi_indices[key] = value.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))

        # Compute SPI results immediately using threaded scheduler
        logger.info("  Computing SPI results (this may take a few minutes)...")
        with dask.config.set(scheduler='threads'):
            for key in spi_indices.keys():
                spi_indices[key] = spi_indices[key].compute()

        logger.info(f"  Calculated {len(spi_indices)} SPI indices")

        # Calculate dry spell indices (4 indices)
        logger.info("Calculating dry spell indices...")
        dry_spell_indices = self.calculate_dry_spell_indices(precip_ds)
        logger.info(f"  Calculated {len(dry_spell_indices)} dry spell indices")

        # Calculate precipitation intensity indices (3 indices)
        logger.info("Calculating precipitation intensity indices...")
        intensity_indices = self.calculate_precip_intensity_indices(precip_ds)
        logger.info(f"  Calculated {len(intensity_indices)} intensity indices")

        # Merge all indices
        all_indices = {**spi_indices, **dry_spell_indices, **intensity_indices}
        logger.info(f"  Total: {len(all_indices)} drought indices")
        logger.info(f"    SPI: {len(spi_indices)}, Dry Spell: {len(dry_spell_indices)}, Intensity: {len(intensity_indices)}")

        if not all_indices:
            logger.warning("No indices calculated")
            return None

        # Combine indices into dataset
        logger.info(f"Combining {len(all_indices)} indices into dataset...")
        result_ds = xr.Dataset(all_indices)

        # Add global metadata
        result_ds.attrs['creation_date'] = datetime.now().isoformat()
        result_ds.attrs['software'] = 'xclim-timber drought pipeline v1.0 (Phase 10)'
        result_ds.attrs['time_range'] = f'{start_year}-{end_year}'
        result_ds.attrs['indices_count'] = len(all_indices)
        result_ds.attrs['phase'] = 'Phase 10: Drought Indices (+9 indices, total 77)'
        result_ds.attrs['spi_calibration_period'] = '1981-01-01 to 2010-12-31'
        result_ds.attrs['spi_distribution'] = 'gamma (McKee et al. 1993)'
        result_ds.attrs['spi_method'] = 'ML (Maximum Likelihood)'
        result_ds.attrs['note'] = 'Comprehensive drought monitoring indices. SPI uses 30-year calibration (1981-2010). Dry spell threshold: 1mm/day (ETCCDI standard). Baseline percentiles: 1981-2000.'

        # Save output
        output_file = output_dir / f'drought_indices_{start_year}_{end_year}.nc'
        logger.info(f"Saving to {output_file}...")

        with dask.config.set(scheduler='threads'):
            encoding = {}
            for var_name in result_ds.data_vars:
                # SPI indices have monthly frequency, different chunk size
                if 'spi_' in var_name:
                    encoding[var_name] = {
                        'zlib': True,
                        'complevel': 4,
                        'chunksizes': (12, 69, 281)  # ~1 year of monthly data
                    }
                else:
                    # Annual indices
                    encoding[var_name] = {
                        'zlib': True,
                        'complevel': 4,
                        'chunksizes': (1, 69, 281)  # 1 year chunks
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
        logger.info("DROUGHT INDICES PIPELINE")
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
        description="Drought Indices Pipeline: Calculate 12 drought and water deficit indices (Phase 10)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Indices calculated (9 total):
  SPI (5): Standardized Precipitation Index at 1, 3, 6, 12, 24-month windows
  Dry Spell (2): Maximum consecutive dry days (CDD), total dry days count
  Intensity (2): SDII (simple daily intensity), fraction of heavy precipitation

Examples:
  # Process default period (1981-2024)
  python drought_pipeline.py

  # Process single year
  python drought_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python drought_pipeline.py --output-dir ./results
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
    pipeline = DroughtPipeline(
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

#!/usr/bin/env python3
"""
Humidity indices pipeline for xclim-timber.
Efficiently processes humidity-based climate indices using Zarr streaming.
Calculates 8 humidity indices including vapor pressure deficit, dewpoint, and moisture stress metrics.
"""

import logging
import sys
from typing import Dict

import xarray as xr

from core import BasePipeline, PipelineConfig, PipelineCLI

logger = logging.getLogger(__name__)


class HumidityPipeline(BasePipeline):
    """
    Memory-efficient humidity indices pipeline using Zarr streaming.
    Processes 8 humidity indices without loading full dataset into memory.

    Indices:
    - Dewpoint statistics (3): Mean, min, max dewpoint temperature
    - Humidity thresholds (1): Humid days (dewpoint > 18°C)
    - VPD statistics (2): Mean vpdmax, mean vpdmin
    - VPD thresholds (2): Extreme VPD days (>4 kPa), low VPD days (<0.5 kPa)
    """

    def __init__(self, **kwargs):
        """
        Initialize the pipeline.

        Args:
            **kwargs: Additional arguments passed to BasePipeline (chunk_years, enable_dashboard)
        """
        # Initialize BasePipeline with humidity Zarr store
        super().__init__(
            zarr_paths={'humidity': PipelineConfig.HUMIDITY_ZARR},
            chunk_config=PipelineConfig.DEFAULT_CHUNKS,
            **kwargs
        )

    def _preprocess_datasets(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """
        Preprocess humidity datasets (rename variables, fix units).

        Args:
            datasets: Dictionary with 'humidity' dataset

        Returns:
            Preprocessed datasets dictionary
        """
        humidity_ds = datasets['humidity']

        # Rename humidity variables for consistency
        humidity_ds = self._rename_variables(humidity_ds, PipelineConfig.HUMIDITY_RENAME_MAP)

        # Fix units for humidity variables
        humidity_ds = self._fix_units(humidity_ds, PipelineConfig.HUMIDITY_UNIT_FIXES)

        # Add CF standard names
        for var_name in ['tdew', 'vpdmax', 'vpdmin']:
            if var_name in humidity_ds:
                humidity_ds[var_name].attrs['standard_name'] = PipelineConfig.CF_STANDARD_NAMES.get(
                    var_name, ''
                )

        datasets['humidity'] = humidity_ds
        return datasets

    def calculate_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Calculate all humidity indices.

        Args:
            datasets: Dictionary with 'humidity' dataset

        Returns:
            Dictionary of calculated indices
        """
        ds = datasets['humidity']
        return self.calculate_humidity_indices(ds)

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
                    data_array.attrs['units'] = '1'  # Dimensionless count
                    data_array.attrs['standard_name'] = 'number_of_days'
                else:
                    data_array.attrs['units'] = 'kPa'
                    data_array.attrs['standard_name'] = 'vapor_pressure_deficit'
            elif 'humid_days' in key:
                data_array.attrs['units'] = '1'  # Dimensionless count
                data_array.attrs['standard_name'] = 'number_of_days_with_high_humidity'

        return indices_dict

    def _add_global_metadata(
        self,
        result_ds: xr.Dataset,
        start_year: int,
        end_year: int,
        pipeline_name: str,
        indices_count: int,
        additional_attrs: dict = None
    ) -> xr.Dataset:
        """
        Override to add humidity-specific metadata.

        Args:
            result_ds: Result dataset
            start_year: Start year
            end_year: End year
            pipeline_name: Name of the pipeline
            indices_count: Number of indices calculated
            additional_attrs: Additional attributes to add

        Returns:
            Dataset with global metadata
        """
        # Call base implementation
        result_ds = super()._add_global_metadata(
            result_ds, start_year, end_year, pipeline_name, indices_count, additional_attrs
        )

        # Add humidity-specific metadata
        result_ds.attrs['indices_description'] = (
            '8 humidity indices: dewpoint statistics (3), humid days (1), '
            'VPD statistics (2), VPD threshold days (2)'
        )
        result_ds.attrs['thresholds'] = (
            'humid_days: dewpoint > 18°C; extreme_vpd_days: vpdmax > 4 kPa; '
            'low_vpd_days: vpdmin < 0.5 kPa'
        )
        result_ds.attrs['note'] = (
            'Count indices stored as dimensionless (units=1) to prevent CF timedelta encoding. '
            'All statistics computed from daily PRISM humidity data.'
        )

        return result_ds


def main():
    """Main entry point with command-line interface."""
    indices_list = """
  Dewpoint statistics (3): Mean, min, max dewpoint temperature
  Humidity thresholds (1): Humid days (dewpoint > 18°C)
  VPD statistics (2): Mean vpdmax, mean vpdmin
  VPD thresholds (2): Extreme VPD days (>4 kPa), low VPD days (<0.5 kPa)
"""

    examples = """
  # Process default period (1981-2024)
  python humidity_pipeline.py

  # Process single year
  python humidity_pipeline.py --start-year 2023 --end-year 2023

  # Process with custom output directory
  python humidity_pipeline.py --output-dir ./results
"""

    parser = PipelineCLI.create_parser(
        "Humidity Indices",
        "Calculate 8 humidity-based climate indices",
        indices_list,
        examples
    )

    args = parser.parse_args()

    # Handle common setup (logging, warnings)
    PipelineCLI.handle_common_setup(args)

    # Validate year range
    PipelineCLI.validate_years(args.start_year, args.end_year)

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

#!/usr/bin/env python3
"""
Human comfort indices pipeline for xclim-timber.
Efficiently processes human comfort climate indices using Zarr streaming.
Calculates 3 comfort indices combining temperature and humidity data.
"""

import logging
import sys
from pathlib import Path
from typing import Dict

import xarray as xr
import xclim.indicators.atmos as atmos

from core import BasePipeline, PipelineConfig, PipelineCLI, SpatialTilingMixin

logger = logging.getLogger(__name__)


class HumanComfortPipeline(BasePipeline, SpatialTilingMixin):
    """
    Memory-efficient human comfort indices pipeline using Zarr streaming.
    Processes 3 comfort indices combining temperature and humidity without loading full dataset into memory.

    Indices:
    - Relative Humidity: Derived from dewpoint temperature
    - Heat Index: Combined heat and humidity stress (US National Weather Service)
    - Humidex: Canadian measure of perceived temperature (Meteorological Service of Canada)

    Note:
        Heat stress indices use annual MAXIMUM (not mean) to capture
        worst-case conditions following WMO standards for heat stress assessment.
    """

    def __init__(self, n_tiles: int = 4, **kwargs):
        """
        Initialize the pipeline with parallel spatial tiling.

        Args:
            n_tiles: Number of spatial tiles (2, 4, or 8, default: 4 for quadrants)
            **kwargs: Additional arguments passed to BasePipeline (chunk_years, enable_dashboard)
        """
        # Initialize BasePipeline with both temperature and humidity
        BasePipeline.__init__(
            self,
            zarr_paths={
                'temperature': PipelineConfig.TEMP_ZARR,
                'humidity': PipelineConfig.HUMIDITY_ZARR
            },
            chunk_config=PipelineConfig.DEFAULT_CHUNKS,
            **kwargs
        )

        # Initialize SpatialTilingMixin
        SpatialTilingMixin.__init__(self, n_tiles=n_tiles)

    def _preprocess_datasets(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """
        Preprocess and merge temperature and humidity datasets.

        Args:
            datasets: Dictionary with 'temperature' and 'humidity' datasets

        Returns:
            Dictionary with single 'combined' dataset
        """
        temp_ds = datasets['temperature']
        humid_ds = datasets['humidity']

        # Select only needed variables (memory efficiency)
        if 'tmean' in temp_ds:
            temp_ds = temp_ds[['tmean']]

        if 'tdmean' in humid_ds:
            humid_ds = humid_ds[['tdmean']]

        # Rename variables for xclim compatibility
        temp_ds = self._rename_variables(temp_ds, {'tmean': 'tas'})
        humid_ds = self._rename_variables(humid_ds, {'tdmean': 'tdew'})

        # Fix units
        temp_ds = self._fix_units(temp_ds, {'tas': 'degC'})
        humid_ds = self._fix_units(humid_ds, {'tdew': 'degC'})

        # Add CF standard names
        if 'tas' in temp_ds:
            temp_ds['tas'].attrs['standard_name'] = PipelineConfig.CF_STANDARD_NAMES.get(
                'tas', 'air_temperature'
            )
        if 'tdew' in humid_ds:
            humid_ds['tdew'].attrs['standard_name'] = PipelineConfig.CF_STANDARD_NAMES.get(
                'tdew', 'dew_point_temperature'
            )

        # Validate coordinate alignment
        logger.info("Validating coordinate alignment...")
        self._validate_coordinates(temp_ds, humid_ds, ['time', 'lat', 'lon'])

        # Merge datasets
        logger.info("Merging temperature and humidity datasets...")
        combined_ds = xr.merge([temp_ds, humid_ds])

        return {'combined': combined_ds}

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
            # Check existence
            if coord not in ds1.coords:
                raise ValueError(
                    f"Coordinate '{coord}' not found in temperature dataset. "
                    f"Available coordinates: {list(ds1.coords.keys())}"
                )
            if coord not in ds2.coords:
                raise ValueError(
                    f"Coordinate '{coord}' not found in humidity dataset. "
                    f"Available coordinates: {list(ds2.coords.keys())}"
                )

            # Check shapes match
            if ds1[coord].shape != ds2[coord].shape:
                raise ValueError(
                    f"Coordinate '{coord}' shape mismatch: "
                    f"temperature {ds1[coord].shape} vs humidity {ds2[coord].shape}"
                )

            # Check values match (with tolerance for spatial coordinates)
            if coord in ['lat', 'lon']:
                # Use floating-point tolerance for spatial coordinates
                if not np.allclose(ds1[coord].values, ds2[coord].values, rtol=1e-6):
                    max_diff = float(np.max(np.abs(ds1[coord].values - ds2[coord].values)))
                    raise ValueError(
                        f"Coordinate '{coord}' values mismatch. Max difference: {max_diff}"
                    )
            else:
                # Time coordinate must match exactly
                if not ds1[coord].equals(ds2[coord]):
                    raise ValueError(
                        f"Coordinate '{coord}' values don't match between datasets"
                    )

        logger.info(f"  ✓ All coordinates validated: {coord_names}")

    def calculate_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Calculate all human comfort indices.

        Args:
            datasets: Dictionary with 'combined' dataset containing both temp and humidity

        Returns:
            Dictionary of calculated indices
        """
        combined_ds = datasets['combined']

        # Calculate all human comfort indices
        indices = self._calculate_comfort_indices(combined_ds)
        return indices

    def _calculate_comfort_indices(self, ds: xr.Dataset) -> Dict[str, xr.DataArray]:
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

    def _process_single_tile(
        self,
        ds: xr.Dataset,
        lat_slice: slice,
        lon_slice: slice,
        tile_name: str
    ) -> Dict[str, xr.DataArray]:
        """
        Process a single spatial tile (human-comfort-specific override).

        This override ensures the combined dataset is passed with the correct key
        to calculate_indices().

        Args:
            ds: Full combined dataset
            lat_slice: Latitude slice for this tile
            lon_slice: Longitude slice for this tile
            tile_name: Name of this tile (for logging)

        Returns:
            Dictionary of calculated indices for this tile
        """
        logger.info(f"  Processing tile: {tile_name}")

        # Select spatial subset
        tile_ds = ds.isel(lat=lat_slice, lon=lon_slice)

        # Calculate indices for this tile
        # Pass with 'combined' key to match calculate_indices() expectations
        tile_indices = self.calculate_indices({'combined': tile_ds})

        return tile_indices

    def _calculate_all_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Override to implement spatial tiling for human comfort indices.

        Uses the SpatialTilingMixin to process the dataset in parallel spatial tiles
        for better memory efficiency and performance, then merges the results.

        Args:
            datasets: Dictionary with 'combined' dataset

        Returns:
            Dictionary mapping index name to calculated DataArray
        """
        combined_ds = datasets['combined']

        # Define expected dimensions for validation
        # PRISM CONUS dimensions: 621 lat × 1405 lon
        # Time dimension after annual aggregation (freq='YS'):
        #   - Single year: time=1
        #   - Multiple years: time=num_years
        num_years = len(combined_ds.time.groupby('time.year').groups)
        expected_dims = {
            'time': num_years,  # Number of years (aggregated to annual)
            'lat': 621,
            'lon': 1405
        }

        # Create temporary directory for tiles
        output_dir = Path('./outputs')
        output_dir.mkdir(parents=True, exist_ok=True)

        # Use the mixin's spatial tiling functionality
        all_indices = self.process_with_spatial_tiling(
            ds=combined_ds,
            output_dir=output_dir,
            expected_dims=expected_dims
        )

        return all_indices

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
        Override to add human-comfort-specific metadata.

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

        # Add human-comfort-specific metadata
        result_ds.attrs['phase'] = 'Human Comfort Indices (+3 indices)'
        result_ds.attrs['processing'] = f'Parallel processing of {self.n_tiles} spatial tiles'
        result_ds.attrs['note'] = (
            'Processed with parallel spatial tiling for optimal memory and performance. '
            'Heat stress indices use annual maximum to capture worst-case conditions per year. '
            'Follows WMO and Canadian MSC standards for heat stress assessment.'
        )

        return result_ds


def main():
    """Main entry point with command-line interface."""
    indices_list = """
  1. Relative Humidity - Derived from dewpoint temperature (%)
  2. Heat Index - Combined heat and humidity stress (US National Weather Service)
  3. Humidex - Canadian measure of perceived temperature (Meteorological Service of Canada)
"""

    examples = """
  # Process with 2 tiles (east/west split)
  python human_comfort_pipeline.py --n-tiles 2

  # Process with 4 tiles (quadrants, default)
  python human_comfort_pipeline.py --n-tiles 4

  # Process single year
  python human_comfort_pipeline.py --start-year 2023 --end-year 2023
"""

    parser = PipelineCLI.create_parser(
        "Human Comfort Indices",
        "Calculate 3 human comfort indices combining temperature and humidity",
        indices_list,
        examples
    )

    parser.add_argument(
        '--n-tiles',
        type=int,
        default=4,
        choices=[2, 4, 8],
        help='Number of spatial tiles: 2 (east/west), 4 (quadrants), or 8 (octants) (default: 4)'
    )

    args = parser.parse_args()

    # Handle common setup (logging, warnings)
    PipelineCLI.handle_common_setup(args)

    # Validate year range
    PipelineCLI.validate_years(args.start_year, args.end_year)

    # Create and run pipeline
    pipeline = HumanComfortPipeline(
        n_tiles=args.n_tiles,
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

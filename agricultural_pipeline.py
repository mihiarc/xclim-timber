#!/usr/bin/env python3
"""
Agricultural indices pipeline for xclim-timber.
Efficiently processes agricultural and growing season indices using Zarr streaming.
Calculates 5 agricultural indices (Phase 8).
"""

import logging
import sys
from pathlib import Path
from typing import Dict

import xarray as xr
import xclim.indicators.atmos as atmos

from core import BasePipeline, PipelineConfig, PipelineCLI, SpatialTilingMixin

logger = logging.getLogger(__name__)


class AgriculturalPipeline(BasePipeline, SpatialTilingMixin):
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

    def __init__(self, n_tiles: int = 4, **kwargs):
        """
        Initialize the pipeline with parallel spatial tiling.

        Args:
            n_tiles: Number of spatial tiles (2, 4, or 8, default: 4 for quadrants)
            **kwargs: Additional arguments passed to BasePipeline (chunk_years, enable_dashboard)
        """
        # Initialize BasePipeline with both temperature and precipitation
        BasePipeline.__init__(
            self,
            zarr_paths={
                'temperature': PipelineConfig.TEMP_ZARR,
                'precipitation': PipelineConfig.PRECIP_ZARR
            },
            chunk_config=PipelineConfig.DEFAULT_CHUNKS,
            **kwargs
        )

        # Initialize SpatialTilingMixin
        SpatialTilingMixin.__init__(self, n_tiles=n_tiles)

    def _preprocess_datasets(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """
        Preprocess and merge temperature and precipitation datasets.

        Args:
            datasets: Dictionary with 'temperature' and 'precipitation' datasets

        Returns:
            Dictionary with single 'combined' dataset
        """
        temp_ds = datasets['temperature']
        precip_ds = datasets['precipitation']

        # Select only needed variables (memory efficiency)
        temp_vars = ['tmean', 'tmax', 'tmin']
        available_temp_vars = [v for v in temp_vars if v in temp_ds]
        if available_temp_vars:
            temp_ds = temp_ds[available_temp_vars]

        if 'ppt' in precip_ds:
            precip_ds = precip_ds[['ppt']]

        # Rename temperature variables for xclim compatibility
        temp_rename_map = {
            'tmean': 'tas',
            'tmax': 'tasmax',
            'tmin': 'tasmin'
        }
        temp_ds = self._rename_variables(temp_ds, temp_rename_map)

        # Rename precipitation variable
        precip_ds = self._rename_variables(precip_ds, PipelineConfig.PRECIP_RENAME_MAP)

        # Fix units
        temp_unit_fixes = {
            'tas': 'degC',
            'tasmax': 'degC',
            'tasmin': 'degC'
        }
        temp_ds = self._fix_units(temp_ds, temp_unit_fixes)
        precip_ds = self._fix_units(precip_ds, PipelineConfig.PRECIP_UNIT_FIXES)

        # Add CF standard names
        if 'tas' in temp_ds:
            temp_ds['tas'].attrs['standard_name'] = PipelineConfig.CF_STANDARD_NAMES.get(
                'tas', 'air_temperature'
            )
        if 'tasmax' in temp_ds:
            temp_ds['tasmax'].attrs['standard_name'] = PipelineConfig.CF_STANDARD_NAMES.get(
                'tasmax', 'air_temperature'
            )
        if 'tasmin' in temp_ds:
            temp_ds['tasmin'].attrs['standard_name'] = PipelineConfig.CF_STANDARD_NAMES.get(
                'tasmin', 'air_temperature'
            )
        if 'pr' in precip_ds:
            precip_ds['pr'].attrs['standard_name'] = PipelineConfig.CF_STANDARD_NAMES.get(
                'pr', 'precipitation_flux'
            )

        # Validate coordinate alignment
        logger.info("Validating coordinate alignment...")
        self._validate_coordinates(temp_ds, precip_ds, ['time', 'lat', 'lon'])

        # Merge datasets
        logger.info("Merging temperature and precipitation datasets...")
        combined_ds = xr.merge([temp_ds, precip_ds])

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
                    f"Coordinate '{coord}' not found in first dataset. "
                    f"Available coordinates: {list(ds1.coords.keys())}"
                )
            if coord not in ds2.coords:
                raise ValueError(
                    f"Coordinate '{coord}' not found in second dataset. "
                    f"Available coordinates: {list(ds2.coords.keys())}"
                )

            # Check shapes match
            if ds1[coord].shape != ds2[coord].shape:
                raise ValueError(
                    f"Coordinate '{coord}' shape mismatch: "
                    f"{ds1[coord].shape} vs {ds2[coord].shape}"
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
        Calculate all agricultural indices.

        Args:
            datasets: Dictionary with 'combined' dataset containing both temp and precip

        Returns:
            Dictionary of calculated indices
        """
        combined_ds = datasets['combined']

        # Calculate all agricultural indices
        indices = self._calculate_agricultural_indices(combined_ds)
        return indices

    def _calculate_agricultural_indices(self, ds: xr.Dataset) -> Dict[str, xr.DataArray]:
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
            ds: Combined dataset with temperature (tas, tasmax, tasmin) and precipitation (pr)

        Returns:
            Dictionary of calculated agricultural indices
        """
        indices = {}

        # 1. Growing Season Length (ETCCDI standard)
        if 'tas' in ds:
            try:
                logger.info("  - Calculating growing season length...")
                indices['growing_season_length'] = atmos.growing_season_length(
                    tas=ds.tas,
                    thresh='5.0 degC',
                    window=6,
                    mid_date='07-01',
                    freq='YS'
                )
                # Fix units metadata for CF-compliance
                indices['growing_season_length'].attrs['units'] = 'days'
                indices['growing_season_length'].attrs['long_name'] = 'Growing Season Length'
                indices['growing_season_length'].attrs['description'] = (
                    'Number of days between first and last occurrence of 6+ consecutive days '
                    'with temperature above 5°C'
                )
            except Exception as e:
                logger.error(f"Failed to calculate growing_season_length: {e}")

        # 2. Potential Evapotranspiration (Baier-Robertson 1965 method)
        if 'tasmin' in ds and 'tasmax' in ds:
            try:
                logger.info("  - Calculating potential evapotranspiration (BR65 method)...")

                # Extract latitude from coordinates
                lat = ds.lat

                # Calculate daily PET using Baier-Robertson method (temperature-only)
                from xclim.indices import potential_evapotranspiration as pet_index
                pet_daily = pet_index(
                    tasmin=ds.tasmin,
                    tasmax=ds.tasmax,
                    lat=lat,
                    method='BR65'
                )

                # Aggregate to annual sum
                indices['potential_evapotranspiration'] = pet_daily.resample(time='YS').sum()

                # Fix metadata for CF-compliance
                indices['potential_evapotranspiration'].attrs['long_name'] = (
                    'Annual Potential Evapotranspiration (BR65)'
                )
                indices['potential_evapotranspiration'].attrs['description'] = (
                    'Annual sum of potential evapotranspiration using Baier-Robertson 1965 '
                    'method (temperature-only)'
                )
                indices['potential_evapotranspiration'].attrs['standard_name'] = (
                    'water_evapotranspiration_amount'
                )
            except Exception as e:
                logger.error(f"Failed to calculate potential_evapotranspiration: {e}")

        # 3. Corn Heat Units (crop-specific)
        if 'tasmin' in ds and 'tasmax' in ds:
            try:
                logger.info("  - Calculating corn heat units...")

                # Calculate daily CHU
                from xclim.indices import corn_heat_units as chu_index
                chu_daily = chu_index(
                    tasmin=ds.tasmin,
                    tasmax=ds.tasmax,
                    thresh_tasmin='4.44 degC',
                    thresh_tasmax='10 degC'
                )

                # Aggregate to annual sum
                indices['corn_heat_units'] = chu_daily.resample(time='YS').sum()

                # Fix units metadata for CF-compliance
                indices['corn_heat_units'].attrs['units'] = '1'  # Dimensionless index
                indices['corn_heat_units'].attrs['long_name'] = 'Annual Corn Heat Units'
                indices['corn_heat_units'].attrs['description'] = (
                    'Annual sum of corn heat units for crop development and maturity prediction '
                    '(USDA standard)'
                )
            except Exception as e:
                logger.error(f"Failed to calculate corn_heat_units: {e}")

        # 4. Thawing Degree Days (permafrost monitoring)
        if 'tas' in ds:
            try:
                logger.info("  - Calculating thawing degree days...")
                indices['thawing_degree_days'] = atmos.growing_degree_days(
                    tas=ds.tas,
                    thresh='0 degC',
                    freq='YS'
                )
                # Update metadata to reflect thawing focus
                indices['thawing_degree_days'].attrs['long_name'] = 'Thawing Degree Days'
                indices['thawing_degree_days'].attrs['description'] = (
                    'Sum of degree-days above 0°C (permafrost monitoring, spring melt timing)'
                )
                indices['thawing_degree_days'].attrs['standard_name'] = (
                    'integral_of_air_temperature_excess_wrt_time'
                )
            except Exception as e:
                logger.error(f"Failed to calculate thawing_degree_days: {e}")

        # 5. Growing Season Precipitation (water availability during growing season)
        if 'pr' in ds:
            try:
                logger.info("  - Calculating growing season precipitation...")

                # Select growing season months (April-October, typical for northern hemisphere)
                pr_growing = ds.pr.where(
                    (ds.pr.time.dt.month >= 4) & (ds.pr.time.dt.month <= 10)
                )

                # Sum precipitation during growing season
                indices['growing_season_precipitation'] = pr_growing.resample(time='YS').sum()

                # Fix units metadata
                indices['growing_season_precipitation'].attrs['units'] = 'mm'
                indices['growing_season_precipitation'].attrs['long_name'] = (
                    'Growing Season Precipitation'
                )
                indices['growing_season_precipitation'].attrs['description'] = (
                    'Total precipitation during growing season (April-October)'
                )
                indices['growing_season_precipitation'].attrs['standard_name'] = (
                    'precipitation_amount'
                )
            except Exception as e:
                logger.error(f"Failed to calculate growing_season_precipitation: {e}")

        return indices

    def _process_single_tile(
        self,
        ds: xr.Dataset,
        lat_slice: slice,
        lon_slice: slice,
        tile_name: str
    ) -> Dict[str, xr.DataArray]:
        """
        Process a single spatial tile (agricultural-specific override).

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
        Override to implement spatial tiling for agricultural indices.

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
        Override to add agricultural-specific metadata.

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

        # Add agricultural-specific metadata
        result_ds.attrs['phase'] = 'Phase 8: Agricultural & Growing Season Indices (+5 indices)'
        result_ds.attrs['processing'] = f'Parallel processing of {self.n_tiles} spatial tiles'
        result_ds.attrs['note'] = (
            'Processed with parallel spatial tiling for optimal memory and performance. '
            'PET uses Baier-Robertson (1965) method (temperature-only). '
            'No baseline percentiles required.'
        )

        return result_ds


def main():
    """Main entry point with command-line interface."""
    indices_list = """
  1. Growing Season Length - Days suitable for plant growth (ETCCDI standard)
  2. Potential Evapotranspiration - Annual water demand (Baier-Robertson 1965 method)
  3. Corn Heat Units - Crop-specific temperature index (USDA standard)
  4. Thawing Degree Days - Spring warming accumulation (permafrost monitoring)
  5. Growing Season Precipitation - Water availability during growing season (April-October)
"""

    examples = """
  # Process with 2 tiles (east/west split)
  python agricultural_pipeline.py --n-tiles 2

  # Process with 4 tiles (quadrants, default)
  python agricultural_pipeline.py --n-tiles 4

  # Process single year
  python agricultural_pipeline.py --start-year 2023 --end-year 2023
"""

    parser = PipelineCLI.create_parser(
        "Agricultural Indices",
        "Calculate 5 agricultural and growing season indices (Phase 8)",
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
    pipeline = AgriculturalPipeline(
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

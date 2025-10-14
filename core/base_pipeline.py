#!/usr/bin/env python3
"""
Base pipeline class for xclim-timber.

Provides common functionality for all climate index pipelines, eliminating
~2,800 lines of duplicate code across 7 pipeline implementations.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import logging
import warnings

import xarray as xr
import dask
import psutil
import os

logger = logging.getLogger(__name__)


class BasePipeline(ABC):
    """
    Abstract base class for climate index pipelines.

    Provides common functionality for:
    - Zarr data loading
    - Variable renaming and unit fixing
    - Temporal chunking
    - NetCDF output with compression
    - Memory and performance tracking
    - Error handling

    Subclasses must implement:
    - calculate_indices(): Compute indices specific to the pipeline
    """

    def __init__(
        self,
        zarr_paths: Dict[str, str],
        chunk_config: Optional[Dict[str, int]] = None,
        chunk_years: int = 1,
        enable_dashboard: bool = False
    ):
        """
        Initialize pipeline with common configuration.

        Args:
            zarr_paths: Dictionary mapping data type to Zarr store path
                       e.g., {'temperature': '/path/to/temp.zarr'}
            chunk_config: Dask chunk configuration for lat/lon/time
            chunk_years: Number of years to process per temporal chunk
            enable_dashboard: Whether to enable Dask dashboard (unused, threaded only)
        """
        self.zarr_paths = zarr_paths
        self.chunk_config = chunk_config or self._default_chunk_config()
        self.chunk_years = chunk_years
        self.enable_dashboard = enable_dashboard

    @staticmethod
    def _default_chunk_config() -> Dict[str, int]:
        """Default chunk configuration optimized for PRISM data."""
        return {
            'time': 365,   # One year of daily data
            'lat': 103,    # 621 / 103 = 6 chunks
            'lon': 201     # 1405 / 201 = 7 chunks
        }

    def setup_dask_client(self):
        """
        Initialize Dask scheduler.

        Uses threaded scheduler (no distributed client) for lower memory overhead
        and better compatibility with xclim operations.
        """
        logger.info("Using Dask threaded scheduler (no distributed client for memory efficiency)")

    @abstractmethod
    def calculate_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Calculate climate indices (implemented by subclasses).

        Args:
            datasets: Dictionary mapping data type to xarray Dataset
                     e.g., {'temperature': temp_ds, 'precipitation': precip_ds}

        Returns:
            Dictionary mapping index name to calculated DataArray
            e.g., {'tg_mean': tg_mean_array, 'frost_days': frost_days_array}
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement calculate_indices()"
        )

    def _calculate_all_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.DataArray]:
        """
        Calculate all indices with optional spatial tiling support.

        This method provides an extension point for pipelines that need spatial tiling
        (e.g., temperature pipeline with 2x2 or 4x4 tiling).

        Default implementation calls calculate_indices() directly.
        Override in subclasses for spatial tiling or other custom workflows.

        Args:
            datasets: Dictionary mapping data type to xarray Dataset

        Returns:
            Dictionary mapping index name to calculated DataArray
        """
        return self.calculate_indices(datasets)

    def _load_zarr_data(
        self,
        zarr_path: str,
        start_year: int,
        end_year: int
    ) -> xr.Dataset:
        """
        Load data from Zarr store for specified time range.

        Args:
            zarr_path: Path to Zarr store
            start_year: Start year
            end_year: End year

        Returns:
            xarray Dataset with selected time range
        """
        logger.debug(f"Loading Zarr data from {zarr_path}")
        ds = xr.open_zarr(zarr_path, chunks=self.chunk_config)

        # Select time range
        ds_subset = ds.sel(time=slice(f'{start_year}-01-01', f'{end_year}-12-31'))
        logger.debug(f"  Loaded {len(ds_subset.time)} time steps")

        return ds_subset

    def _rename_variables(
        self,
        ds: xr.Dataset,
        rename_map: Dict[str, str]
    ) -> xr.Dataset:
        """
        Rename variables for xclim compatibility.

        Args:
            ds: Dataset to rename
            rename_map: Dictionary mapping old names to new names

        Returns:
            Dataset with renamed variables
        """
        for old_name, new_name in rename_map.items():
            if old_name in ds and old_name != new_name:
                ds = ds.rename({old_name: new_name})
                logger.debug(f"Renamed {old_name} to {new_name}")

        return ds

    def _fix_units(
        self,
        ds: xr.Dataset,
        unit_fixes: Dict[str, str]
    ) -> xr.Dataset:
        """
        Fix variable units for CF compliance.

        Args:
            ds: Dataset to fix
            unit_fixes: Dictionary mapping variable name to correct unit

        Returns:
            Dataset with fixed units
        """
        for var_name, unit in unit_fixes.items():
            if var_name in ds:
                ds[var_name].attrs['units'] = unit

        return ds

    def _add_global_metadata(
        self,
        result_ds: xr.Dataset,
        start_year: int,
        end_year: int,
        pipeline_name: str,
        indices_count: int,
        additional_attrs: Optional[Dict] = None
    ) -> xr.Dataset:
        """
        Add global metadata to result dataset.

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
        result_ds.attrs['creation_date'] = datetime.now().isoformat()
        result_ds.attrs['software'] = f'xclim-timber {pipeline_name} v2.0 (refactored)'
        result_ds.attrs['time_range'] = f'{start_year}-{end_year}'
        result_ds.attrs['indices_count'] = indices_count

        if additional_attrs:
            result_ds.attrs.update(additional_attrs)

        return result_ds

    def _save_result(
        self,
        result_ds: xr.Dataset,
        output_file: Path,
        encoding_config: Optional[Dict] = None
    ):
        """
        Save result dataset to NetCDF with compression.

        Args:
            result_ds: Dataset to save
            output_file: Output file path
            encoding_config: Optional custom encoding configuration
        """
        logger.info(f"Saving to {output_file}...")

        with dask.config.set(scheduler='threads'):
            # Default encoding: compression for all variables
            encoding = encoding_config or {}
            if not encoding_config:
                # Calculate dynamic chunk sizes based on dataset dimensions
                # Use 1/9 of each spatial dimension for reasonable chunk sizes
                time_chunk = min(result_ds.sizes.get('time', 1), 1)
                lat_chunk = max(result_ds.sizes.get('lat', 69) // 9, 1)
                lon_chunk = max(result_ds.sizes.get('lon', 281) // 9, 1)

                for var_name in result_ds.data_vars:
                    encoding[var_name] = {
                        'zlib': True,
                        'complevel': 4,
                        'chunksizes': (time_chunk, lat_chunk, lon_chunk)
                    }

            result_ds.to_netcdf(
                output_file,
                engine='netcdf4',
                encoding=encoding
            )

    def process_time_chunk(
        self,
        start_year: int,
        end_year: int,
        output_dir: Path
    ) -> Optional[Path]:
        """
        Process a single time chunk.

        Standard workflow:
        1. Track initial memory
        2. Load data from Zarr stores
        3. Rename variables and fix units
        4. Call calculate_indices() (subclass-specific)
        5. Create result dataset
        6. Add metadata
        7. Save to NetCDF
        8. Report metrics

        Args:
            start_year: Start year for this chunk
            end_year: End year for this chunk
            output_dir: Output directory

        Returns:
            Path to output file, or None if no indices calculated
        """
        logger.info(f"\nProcessing chunk: {start_year}-{end_year}")

        # Track memory
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"Initial memory: {initial_memory:.1f} MB")

        # Load datasets
        datasets = {}
        for data_type, zarr_path in self.zarr_paths.items():
            logger.info(f"Loading {data_type} data...")
            datasets[data_type] = self._load_zarr_data(zarr_path, start_year, end_year)

        # Preprocess datasets (rename, fix units) - call subclass hook if exists
        if hasattr(self, '_preprocess_datasets'):
            datasets = self._preprocess_datasets(datasets)

        # Calculate indices (subclass implementation with optional spatial tiling)
        logger.info("Calculating indices...")
        all_indices = self._calculate_all_indices(datasets)
        logger.info(f"  Calculated {len(all_indices)} indices")

        if not all_indices:
            logger.warning("No indices calculated")
            return None

        # Combine indices into dataset
        logger.info(f"Combining {len(all_indices)} indices into dataset...")
        result_ds = xr.Dataset(all_indices)

        # Add metadata (call subclass hook if exists)
        pipeline_name = self.__class__.__name__.replace('Pipeline', '').lower()
        result_ds = self._add_global_metadata(
            result_ds,
            start_year,
            end_year,
            pipeline_name,
            len(all_indices)
        )

        # Save output - sanitize pipeline_name to prevent path traversal
        safe_pipeline_name = os.path.basename(pipeline_name)
        output_file = output_dir / f'{safe_pipeline_name}_indices_{start_year}_{end_year}.nc'
        self._save_result(result_ds, output_file)

        # Report metrics
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        logger.info(f"Final memory: {final_memory:.1f} MB (increase: {final_memory - initial_memory:.1f} MB)")

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

        Processes data in temporal chunks, handling the full time range
        by breaking it into manageable pieces.

        Args:
            start_year: Start year
            end_year: End year
            output_dir: Output directory path

        Returns:
            List of output file paths
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        pipeline_name = self.__class__.__name__.replace('Pipeline', '').upper()
        logger.info("=" * 60)
        logger.info(f"{pipeline_name} INDICES PIPELINE")
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

        return output_files

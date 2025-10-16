#!/usr/bin/env python3
"""
Spatial tiling mixin for memory-efficient climate index processing.

Provides reusable spatial tiling capabilities that can be mixed into any
climate pipeline to reduce memory footprint through parallel tile processing.
"""

import logging
import threading
from pathlib import Path
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import xarray as xr
import dask

logger = logging.getLogger(__name__)

# Global thread lock for NetCDF file writing
# HDF5/NetCDF4 backend is not fully thread-safe for concurrent writes
netcdf_write_lock = threading.Lock()


class SpatialTilingMixin:
    """
    Mixin to add spatial tiling capabilities to climate pipelines.

    Provides memory-efficient parallel processing by:
    - Splitting spatial domain into tiles (2, 4, or 8 tiles)
    - Processing each tile independently in parallel
    - Merging results with proper coordinate handling

    Memory Reduction:
    - 2 tiles: 50% memory reduction (east/west split)
    - 4 tiles: 75% memory reduction (2x2 quadrants)
    - 8 tiles: 87.5% memory reduction (2x4 or 4x2 grid)

    Usage:
        class MyPipeline(BasePipeline, SpatialTilingMixin):
            def __init__(self, **kwargs):
                BasePipeline.__init__(self, ...)
                SpatialTilingMixin.__init__(self, n_tiles=4)
    """

    def __init__(self, n_tiles: int = 4):
        """
        Initialize spatial tiling configuration.

        Args:
            n_tiles: Number of spatial tiles (1, 2, 4, or 8)
                    1 = no tiling (process full domain, no parallelism)
                    2 = east/west split (2x1 grid)
                    4 = quadrants (2x2 grid)
                    8 = octants (2x4 or 4x2 grid)
        """
        if n_tiles not in [1, 2, 4, 8]:
            raise ValueError(f"n_tiles must be 1, 2, 4, or 8, got {n_tiles}")

        self.n_tiles = n_tiles
        self.use_spatial_tiling = True

        # Generate unique tile ID to prevent file collisions when multiple pipelines run concurrently
        import os
        import uuid
        self._tile_id = f"{os.getpid()}_{uuid.uuid4().hex[:8]}"

    def _get_spatial_tiles(self, ds: xr.Dataset) -> List[Tuple[slice, slice, str]]:
        """
        Calculate spatial tile boundaries.

        Splits the spatial domain into tiles with proper alignment.
        Uses simple midpoint splitting to ensure equal-sized tiles.

        Args:
            ds: Dataset to tile

        Returns:
            List of tuples (lat_slice, lon_slice, tile_name)
            e.g., [(slice(0, 310), slice(0, 702), "northwest"), ...]

        Raises:
            ValueError: If dataset missing required dimensions or has empty dimensions
        """
        # Validate required dimensions exist
        if 'lat' not in ds.dims:
            raise ValueError(
                f"Dataset must have 'lat' dimension. "
                f"Available dimensions: {list(ds.dims.keys())}"
            )
        if 'lon' not in ds.dims:
            raise ValueError(
                f"Dataset must have 'lon' dimension. "
                f"Available dimensions: {list(ds.dims.keys())}"
            )

        lat_vals = ds.lat.values
        lon_vals = ds.lon.values

        # Validate dimensions are not empty
        if len(lat_vals) == 0:
            raise ValueError("lat dimension is empty (size=0)")
        if len(lon_vals) == 0:
            raise ValueError("lon dimension is empty (size=0)")

        lat_mid = len(lat_vals) // 2
        lon_mid = len(lon_vals) // 2

        if self.n_tiles == 1:
            # No tiling - process full domain (no parallelism)
            tiles = [
                (slice(None), slice(None), "full")
            ]
        elif self.n_tiles == 2:
            # Split east-west (2x1 grid)
            tiles = [
                (slice(None), slice(0, lon_mid), "west"),
                (slice(None), slice(lon_mid, None), "east")
            ]
        elif self.n_tiles == 4:
            # Split into quadrants (2x2 grid)
            tiles = [
                (slice(0, lat_mid), slice(0, lon_mid), "northwest"),
                (slice(0, lat_mid), slice(lon_mid, None), "northeast"),
                (slice(lat_mid, None), slice(0, lon_mid), "southwest"),
                (slice(lat_mid, None), slice(lon_mid, None), "southeast")
            ]
        elif self.n_tiles == 8:
            # Split into octants (2x4 grid - longitude heavy)
            lon_quarter = len(lon_vals) // 4
            tiles = [
                (slice(0, lat_mid), slice(0, lon_quarter), "nw1"),
                (slice(0, lat_mid), slice(lon_quarter, 2*lon_quarter), "nw2"),
                (slice(0, lat_mid), slice(2*lon_quarter, 3*lon_quarter), "ne1"),
                (slice(0, lat_mid), slice(3*lon_quarter, None), "ne2"),
                (slice(lat_mid, None), slice(0, lon_quarter), "sw1"),
                (slice(lat_mid, None), slice(lon_quarter, 2*lon_quarter), "sw2"),
                (slice(lat_mid, None), slice(2*lon_quarter, 3*lon_quarter), "se1"),
                (slice(lat_mid, None), slice(3*lon_quarter, None), "se2")
            ]
        else:
            raise ValueError(f"Unsupported n_tiles: {self.n_tiles}")

        logger.info(f"Created {len(tiles)} spatial tiles")
        return tiles

    def _process_single_tile(
        self,
        ds: xr.Dataset,
        lat_slice: slice,
        lon_slice: slice,
        tile_name: str
    ) -> Dict[str, xr.DataArray]:
        """
        Process a single spatial tile.

        This method must be overridden by subclasses that need to pass
        additional data (like baseline percentiles) to their tile processing.

        Default implementation:
        1. Selects spatial subset
        2. Calls self.calculate_indices() with subset
        3. Returns calculated indices

        Args:
            ds: Full dataset
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
        # Subclasses should override this method if they need to pass
        # additional data (e.g., baseline percentiles)
        tile_indices = self.calculate_indices({'primary': tile_ds})

        return tile_indices

    def _save_tile(
        self,
        tile_indices: Dict[str, xr.DataArray],
        tile_name: str,
        output_dir: Path
    ) -> Path:
        """
        Save a tile to NetCDF with thread-safe file writing.

        Args:
            tile_indices: Dictionary of calculated indices
            tile_name: Name of this tile
            output_dir: Directory to save tile files

        Returns:
            Path to saved tile file
        """
        tile_ds = xr.Dataset(tile_indices)

        # Apply any index-specific fixes (e.g., count indices for temperature)
        if hasattr(self, 'fix_count_indices'):
            tile_ds = self.fix_count_indices(tile_ds)

        # Use unique tile ID to prevent file collisions between concurrent pipeline runs
        tile_file = output_dir / f'tile_{self._tile_id}_{tile_name}.nc'
        logger.info(f"  Saving tile {tile_name} to {tile_file}...")

        # Compute dataset before NetCDF write to avoid Dask scheduler thread safety issues
        # dask.config.set() is not thread-safe in parallel contexts
        tile_ds_computed = tile_ds.compute()

        # Thread-safe NetCDF write (HDF5 library limitation)
        with netcdf_write_lock:
            encoding = {}
            for var_name in tile_ds_computed.data_vars:
                encoding[var_name] = {
                    'zlib': True,
                    'complevel': 4
                }
            try:
                tile_ds_computed.to_netcdf(tile_file, engine='netcdf4', encoding=encoding)
            except OSError as e:
                logger.error(f"Failed to write tile {tile_name}: {e}")
                # Clean up partial file
                if tile_file.exists():
                    tile_file.unlink()
                raise RuntimeError(f"Disk space exhaustion or I/O error writing {tile_file}: {e}") from e

        # Free memory
        del tile_indices, tile_ds, tile_ds_computed
        return tile_file

    def _merge_tiles(
        self,
        tile_files: List[Path],
        expected_dims: Dict[str, int]
    ) -> xr.Dataset:
        """
        Merge tile files back into a single dataset.

        Handles coordinate alignment and dimension validation.
        Uses lazy loading to avoid loading all tiles into memory at once.

        Args:
            tile_files: List of tile file paths (in correct order for concatenation)
            expected_dims: Expected final dimensions (for validation)

        Returns:
            Merged dataset

        Raises:
            ValueError: If dimensions don't match expected values
        """
        logger.info(f"Merging {len(tile_files)} tile files...")

        # Load tile datasets lazily
        tile_datasets = [xr.open_dataset(f, chunks='auto') for f in tile_files]

        # Concatenate based on number of tiles
        if self.n_tiles == 1:
            # Single tile - no merging needed
            merged_ds = tile_datasets[0]
        elif self.n_tiles == 2:
            # West + East (lon concatenation)
            merged_ds = xr.concat(tile_datasets, dim='lon')

        elif self.n_tiles == 4:
            # Quadrants: (NW + NE = North), (SW + SE = South), then North + South
            north = xr.concat([tile_datasets[0], tile_datasets[1]], dim='lon')
            south = xr.concat([tile_datasets[2], tile_datasets[3]], dim='lon')
            merged_ds = xr.concat([north, south], dim='lat')

        elif self.n_tiles == 8:
            # Octants: 4 pairs in lon direction, then 2 pairs in lat direction
            north1 = xr.concat([tile_datasets[0], tile_datasets[1]], dim='lon')
            north2 = xr.concat([tile_datasets[2], tile_datasets[3]], dim='lon')
            south1 = xr.concat([tile_datasets[4], tile_datasets[5]], dim='lon')
            south2 = xr.concat([tile_datasets[6], tile_datasets[7]], dim='lon')
            north = xr.concat([north1, north2], dim='lon')
            south = xr.concat([south1, south2], dim='lon')
            merged_ds = xr.concat([north, south], dim='lat')
        else:
            raise ValueError(f"Unsupported n_tiles: {self.n_tiles}")

        # Validate dimensions after merge
        actual_dims = dict(merged_ds.dims)
        if actual_dims != expected_dims:
            raise ValueError(
                f"Dimension mismatch after tile merge!\n"
                f"Expected: {expected_dims}\n"
                f"Actual: {actual_dims}\n"
                f"This indicates a tile concatenation bug."
            )

        logger.info(f"  Successfully merged to dimensions: {actual_dims}")

        # Apply any final fixes
        if hasattr(self, 'fix_count_indices'):
            merged_ds = self.fix_count_indices(merged_ds)

        # Clean up tile datasets
        for tile_ds in tile_datasets:
            try:
                tile_ds.close()
            except Exception as e:
                logger.warning(f"Failed to close tile dataset: {e}")

        return merged_ds

    def _cleanup_tile_files(self, tile_files: List[Path]):
        """
        Delete temporary tile files.

        Args:
            tile_files: List of tile file paths to delete
        """
        for tile_file in tile_files:
            try:
                tile_file.unlink()
                logger.debug(f"  Cleaned up {tile_file}")
            except Exception as e:
                logger.warning(f"Failed to delete tile file {tile_file}: {e}")

    def process_with_spatial_tiling(
        self,
        ds: xr.Dataset,
        output_dir: Path,
        expected_dims: Dict[str, int]
    ) -> Dict[str, xr.DataArray]:
        """
        Process dataset using spatial tiling.

        Main workflow:
        1. Split dataset into spatial tiles
        2. Process each tile in parallel using ThreadPoolExecutor
        3. Save each tile immediately (memory efficient)
        4. Merge tiles back together
        5. Clean up temporary tile files

        Args:
            ds: Dataset to process
            output_dir: Directory for temporary tile files
            expected_dims: Expected final dimensions for validation

        Returns:
            Dictionary of calculated indices (merged from all tiles)
        """
        logger.info(f"Processing with parallel spatial tiling ({self.n_tiles} tiles)")

        # Calculate tile boundaries
        tiles = self._get_spatial_tiles(ds)

        # Process and save tiles in parallel
        tile_files_dict = {}
        tile_files_lock = threading.Lock()

        def process_and_save_tile_wrapper(tile_info):
            """Process and save a single tile (thread-safe)."""
            lat_slice, lon_slice, tile_name = tile_info

            # Process tile
            tile_indices = self._process_single_tile(ds, lat_slice, lon_slice, tile_name)

            # Save tile
            tile_file = self._save_tile(tile_indices, tile_name, output_dir)

            # Store result in dict (thread-safe)
            with tile_files_lock:
                tile_files_dict[tile_name] = tile_file

            return tile_file

        # Execute in parallel
        with ThreadPoolExecutor(max_workers=self.n_tiles) as executor:
            future_to_tile = {
                executor.submit(process_and_save_tile_wrapper, tile): tile
                for tile in tiles
            }

            for future in as_completed(future_to_tile):
                tile_info = future_to_tile[future]
                tile_name = tile_info[2]
                try:
                    future.result()
                    logger.info(f"  ✓ Tile {tile_name} completed successfully")
                except Exception as e:
                    logger.error(f"  ✗ Tile {tile_name} failed: {e}")
                    raise

        # Verify we have all tiles
        if len(tile_files_dict) != self.n_tiles:
            raise ValueError(
                f"Expected {self.n_tiles} tile files, but got {len(tile_files_dict)}"
            )

        # Build tile_files list in correct order for concatenation
        tile_files = self._get_ordered_tile_files(tile_files_dict)

        # Merge tiles
        merged_ds = self._merge_tiles(tile_files, expected_dims)

        # Compute merged dataset to materialize data before tile cleanup
        # Without this, lazy-loaded arrays will be inaccessible after tile deletion
        logger.info("  Computing merged dataset...")
        merged_ds_computed = merged_ds.compute()

        # Clean up tile files (safe now that data is materialized)
        self._cleanup_tile_files(tile_files)

        # Extract indices as dictionary
        all_indices = {var: merged_ds_computed[var] for var in merged_ds_computed.data_vars}

        # Close computed dataset to free memory
        try:
            merged_ds.close()
            merged_ds_computed.close()
        except Exception as e:
            logger.warning(f"Failed to close merged datasets: {e}")
        del merged_ds, merged_ds_computed

        return all_indices

    def _get_ordered_tile_files(self, tile_files_dict: Dict[str, Path]) -> List[Path]:
        """
        Get tile files in correct order for concatenation.

        Args:
            tile_files_dict: Dictionary mapping tile name to file path

        Returns:
            List of tile files in concatenation order
        """
        if self.n_tiles == 1:
            return [tile_files_dict['full']]
        elif self.n_tiles == 2:
            return [tile_files_dict['west'], tile_files_dict['east']]
        elif self.n_tiles == 4:
            return [
                tile_files_dict['northwest'],
                tile_files_dict['northeast'],
                tile_files_dict['southwest'],
                tile_files_dict['southeast']
            ]
        elif self.n_tiles == 8:
            return [
                tile_files_dict['nw1'], tile_files_dict['nw2'],
                tile_files_dict['ne1'], tile_files_dict['ne2'],
                tile_files_dict['sw1'], tile_files_dict['sw2'],
                tile_files_dict['se1'], tile_files_dict['se2']
            ]
        else:
            raise ValueError(f"Unsupported n_tiles: {self.n_tiles}")

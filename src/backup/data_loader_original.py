"""
Data loader module for climate data - Zarr-exclusive implementation.
Optimized for loading climate data from Zarr stores on external drives.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

import xarray as xr
import numpy as np
from tqdm import tqdm


logger = logging.getLogger(__name__)


class ClimateDataLoader:
    """Load and manage climate data from Zarr stores."""

    def __init__(self, config: 'Config'):
        """
        Initialize the data loader.

        Parameters:
        -----------
        config : Config
            Configuration object
        """
        self.config = config
        self.datasets = {}

    def scan_directory(self, path: Union[str, Path],
                      patterns: Optional[List[str]] = None) -> List[Path]:
        """
        Scan directory for Zarr stores.

        Parameters:
        -----------
        path : str or Path
            Directory to scan
        patterns : list of str, optional
            Patterns to match (default: ['*.zarr'])

        Returns:
        --------
        List[Path]
            List of found Zarr store paths
        """
        path = Path(path)
        if not path.exists():
            logger.warning(f"Path {path} does not exist")
            return []

        stores = []
        if patterns is None:
            patterns = ['*.zarr']

        for pattern in patterns:
            stores.extend(path.rglob(pattern))

        # Also find directories containing .zarray (Zarr stores without .zarr extension)
        for item in path.rglob('*'):
            if item.is_dir() and (item / '.zarray').exists():
                stores.append(item)

        logger.info(f"Found {len(stores)} Zarr stores in {path}")
        return sorted(list(set(stores)))

    def load_zarr(self, store_path: Union[str, Path],
                  chunks: Optional[Dict] = None,
                  consolidated: bool = True) -> xr.Dataset:
        """
        Load local Zarr store.

        Parameters:
        -----------
        store_path : str or Path
            Path to local Zarr store
        chunks : dict, optional
            Chunk sizes for dask (if None, uses Zarr's native chunks)
        consolidated : bool, default True
            Whether to use consolidated metadata for faster loading

        Returns:
        --------
        xr.Dataset
            Loaded dataset
        """
        store_path = Path(store_path)
        logger.info(f"Loading Zarr store: {store_path}")

        try:
            # Load with optimized settings
            ds = xr.open_zarr(store_path, chunks=chunks, consolidated=consolidated)

            # Standardize dimension names
            ds = self._standardize_dims(ds)

            # Store metadata about the source
            ds.attrs['source_format'] = 'zarr'
            ds.attrs['source_path'] = str(store_path)

            return ds

        except Exception as e:
            logger.error(f"Error loading Zarr store {store_path}: {e}")
            raise

    def load_multiple_stores(self, store_paths: List[Union[str, Path]],
                            concat_dim: str = 'time',
                            chunks: Optional[Dict] = None) -> xr.Dataset:
        """
        Load and concatenate multiple Zarr stores.

        Parameters:
        -----------
        store_paths : list of paths
            Paths to Zarr stores to load
        concat_dim : str
            Dimension to concatenate along
        chunks : dict, optional
            Chunk sizes for dask

        Returns:
        --------
        xr.Dataset
            Combined dataset
        """
        if not store_paths:
            raise ValueError("No Zarr stores to load")

        logger.info(f"Loading {len(store_paths)} Zarr stores")

        # Load stores sequentially
        datasets = []
        for store_path in tqdm(store_paths, desc="Loading Zarr stores"):
            try:
                ds = self.load_zarr(store_path, chunks)
                if ds is not None:
                    datasets.append(ds)
            except Exception as e:
                logger.warning(f"Failed to load {store_path}: {e}")
                continue

        # Concatenate datasets
        if len(datasets) == 1:
            return datasets[0]

        try:
            # Ensure all datasets have the concat dimension
            for ds in datasets:
                if concat_dim not in ds.dims and concat_dim == 'time':
                    # Add time dimension if missing
                    ds = ds.expand_dims(time=[0])

            combined = xr.concat(datasets, dim=concat_dim)
            return combined

        except Exception as e:
            logger.error(f"Error concatenating datasets: {e}")
            raise

    def load_variable_data(self, variable: str,
                          time_range: Optional[tuple] = None) -> xr.Dataset:
        """
        Load all data for a specific climate variable from Zarr stores.

        Parameters:
        -----------
        variable : str
            Variable name (e.g., 'temperature', 'precipitation')
        time_range : tuple, optional
            Time range to load (start, end)

        Returns:
        --------
        xr.Dataset
            Dataset containing the variable data
        """
        # Get Zarr store patterns for this variable
        patterns = self.config.get(f'data.zarr_stores.{variable}', [])
        if not patterns:
            # Fall back to general patterns if no specific ones defined
            patterns = ['*.zarr']
            logger.info(f"Using default pattern for variable {variable}")

        # Scan for Zarr stores
        input_path = self.config.input_path
        stores = self.scan_directory(input_path, patterns)

        if not stores:
            logger.warning(f"No Zarr stores found for variable {variable}")
            return None

        # Load stores
        ds = self.load_multiple_stores(stores)

        # Apply time range if specified
        if time_range and 'time' in ds.dims:
            ds = ds.sel(time=slice(*time_range))

        # Store in cache
        self.datasets[variable] = ds

        return ds

    def _standardize_dims(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Standardize dimension names to CF conventions.

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset to standardize

        Returns:
        --------
        xr.Dataset
            Dataset with standardized dimensions
        """
        dim_mapping = {
            # Latitude variations
            'latitude': 'lat',
            'Latitude': 'lat',
            'LAT': 'lat',
            'y': 'lat',
            'Y': 'lat',
            # Longitude variations
            'longitude': 'lon',
            'Longitude': 'lon',
            'LON': 'lon',
            'long': 'lon',
            'x': 'lon',
            'X': 'lon',
            # Time variations
            'Time': 'time',
            'TIME': 'time',
            't': 'time',
            'T': 'time',
            # Band/level variations
            'band': 'z',
            'level': 'z',
            'height': 'z',
            'depth': 'z'
        }

        for old_name, new_name in dim_mapping.items():
            if old_name in ds.dims and new_name not in ds.dims:
                ds = ds.rename({old_name: new_name})

        return ds

    def get_info(self) -> Dict:
        """
        Get information about loaded datasets.

        Returns:
        --------
        dict
            Information about loaded datasets
        """
        info = {}
        for name, ds in self.datasets.items():
            info[name] = {
                'dimensions': dict(ds.sizes),
                'variables': list(ds.data_vars),
                'shape': dict(ds.sizes),
                'chunks': ds.chunks if ds.chunks else None,
                'memory_size': ds.nbytes / 1e9,  # GB
                'time_range': [str(ds.time.min().values), str(ds.time.max().values)]
                              if 'time' in ds.dims else None
            }
        return info

    def optimize_zarr_store(self, store_path: Union[str, Path],
                           target_chunks: Optional[Dict] = None) -> None:
        """
        Optimize a Zarr store by rechunking and consolidating metadata.

        Parameters:
        -----------
        store_path : str or Path
            Path to Zarr store to optimize
        target_chunks : dict, optional
            Target chunk sizes for optimization
        """
        store_path = Path(store_path)
        logger.info(f"Optimizing Zarr store: {store_path}")

        try:
            # Open the store
            ds = xr.open_zarr(store_path)

            # Define optimal chunks if not provided
            if target_chunks is None:
                target_chunks = {
                    'time': min(365, ds.sizes.get('time', 1)),
                    'lat': min(100, ds.sizes.get('lat', 1)),
                    'lon': min(100, ds.sizes.get('lon', 1))
                }

            # Rechunk the dataset
            ds_rechunked = ds.chunk(target_chunks)

            # Save with optimization
            output_path = store_path.parent / f"{store_path.stem}_optimized.zarr"
            ds_rechunked.to_zarr(output_path, mode='w', consolidated=True)

            logger.info(f"Optimized store saved to: {output_path}")

        except Exception as e:
            logger.error(f"Error optimizing Zarr store: {e}")
            raise


# Example usage
if __name__ == "__main__":
    from config import Config

    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Create configuration
    config = Config()

    # Initialize loader
    loader = ClimateDataLoader(config)

    # Example: Load temperature data from Zarr stores
    print("Scanning for Zarr stores...")
    stores = loader.scan_directory(config.input_path)
    print(f"Found {len(stores)} Zarr stores")

    if stores:
        # Load first store as example
        ds = loader.load_zarr(stores[0])
        print(f"Loaded dataset with dimensions: {dict(ds.dims)}")
        print(f"Variables: {list(ds.data_vars)}")
"""
Data loader module for climate raster data.
Handles loading of GeoTIFF and NetCDF files from external drives.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union
import warnings

import xarray as xr
import rioxarray as rxr
import dask.array as da
from dask.distributed import Client, as_completed
import numpy as np


logger = logging.getLogger(__name__)


class ClimateDataLoader:
    """Load and manage climate raster data from various formats."""
    
    def __init__(self, config: 'Config', client: Optional[Client] = None):
        """
        Initialize the data loader.
        
        Parameters:
        -----------
        config : Config
            Configuration object
        client : Client, optional
            Dask client for parallel processing
        """
        self.config = config
        self.client = client
        self.datasets = {}
        
    def scan_directory(self, path: Union[str, Path], 
                      patterns: Optional[List[str]] = None) -> List[Path]:
        """
        Scan directory for climate data files.
        
        Parameters:
        -----------
        path : str or Path
            Directory to scan
        patterns : list of str, optional
            File patterns to match (e.g., ['*.tif', '*.nc'])
        
        Returns:
        --------
        List[Path]
            List of found file paths
        """
        path = Path(path)
        if not path.exists():
            logger.warning(f"Path {path} does not exist")
            return []
        
        files = []
        if patterns is None:
            patterns = ['*.tif', '*.tiff', '*.nc', '*.nc4', '*.netcdf']
        
        for pattern in patterns:
            files.extend(path.rglob(pattern))
        
        logger.info(f"Found {len(files)} files in {path}")
        return sorted(files)
    
    def load_geotiff(self, filepath: Union[str, Path], 
                     chunks: Optional[Dict] = None) -> xr.Dataset:
        """
        Load GeoTIFF file using rioxarray.
        
        Parameters:
        -----------
        filepath : str or Path
            Path to GeoTIFF file
        chunks : dict, optional
            Chunk sizes for dask
        
        Returns:
        --------
        xr.Dataset
            Loaded dataset
        """
        filepath = Path(filepath)
        logger.info(f"Loading GeoTIFF: {filepath}")
        
        try:
            # Open with rioxarray
            ds = rxr.open_rasterio(filepath, chunks=chunks or 'auto')
            
            # Convert to dataset if it's a DataArray
            if isinstance(ds, xr.DataArray):
                var_name = filepath.stem.split('_')[0] if '_' in filepath.stem else 'data'
                ds = ds.to_dataset(name=var_name)
            
            # Ensure CRS is set
            if not ds.rio.crs:
                logger.warning(f"No CRS found in {filepath}, setting to EPSG:4326")
                ds = ds.rio.write_crs('EPSG:4326')
            
            # Standardize dimension names
            ds = self._standardize_dims(ds)
            
            return ds
            
        except Exception as e:
            logger.error(f"Error loading GeoTIFF {filepath}: {e}")
            raise
    
    def load_netcdf(self, filepath: Union[str, Path], 
                    chunks: Optional[Dict] = None) -> xr.Dataset:
        """
        Load NetCDF file.
        
        Parameters:
        -----------
        filepath : str or Path
            Path to NetCDF file
        chunks : dict, optional
            Chunk sizes for dask
        
        Returns:
        --------
        xr.Dataset
            Loaded dataset
        """
        filepath = Path(filepath)
        logger.info(f"Loading NetCDF: {filepath}")
        
        try:
            # Determine chunks
            if chunks is None:
                chunks = self.config.chunk_sizes
            
            # Open with xarray
            ds = xr.open_dataset(filepath, chunks=chunks, engine='netcdf4')
            
            # Standardize dimension names
            ds = self._standardize_dims(ds)
            
            # Check for CRS information
            if hasattr(ds, 'rio') and not ds.rio.crs:
                # Try to infer CRS from attributes
                crs = self._infer_crs(ds)
                if crs:
                    ds = ds.rio.write_crs(crs)
            
            return ds
            
        except Exception as e:
            logger.error(f"Error loading NetCDF {filepath}: {e}")
            raise
    
    def load_file(self, filepath: Union[str, Path], 
                  chunks: Optional[Dict] = None) -> xr.Dataset:
        """
        Load a climate data file (auto-detect format).
        
        Parameters:
        -----------
        filepath : str or Path
            Path to file
        chunks : dict, optional
            Chunk sizes for dask
        
        Returns:
        --------
        xr.Dataset
            Loaded dataset
        """
        filepath = Path(filepath)
        
        if filepath.suffix.lower() in ['.tif', '.tiff']:
            return self.load_geotiff(filepath, chunks)
        elif filepath.suffix.lower() in ['.nc', '.nc4', '.netcdf']:
            return self.load_netcdf(filepath, chunks)
        else:
            raise ValueError(f"Unsupported file format: {filepath.suffix}")
    
    def load_multiple_files(self, filepaths: List[Union[str, Path]], 
                           concat_dim: str = 'time',
                           chunks: Optional[Dict] = None) -> xr.Dataset:
        """
        Load and concatenate multiple files.
        
        Parameters:
        -----------
        filepaths : list of paths
            Paths to files to load
        concat_dim : str
            Dimension to concatenate along
        chunks : dict, optional
            Chunk sizes for dask
        
        Returns:
        --------
        xr.Dataset
            Combined dataset
        """
        if not filepaths:
            raise ValueError("No files to load")
        
        logger.info(f"Loading {len(filepaths)} files")
        
        # Load files in parallel if client is available
        if self.client:
            futures = []
            for filepath in filepaths:
                future = self.client.submit(self.load_file, filepath, chunks)
                futures.append(future)
            
            datasets = []
            for future in as_completed(futures):
                datasets.append(future.result())
        else:
            datasets = [self.load_file(fp, chunks) for fp in filepaths]
        
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
        Load all data for a specific climate variable.
        
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
        # Get file patterns for this variable
        patterns = self.config.get(f'data.file_patterns.{variable}', [])
        if not patterns:
            logger.warning(f"No file patterns defined for variable {variable}")
            return None
        
        # Scan for files
        input_path = self.config.input_path
        files = []
        for pattern in patterns:
            files.extend(self.scan_directory(input_path, [pattern]))
        
        if not files:
            logger.warning(f"No files found for variable {variable}")
            return None
        
        # Load files
        ds = self.load_multiple_files(files)
        
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
    
    def _infer_crs(self, ds: xr.Dataset) -> Optional[str]:
        """
        Infer CRS from dataset attributes.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Dataset to check
        
        Returns:
        --------
        str or None
            CRS string if found
        """
        # Check common attribute names
        crs_attrs = ['crs', 'spatial_ref', 'grid_mapping', 'projection']
        
        for attr in crs_attrs:
            if attr in ds.attrs:
                crs_value = ds.attrs[attr]
                if isinstance(crs_value, str) and 'EPSG' in crs_value:
                    return crs_value
        
        # Check variable attributes
        for var in ds.data_vars:
            if 'grid_mapping' in ds[var].attrs:
                grid_mapping = ds[var].attrs['grid_mapping']
                if grid_mapping in ds.data_vars:
                    if 'spatial_ref' in ds[grid_mapping].attrs:
                        return ds[grid_mapping].attrs['spatial_ref']
        
        # Default to WGS84 if we have lat/lon
        if 'lat' in ds.dims and 'lon' in ds.dims:
            lat_range = float(ds.lat.max() - ds.lat.min())
            lon_range = float(ds.lon.max() - ds.lon.min())
            if lat_range <= 180 and lon_range <= 360:
                return 'EPSG:4326'
        
        return None
    
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
                'dimensions': dict(ds.dims),
                'variables': list(ds.data_vars),
                'shape': {dim: size for dim, size in ds.dims.items()},
                'chunks': ds.chunks if ds.chunks else None,
                'crs': ds.rio.crs if hasattr(ds, 'rio') and ds.rio.crs else None,
                'memory_size': ds.nbytes / 1e9,  # GB
                'time_range': [str(ds.time.min().values), str(ds.time.max().values)] 
                              if 'time' in ds.dims else None
            }
        return info


# Example usage
if __name__ == "__main__":
    from config import Config
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create configuration
    config = Config()
    
    # Initialize loader
    loader = ClimateDataLoader(config)
    
    # Example: Load temperature data
    print("Scanning for climate data files...")
    files = loader.scan_directory(config.input_path)
    print(f"Found {len(files)} files")
    
    if files:
        # Load first file as example
        ds = loader.load_file(files[0])
        print(f"Loaded dataset with dimensions: {dict(ds.dims)}")
        print(f"Variables: {list(ds.data_vars)}")
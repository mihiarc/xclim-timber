"""
Simplified data loader for PRISM Zarr stores.
Direct loading without unnecessary abstractions.
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import xarray as xr

logger = logging.getLogger(__name__)


class PrismDataLoader:
    """Simple loader for PRISM Zarr data."""

    def __init__(self, config: 'Config'):
        """
        Initialize with configuration.

        Parameters:
        -----------
        config : Config
            Configuration object with Zarr paths
        """
        self.config = config
        self.datasets = {}

    def load_temperature(self) -> xr.Dataset:
        """Load temperature data (tmax, tmean, tmin)."""
        path = self.config.get_zarr_path('temperature')
        logger.info(f"Loading temperature data from {path}")

        ds = xr.open_zarr(path)
        self.datasets['temperature'] = ds
        return ds

    def load_precipitation(self) -> xr.Dataset:
        """Load precipitation data (ppt)."""
        path = self.config.get_zarr_path('precipitation')
        logger.info(f"Loading precipitation data from {path}")

        ds = xr.open_zarr(path)
        self.datasets['precipitation'] = ds
        return ds

    def load_humidity(self) -> xr.Dataset:
        """Load humidity data (tdmean, vpdmax, vpdmin)."""
        path = self.config.get_zarr_path('humidity')
        logger.info(f"Loading humidity data from {path}")

        ds = xr.open_zarr(path)
        self.datasets['humidity'] = ds
        return ds

    def load_all(self) -> Dict[str, xr.Dataset]:
        """Load all available datasets."""
        self.load_temperature()
        self.load_precipitation()
        self.load_humidity()
        return self.datasets

    def get_combined_dataset(self) -> xr.Dataset:
        """
        Combine all datasets into a single dataset for indices calculation.

        Returns:
        --------
        xr.Dataset
            Combined dataset with all variables
        """
        if not self.datasets:
            self.load_all()

        # Merge all datasets - they share the same coordinates
        combined = xr.merge(list(self.datasets.values()))

        # Map PRISM variable names to xclim expected names
        variable_mapping = {
            'tmax': 'tasmax',
            'tmin': 'tasmin',
            'tmean': 'tas',
            'ppt': 'pr',
            'tdmean': 'tdew',  # dewpoint temperature
            'vpdmax': 'vpdmax',  # vapor pressure deficit max
            'vpdmin': 'vpdmin'   # vapor pressure deficit min
        }

        # Rename variables to match xclim expectations
        for old_name, new_name in variable_mapping.items():
            if old_name in combined:
                combined = combined.rename({old_name: new_name})

        logger.info(f"Combined dataset with variables: {list(combined.data_vars)}")
        return combined

    def subset_time(self, ds: xr.Dataset,
                   start_year: Optional[int] = None,
                   end_year: Optional[int] = None) -> xr.Dataset:
        """
        Subset dataset to specific time range.

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset to subset
        start_year : int, optional
            Start year
        end_year : int, optional
            End year

        Returns:
        --------
        xr.Dataset
            Subsetted dataset
        """
        if start_year or end_year:
            start_date = f"{start_year}-01-01" if start_year else None
            end_date = f"{end_year}-12-31" if end_year else None
            ds = ds.sel(time=slice(start_date, end_date))
            logger.info(f"Subsetted to {start_year or 'start'}-{end_year or 'end'}")

        return ds

    def subset_region(self, ds: xr.Dataset,
                     lat_bounds: Optional[tuple] = None,
                     lon_bounds: Optional[tuple] = None) -> xr.Dataset:
        """
        Subset dataset to specific spatial region.

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset to subset
        lat_bounds : tuple, optional
            (min_lat, max_lat)
        lon_bounds : tuple, optional
            (min_lon, max_lon)

        Returns:
        --------
        xr.Dataset
            Subsetted dataset
        """
        if lat_bounds:
            ds = ds.sel(lat=slice(*lat_bounds))
            logger.info(f"Subsetted latitude to {lat_bounds}")

        if lon_bounds:
            ds = ds.sel(lon=slice(*lon_bounds))
            logger.info(f"Subsetted longitude to {lon_bounds}")

        return ds
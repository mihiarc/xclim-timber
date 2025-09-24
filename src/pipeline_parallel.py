#!/usr/bin/env python
"""
Parallel climate data processing pipeline optimized for PRISM Zarr stores.
Uses best practices for memory-efficient processing of large datasets.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import xarray as xr
import numpy as np
from datetime import datetime
import warnings
import dask
import dask.array as da
from tqdm import tqdm
import pandas as pd

# Suppress common warnings
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*All-NaN slice.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*divide.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*invalid value.*')

logger = logging.getLogger(__name__)


class ParallelPrismPipeline:
    """
    Optimized pipeline for processing PRISM Zarr data.

    Key features:
    - Lazy loading with xr.open_zarr for memory efficiency
    - Chunked processing for temporal data
    - Parallel computation with Dask
    - Streaming writes to avoid memory issues
    """

    def __init__(self, config, output_path=None):
        """Initialize with configuration object and optional output path override."""
        self.config = config
        self.output_path = Path(output_path) if output_path else Path(config.output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

    def process_year_chunk(self,
                          start_year: int,
                          end_year: int,
                          variables: List[str] = None,
                          indices_categories: List[str] = None) -> str:
        """
        Process a chunk of years efficiently.

        Parameters:
        -----------
        start_year : int
            Start year for processing
        end_year : int
            End year for processing
        variables : List[str]
            Variables to process
        indices_categories : List[str]
            Categories of indices to calculate

        Returns:
        --------
        str
            Path to output file
        """
        logger.info(f"Processing years {start_year}-{end_year}")

        # Define date range
        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"

        # Load data lazily using Zarr best practices
        datasets = self._load_zarr_lazy(start_date, end_date, variables)

        # Combine datasets efficiently
        combined_ds = self._combine_datasets(datasets)

        # Calculate indices
        from indices import ClimateIndicesCalculator
        calculator = ClimateIndicesCalculator()

        # Process by variable to minimize memory usage
        all_indices = {}

        for var in combined_ds.data_vars:
            if var in ['tas', 'tasmax', 'tasmin', 'pr']:
                logger.info(f"Calculating indices for {var}")

                # Create single-variable dataset
                var_ds = combined_ds[[var]]

                # Calculate indices for this variable
                indices = calculator.calculate_for_variable(
                    var_ds,
                    variable=var,
                    baseline_percentiles={},  # Empty dict if no percentiles needed
                    freq='YS'
                )

                all_indices.update(indices)

        # Save results with compression
        output_file = self._save_results(all_indices, start_year, end_year)

        return output_file

    def _load_zarr_lazy(self,
                       start_date: str,
                       end_date: str,
                       variables: List[str] = None) -> Dict[str, xr.Dataset]:
        """
        Load Zarr stores lazily with proper chunking.

        This follows Zarr/Dask best practices:
        - Uses chunks='auto' to respect Zarr's native chunking
        - Keeps data lazy until computation
        - Only loads requested time range
        """
        datasets = {}

        # Define Zarr store paths
        stores = {
            'temperature': self.config.get_zarr_path('temperature'),
            'precipitation': self.config.get_zarr_path('precipitation'),
            'humidity': self.config.get_zarr_path('humidity')
        }

        for name, store_path in stores.items():
            if variables and name not in variables:
                continue

            logger.info(f"Opening {name} Zarr store: {store_path}")

            # Open with chunks='auto' to use Zarr's native chunking
            ds = xr.open_zarr(store_path, chunks='auto')

            # Select time range (still lazy!)
            if 'time' in ds.dims:
                ds = ds.sel(time=slice(start_date, end_date))
                logger.info(f"  Selected time range: {ds.time.values[0]} to {ds.time.values[-1]}")

            datasets[name] = ds

            # Log chunk information
            if hasattr(ds, 'chunks'):
                logger.info(f"  Chunk structure: {dict(zip(ds.dims, ds.chunks))}")

        return datasets

    def _combine_datasets(self, datasets: Dict[str, xr.Dataset]) -> xr.Dataset:
        """
        Combine multiple datasets efficiently.

        Renames variables to match xclim conventions.
        """
        # Collect all data arrays with proper renaming
        combined_vars = {}

        # Variable mapping for xclim
        variable_mapping = {
            'tmax': 'tasmax',
            'tmin': 'tasmin',
            'tmean': 'tas',
            'ppt': 'pr',
            'tdmean': 'tdew',
            'vpdmax': 'vpdmax',
            'vpdmin': 'vpdmin'
        }

        for ds in datasets.values():
            for var in ds.data_vars:
                # Rename to xclim convention
                new_name = variable_mapping.get(var, var)
                combined_vars[new_name] = ds[var]

                # Ensure proper units attribute
                if new_name in ['tas', 'tasmax', 'tasmin']:
                    combined_vars[new_name].attrs['units'] = 'degC'
                elif new_name == 'pr':
                    # xclim expects mm/day for precipitation
                    combined_vars[new_name].attrs['units'] = 'mm/day'

        # Create combined dataset
        combined = xr.Dataset(combined_vars)

        logger.info(f"Combined dataset variables: {list(combined.data_vars)}")
        logger.info(f"Dataset size: {combined.nbytes / 1e9:.2f} GB")

        return combined

    def _save_results(self,
                     indices: Dict[str, xr.DataArray],
                     start_year: int,
                     end_year: int) -> str:
        """
        Save results efficiently with compression.
        """
        # Create dataset from indices
        indices_ds = xr.Dataset(indices)

        # Add metadata
        indices_ds.attrs.update({
            'title': 'PRISM Climate Indices',
            'institution': 'Calculated using xclim-timber pipeline',
            'source': 'PRISM Zarr data',
            'history': f"{datetime.now().isoformat()}: Calculated indices",
            'time_range': f"{start_year}-{end_year}",
            'software': 'xclim-timber v1.0'
        })

        # Generate output filename
        output_file = self.output_path / f"indices_{start_year}_{end_year}.nc"

        # Save with compression
        encoding = {
            var: {'zlib': True, 'complevel': 4}
            for var in indices_ds.data_vars
        }

        logger.info(f"Saving {len(indices)} indices to {output_file}")
        indices_ds.to_netcdf(output_file, encoding=encoding)

        return str(output_file)

    def process_parcels(self,
                       parcels_file: str,
                       start_year: int,
                       end_year: int) -> str:
        """
        Extract climate indices at specific parcel locations.

        Parameters:
        -----------
        parcels_file : str
            Path to CSV with lat/lon coordinates
        start_year : int
            Start year
        end_year : int
            End year

        Returns:
        --------
        str
            Path to output CSV file
        """
        # Load parcel locations
        parcels = pd.read_csv(parcels_file)

        if 'lat' not in parcels or 'lon' not in parcels:
            raise ValueError("Parcels file must have 'lat' and 'lon' columns")

        logger.info(f"Processing {len(parcels)} parcel locations")

        # Process the time period
        output_nc = self.process_year_chunk(start_year, end_year)

        # Load the results
        indices_ds = xr.open_dataset(output_nc)

        # Extract at parcel locations
        results = []
        for _, parcel in tqdm(parcels.iterrows(), total=len(parcels), desc="Extracting parcels"):
            # Find nearest point
            point_data = indices_ds.sel(
                lat=parcel['lat'],
                lon=parcel['lon'],
                method='nearest'
            )

            # Convert to dictionary
            point_dict = {
                'lat': parcel['lat'],
                'lon': parcel['lon'],
                'year': f"{start_year}-{end_year}"
            }

            # Add parcel metadata
            for col in parcels.columns:
                if col not in ['lat', 'lon']:
                    point_dict[col] = parcel[col]

            # Add index values (averaged over time)
            for var in indices_ds.data_vars:
                values = point_data[var].values
                if values.size > 1:
                    point_dict[var] = float(np.mean(values))
                else:
                    point_dict[var] = float(values)

            results.append(point_dict)

        # Save as CSV
        results_df = pd.DataFrame(results)
        output_csv = self.output_path / f"parcel_indices_{start_year}_{end_year}.csv"
        results_df.to_csv(output_csv, index=False)

        logger.info(f"Saved parcel results to {output_csv}")
        return str(output_csv)
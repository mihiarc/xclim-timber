#!/usr/bin/env python
"""
Simplified climate indices calculation for specific parcel locations.
Extracts data only at points defined in parcel_coordinates.csv.
"""

import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
import xarray as xr
import xclim
from xclim import atmos
from datetime import datetime
from typing import List, Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


class ParcelClimateExtractor:
    """Extract climate data and calculate indices at specific parcel locations."""
    
    def __init__(self, parcel_csv: str):
        """
        Initialize with parcel coordinates.
        
        Parameters:
        -----------
        parcel_csv : str
            Path to CSV file with parcel coordinates
        """
        self.parcels = self.load_parcels(parcel_csv)
        self.results = {}
        
    def load_parcels(self, csv_path: str) -> pd.DataFrame:
        """Load parcel coordinates from CSV."""
        logger.info(f"Loading parcel coordinates from {csv_path}")
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} parcel locations")
        
        # Ensure we have the required columns
        required_cols = ['parcel_level_latitude', 'parcel_level_longitude']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"CSV must contain columns: {required_cols}")
        
        # Rename for convenience
        df = df.rename(columns={
            'parcel_level_latitude': 'lat',
            'parcel_level_longitude': 'lon'
        })
        
        return df
    
    def find_nearest_grid_points(self, ds: xr.Dataset) -> pd.DataFrame:
        """
        Find nearest grid points in dataset for each parcel.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Climate dataset with lat/lon coordinates
        
        Returns:
        --------
        pd.DataFrame
            DataFrame with nearest grid indices added
        """
        logger.info("Finding nearest grid points for each parcel")
        
        # Get dataset coordinates
        ds_lats = ds.lat.values
        ds_lons = ds.lon.values
        
        # For each parcel, find nearest grid point
        lat_indices = []
        lon_indices = []
        
        for idx, row in self.parcels.iterrows():
            # Find nearest latitude
            lat_idx = int(np.argmin(np.abs(ds_lats - row['lat'])))
            lat_indices.append(lat_idx)
            
            # Find nearest longitude
            # Handle negative longitudes (convert to 0-360 if needed)
            parcel_lon = row['lon']
            if parcel_lon < 0:
                parcel_lon = parcel_lon + 360
            
            lon_idx = int(np.argmin(np.abs(ds_lons - parcel_lon)))
            lon_indices.append(lon_idx)
        
        self.parcels['lat_idx'] = lat_indices
        self.parcels['lon_idx'] = lon_indices
        self.parcels['grid_lat'] = [ds_lats[i] for i in lat_indices]
        self.parcels['grid_lon'] = [ds_lons[i] for i in lon_indices]
        
        # Convert grid longitudes back to -180 to 180 for distance calculation
        grid_lons_adjusted = self.parcels['grid_lon'].values.copy()
        grid_lons_adjusted[grid_lons_adjusted > 180] -= 360
        
        # Calculate distance to grid point (for quality check)
        self.parcels['distance_km'] = self.calculate_distances(
            self.parcels['lat'].values, self.parcels['lon'].values,
            self.parcels['grid_lat'].values, grid_lons_adjusted
        )
        
        logger.info(f"Mean distance to grid points: {self.parcels['distance_km'].mean():.2f} km")
        logger.info(f"Max distance to grid points: {self.parcels['distance_km'].max():.2f} km")
        
        return self.parcels
    
    def calculate_distances(self, lat1, lon1, lat2, lon2):
        """Calculate distances between points using Haversine formula."""
        R = 6371  # Earth radius in km
        
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        return R * c
    
    def extract_timeseries(self, ds: xr.Dataset, variable: str = None) -> pd.DataFrame:
        """
        Extract time series data at all parcel locations.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Climate dataset
        variable : str
            Variable name to extract (auto-detect if None)
        
        Returns:
        --------
        pd.DataFrame
            DataFrame with time series for each parcel
        """
        logger.info("Extracting time series at parcel locations")
        
        # Find variable if not specified
        if variable is None:
            # Look for temperature variable
            temp_vars = ['tas', 'temperature', 'temp', 'tmean']
            for var in temp_vars:
                if var in ds.data_vars:
                    variable = var
                    break
            if variable is None:
                variable = list(ds.data_vars)[0]
        
        logger.info(f"Extracting variable: {variable}")
        
        # Find nearest grid points if not already done
        if 'lat_idx' not in self.parcels.columns:
            self.find_nearest_grid_points(ds)
        
        # Extract data for each parcel
        all_data = []
        
        for idx, row in self.parcels.iterrows():
            # Extract time series at this location
            # Ensure indices are integers
            lat_idx = int(row['lat_idx'])
            lon_idx = int(row['lon_idx'])
            
            point_data = ds[variable].isel(
                lat=lat_idx,
                lon=lon_idx
            )
            
            # Convert to dataframe
            df_point = point_data.to_dataframe(name=variable).reset_index()
            
            # Add parcel identifiers
            df_point['saleid'] = row.get('saleid', idx)
            df_point['parcelid'] = row.get('parcelid', 1)
            df_point['parcel_lat'] = row['lat']
            df_point['parcel_lon'] = row['lon']
            
            all_data.append(df_point)
            
            # Log progress every 1000 parcels
            if (idx + 1) % 1000 == 0:
                logger.info(f"Processed {idx + 1}/{len(self.parcels)} parcels")
        
        # Combine all data
        result = pd.concat(all_data, ignore_index=True)
        
        # Convert units if needed
        if variable == 'tas' and result[variable].mean() > 200:
            logger.info("Converting temperature from Kelvin to Celsius")
            result[variable] = result[variable] - 273.15
        
        return result
    
    def calculate_indices(self, df: pd.DataFrame, variable: str = 'tas') -> pd.DataFrame:
        """
        Calculate climate indices for each parcel.
        
        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame with time series data
        variable : str
            Variable name
        
        Returns:
        --------
        pd.DataFrame
            DataFrame with calculated indices
        """
        logger.info("Calculating climate indices for each parcel")
        
        indices_list = []
        
        # Group by parcel
        grouped = df.groupby(['saleid', 'parcelid'])
        
        for (saleid, parcelid), group in grouped:
            # Convert back to xarray for xclim
            times = pd.to_datetime(group['time'])
            values = group[variable].values
            
            # Create DataArray
            da = xr.DataArray(
                values,
                dims=['time'],
                coords={'time': times},
                attrs={'units': 'degC' if variable == 'tas' else ''}
            )
            
            # Calculate indices
            indices = {
                'saleid': saleid,
                'parcelid': parcelid,
                'lat': group['parcel_lat'].iloc[0],
                'lon': group['parcel_lon'].iloc[0]
            }
            
            # Annual mean
            indices['annual_mean'] = float(da.mean())
            
            # Temperature indices (if temperature data)
            if variable in ['tas', 'temperature', 'temp']:
                # Annual min/max
                indices['annual_min'] = float(da.min())
                indices['annual_max'] = float(da.max())
                
                # Frost days (Tmin < 0°C)
                indices['frost_days'] = int((da < 0).sum())
                
                # Summer days (Tmax > 25°C)
                indices['summer_days'] = int((da > 25).sum())
                
                # Tropical nights (Tmin > 20°C)
                indices['tropical_nights'] = int((da > 20).sum())
                
                # Growing degree days (base 10°C)
                gdd = da - 10
                gdd = gdd.where(gdd > 0, 0)
                indices['growing_degree_days'] = float(gdd.sum())
                
                # Try xclim indices
                try:
                    # Annual mean using xclim
                    tg_mean = atmos.tg_mean(da, freq='YS')
                    if len(tg_mean) > 0:
                        indices['tg_mean_xclim'] = float(tg_mean.values[0])
                        # Convert from Kelvin if needed
                        if indices['tg_mean_xclim'] > 200:
                            indices['tg_mean_xclim'] -= 273.15
                except:
                    pass
            
            indices_list.append(indices)
            
            # Log progress every 1000 parcels
            if len(indices_list) % 1000 == 0:
                logger.info(f"Calculated indices for {len(indices_list)} parcels")
        
        return pd.DataFrame(indices_list)
    
    def process_year(self, nc_file: str, year: int = None) -> pd.DataFrame:
        """
        Process a single year of climate data.
        
        Parameters:
        -----------
        nc_file : str
            Path to NetCDF file
        year : int
            Year being processed (for labeling)
        
        Returns:
        --------
        pd.DataFrame
            DataFrame with indices for all parcels
        """
        logger.info(f"Processing {nc_file}")
        
        # Load dataset
        ds = xr.open_dataset(nc_file)
        
        # Extract time series
        df_timeseries = self.extract_timeseries(ds)
        
        # Calculate indices
        df_indices = self.calculate_indices(df_timeseries)
        
        # Add year if provided
        if year:
            df_indices['year'] = year
        
        # Store results
        self.results[year or 'data'] = df_indices
        
        return df_indices
    
    def save_results(self, output_file: str):
        """Save results to CSV file."""
        if not self.results:
            logger.warning("No results to save")
            return
        
        # Combine all years if multiple
        if len(self.results) > 1:
            df_all = pd.concat(self.results.values(), ignore_index=True)
        else:
            df_all = list(self.results.values())[0]
        
        # Save to CSV
        df_all.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
        
        # Print summary statistics
        logger.info("\n=== Summary Statistics ===")
        numeric_cols = df_all.select_dtypes(include=[np.number]).columns
        numeric_cols = [c for c in numeric_cols if c not in ['saleid', 'parcelid', 'year']]
        
        for col in numeric_cols:
            if col in df_all.columns:
                mean_val = df_all[col].mean()
                std_val = df_all[col].std()
                logger.info(f"{col}: mean={mean_val:.2f}, std={std_val:.2f}")


def main():
    """Main function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract climate indices at parcel locations')
    parser.add_argument('--parcels', default='parcel_coordinates.csv',
                       help='CSV file with parcel coordinates')
    parser.add_argument('--input', '-i', required=True,
                       help='Input NetCDF file')
    parser.add_argument('--output', '-o', default='parcel_indices.csv',
                       help='Output CSV file')
    parser.add_argument('--year', type=int,
                       help='Year label for the data')
    
    args = parser.parse_args()
    
    # Initialize extractor
    extractor = ParcelClimateExtractor(args.parcels)
    
    # Process data
    df_indices = extractor.process_year(args.input, args.year)
    
    # Save results
    extractor.save_results(args.output)
    
    logger.info("Processing complete!")


if __name__ == "__main__":
    main()
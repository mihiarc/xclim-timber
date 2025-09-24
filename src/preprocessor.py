"""
Data preprocessing module for climate data.
Handles data cleaning, validation, unit conversion, and format standardization.
"""

import logging
from typing import Dict, List, Optional, Tuple, Union
import warnings

import numpy as np
import xarray as xr
import pandas as pd
from scipy import stats
import xclim
from xclim import core


logger = logging.getLogger(__name__)


class ClimateDataPreprocessor:
    """Preprocess climate data for analysis."""
    
    def __init__(self, config: 'Config'):
        """
        Initialize the preprocessor.
        
        Parameters:
        -----------
        config : Config
            Configuration object
        """
        self.config = config
        self.qc_config = config.get('quality_control', {})
        
    def preprocess(self, ds: xr.Dataset, 
                   variable_type: Optional[str] = None) -> xr.Dataset:
        """
        Apply full preprocessing pipeline to dataset.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        variable_type : str, optional
            Type of variable ('temperature', 'precipitation', etc.)
        
        Returns:
        --------
        xr.Dataset
            Preprocessed dataset
        """
        logger.info("Starting preprocessing pipeline")
        
        # Step 1: Validate and standardize coordinates
        ds = self.standardize_coordinates(ds)
        
        # Step 2: Handle missing values
        ds = self.handle_missing_values(ds)
        
        # Step 3: Check and convert units
        if variable_type:
            ds = self.convert_units(ds, variable_type)
        
        # Step 4: Apply quality control
        if self.qc_config.get('check_outliers', True):
            ds = self.remove_outliers(ds)
        
        # Step 5: Ensure CF compliance
        ds = self.ensure_cf_compliance(ds)
        
        # Step 6: Add metadata
        ds = self.add_metadata(ds)
        
        logger.info("Preprocessing complete")
        return ds
    
    def standardize_coordinates(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Standardize coordinate systems and ensure proper ordering.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        
        Returns:
        --------
        xr.Dataset
            Dataset with standardized coordinates
        """
        logger.info("Standardizing coordinates")
        
        # Ensure standard coordinate names (done in loader, but double-check)
        coord_standards = {
            'lat': {'standard_name': 'latitude', 'units': 'degrees_north', 'axis': 'Y'},
            'lon': {'standard_name': 'longitude', 'units': 'degrees_east', 'axis': 'X'},
            'time': {'standard_name': 'time', 'axis': 'T'}
        }
        
        for coord, attrs in coord_standards.items():
            if coord in ds.coords:
                for attr_name, attr_value in attrs.items():
                    ds.coords[coord].attrs[attr_name] = attr_value
        
        # Ensure proper coordinate ordering
        if 'lat' in ds.dims and 'lon' in ds.dims:
            # Check if latitude is decreasing (common in some datasets)
            if ds.lat[0] > ds.lat[-1]:
                logger.info("Reversing latitude axis to increasing order")
                ds = ds.reindex(lat=ds.lat[::-1])
            
            # Ensure longitude is in [-180, 180] or [0, 360]
            if ds.lon.min() < -180 or ds.lon.max() > 360:
                logger.warning("Longitude values out of expected range")
        
        # Sort time dimension if present
        if 'time' in ds.dims:
            if not ds.indexes['time'].is_monotonic_increasing:
                logger.info("Sorting time dimension")
                ds = ds.sortby('time')
        
        return ds
    
    def handle_missing_values(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Handle missing values in the dataset.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        
        Returns:
        --------
        xr.Dataset
            Dataset with missing values handled
        """
        logger.info("Handling missing values")
        
        missing_threshold = self.qc_config.get('missing_threshold', 0.1)
        
        for var in ds.data_vars:
            data = ds[var]
            
            # Calculate missing data percentage
            total_size = data.size
            missing_count = data.isnull().sum().values
            missing_pct = missing_count / total_size

            # For PRISM/gridded data, ~40-50% missing is normal (ocean/water mask)
            # Only log details if it's outside the expected range
            if 0.40 <= missing_pct <= 0.50:
                # This is expected ocean masking - use debug level
                logger.debug(f"Variable {var}: {missing_pct:.2%} missing values (expected ocean mask)")
            elif missing_pct > 0.50:
                # This is higher than expected even with ocean mask
                logger.warning(f"Variable {var}: {missing_pct:.2%} missing values (higher than expected)")
            elif missing_pct > missing_threshold:
                # Between threshold and ocean mask range
                logger.info(f"Variable {var}: {missing_pct:.2%} missing values")
            else:
                # Low missing values - just debug
                logger.debug(f"Variable {var}: {missing_pct:.2%} missing values")
            
            # Apply interpolation for small gaps
            if missing_pct > 0 and missing_pct < missing_threshold:
                if 'time' in data.dims:
                    # Interpolate along time dimension for small gaps
                    ds[var] = data.interpolate_na(dim='time', method='linear', limit=5)
                    logger.info(f"Applied temporal interpolation to {var}")
        
        return ds
    
    def convert_units(self, ds: xr.Dataset, variable_type: str) -> xr.Dataset:
        """
        Convert units to standard units for climate indices.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        variable_type : str
            Type of variable
        
        Returns:
        --------
        xr.Dataset
            Dataset with converted units
        """
        logger.info(f"Converting units for {variable_type}")
        
        # Define standard units for different variable types
        standard_units = {
            'temperature': 'degC',  # Celsius
            'precipitation': 'mm',   # millimeters
            'humidity': '%',         # percentage
            'wind': 'm s-1',        # meters per second
            'pressure': 'hPa'       # hectopascals
        }
        
        target_unit = standard_units.get(variable_type)
        if not target_unit:
            logger.warning(f"No standard unit defined for {variable_type}")
            return ds
        
        for var in ds.data_vars:
            if 'units' not in ds[var].attrs:
                logger.warning(f"No units attribute for {var}, skipping conversion")
                continue
            
            current_unit = ds[var].attrs['units']
            
            # Temperature conversions
            if variable_type == 'temperature':
                if current_unit in ['K', 'kelvin', 'Kelvin']:
                    ds[var] = ds[var] - 273.15
                    ds[var].attrs['units'] = 'degC'
                    logger.info(f"Converted {var} from Kelvin to Celsius")
                elif current_unit in ['F', 'fahrenheit', 'Fahrenheit']:
                    ds[var] = (ds[var] - 32) * 5/9
                    ds[var].attrs['units'] = 'degC'
                    logger.info(f"Converted {var} from Fahrenheit to Celsius")
            
            # Precipitation conversions
            elif variable_type == 'precipitation':
                if current_unit in ['m', 'meters']:
                    ds[var] = ds[var] * 1000
                    ds[var].attrs['units'] = 'mm'
                    logger.info(f"Converted {var} from meters to millimeters")
                elif current_unit in ['kg m-2 s-1']:
                    # Convert precipitation flux to mm/day
                    ds[var] = ds[var] * 86400  # seconds to day
                    ds[var].attrs['units'] = 'mm/day'
                    logger.info(f"Converted {var} from kg m-2 s-1 to mm/day")
                elif current_unit in ['in', 'inches']:
                    ds[var] = ds[var] * 25.4
                    ds[var].attrs['units'] = 'mm'
                    logger.info(f"Converted {var} from inches to millimeters")
        
        return ds
    
    def remove_outliers(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Remove statistical outliers from the dataset.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        
        Returns:
        --------
        xr.Dataset
            Dataset with outliers removed
        """
        logger.info("Checking for outliers")
        
        method = self.qc_config.get('outlier_method', 'zscore')
        threshold = self.qc_config.get('outlier_threshold', 5)
        
        for var in ds.data_vars:
            data = ds[var]
            
            if method == 'zscore':
                # Calculate z-scores
                mean = data.mean(skipna=True)
                std = data.std(skipna=True)
                z_scores = np.abs((data - mean) / std)
                
                # Mark outliers as NaN
                outlier_mask = z_scores > threshold
                outlier_count = outlier_mask.sum().values
                
                if outlier_count > 0:
                    logger.info(f"Found {outlier_count} outliers in {var} (z-score > {threshold})")
                    ds[var] = data.where(~outlier_mask)
            
            elif method == 'iqr':
                # Interquartile range method
                q1 = data.quantile(0.25, skipna=True)
                q3 = data.quantile(0.75, skipna=True)
                iqr = q3 - q1
                
                lower_bound = q1 - threshold * iqr
                upper_bound = q3 + threshold * iqr
                
                outlier_mask = (data < lower_bound) | (data > upper_bound)
                outlier_count = outlier_mask.sum().values
                
                if outlier_count > 0:
                    logger.info(f"Found {outlier_count} outliers in {var} (IQR method)")
                    ds[var] = data.where(~outlier_mask)
        
        return ds
    
    def ensure_cf_compliance(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Ensure dataset follows CF (Climate and Forecast) conventions.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        
        Returns:
        --------
        xr.Dataset
            CF-compliant dataset
        """
        logger.info("Ensuring CF compliance")
        
        # Add global attributes
        cf_attrs = {
            'Conventions': 'CF-1.8',
            'featureType': 'grid'
        }
        ds.attrs.update(cf_attrs)
        
        # Ensure time has proper attributes
        if 'time' in ds.coords:
            if 'calendar' not in ds.time.attrs:
                ds.time.attrs['calendar'] = 'standard'
            if 'units' not in ds.time.attrs:
                # Try to infer units
                try:
                    time_units = pd.to_datetime(ds.time.values[0])
                    ds.time.attrs['units'] = f'days since {time_units.strftime("%Y-%m-%d")}'
                except:
                    ds.time.attrs['units'] = 'days since 1900-01-01'
        
        # Add standard names for common variables
        standard_names = {
            'tas': 'air_temperature',
            'tasmax': 'air_temperature_maximum',
            'tasmin': 'air_temperature_minimum',
            'pr': 'precipitation_amount',
            'prc': 'convective_precipitation_amount',
            'hurs': 'relative_humidity',
            'huss': 'specific_humidity',
            'sfcWind': 'wind_speed',
            'ps': 'surface_air_pressure'
        }
        
        for var in ds.data_vars:
            if var in standard_names:
                ds[var].attrs['standard_name'] = standard_names[var]
            
            # Ensure all variables have units
            if 'units' not in ds[var].attrs:
                ds[var].attrs['units'] = 'unknown'
            
            # Add _FillValue if missing
            if '_FillValue' not in ds[var].attrs:
                ds[var].attrs['_FillValue'] = np.nan
        
        return ds
    
    def add_metadata(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Add metadata to the dataset.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Input dataset
        
        Returns:
        --------
        xr.Dataset
            Dataset with added metadata
        """
        from datetime import datetime
        
        # Add processing metadata
        ds.attrs.update({
            'processing_date': datetime.now().isoformat(),
            'processing_software': 'xclim-timber pipeline',
            'institution': self.config.get('output.attributes.institution', 'Unknown'),
            'source': self.config.get('output.attributes.source', 'Climate data'),
            'history': f"{datetime.now().isoformat()}: Preprocessed with xclim-timber",
            'references': self.config.get('output.attributes.references', 'https://xclim.readthedocs.io/')
        })
        
        return ds
    


# Example usage
if __name__ == "__main__":
    from config import Config
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create configuration
    config = Config()
    
    # Initialize preprocessor
    preprocessor = ClimateDataPreprocessor(config)
    
    # Example: Create a sample dataset
    import pandas as pd
    
    times = pd.date_range('2020-01-01', '2020-12-31', freq='D')
    lats = np.linspace(-90, 90, 180)
    lons = np.linspace(-180, 180, 360)
    
    # Create temperature data with some noise
    temp_data = 15 + 10 * np.sin(np.arange(len(times)) * 2 * np.pi / 365)
    temp_data = temp_data[:, np.newaxis, np.newaxis] + np.random.randn(len(times), len(lats), len(lons))
    
    ds = xr.Dataset(
        {
            'temperature': (['time', 'lat', 'lon'], temp_data)
        },
        coords={
            'time': times,
            'lat': lats,
            'lon': lons
        }
    )
    
    # Add units
    ds['temperature'].attrs['units'] = 'K'
    
    print("Original dataset:")
    print(ds)
    
    # Preprocess
    ds_processed = preprocessor.preprocess(ds, variable_type='temperature')
    
    print("\nProcessed dataset:")
    print(ds_processed)
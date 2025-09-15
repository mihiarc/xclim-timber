"""
Climate indices calculator using xclim.
Calculates various climate indices from preprocessed data.
"""

import logging
from typing import Dict, List, Optional, Union
import warnings

import numpy as np
import xarray as xr
import xclim
from xclim import atmos, land, generic
from xclim.core.units import convert_units_to
import pandas as pd


logger = logging.getLogger(__name__)


class ClimateIndicesCalculator:
    """Calculate climate indices using xclim."""
    
    def __init__(self, config: 'Config'):
        """
        Initialize the indices calculator.

        Parameters:
        -----------
        config : Config
            Configuration object
        """
        self.config = config
        self.indices_config = config.get('indices', {})
        self.results = {}

        # Baseline period configuration
        baseline_config = self.indices_config.get('baseline_period', {})
        self.baseline_start = baseline_config.get('start', 1981)
        self.baseline_end = baseline_config.get('end', 2010)
        self.use_baseline = self.indices_config.get('use_baseline_for_percentiles', True)
        
    def calculate_all_indices(self, datasets: Dict[str, xr.Dataset]) -> Dict[str, xr.Dataset]:
        """
        Calculate all configured climate indices.
        
        Parameters:
        -----------
        datasets : dict
            Dictionary of datasets by variable type
        
        Returns:
        --------
        dict
            Dictionary of calculated indices
        """
        logger.info("Starting climate indices calculation")
        
        results = {}
        
        # Temperature indices
        if 'temperature' in datasets:
            temp_indices = self.calculate_temperature_indices(datasets['temperature'])
            results.update(temp_indices)
        
        # Precipitation indices
        if 'precipitation' in datasets:
            precip_indices = self.calculate_precipitation_indices(datasets['precipitation'])
            results.update(precip_indices)
        
        # Combined indices (requiring multiple variables)
        if 'temperature' in datasets and 'precipitation' in datasets:
            combined_indices = self.calculate_combined_indices(
                datasets['temperature'], 
                datasets['precipitation']
            )
            results.update(combined_indices)
        
        self.results = results
        logger.info(f"Calculated {len(results)} climate indices")
        
        return results
    
    def calculate_temperature_indices(self, ds: xr.Dataset) -> Dict[str, xr.DataArray]:
        """
        Calculate temperature-based climate indices.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Temperature dataset
        
        Returns:
        --------
        dict
            Dictionary of calculated indices
        """
        logger.info("Calculating temperature indices")
        
        indices = {}
        configured_indices = self.indices_config.get('temperature', [])
        
        # Find temperature variables in dataset
        tas = self._get_variable(ds, ['tas', 'temperature', 'temp', 'tmean'])
        tasmax = self._get_variable(ds, ['tasmax', 'tmax', 'temperature_max'])
        tasmin = self._get_variable(ds, ['tasmin', 'tmin', 'temperature_min'])
        
        # If we only have mean temperature, use it for max/min as well
        if tas is not None and tasmax is None:
            tasmax = tas
        if tas is not None and tasmin is None:
            tasmin = tas
        
        # Calculate each configured index
        if tas is not None:
            # Mean temperature
            if 'tg_mean' in configured_indices:
                try:
                    indices['tg_mean'] = atmos.tg_mean(tas, freq='YS')
                    logger.info("Calculated mean temperature")
                except Exception as e:
                    logger.error(f"Error calculating tg_mean: {e}")
        
        if tasmax is not None:
            # Maximum temperature
            if 'tx_max' in configured_indices:
                try:
                    indices['tx_max'] = atmos.tx_max(tasmax, freq='YS')
                    logger.info("Calculated maximum temperature")
                except Exception as e:
                    logger.error(f"Error calculating tx_max: {e}")
            
            # Summer days (Tmax > 25°C)
            if 'summer_days' in configured_indices:
                try:
                    indices['summer_days'] = atmos.summer_days(tasmax, freq='YS')
                    logger.info("Calculated summer days")
                except Exception as e:
                    logger.error(f"Error calculating summer_days: {e}")
            
            # Ice days (Tmax < 0°C)
            if 'ice_days' in configured_indices:
                try:
                    indices['ice_days'] = atmos.ice_days(tasmax, freq='YS')
                    logger.info("Calculated ice days")
                except Exception as e:
                    logger.error(f"Error calculating ice_days: {e}")
        
        if tasmin is not None:
            # Minimum temperature
            if 'tn_min' in configured_indices:
                try:
                    indices['tn_min'] = atmos.tn_min(tasmin, freq='YS')
                    logger.info("Calculated minimum temperature")
                except Exception as e:
                    logger.error(f"Error calculating tn_min: {e}")
            
            # Tropical nights (Tmin > 20°C)
            if 'tropical_nights' in configured_indices:
                try:
                    indices['tropical_nights'] = atmos.tropical_nights(tasmin, freq='YS')
                    logger.info("Calculated tropical nights")
                except Exception as e:
                    logger.error(f"Error calculating tropical_nights: {e}")
            
            # Frost days (Tmin < 0°C)
            if 'frost_days' in configured_indices:
                try:
                    indices['frost_days'] = atmos.frost_days(tasmin, freq='YS')
                    logger.info("Calculated frost days")
                except Exception as e:
                    logger.error(f"Error calculating frost_days: {e}")
        
        if tas is not None:
            # Growing degree days
            if 'growing_degree_days' in configured_indices:
                try:
                    indices['growing_degree_days'] = atmos.growing_degree_days(
                        tas, thresh='10 degC', freq='YS'
                    )
                    logger.info("Calculated growing degree days")
                except Exception as e:
                    logger.error(f"Error calculating growing_degree_days: {e}")
            
            # Heating degree days
            if 'heating_degree_days' in configured_indices:
                try:
                    indices['heating_degree_days'] = atmos.heating_degree_days(
                        tas, thresh='17 degC', freq='YS'
                    )
                    logger.info("Calculated heating degree days")
                except Exception as e:
                    logger.error(f"Error calculating heating_degree_days: {e}")
            
            # Cooling degree days
            if 'cooling_degree_days' in configured_indices:
                try:
                    indices['cooling_degree_days'] = atmos.cooling_degree_days(
                        tas, thresh='18 degC', freq='YS'
                    )
                    logger.info("Calculated cooling degree days")
                except Exception as e:
                    logger.error(f"Error calculating cooling_degree_days: {e}")
        
        # Extreme temperature indices
        if tasmax is not None and tasmin is not None:
            if 'tx90p' in configured_indices or 'tx90p' in self.indices_config.get('extremes', []):
                try:
                    # Calculate percentile using baseline period
                    tx90_per = self._calculate_baseline_percentile(tasmax, 0.9)
                    tx90 = atmos.tx90p(
                        tasmax,
                        tasmax_per=tx90_per,
                        freq='YS'
                    )
                    indices['tx90p'] = tx90
                    logger.info("Calculated warm days (TX90p)")
                except Exception as e:
                    logger.error(f"Error calculating tx90p: {e}")

            if 'tn10p' in configured_indices or 'tn10p' in self.indices_config.get('extremes', []):
                try:
                    # Calculate percentile using baseline period
                    tn10_per = self._calculate_baseline_percentile(tasmin, 0.1)
                    tn10 = atmos.tn10p(
                        tasmin,
                        tasmin_per=tn10_per,
                        freq='YS'
                    )
                    indices['tn10p'] = tn10
                    logger.info("Calculated cool nights (TN10p)")
                except Exception as e:
                    logger.error(f"Error calculating tn10p: {e}")
        
        return indices
    
    def calculate_precipitation_indices(self, ds: xr.Dataset) -> Dict[str, xr.DataArray]:
        """
        Calculate precipitation-based climate indices.
        
        Parameters:
        -----------
        ds : xr.Dataset
            Precipitation dataset
        
        Returns:
        --------
        dict
            Dictionary of calculated indices
        """
        logger.info("Calculating precipitation indices")
        
        indices = {}
        configured_indices = self.indices_config.get('precipitation', [])
        
        # Find precipitation variable in dataset
        pr = self._get_variable(ds, ['pr', 'precipitation', 'precip', 'prcp'])
        
        if pr is None:
            logger.warning("No precipitation variable found in dataset")
            return indices
        
        # Total precipitation
        if 'prcptot' in configured_indices:
            try:
                indices['prcptot'] = atmos.prcptot(pr, freq='YS')
                logger.info("Calculated total precipitation")
            except Exception as e:
                logger.error(f"Error calculating prcptot: {e}")
        
        # Maximum 1-day precipitation
        if 'rx1day' in configured_indices:
            try:
                indices['rx1day'] = atmos.max_1day_precipitation_amount(pr, freq='YS')
                logger.info("Calculated max 1-day precipitation")
            except Exception as e:
                logger.error(f"Error calculating rx1day: {e}")
        
        # Maximum 5-day precipitation
        if 'rx5day' in configured_indices:
            try:
                indices['rx5day'] = atmos.max_n_day_precipitation_amount(
                    pr, window=5, freq='YS'
                )
                logger.info("Calculated max 5-day precipitation")
            except Exception as e:
                logger.error(f"Error calculating rx5day: {e}")
        
        # Simple daily intensity index
        if 'sdii' in configured_indices:
            try:
                indices['sdii'] = atmos.daily_pr_intensity(pr, freq='YS')
                logger.info("Calculated simple daily intensity index")
            except Exception as e:
                logger.error(f"Error calculating sdii: {e}")
        
        # Consecutive dry days
        if 'cdd' in configured_indices:
            try:
                indices['cdd'] = atmos.maximum_consecutive_dry_days(pr, freq='YS')
                logger.info("Calculated consecutive dry days")
            except Exception as e:
                logger.error(f"Error calculating cdd: {e}")
        
        # Consecutive wet days
        if 'cwd' in configured_indices:
            try:
                indices['cwd'] = atmos.maximum_consecutive_wet_days(pr, freq='YS')
                logger.info("Calculated consecutive wet days")
            except Exception as e:
                logger.error(f"Error calculating cwd: {e}")
        
        # Number of heavy precipitation days (>10mm)
        if 'r10mm' in configured_indices:
            try:
                indices['r10mm'] = atmos.wetdays(pr, thresh='10 mm', freq='YS')
                logger.info("Calculated heavy precipitation days (R10mm)")
            except Exception as e:
                logger.error(f"Error calculating r10mm: {e}")
        
        # Number of very heavy precipitation days (>20mm)
        if 'r20mm' in configured_indices:
            try:
                indices['r20mm'] = atmos.wetdays(pr, thresh='20 mm', freq='YS')
                logger.info("Calculated very heavy precipitation days (R20mm)")
            except Exception as e:
                logger.error(f"Error calculating r20mm: {e}")
        
        # Very wet days (95th percentile)
        if 'r95p' in configured_indices:
            try:
                # Calculate 95th percentile using baseline period
                pr_per = self._calculate_baseline_percentile(pr, 0.95)
                indices['r95ptot'] = atmos.days_over_precip_thresh(
                    pr, pr_per, freq='YS'
                )
                logger.info("Calculated very wet days (R95p)")
            except Exception as e:
                logger.error(f"Error calculating r95p: {e}")

        # Extremely wet days (99th percentile)
        if 'r99p' in configured_indices:
            try:
                # Calculate 99th percentile using baseline period
                pr_per = self._calculate_baseline_percentile(pr, 0.99)
                indices['r99ptot'] = atmos.days_over_precip_thresh(
                    pr, pr_per, freq='YS'
                )
                logger.info("Calculated extremely wet days (R99p)")
            except Exception as e:
                logger.error(f"Error calculating r99p: {e}")
        
        return indices
    
    def calculate_combined_indices(self, temp_ds: xr.Dataset, 
                                  precip_ds: xr.Dataset) -> Dict[str, xr.DataArray]:
        """
        Calculate indices requiring multiple variables.
        
        Parameters:
        -----------
        temp_ds : xr.Dataset
            Temperature dataset
        precip_ds : xr.Dataset
            Precipitation dataset
        
        Returns:
        --------
        dict
            Dictionary of calculated indices
        """
        logger.info("Calculating combined indices")
        
        indices = {}
        agricultural_indices = self.indices_config.get('agricultural', [])
        extreme_indices = self.indices_config.get('extremes', [])
        
        # Get variables
        tas = self._get_variable(temp_ds, ['tas', 'temperature', 'temp', 'tmean'])
        pr = self._get_variable(precip_ds, ['pr', 'precipitation', 'precip'])
        
        # Growing season length
        if 'gsl' in agricultural_indices and tas is not None:
            try:
                indices['gsl'] = atmos.growing_season_length(tas, freq='YS')
                logger.info("Calculated growing season length")
            except Exception as e:
                logger.error(f"Error calculating gsl: {e}")
        
        # Standardized Precipitation Index (SPI)
        if 'spi' in agricultural_indices and pr is not None:
            try:
                # SPI requires monthly data
                pr_monthly = pr.resample(time='M').sum()
                indices['spi_3'] = atmos.standardized_precipitation_index(
                    pr_monthly, 
                    freq='MS',
                    window=3,
                    dist='gamma',
                    method='APP'
                )
                logger.info("Calculated 3-month SPI")
            except Exception as e:
                logger.error(f"Error calculating spi: {e}")
        
        # Note: SPEI requires potential evapotranspiration calculation
        # which needs additional data (radiation, humidity, wind speed)
        # Placeholder for SPEI if all required data is available
        
        return indices
    
    def _get_baseline_data(self, data: xr.DataArray,
                           start_year: Optional[int] = None,
                           end_year: Optional[int] = None) -> xr.DataArray:
        """
        Extract baseline period for percentile calculations.

        Parameters:
        -----------
        data : xr.DataArray
            Input data array
        start_year : int, optional
            Start year of baseline period (default: from config)
        end_year : int, optional
            End year of baseline period (default: from config)

        Returns:
        --------
        xr.DataArray
            Data for baseline period
        """
        start = start_year or self.baseline_start
        end = end_year or self.baseline_end

        try:
            baseline = data.sel(time=slice(str(start), str(end)))
            if len(baseline.time) == 0:
                logger.warning(f"No data found for baseline period {start}-{end}, using full period")
                return data
            logger.info(f"Using baseline period {start}-{end} with {len(baseline.time)} time steps")
            return baseline
        except Exception as e:
            logger.warning(f"Error selecting baseline period: {e}, using full period")
            return data

    def _calculate_baseline_percentile(self, data: xr.DataArray,
                                      percentile: float,
                                      use_baseline: Optional[bool] = None) -> xr.DataArray:
        """
        Calculate percentile using baseline period or full period.

        Parameters:
        -----------
        data : xr.DataArray
            Input data array
        percentile : float
            Percentile to calculate (0-1)
        use_baseline : bool, optional
            Whether to use baseline period (default: from config)

        Returns:
        --------
        xr.DataArray
            Calculated percentile
        """
        use_baseline_flag = use_baseline if use_baseline is not None else self.use_baseline

        if use_baseline_flag:
            baseline_data = self._get_baseline_data(data)
            percentile_value = baseline_data.quantile(percentile, dim='time')
            logger.info(f"Calculated {percentile*100}th percentile using baseline period")
        else:
            percentile_value = data.quantile(percentile, dim='time')
            logger.info(f"Calculated {percentile*100}th percentile using full period")

        return percentile_value

    def _get_variable(self, ds: xr.Dataset,
                     possible_names: List[str]) -> Optional[xr.DataArray]:
        """
        Find a variable in dataset by possible names.

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset to search
        possible_names : list
            List of possible variable names

        Returns:
        --------
        xr.DataArray or None
            Found variable or None
        """
        for name in possible_names:
            if name in ds.data_vars:
                return ds[name]
            # Also check with case-insensitive match
            for var in ds.data_vars:
                if var.lower() == name.lower():
                    return ds[var]
        return None
    
    def save_indices(self, output_path: str, format: str = 'netcdf'):
        """
        Save calculated indices to file.
        
        Parameters:
        -----------
        output_path : str
            Output file path
        format : str
            Output format ('netcdf' or 'geotiff')
        """
        if not self.results:
            logger.warning("No indices to save")
            return
        
        logger.info(f"Saving {len(self.results)} indices to {output_path}")
        
        # Combine all indices into a single dataset
        ds_out = xr.Dataset(self.results)
        
        # Add metadata
        ds_out.attrs.update({
            'title': 'Climate Indices',
            'description': 'Climate indices calculated using xclim',
            'software': 'xclim-timber pipeline',
            'xclim_version': xclim.__version__
        })
        
        if format == 'netcdf':
            # Save as NetCDF
            compression = self.config.get('output.compression', {})
            ds_out.to_netcdf(
                output_path,
                engine=compression.get('engine', 'h5netcdf'),
                encoding={
                    var: {'zlib': True, 'complevel': compression.get('complevel', 4)}
                    for var in ds_out.data_vars
                }
            )
        elif format == 'geotiff':
            # Save each index as a separate GeoTIFF
            import rioxarray
            from pathlib import Path
            
            output_dir = Path(output_path).parent
            for name, data in ds_out.data_vars.items():
                output_file = output_dir / f"{name}.tif"
                
                # Ensure CRS is set
                if not data.rio.crs:
                    data = data.rio.write_crs('EPSG:4326')
                
                # Save to GeoTIFF
                data.rio.to_raster(str(output_file))
                logger.info(f"Saved {name} to {output_file}")
        
        logger.info("Indices saved successfully")
    
    def get_summary(self) -> Dict:
        """
        Get summary statistics of calculated indices.
        
        Returns:
        --------
        dict
            Summary statistics
        """
        summary = {}
        
        for name, data in self.results.items():
            summary[name] = {
                'mean': float(data.mean().values),
                'std': float(data.std().values),
                'min': float(data.min().values),
                'max': float(data.max().values),
                'shape': data.shape,
                'units': data.attrs.get('units', 'unknown')
            }
        
        return summary


# Example usage
if __name__ == "__main__":
    from config import Config
    import pandas as pd
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Create configuration
    config = Config()
    
    # Initialize calculator
    calculator = ClimateIndicesCalculator(config)
    
    # Create sample data
    times = pd.date_range('2020-01-01', '2022-12-31', freq='D')
    lats = np.linspace(-45, 45, 90)
    lons = np.linspace(-180, 180, 180)
    
    # Temperature data (with seasonal variation)
    temp_data = 15 + 10 * np.sin(np.arange(len(times)) * 2 * np.pi / 365)
    temp_data = temp_data[:, np.newaxis, np.newaxis] + \
                5 * np.random.randn(len(times), len(lats), len(lons))
    
    # Precipitation data
    precip_data = np.random.exponential(2, (len(times), len(lats), len(lons)))
    
    # Create datasets
    temp_ds = xr.Dataset(
        {'tas': (['time', 'lat', 'lon'], temp_data)},
        coords={'time': times, 'lat': lats, 'lon': lons}
    )
    temp_ds['tas'].attrs['units'] = 'degC'
    
    precip_ds = xr.Dataset(
        {'pr': (['time', 'lat', 'lon'], precip_data)},
        coords={'time': times, 'lat': lats, 'lon': lons}
    )
    precip_ds['pr'].attrs['units'] = 'mm'
    
    # Calculate indices
    datasets = {'temperature': temp_ds, 'precipitation': precip_ds}
    indices = calculator.calculate_all_indices(datasets)
    
    print(f"Calculated {len(indices)} indices:")
    for name, data in indices.items():
        print(f"  - {name}: shape={data.shape}, units={data.attrs.get('units', 'unknown')}")
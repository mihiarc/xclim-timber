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
        # Add caching for performance optimization
        self._percentile_cache = {}
        self._pressure_array = None

        # Baseline period configuration
        baseline_config = self.indices_config.get('baseline_period', {})
        self.baseline_start = baseline_config.get('start', 1971)
        self.baseline_end = baseline_config.get('end', 2000)
        self.use_baseline = self.indices_config.get('use_baseline_for_percentiles', True)

        # Validate baseline configuration
        if self.baseline_start >= self.baseline_end:
            raise ValueError(f"Invalid baseline period: start year ({self.baseline_start}) must be before end year ({self.baseline_end})")
        if self.baseline_end - self.baseline_start < 10:
            logger.warning(f"Short baseline period ({self.baseline_end - self.baseline_start} years). WMO recommends at least 30 years.")
        
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

        # Humidity indices
        if 'humidity' in datasets:
            temp_ds = datasets.get('temperature', None)
            humidity_indices = self.calculate_humidity_indices(datasets['humidity'], temp_ds)
            results.update(humidity_indices)

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
        tasmax = self._get_variable(ds, ['tasmax', 'tmax', 'temperature_max', 'tasmax'])
        tasmin = self._get_variable(ds, ['tasmin', 'tmin', 'temperature_min', 'tasmin'])
        
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
        
        # Temperature range indices
        if tasmax is not None and tasmin is not None:
            if 'daily_temperature_range' in configured_indices:
                try:
                    indices['daily_temperature_range'] = atmos.daily_temperature_range(
                        tasmin, tasmax, freq='YS'
                    )
                    logger.info("Calculated daily temperature range")
                except Exception as e:
                    logger.error(f"Error calculating daily_temperature_range: {e}")

            if 'daily_temperature_range_variability' in configured_indices:
                try:
                    indices['daily_temperature_range_variability'] = atmos.daily_temperature_range_variability(
                        tasmin, tasmax, freq='YS'
                    )
                    logger.info("Calculated daily temperature range variability")
                except Exception as e:
                    logger.error(f"Error calculating daily_temperature_range_variability: {e}")

        # Enhanced threshold-based temperature indices
        if tasmax is not None:
            # Hot days (Tmax > 30°C)
            if 'hot_days' in configured_indices:
                try:
                    indices['hot_days'] = generic.threshold_count(
                        tasmax, thresh='30 degC', op='>', freq='YS'
                    )
                    logger.info("Calculated hot days")
                except Exception as e:
                    logger.error(f"Error calculating hot_days: {e}")

            # Very hot days (Tmax > 35°C)
            if 'very_hot_days' in configured_indices:
                try:
                    indices['very_hot_days'] = generic.threshold_count(
                        tasmax, thresh='35 degC', op='>', freq='YS'
                    )
                    logger.info("Calculated very hot days")
                except Exception as e:
                    logger.error(f"Error calculating very_hot_days: {e}")

        if tasmin is not None:
            # Warm nights (Tmin > 15°C)
            if 'warm_nights' in configured_indices:
                try:
                    indices['warm_nights'] = generic.threshold_count(
                        tasmin, thresh='15 degC', op='>', freq='YS'
                    )
                    logger.info("Calculated warm nights")
                except Exception as e:
                    logger.error(f"Error calculating warm_nights: {e}")

            # Consecutive frost days
            if 'consecutive_frost_days' in configured_indices:
                try:
                    indices['consecutive_frost_days'] = atmos.consecutive_frost_days(
                        tasmin, freq='YS'
                    )
                    logger.info("Calculated consecutive frost days")
                except Exception as e:
                    logger.error(f"Error calculating consecutive_frost_days: {e}")

        # Extreme temperature indices
        if tasmax is not None and tasmin is not None:
            if 'tx90p' in configured_indices or 'tx90p' in self.indices_config.get('extremes', []):
                try:
                    # Use baseline period if configured, otherwise use cached percentile
                    if self.use_baseline:
                        tx90_per = self._calculate_baseline_percentile(tasmax, 0.9)
                    else:
                        tx90_per = self._get_cached_percentile(tasmax, 0.9)
                    indices['tx90p'] = atmos.tx90p(
                        tasmax,
                        tasmax_per=tx90_per,
                        freq='YS'
                    )
                    logger.info("Calculated warm days (TX90p)")
                except Exception as e:
                    logger.error(f"Error calculating tx90p: {e}")

            if 'tn90p' in configured_indices or 'tn90p' in self.indices_config.get('extremes', []):
                try:
                    # Calculate percentile using baseline if configured
                    if self.use_baseline:
                        tn90_per = self._calculate_baseline_percentile(tasmin, 0.9)
                    else:
                        tn90_per = tasmin.quantile(0.9, dim='time')
                    tn90 = atmos.tn90p(
                        tasmin,
                        tasmin_per=tn90_per,
                        freq='YS'
                    )
                    indices['tn90p'] = tn90
                    logger.info("Calculated warm nights (TN90p)")
                except Exception as e:
                    logger.error(f"Error calculating tn90p: {e}")

            if 'tx10p' in configured_indices or 'tx10p' in self.indices_config.get('extremes', []):
                try:
                    # Calculate percentile using baseline if configured
                    if self.use_baseline:
                        tx10_per = self._calculate_baseline_percentile(tasmax, 0.1)
                    else:
                        tx10_per = tasmax.quantile(0.1, dim='time')
                    tx10 = atmos.tx10p(
                        tasmax,
                        tasmax_per=tx10_per,
                        freq='YS'
                    )
                    indices['tx10p'] = tx10
                    logger.info("Calculated cool days (TX10p)")
                except Exception as e:
                    logger.error(f"Error calculating tx10p: {e}")

            if 'tn10p' in configured_indices or 'tn10p' in self.indices_config.get('extremes', []):
                try:
                    # Calculate percentile using baseline if configured
                    if self.use_baseline:
                        tn10_per = self._calculate_baseline_percentile(tasmin, 0.1)
                    else:
                        tn10_per = tasmin.quantile(0.1, dim='time')
                    tn10 = atmos.tn10p(
                        tasmin,
                        tasmin_per=tn10_per,
                        freq='YS'
                    )
                    indices['tn10p'] = tn10
                    logger.info("Calculated cool nights (TN10p)")
                except Exception as e:
                    logger.error(f"Error calculating tn10p: {e}")

            # Spell duration indices
            if 'warm_spell_duration_index' in configured_indices or 'wsdi' in self.indices_config.get('extremes', []):
                try:
                    indices['warm_spell_duration_index'] = atmos.warm_spell_duration_index(
                        tasmax, tasmax_per=tasmax.quantile(0.9, dim='time'), freq='YS'
                    )
                    logger.info("Calculated warm spell duration index")
                except Exception as e:
                    logger.error(f"Error calculating warm_spell_duration_index: {e}")

            if 'cold_spell_duration_index' in configured_indices or 'csdi' in self.indices_config.get('extremes', []):
                try:
                    indices['cold_spell_duration_index'] = atmos.cold_spell_duration_index(
                        tasmin, tasmin_per=tasmin.quantile(0.1, dim='time'), freq='YS'
                    )
                    logger.info("Calculated cold spell duration index")
                except Exception as e:
                    logger.error(f"Error calculating cold_spell_duration_index: {e}")
        
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
                # Calculate 95th percentile using baseline if configured
                if self.use_baseline:
                    pr_per = self._calculate_baseline_percentile(pr, 0.95)
                else:
                    pr_per = pr.quantile(0.95, dim='time')
                indices['r95ptot'] = atmos.days_over_precip_thresh(
                    pr, pr_per, freq='YS'
                )
                logger.info("Calculated very wet days (R95p)")
            except Exception as e:
                logger.error(f"Error calculating r95p: {e}")
        
        # Extremely wet days (99th percentile)
        if 'r99p' in configured_indices:
            try:
                # Calculate 99th percentile using baseline if configured
                if self.use_baseline:
                    pr_per = self._calculate_baseline_percentile(pr, 0.99)
                else:
                    pr_per = pr.quantile(0.99, dim='time')
                indices['r99ptot'] = atmos.days_over_precip_thresh(
                    pr, pr_per, freq='YS'
                )
                logger.info("Calculated extremely wet days (R99p)")
            except Exception as e:
                logger.error(f"Error calculating r99p: {e}")
        
        return indices

    def calculate_humidity_indices(self, ds: xr.Dataset, temp_ds: xr.Dataset = None) -> Dict[str, xr.DataArray]:
        """
        Calculate humidity-based climate indices.

        Parameters:
        -----------
        ds : xr.Dataset
            Humidity dataset
        temp_ds : xr.Dataset, optional
            Temperature dataset for combined calculations

        Returns:
        --------
        dict
            Dictionary of calculated indices
        """
        logger.info("Calculating humidity-based indices")

        indices = {}
        humidity_indices = self.indices_config.get('humidity', [])
        comfort_indices = self.indices_config.get('comfort', [])

        # Find humidity variables
        hus = self._get_variable(ds, ['hus', 'huss', 'specific_humidity', 'q'])
        hurs = self._get_variable(ds, ['hurs', 'relative_humidity', 'rh'])

        # Get temperature variables if available
        tas = None
        tasmax = None
        tasmin = None
        if temp_ds is not None:
            tas = self._get_variable(temp_ds, ['tas', 'temperature', 'temp', 'tmean'])
            tasmax = self._get_variable(temp_ds, ['tasmax', 'tmax', 'temperature_max'])
            tasmin = self._get_variable(temp_ds, ['tasmin', 'tmin', 'temperature_min'])

        if hus is None and hurs is None:
            logger.warning("No humidity variables found in dataset")
            return indices

        # Dewpoint temperature from specific humidity
        if hus is not None and 'dewpoint_temperature' in humidity_indices:
            try:
                # Assume standard pressure if not available
                ps = self._get_variable(ds, ['ps', 'pressure', 'surface_pressure'])
                if ps is None:
                    # Use cached pressure array for efficiency
                    ps = self._get_or_create_pressure_array(hus)

                # FIXED: Use correct function to calculate dewpoint FROM specific humidity
                indices['dewpoint_temperature'] = atmos.dewpoint_from_specific_humidity(
                    huss=hus, ps=ps, method='sonntag90'
                )
                logger.info("Calculated dewpoint temperature")
            except Exception as e:
                logger.error(f"Error calculating dewpoint_temperature: {e}")

        # Relative humidity from specific humidity
        if hus is not None and tas is not None and 'relative_humidity' in humidity_indices:
            try:
                ps = self._get_variable(ds, ['ps', 'pressure', 'surface_pressure'])
                if ps is None:
                    ps = xr.full_like(hus, 101325)  # Pa
                    ps.attrs['units'] = 'Pa'

                indices['relative_humidity'] = atmos.relative_humidity(
                    tas, huss=hus, ps=ps
                )
                logger.info("Calculated relative humidity")
            except Exception as e:
                logger.error(f"Error calculating relative_humidity: {e}")

        # Heat stress and comfort indices
        if tas is not None and (hurs is not None or hus is not None):
            # Heat index
            if 'heat_index' in comfort_indices:
                try:
                    if hurs is None and hus is not None:
                        # Convert specific humidity to relative humidity first
                        ps = self._get_variable(ds, ['ps', 'pressure', 'surface_pressure'])
                        if ps is None:
                            ps = xr.full_like(hus, 101325)
                            ps.attrs['units'] = 'Pa'
                        hurs = atmos.relative_humidity(tas, huss=hus, ps=ps)

                    indices['heat_index'] = atmos.heat_index(tas, hurs, freq='YS')
                    logger.info("Calculated heat index")
                except Exception as e:
                    logger.error(f"Error calculating heat_index: {e}")

            # Humidex
            if 'humidex' in comfort_indices:
                try:
                    if hurs is None and hus is not None:
                        ps = self._get_variable(ds, ['ps', 'pressure', 'surface_pressure'])
                        if ps is None:
                            ps = xr.full_like(hus, 101325)
                            ps.attrs['units'] = 'Pa'
                        hurs = atmos.relative_humidity(tas, huss=hus, ps=ps)

                    indices['humidex'] = atmos.humidex(tas, hurs, freq='YS')
                    logger.info("Calculated humidex")
                except Exception as e:
                    logger.error(f"Error calculating humidex: {e}")

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

        # Enhanced evapotranspiration calculations
        evapotranspiration_indices = self.indices_config.get('evapotranspiration', [])

        if tas is not None and 'potential_evapotranspiration' in evapotranspiration_indices:
            try:
                # Simple Thornthwaite method using only temperature
                indices['potential_evapotranspiration'] = atmos.potential_evapotranspiration(
                    tas, method='thornthwaite', freq='MS'
                )
                logger.info("Calculated potential evapotranspiration (Thornthwaite)")
            except Exception as e:
                logger.error(f"Error calculating potential_evapotranspiration: {e}")

        # Multivariate temperature-precipitation indices
        multivariate_indices = self.indices_config.get('multivariate', [])

        if tas is not None and pr is not None:
            # Combined temperature-precipitation threshold days
            if 'cold_and_dry_days' in multivariate_indices:
                try:
                    # Define thresholds (10th percentiles)
                    tas_thresh = tas.quantile(0.1, dim='time')
                    pr_thresh = pr.quantile(0.1, dim='time')

                    indices['cold_and_dry_days'] = atmos.cold_and_dry_days(
                        tas, pr, tas_per=tas_thresh, pr_per=pr_thresh, freq='YS'
                    )
                    logger.info("Calculated cold and dry days")
                except Exception as e:
                    logger.error(f"Error calculating cold_and_dry_days: {e}")

            if 'warm_and_wet_days' in multivariate_indices:
                try:
                    # Define thresholds (90th percentiles)
                    tas_thresh = tas.quantile(0.9, dim='time')
                    pr_thresh = pr.quantile(0.9, dim='time')

                    indices['warm_and_wet_days'] = atmos.warm_and_wet_days(
                        tas, pr, tas_per=tas_thresh, pr_per=pr_thresh, freq='YS'
                    )
                    logger.info("Calculated warm and wet days")
                except Exception as e:
                    logger.error(f"Error calculating warm_and_wet_days: {e}")

        # Note: SPEI requires potential evapotranspiration calculation
        # Enhanced SPEI with better ET estimation
        if 'spei' in agricultural_indices and tas is not None and pr is not None:
            try:
                # Calculate PET using temperature-based method
                pet = atmos.potential_evapotranspiration(tas, method='thornthwaite', freq='D')
                # Calculate water balance (P - PET)
                water_balance = pr - pet

                # Resample to monthly for SPEI calculation
                wb_monthly = water_balance.resample(time='M').sum()

                indices['spei_3'] = atmos.standardized_precipitation_evapotranspiration_index(
                    wb_monthly, freq='MS', window=3, dist='gamma'
                )
                logger.info("Calculated 3-month SPEI")
            except Exception as e:
                logger.error(f"Error calculating spei: {e}")
        
        return indices
    
    def _get_cached_percentile(self, data: xr.DataArray, percentile: float, dim: str = 'time') -> xr.DataArray:
        """
        Get cached percentile calculation to avoid redundant computations.

        Parameters:
        -----------
        data : xr.DataArray
            Data to calculate percentile from
        percentile : float
            Percentile value (0-1)
        dim : str
            Dimension to calculate percentile along

        Returns:
        --------
        xr.DataArray
            Cached percentile result
        """
        # Create a unique cache key based on data variable name and percentile
        cache_key = f"{data.name}_{percentile}_{dim}"

        if cache_key not in self._percentile_cache:
            self._percentile_cache[cache_key] = data.quantile(percentile, dim=dim)

        return self._percentile_cache[cache_key]

    def _get_or_create_pressure_array(self, template: xr.DataArray) -> xr.DataArray:
        """
        Get or create a standard pressure array, reusing if already created.

        Parameters:
        -----------
        template : xr.DataArray
            Template array for shape and coordinates

        Returns:
        --------
        xr.DataArray
            Pressure array
        """
        if self._pressure_array is None:
            self._pressure_array = xr.full_like(template, 101325, dtype=np.float32)
            self._pressure_array.attrs['units'] = 'Pa'
        return self._pressure_array

    def _get_variable(self, ds: xr.Dataset,
                     possible_names: List[str]) -> Optional[xr.DataArray]:
        """
        Find a variable in dataset by possible names with enhanced validation.

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
                var = ds[name]
                # Basic validation
                if self._validate_variable(var, name):
                    return var
            # Also check with case-insensitive match
            for var_name in ds.data_vars:
                if var_name.lower() == name.lower():
                    var = ds[var_name]
                    if self._validate_variable(var, name):
                        return var
        return None

    def _validate_variable(self, var: xr.DataArray, expected_name: str) -> bool:
        """
        Validate a climate variable for basic requirements.

        Parameters:
        -----------
        var : xr.DataArray
            Variable to validate
        expected_name : str
            Expected variable name/type

        Returns:
        --------
        bool
            True if variable passes validation
        """
        try:
            # Check for required dimensions
            required_dims = ['time']
            for dim in required_dims:
                if dim not in var.dims:
                    logger.warning(f"Variable {var.name} missing required dimension: {dim}")
                    return False

            # Check for reasonable data range based on variable type
            if any(temp_name in expected_name.lower() for temp_name in ['tas', 'temp', 'tmax', 'tmin']):
                # Temperature checks (should be reasonable range in Kelvin or Celsius)
                min_val = float(var.min())
                max_val = float(var.max())

                # Check if values are in Kelvin range (typical: 200-350K)
                if min_val > 100 and max_val < 400:
                    logger.info(f"Temperature variable {var.name} appears to be in Kelvin: {min_val:.1f}-{max_val:.1f}")
                # Check if values are in Celsius range (typical: -80 to 80°C)
                elif min_val > -100 and max_val < 100:
                    logger.info(f"Temperature variable {var.name} appears to be in Celsius: {min_val:.1f}-{max_val:.1f}")
                else:
                    logger.warning(f"Temperature variable {var.name} has unusual range: {min_val:.1f}-{max_val:.1f}")

            elif 'pr' in expected_name.lower() or 'precip' in expected_name.lower():
                # Precipitation checks (should be non-negative)
                min_val = float(var.min())
                if min_val < 0:
                    logger.warning(f"Precipitation variable {var.name} has negative values: {min_val}")

            elif any(hum_name in expected_name.lower() for hum_name in ['hus', 'hurs', 'humid']):
                # Humidity checks
                min_val = float(var.min())
                max_val = float(var.max())

                if 'hurs' in expected_name.lower():  # Relative humidity (0-100%)
                    if min_val < 0 or max_val > 100:
                        logger.warning(f"Relative humidity variable {var.name} outside 0-100% range: {min_val:.1f}-{max_val:.1f}")
                elif 'hus' in expected_name.lower():  # Specific humidity (typically 0-0.04 kg/kg)
                    if min_val < 0 or max_val > 0.04:
                        logger.warning(f"Specific humidity variable {var.name} outside typical range: {min_val:.4f}-{max_val:.4f}")
                        # Values above 0.04 kg/kg are physically unrealistic at Earth's surface
                        if max_val > 0.05:
                            logger.error(f"Specific humidity values exceed physical limits: {max_val:.4f} kg/kg")
                            return False

            # Check for excessive missing values
            if hasattr(var, 'isnull'):
                missing_fraction = float(var.isnull().sum()) / var.size
                if missing_fraction > 0.5:
                    logger.warning(f"Variable {var.name} has {missing_fraction:.1%} missing values")
                elif missing_fraction > 0.1:
                    logger.info(f"Variable {var.name} has {missing_fraction:.1%} missing values")

            return True

        except Exception as e:
            logger.error(f"Error validating variable {var.name}: {e}")
            return False

    def _get_baseline_data(self, data: xr.DataArray) -> xr.DataArray:
        """Extract baseline period data for percentile calculation.

        Parameters:
        -----------
        data : xr.DataArray
            Full time series data

        Returns:
        --------
        xr.DataArray
            Data subset for baseline period
        """
        if not self.use_baseline:
            return data

        # Extract baseline period
        baseline_data = data.sel(
            time=slice(f"{self.baseline_start}-01-01", f"{self.baseline_end}-12-31")
        )

        # Check data coverage
        coverage = baseline_data.count(dim='time') / len(baseline_data.time)
        if coverage.min() < 0.8:
            logger.warning(
                f"Baseline period has low data coverage (min: {coverage.min():.1%}). "
                f"Results may not be representative."
            )

        return baseline_data

    def _calculate_baseline_percentile(self, data: xr.DataArray, percentile: float) -> xr.DataArray:
        """Calculate percentile from baseline period.

        Parameters:
        -----------
        data : xr.DataArray
            Full time series data
        percentile : float
            Percentile to calculate (0-1)

        Returns:
        --------
        xr.DataArray
            Percentile threshold from baseline period
        """
        if not self.use_baseline:
            return data.quantile(percentile, dim='time')

        baseline_data = self._get_baseline_data(data)
        return baseline_data.quantile(percentile, dim='time')

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
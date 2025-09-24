#!/usr/bin/env python
"""
Temperature indices calculation module.
Handles 17 temperature-based climate indices as defined in DATA_DICTIONARY.md
"""

import logging
import xarray as xr
import xclim.indicators.atmos as atmos
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class TemperatureIndicesCalculator:
    """Calculate temperature-based climate indices using xclim."""

    def calculate_all(self, ds: xr.Dataset, freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate all temperature indices.

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset containing tas, tasmax, and/or tasmin variables
        freq : str
            Frequency for calculation (default: 'YS' for annual)

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of calculated indices
        """
        indices = {}

        # Basic temperature statistics
        if 'tas' in ds:
            indices['tg_mean'] = self._safe_calc(atmos.tg_mean, ds.tas, freq=freq)

        if 'tasmax' in ds:
            indices['tx_max'] = self._safe_calc(atmos.tx_max, ds.tasmax, freq=freq)

        if 'tasmin' in ds:
            indices['tn_min'] = self._safe_calc(atmos.tn_min, ds.tasmin, freq=freq)

        if 'tasmax' in ds and 'tasmin' in ds:
            indices['daily_temperature_range'] = self._safe_calc(
                atmos.daily_temperature_range, ds.tasmin, ds.tasmax, freq=freq
            )
            indices['daily_temperature_range_variability'] = self._safe_calc(
                atmos.daily_temperature_range_variability, ds.tasmin, ds.tasmax, freq=freq
            )

        # Threshold-based counts
        if 'tasmin' in ds:
            indices['tropical_nights'] = self._safe_calc(
                atmos.tropical_nights, ds.tasmin, freq=freq
            )
            indices['frost_days'] = self._safe_calc(
                atmos.frost_days, ds.tasmin, freq=freq
            )
            indices['warm_nights'] = self._safe_calc(
                atmos.tn_days_above, ds.tasmin, thresh='15 degC', freq=freq
            )
            indices['consecutive_frost_days'] = self._safe_calc(
                atmos.consecutive_frost_days, ds.tasmin, freq=freq
            )

        if 'tasmax' in ds:
            indices['ice_days'] = self._safe_calc(
                atmos.ice_days, ds.tasmax, freq=freq
            )
            indices['summer_days'] = self._safe_calc(
                atmos.tx_days_above, ds.tasmax, thresh='25 degC', freq=freq
            )
            indices['hot_days'] = self._safe_calc(
                atmos.tx_days_above, ds.tasmax, thresh='30 degC', freq=freq
            )
            indices['very_hot_days'] = self._safe_calc(
                atmos.tx_days_above, ds.tasmax, thresh='35 degC', freq=freq
            )

        # Degree day metrics
        if 'tas' in ds:
            indices['growing_degree_days'] = self._safe_calc(
                atmos.growing_degree_days, ds.tas, thresh='10 degC', freq=freq
            )
            indices['heating_degree_days'] = self._safe_calc(
                atmos.heating_degree_days, ds.tas, thresh='17 degC', freq=freq
            )
            indices['cooling_degree_days'] = self._safe_calc(
                atmos.cooling_degree_days, ds.tas, thresh='18 degC', freq=freq
            )

        return indices

    def _safe_calc(self, func, *args, **kwargs) -> Optional[xr.DataArray]:
        """
        Safely calculate an index with error handling.

        Parameters:
        -----------
        func : callable
            xclim indicator function
        *args : tuple
            Arguments for the function
        **kwargs : dict
            Keyword arguments for the function

        Returns:
        --------
        xr.DataArray or None
            Calculated index or None if error
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            func_name = getattr(func, '__name__', str(func))
            logger.error(f"Error calculating {func_name}: {e}")
            return None
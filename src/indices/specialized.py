#!/usr/bin/env python
"""
Specialized indices calculation module.
Handles humidity, human comfort, evapotranspiration, multivariate, and agricultural indices.
"""

import logging
import xarray as xr
import xclim.indicators.atmos as atmos
import xclim.indices as indices
from typing import Dict, Optional
import numpy as np

logger = logging.getLogger(__name__)


class SpecializedIndicesCalculator:
    """Calculate specialized climate indices using xclim."""

    def calculate_humidity(self, ds: xr.Dataset, freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate humidity indices (2 indices).

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset containing humidity and temperature variables
        freq : str
            Frequency for calculation

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of humidity indices
        """
        indices_dict = {}

        # Dewpoint temperature
        if 'hus' in ds and 'tas' in ds:
            try:
                # Convert specific humidity to dewpoint
                indices_dict['dewpoint_temperature'] = self._calc_dewpoint(
                    ds.tas, ds.hus
                )
            except Exception as e:
                logger.error(f"Error calculating dewpoint: {e}")

        # Relative humidity
        if 'hurs' in ds:
            indices_dict['relative_humidity'] = ds.hurs
        elif 'hus' in ds and 'tas' in ds and 'ps' in ds:
            try:
                indices_dict['relative_humidity'] = self._calc_relative_humidity(
                    ds.tas, ds.hus, ds.ps
                )
            except Exception as e:
                logger.error(f"Error calculating relative humidity: {e}")

        return indices_dict

    def calculate_comfort(self, ds: xr.Dataset, freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate human comfort indices (2 indices).

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset containing temperature and humidity variables
        freq : str
            Frequency for calculation

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of comfort indices
        """
        indices_dict = {}

        if 'tas' in ds:
            # Heat index
            if 'hurs' in ds:
                indices_dict['heat_index'] = self._safe_calc(
                    atmos.heat_index, ds.tas, ds.hurs, freq=freq
                )

            # Humidex
            if 'hurs' in ds or 'hus' in ds:
                # Convert to dewpoint if needed
                if 'hurs' in ds:
                    dewpoint = self._calc_dewpoint_from_rh(ds.tas, ds.hurs)
                else:
                    dewpoint = self._calc_dewpoint(ds.tas, ds.hus)

                indices_dict['humidex'] = self._safe_calc(
                    atmos.humidex, ds.tas, dewpoint, freq=freq
                )

        return indices_dict

    def calculate_evapotranspiration(self, ds: xr.Dataset, freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate evapotranspiration indices (3 indices).

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset containing required variables
        freq : str
            Frequency for calculation

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of ET indices
        """
        indices_dict = {}

        if 'tas' in ds:
            # Potential evapotranspiration (Thornthwaite method)
            indices_dict['potential_evapotranspiration'] = self._safe_calc(
                atmos.potential_evapotranspiration, ds.tas, method='thornthwaite', freq=freq
            )

        # Reference ET and SPEI require additional variables
        # These are placeholders for more complex calculations
        if 'tas' in ds and 'pr' in ds:
            logger.info("Reference ET and SPEI calculations require additional implementation")

        return indices_dict

    def calculate_multivariate(self, ds: xr.Dataset, freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate multivariate indices (4 combined temperature-precipitation events).

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset containing temperature and precipitation variables
        freq : str
            Frequency for calculation

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of multivariate indices
        """
        indices_dict = {}

        if 'tas' in ds and 'pr' in ds:
            # Calculate percentiles for thresholds
            t25 = ds.tas.quantile(0.25, dim='time')
            t75 = ds.tas.quantile(0.75, dim='time')
            p25 = ds.pr.quantile(0.25, dim='time')
            p75 = ds.pr.quantile(0.75, dim='time')

            # Cold and dry days
            indices_dict['cold_and_dry_days'] = self._count_combined_days(
                ds.tas < t25, ds.pr < p25, freq
            )

            # Cold and wet days
            indices_dict['cold_and_wet_days'] = self._count_combined_days(
                ds.tas < t25, ds.pr > p75, freq
            )

            # Warm and dry days
            indices_dict['warm_and_dry_days'] = self._count_combined_days(
                ds.tas > t75, ds.pr < p25, freq
            )

            # Warm and wet days
            indices_dict['warm_and_wet_days'] = self._count_combined_days(
                ds.tas > t75, ds.pr > p75, freq
            )

        return indices_dict

    def calculate_agricultural(self, ds: xr.Dataset, freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate agricultural indices (3 indices).

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset containing required variables
        freq : str
            Frequency for calculation

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of agricultural indices
        """
        indices_dict = {}

        # Growing season length
        if 'tas' in ds:
            indices_dict['growing_season_length'] = self._safe_calc(
                atmos.growing_season_length, ds.tas, thresh='5 degC', freq=freq
            )

        # SPI and SPEI require specialized calculations
        if 'pr' in ds:
            logger.info("SPI calculation requires fitting to gamma distribution")
            # Placeholder for SPI-3 calculation
            # indices_dict['spi_3'] = calculate_spi(ds.pr, window=3)

        return indices_dict

    def _calc_dewpoint(self, tas: xr.DataArray, hus: xr.DataArray) -> xr.DataArray:
        """Calculate dewpoint temperature from specific humidity."""
        # Simplified calculation - should use proper thermodynamic equations
        e = hus * 1013.25 / (0.622 + 0.378 * hus)  # Vapor pressure
        dewpoint = 243.5 * np.log(e / 6.112) / (17.67 - np.log(e / 6.112))
        return dewpoint

    def _calc_dewpoint_from_rh(self, tas: xr.DataArray, hurs: xr.DataArray) -> xr.DataArray:
        """Calculate dewpoint from relative humidity."""
        # Magnus formula approximation
        a = 17.27
        b = 237.7
        alpha = ((a * tas) / (b + tas)) + np.log(hurs / 100.0)
        dewpoint = (b * alpha) / (a - alpha)
        return dewpoint

    def _calc_relative_humidity(self, tas: xr.DataArray, hus: xr.DataArray,
                               ps: xr.DataArray) -> xr.DataArray:
        """Calculate relative humidity from specific humidity."""
        # Simplified calculation
        e = hus * ps / (0.622 + 0.378 * hus)  # Vapor pressure
        es = 6.112 * np.exp((17.67 * tas) / (tas + 243.5))  # Saturation vapor pressure
        rh = 100 * e / es
        return rh.clip(0, 100)

    def _count_combined_days(self, cond1: xr.DataArray, cond2: xr.DataArray,
                            freq: str) -> xr.DataArray:
        """Count days where both conditions are true."""
        combined = cond1 & cond2
        return combined.resample(time=freq).sum()

    def _safe_calc(self, func, *args, **kwargs) -> Optional[xr.DataArray]:
        """Safely calculate an index with error handling."""
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error calculating {func.__name__}: {e}")
            return None
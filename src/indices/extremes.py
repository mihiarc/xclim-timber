#!/usr/bin/env python
"""
Extreme weather indices calculation module.
Handles 6 percentile-based extreme indices as defined in DATA_DICTIONARY.md
"""

import logging
import xarray as xr
import xclim.indicators.atmos as atmos
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class ExtremeIndicesCalculator:
    """Calculate extreme weather indices using xclim."""

    def calculate_all(self, ds: xr.Dataset, baseline_percentiles: Dict[str, xr.DataArray],
                     freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate all extreme weather indices.

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset containing tasmax and tasmin variables
        baseline_percentiles : Dict[str, xr.DataArray]
            Pre-calculated baseline percentiles from calculate_baseline_percentiles.py
            Should contain: tx90_per, tx10_per, tn90_per, tn10_per
        freq : str
            Frequency for calculation (default: 'YS' for annual)

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of calculated indices
        """
        indices = {}

        # Temperature extremes (percentile-based)
        if 'tasmax' in ds and 'tx90_per' in baseline_percentiles and 'tx10_per' in baseline_percentiles:
            indices['tx90p'] = self._safe_calc(
                atmos.tx90p, ds.tasmax, baseline_percentiles['tx90_per'], freq=freq
            )
            indices['tx10p'] = self._safe_calc(
                atmos.tx10p, ds.tasmax, baseline_percentiles['tx10_per'], freq=freq
            )

            # Warm spell duration index
            indices['warm_spell_duration_index'] = self._safe_calc(
                atmos.warm_spell_duration_index, ds.tasmax, baseline_percentiles['tx90_per'],
                window=6, freq=freq
            )

        if 'tasmin' in ds and 'tn90_per' in baseline_percentiles and 'tn10_per' in baseline_percentiles:
            indices['tn90p'] = self._safe_calc(
                atmos.tn90p, ds.tasmin, baseline_percentiles['tn90_per'], freq=freq
            )
            indices['tn10p'] = self._safe_calc(
                atmos.tn10p, ds.tasmin, baseline_percentiles['tn10_per'], freq=freq
            )

            # Cold spell duration index
            indices['cold_spell_duration_index'] = self._safe_calc(
                atmos.cold_spell_duration_index, ds.tasmin, baseline_percentiles['tn10_per'],
                window=6, freq=freq
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
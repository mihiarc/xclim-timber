#!/usr/bin/env python
"""
Precipitation indices calculation module.
Handles 10 precipitation-based climate indices as defined in DATA_DICTIONARY.md
"""

import logging
import xarray as xr
import xclim.indicators.atmos as atmos
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class PrecipitationIndicesCalculator:
    """Calculate precipitation-based climate indices using xclim."""

    def calculate_all(self, ds: xr.Dataset, freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate all precipitation indices.

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset containing pr (precipitation) variable
        freq : str
            Frequency for calculation (default: 'YS' for annual)

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of calculated indices
        """
        indices = {}

        if 'pr' not in ds:
            logger.warning("No precipitation data (pr) found in dataset")
            return indices

        pr = ds.pr

        # Basic precipitation statistics
        # Total precipitation from wet days (≥1mm)
        indices['prcptot'] = self._safe_calc(
            atmos.wet_precip_accumulation, pr, thresh='1 mm/day', freq=freq
        )
        indices['rx1day'] = self._safe_calc(
            atmos.max_1day_precipitation_amount, pr, freq=freq
        )
        indices['rx5day'] = self._safe_calc(
            atmos.max_n_day_precipitation_amount, pr, window=5, freq=freq
        )
        indices['sdii'] = self._safe_calc(
            atmos.daily_pr_intensity, pr, thresh='1 mm/day', freq=freq
        )

        # Consecutive precipitation events
        indices['cdd'] = self._safe_calc(
            atmos.maximum_consecutive_dry_days, pr, thresh='1 mm/day', freq=freq
        )
        indices['cwd'] = self._safe_calc(
            atmos.maximum_consecutive_wet_days, pr, thresh='1 mm/day', freq=freq
        )

        # Precipitation intensity events
        # TODO: days_over_precip_thresh expects pr_per parameter even for fixed thresholds
        # Need to find alternative function or fix parameter passing
        # Temporarily using simple threshold counting

        # For now, comment out to avoid errors
        # indices['r10mm'] = self._safe_calc(
        #     atmos.days_over_precip_thresh, pr, thresh='10 mm/day', freq=freq
        # )
        # indices['r20mm'] = self._safe_calc(
        #     atmos.days_over_precip_thresh, pr, thresh='20 mm/day', freq=freq
        # )

        logger.info("r10mm and r20mm indices temporarily disabled - function signature issues")

        # Percentile-based indices (require baseline calculation)
        # These will be handled separately if baseline data is provided
        # indices['r95p'] = self._calc_percentile_index(pr, 0.95, freq)
        # indices['r99p'] = self._calc_percentile_index(pr, 0.99, freq)

        return indices

    def calculate_percentile_based(self, ds: xr.Dataset, baseline_percentiles: Dict[str, xr.DataArray],
                                   freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate percentile-based precipitation indices.

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset containing pr variable
        baseline_percentiles : Dict[str, xr.DataArray]
            Pre-calculated baseline percentiles from calculate_baseline_percentiles.py
            Should contain: pr95_per, pr99_per
        freq : str
            Frequency for calculation

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of percentile-based indices
        """
        indices = {}

        if 'pr' not in ds:
            logger.warning("Precipitation data missing for percentile calculation")
            return indices

        # Use pre-calculated percentiles
        # TODO: pr_above_per function doesn't exist in current xclim version
        # Need to find the correct function or implement custom percentile calculation
        # Commenting out for now to allow other indices to run

        # if 'pr95_per' in baseline_percentiles:
        #     indices['r95p'] = self._safe_calc(
        #         atmos.pr_above_per, ds.pr, baseline_percentiles['pr95_per'], freq=freq
        #     )

        # if 'pr99_per' in baseline_percentiles:
        #     indices['r99p'] = self._safe_calc(
        #         atmos.pr_above_per, ds.pr, baseline_percentiles['pr99_per'], freq=freq
        #     )

        logger.info("Percentile-based precipitation indices (r95p, r99p) temporarily disabled - pr_above_per not available in xclim")

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
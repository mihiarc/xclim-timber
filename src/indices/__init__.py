#!/usr/bin/env python
"""
Climate indices calculation package.
Modular implementation following DATA_DICTIONARY.md categories.
"""

import logging
from typing import Dict, List, Optional
import xarray as xr

from .temperature import TemperatureIndicesCalculator
from .precipitation import PrecipitationIndicesCalculator
from .extremes import ExtremeIndicesCalculator
from .specialized import SpecializedIndicesCalculator

logger = logging.getLogger(__name__)


class ClimateIndicesCalculator:
    """
    Main coordinator for all climate indices calculations.

    This class orchestrates the calculation of all 84 climate indices
    organized into 8 categories as defined in DATA_DICTIONARY.md.
    """

    def __init__(self):
        """Initialize all calculator modules."""
        self.temperature = TemperatureIndicesCalculator()
        self.precipitation = PrecipitationIndicesCalculator()
        self.extremes = ExtremeIndicesCalculator()
        self.specialized = SpecializedIndicesCalculator()

    def calculate_all(self, ds: xr.Dataset,
                     baseline_percentiles: Dict[str, xr.DataArray],
                     categories: Optional[List[str]] = None,
                     freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate all or selected categories of climate indices.

        Parameters:
        -----------
        ds : xr.Dataset
            Input climate dataset
        baseline_percentiles : Dict[str, xr.DataArray]
            Pre-calculated baseline percentiles from calculate_baseline_percentiles.py
        categories : List[str], optional
            List of categories to calculate. If None, calculates all.
            Valid categories: temperature, precipitation, extremes,
                           humidity, comfort, evapotranspiration,
                           multivariate, agricultural
        freq : str
            Frequency for calculations (default: 'YS' for annual)

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of all calculated indices
        """
        if categories is None:
            categories = [
                'temperature', 'precipitation', 'extremes',
                'humidity', 'comfort', 'evapotranspiration',
                'multivariate', 'agricultural'
            ]

        all_indices = {}

        # Temperature indices (17 indices)
        if 'temperature' in categories:
            logger.info("Calculating temperature indices...")
            temp_indices = self.temperature.calculate_all(ds, freq)
            all_indices.update(temp_indices)
            logger.info(f"Calculated {len(temp_indices)} temperature indices")

        # Precipitation indices (10 indices)
        if 'precipitation' in categories:
            logger.info("Calculating precipitation indices...")
            precip_indices = self.precipitation.calculate_all(ds, freq)

            # Add percentile-based indices
            percentile_indices = self.precipitation.calculate_percentile_based(
                ds, baseline_percentiles, freq
            )
            precip_indices.update(percentile_indices)

            all_indices.update(precip_indices)
            logger.info(f"Calculated {len(precip_indices)} precipitation indices")

        # Extreme weather indices (6 indices)
        if 'extremes' in categories:
            logger.info("Calculating extreme weather indices...")
            extreme_indices = self.extremes.calculate_all(ds, baseline_percentiles, freq)
            all_indices.update(extreme_indices)
            logger.info(f"Calculated {len(extreme_indices)} extreme indices")

        # Humidity indices (2 indices)
        if 'humidity' in categories:
            logger.info("Calculating humidity indices...")
            humidity_indices = self.specialized.calculate_humidity(ds, freq)
            all_indices.update(humidity_indices)
            logger.info(f"Calculated {len(humidity_indices)} humidity indices")

        # Human comfort indices (2 indices)
        if 'comfort' in categories:
            logger.info("Calculating human comfort indices...")
            comfort_indices = self.specialized.calculate_comfort(ds, freq)
            all_indices.update(comfort_indices)
            logger.info(f"Calculated {len(comfort_indices)} comfort indices")

        # Evapotranspiration indices (3 indices)
        if 'evapotranspiration' in categories:
            logger.info("Calculating evapotranspiration indices...")
            et_indices = self.specialized.calculate_evapotranspiration(ds, freq)
            all_indices.update(et_indices)
            logger.info(f"Calculated {len(et_indices)} evapotranspiration indices")

        # Multivariate indices (4 indices)
        if 'multivariate' in categories:
            logger.info("Calculating multivariate indices...")
            multi_indices = self.specialized.calculate_multivariate(ds, freq)
            all_indices.update(multi_indices)
            logger.info(f"Calculated {len(multi_indices)} multivariate indices")

        # Agricultural indices (3 indices)
        if 'agricultural' in categories:
            logger.info("Calculating agricultural indices...")
            agri_indices = self.specialized.calculate_agricultural(ds, freq)
            all_indices.update(agri_indices)
            logger.info(f"Calculated {len(agri_indices)} agricultural indices")

        logger.info(f"Total indices calculated: {len(all_indices)}")
        return all_indices

    def calculate_for_variable(self, ds: xr.Dataset,
                              variable: str,
                              baseline_percentiles: Dict[str, xr.DataArray],
                              freq: str = 'YS') -> Dict[str, xr.DataArray]:
        """
        Calculate indices relevant to a specific variable.

        Parameters:
        -----------
        ds : xr.Dataset
            Input climate dataset
        variable : str
            Variable name (tas, tasmax, tasmin, pr, etc.)
        baseline_percentiles : Dict[str, xr.DataArray]
            Pre-calculated baseline percentiles
        freq : str
            Frequency for calculations

        Returns:
        --------
        Dict[str, xr.DataArray]
            Dictionary of relevant indices for the variable
        """
        indices = {}

        if variable in ['tas', 'tasmax', 'tasmin']:
            indices.update(self.temperature.calculate_all(ds, freq))
            indices.update(self.extremes.calculate_all(ds, baseline_percentiles, freq))

        elif variable == 'pr':
            indices.update(self.precipitation.calculate_all(ds, freq))
            indices.update(
                self.precipitation.calculate_percentile_based(ds, baseline_percentiles, freq))

        elif variable in ['hus', 'hurs']:
            indices.update(self.specialized.calculate_humidity(ds, freq))
            indices.update(self.specialized.calculate_comfort(ds, freq))

        return indices


# Convenience function
def calculate_indices(ds: xr.Dataset,
                     baseline_percentiles: Dict[str, xr.DataArray],
                     categories: Optional[List[str]] = None,
                     freq: str = 'YS') -> Dict[str, xr.DataArray]:
    """
    Calculate climate indices for the given dataset.

    This is a convenience function that creates a calculator
    and runs the calculation.
    """
    calculator = ClimateIndicesCalculator()
    return calculator.calculate_all(ds, baseline_percentiles, categories, freq)
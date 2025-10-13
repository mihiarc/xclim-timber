#!/usr/bin/env python3
"""
Baseline percentile loader for xclim-timber pipelines.

Provides cached loading and validation of baseline percentiles used for
extreme climate indices.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import xarray as xr

from core.config import PipelineConfig

logger = logging.getLogger(__name__)


class BaselineLoader:
    """
    Load and manage baseline percentiles for extreme indices.

    Provides:
    - Cached loading to avoid repeated file reads
    - Validation of baseline period and variables
    - Convenient methods for different index types
    """

    def __init__(self, baseline_file: Optional[Path] = None):
        """
        Initialize baseline loader.

        Args:
            baseline_file: Path to baseline percentiles file
                          (defaults to PipelineConfig.BASELINE_FILE)
        """
        self.baseline_file = baseline_file or Path(PipelineConfig.BASELINE_FILE)
        self._baseline_cache: Optional[xr.Dataset] = None

    def _load_baseline_file(self) -> xr.Dataset:
        """
        Load baseline percentiles file with caching.

        Returns:
            xarray Dataset with baseline percentiles

        Raises:
            FileNotFoundError: If baseline file doesn't exist
            RuntimeError: If baseline file is corrupted
        """
        # Return cached version if available
        if self._baseline_cache is not None:
            return self._baseline_cache

        if not self.baseline_file.exists():
            error_msg = f"""
ERROR: Baseline percentiles file not found at {self.baseline_file}

Please generate baseline percentiles first:
  python tools/calculate_baseline_percentiles.py

This is a one-time operation that takes ~15-25 minutes.
The file should contain temperature, precipitation, and multivariate percentiles.
"""
            raise FileNotFoundError(error_msg)

        try:
            logger.info(f"Loading baseline percentiles from {self.baseline_file}")
            ds = xr.open_dataset(self.baseline_file)

            # Validate baseline period
            baseline_period = ds.attrs.get('baseline_period')
            if baseline_period != PipelineConfig.BASELINE_PERIOD:
                logger.warning(
                    f"Baseline period mismatch: expected '{PipelineConfig.BASELINE_PERIOD}', "
                    f"got '{baseline_period}'. Results may not be comparable to standard indices."
                )

            # Cache for future use
            self._baseline_cache = ds
            logger.info(f"  Loaded baseline file with {len(ds.data_vars)} variables")

            return ds

        except (OSError, IOError) as e:
            raise RuntimeError(f"Failed to read baseline file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading baseline percentiles: {e}")

    def load_baseline_percentiles(
        self,
        required_vars: List[str],
        allow_missing: bool = False
    ) -> Dict[str, xr.DataArray]:
        """
        Load specific baseline percentile variables.

        Args:
            required_vars: List of required variable names
            allow_missing: If True, missing variables won't raise an error

        Returns:
            Dictionary mapping variable name to DataArray

        Raises:
            ValueError: If required variables are missing (unless allow_missing=True)
        """
        ds = self._load_baseline_file()
        percentiles = {}
        missing_vars = []

        for var in required_vars:
            if var not in ds:
                missing_vars.append(var)
                if not allow_missing:
                    continue
                logger.warning(f"Missing expected baseline variable: {var}")
            else:
                data = ds[var]

                # Validate dimensions
                if 'dayofyear' not in data.dims:
                    logger.warning(f"{var} missing required 'dayofyear' dimension")

                # Validate data integrity
                if data.isnull().all():
                    logger.warning(f"{var} contains all NaN values - baseline may be corrupted")

                percentiles[var] = data
                logger.debug(f"  Loaded {var}: shape={data.shape}")

        if missing_vars and not allow_missing:
            raise ValueError(
                f"Missing required baseline variables: {missing_vars}. "
                f"Please regenerate baseline percentiles."
            )

        return percentiles

    def get_temperature_baselines(self) -> Dict[str, xr.DataArray]:
        """
        Get baseline percentiles for temperature extreme indices.

        Returns:
            Dictionary with tx90p, tx10p, tn90p, tn10p thresholds

        Raises:
            ValueError: If temperature baselines are missing
        """
        logger.info("Loading temperature baseline percentiles")
        return self.load_baseline_percentiles(PipelineConfig.TEMP_BASELINE_VARS)

    def get_precipitation_baselines(self) -> Dict[str, xr.DataArray]:
        """
        Get baseline percentiles for precipitation extreme indices.

        Returns:
            Dictionary with pr95p, pr99p, pr_75p thresholds

        Raises:
            ValueError: If precipitation baselines are missing
        """
        logger.info("Loading precipitation baseline percentiles")
        return self.load_baseline_percentiles(
            PipelineConfig.PRECIP_BASELINE_VARS,
            allow_missing=True  # pr_75p might not be in older baseline files
        )

    def get_multivariate_baselines(self) -> Dict[str, xr.DataArray]:
        """
        Get baseline percentiles for multivariate (compound extreme) indices.

        Returns:
            Dictionary with tas_25p, tas_75p, pr_25p, pr_75p thresholds

        Raises:
            ValueError: If multivariate baselines are missing
        """
        logger.info("Loading multivariate baseline percentiles")
        return self.load_baseline_percentiles(PipelineConfig.MULTIVARIATE_BASELINE_VARS)

    def has_baseline_file(self) -> bool:
        """
        Check if baseline file exists.

        Returns:
            True if baseline file exists, False otherwise
        """
        return self.baseline_file.exists()

    def clear_cache(self):
        """Clear cached baseline data to free memory."""
        self._baseline_cache = None
        logger.debug("Baseline cache cleared")

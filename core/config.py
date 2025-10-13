#!/usr/bin/env python3
"""
Shared configuration constants for xclim-timber pipelines.

Centralizes all configuration to eliminate duplication across pipelines.
"""

import warnings
from typing import Dict


class PipelineConfig:
    """
    Centralized configuration for all climate index pipelines.

    Provides:
    - Zarr store paths
    - Default chunk configurations
    - Variable name mappings
    - CF-compliant standard names
    - Common warning filters
    """

    # ==================== Zarr Store Paths ====================
    TEMP_ZARR = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'
    PRECIP_ZARR = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation'
    HUMIDITY_ZARR = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/humidity'

    # ==================== Chunk Configuration ====================
    DEFAULT_CHUNKS = {
        'time': 365,   # One year of daily data
        'lat': 103,    # 621 / 103 = 6 chunks (memory optimized)
        'lon': 201     # 1405 / 201 = 7 chunks (memory optimized)
    }

    # ==================== Variable Renaming ====================
    # Maps PRISM variable names to xclim-compatible names
    TEMP_RENAME_MAP = {
        'tmean': 'tas',       # Mean temperature
        'tmax': 'tasmax',     # Maximum temperature
        'tmin': 'tasmin'      # Minimum temperature
    }

    PRECIP_RENAME_MAP = {
        'ppt': 'pr'           # Precipitation
    }

    HUMIDITY_RENAME_MAP = {
        'tdmean': 'tdew',     # Dewpoint temperature
        'vpdmax': 'vpdmax',   # Maximum vapor pressure deficit
        'vpdmin': 'vpdmin'    # Minimum vapor pressure deficit
    }

    # ==================== Unit Fixes ====================
    # CF-compliant unit specifications
    TEMP_UNIT_FIXES = {
        'tas': 'degC',
        'tasmax': 'degC',
        'tasmin': 'degC'
    }

    PRECIP_UNIT_FIXES = {
        'pr': 'mm d-1'  # CF-compliant format
    }

    HUMIDITY_UNIT_FIXES = {
        'tdew': 'degC',
        'vpdmax': 'kPa',
        'vpdmin': 'kPa'
    }

    # ==================== CF Standard Names ====================
    CF_STANDARD_NAMES: Dict[str, str] = {
        # Temperature
        'tas': 'air_temperature',
        'tasmax': 'air_temperature',
        'tasmin': 'air_temperature',
        # Precipitation
        'pr': 'precipitation_flux',
        # Humidity
        'tdew': 'dew_point_temperature',
        'vpdmax': 'vapor_pressure_deficit',
        'vpdmin': 'vapor_pressure_deficit'
    }

    # ==================== Baseline Configuration ====================
    BASELINE_FILE = 'data/baselines/baseline_percentiles_1981_2000.nc'
    BASELINE_PERIOD = '1981-2000'

    # Temperature baseline variables
    TEMP_BASELINE_VARS = ['tx90p_threshold', 'tx10p_threshold', 'tn90p_threshold', 'tn10p_threshold']

    # Precipitation baseline variables
    PRECIP_BASELINE_VARS = ['pr95p_threshold', 'pr99p_threshold', 'pr_75p_threshold']

    # Multivariate baseline variables
    MULTIVARIATE_BASELINE_VARS = ['tas_25p_threshold', 'tas_75p_threshold', 'pr_25p_threshold', 'pr_75p_threshold']

    # ==================== Warning Filters ====================
    @staticmethod
    def setup_warning_filters():
        """
        Configure common warning filters to suppress non-critical messages.

        These warnings are suppressed because they don't affect functionality:
        - Cell methods warnings (xclim metadata)
        - Chunk specification warnings (expected for large datasets)
        - All-NaN slice warnings (expected for edge cases)
        - Division warnings (handled by xarray)
        - Future warnings about Dataset.dims return type
        """
        warnings.filterwarnings('ignore', category=UserWarning, message='.*cell_methods.*')
        warnings.filterwarnings('ignore', category=UserWarning, message='.*specified chunks.*')
        warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*All-NaN slice.*')
        warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*divide.*')
        warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*invalid value.*')
        warnings.filterwarnings('ignore', category=FutureWarning, message='.*return type of.*Dataset.dims.*')

    # ==================== Default Processing Options ====================
    DEFAULT_CHUNK_YEARS = 1  # Process 1 year at a time for memory efficiency
    DEFAULT_OUTPUT_DIR = './outputs'
    DEFAULT_START_YEAR = 1981
    DEFAULT_END_YEAR = 2024

    # ==================== NetCDF Encoding ====================
    @staticmethod
    def default_encoding(chunksizes=(1, 69, 281)):
        """
        Get default NetCDF encoding configuration.

        Args:
            chunksizes: Chunk sizes for (time, lat, lon)

        Returns:
            Dictionary with compression settings
        """
        return {
            'zlib': True,
            'complevel': 4,
            'chunksizes': chunksizes
        }

"""
Configuration settings for the xclim climate data pipeline.
"""

import os
from pathlib import Path
from typing import Dict, List, Optional
import yaml


class Config:
    """Configuration management for the climate data pipeline."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize configuration.
        
        Parameters:
        -----------
        config_file : str, optional
            Path to YAML configuration file
        """
        self.config_file = config_file
        self.config = self._load_defaults()
        
        if config_file and Path(config_file).exists():
            self._load_from_file(config_file)
    
    def _load_defaults(self) -> Dict:
        """Load default configuration settings."""
        return {
            # Data paths
            'data': {
                'input_path': '/media/external_drive',  # Path to external drive
                'output_path': './outputs',
                'log_path': './logs',
                'file_patterns': {
                    'temperature': ['*tas*.tif', '*tas*.nc', '*temp*.tif', '*temp*.nc',
                                   '*tasmax*.tif', '*tasmax*.nc', '*tasmin*.tif', '*tasmin*.nc',
                                   '*tmax*.tif', '*tmax*.nc', '*tmin*.tif', '*tmin*.nc'],
                    'precipitation': ['*pr*.tif', '*pr*.nc', '*precip*.tif', '*precip*.nc'],
                    'humidity': ['*hurs*.tif', '*hurs*.nc', '*humid*.tif', '*humid*.nc',
                                '*hus*.tif', '*hus*.nc', '*huss*.tif', '*huss*.nc',
                                '*specific_humidity*.tif', '*specific_humidity*.nc'],
                    'wind': ['*sfcWind*.tif', '*sfcWind*.nc', '*wind*.tif', '*wind*.nc']
                }
            },
            
            # Processing settings
            'processing': {
                'chunk_size': {
                    'time': 365,  # Process one year at a time
                    'lat': 100,
                    'lon': 100
                },
                'dask': {
                    'n_workers': 4,
                    'threads_per_worker': 2,
                    'memory_limit': '4GB',
                    'dashboard_address': ':8787'
                },
                'resampling': {
                    'temporal': 'D',  # Daily
                    'spatial_resolution': None  # Keep original
                },
                'crs': 'EPSG:4326'  # Default CRS
            },
            
            # Climate indices to calculate
            'indices': {
                # Baseline period for percentile calculations
                'baseline_period': {
                    'start': 1971,  # WMO standard baseline start
                    'end': 2000,    # WMO standard baseline end
                },
                'use_baseline_for_percentiles': True,  # Use baseline for percentile indices
                'temperature': [
                    # Basic statistics
                    'tg_mean',  # Mean temperature
                    'tx_max',   # Maximum temperature
                    'tn_min',   # Minimum temperature
                    'daily_temperature_range',  # Mean daily temperature range
                    'daily_temperature_range_variability',  # Temperature range variability

                    # Threshold-based counts
                    'tropical_nights',  # Number of tropical nights (>20°C)
                    'frost_days',      # Number of frost days (<0°C)
                    'ice_days',        # Number of ice days (<0°C)
                    'summer_days',     # Number of summer days (>25°C)
                    'hot_days',        # Number of hot days (>30°C)
                    'very_hot_days',   # Number of very hot days (>35°C)
                    'warm_nights',     # Number of warm nights (>15°C)
                    'consecutive_frost_days',  # Consecutive frost days

                    # Degree days
                    'growing_degree_days',  # Growing degree days
                    'heating_degree_days',  # Heating degree days
                    'cooling_degree_days'   # Cooling degree days
                ],
                'precipitation': [
                    # Basic statistics
                    'prcptot',  # Total precipitation
                    'rx1day',   # Max 1-day precipitation
                    'rx5day',   # Max 5-day precipitation
                    'sdii',     # Simple daily intensity index

                    # Consecutive events
                    'cdd',      # Consecutive dry days
                    'cwd',      # Consecutive wet days

                    # Threshold events
                    'r10mm',    # Number of heavy precipitation days (≥10mm)
                    'r20mm',    # Number of very heavy precipitation days (≥20mm)
                    'r95p',     # Very wet days (>95th percentile)
                    'r99p'      # Extremely wet days (>99th percentile)
                ],
                'extremes': [
                    # Temperature extremes
                    'tx90p',    # Warm days (TX > 90th percentile)
                    'tn90p',    # Warm nights (TN > 90th percentile)
                    'tx10p',    # Cool days (TX < 10th percentile)
                    'tn10p',    # Cool nights (TN < 10th percentile)

                    # Spell duration indices
                    'warm_spell_duration_index',  # Warm spell duration (WSDI)
                    'cold_spell_duration_index'   # Cold spell duration (CSDI)
                ],
                'humidity': [
                    # Basic humidity calculations
                    'dewpoint_temperature',      # Dewpoint from specific humidity
                    'relative_humidity',         # Relative humidity calculation
                ],
                'comfort': [
                    # Human comfort indices
                    'heat_index',               # Heat index (temperature + humidity)
                    'humidex',                  # Humidex (Canadian heat comfort)
                ],
                'evapotranspiration': [
                    # Evapotranspiration indices
                    'potential_evapotranspiration',     # PET calculation
                    'reference_evapotranspiration',     # FAO-56 Penman-Monteith
                ],
                'agricultural': [
                    # Agricultural indices
                    'gsl',      # Growing season length
                    'spi',      # Standardized precipitation index
                    'spei'      # Standardized precipitation evapotranspiration index
                ],
                'multivariate': [
                    # Combined variable indices
                    'cold_and_dry_days',        # Days with low temp + low precip
                    'cold_and_wet_days',        # Days with low temp + high precip
                    'warm_and_dry_days',        # Days with high temp + low precip
                    'warm_and_wet_days'         # Days with high temp + high precip
                ]
            },
            
            # Output settings
            'output': {
                'format': 'netcdf',  # 'netcdf' or 'geotiff'
                'compression': {
                    'complevel': 4,
                    'engine': 'h5netcdf'
                },
                'attributes': {
                    'institution': 'Your Institution',
                    'source': 'xclim climate data pipeline',
                    'history': 'Created with xclim',
                    'references': 'https://xclim.readthedocs.io/'
                }
            },
            
            # Quality control
            'quality_control': {
                'check_missing': True,
                'missing_threshold': 0.1,  # Max 10% missing data
                'check_outliers': True,
                'outlier_method': 'zscore',
                'outlier_threshold': 5
            }
        }
    
    def _load_from_file(self, config_file: str):
        """Load configuration from YAML file."""
        with open(config_file, 'r') as f:
            user_config = yaml.safe_load(f)
        
        # Deep merge with defaults
        self.config = self._deep_merge(self.config, user_config)
    
    def _deep_merge(self, default: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries."""
        result = default.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def get(self, key: str, default=None):
        """Get configuration value by dot-notation key."""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value
    
    def set(self, key: str, value):
        """Set configuration value by dot-notation key."""
        keys = key.split('.')
        config = self.config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
    
    def save(self, output_file: str):
        """Save configuration to YAML file."""
        with open(output_file, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)
    
    @property
    def input_path(self) -> Path:
        """Get input data path."""
        return Path(self.get('data.input_path'))
    
    @property
    def output_path(self) -> Path:
        """Get output data path."""
        return Path(self.get('data.output_path'))
    
    @property
    def log_path(self) -> Path:
        """Get log path."""
        return Path(self.get('data.log_path'))
    
    @property
    def chunk_sizes(self) -> Dict:
        """Get chunk sizes for dask."""
        return self.get('processing.chunk_size')
    
    @property
    def dask_config(self) -> Dict:
        """Get dask configuration."""
        return self.get('processing.dask')
    
    @property
    def indices_config(self) -> Dict:
        """Get climate indices configuration."""
        return self.get('indices')
    
    def validate(self) -> bool:
        """Validate configuration."""
        # Check if input path exists
        if not self.input_path.exists():
            print(f"Warning: Input path {self.input_path} does not exist")
            return False
        
        # Create output directories if they don't exist
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.log_path.mkdir(parents=True, exist_ok=True)
        
        return True


# Example usage
if __name__ == "__main__":
    # Create default configuration
    config = Config()
    
    # Save to file for user customization
    config.save('config.yaml')
    print("Configuration saved to config.yaml")
    
    # Validate configuration
    if config.validate():
        print("Configuration is valid")
    else:
        print("Configuration validation failed")
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
                    'temperature': ['*tas*.tif', '*tas*.nc', '*temp*.tif', '*temp*.nc'],
                    'precipitation': ['*pr*.tif', '*pr*.nc', '*precip*.tif', '*precip*.nc'],
                    'humidity': ['*hurs*.tif', '*hurs*.nc', '*humid*.tif', '*humid*.nc'],
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
                'temperature': [
                    'tg_mean',  # Mean temperature
                    'tx_max',   # Maximum temperature
                    'tn_min',   # Minimum temperature
                    'tropical_nights',  # Number of tropical nights
                    'frost_days',      # Number of frost days
                    'ice_days',        # Number of ice days
                    'summer_days',     # Number of summer days
                    'growing_degree_days',  # Growing degree days
                    'heating_degree_days',  # Heating degree days
                    'cooling_degree_days'   # Cooling degree days
                ],
                'precipitation': [
                    'prcptot',  # Total precipitation
                    'rx1day',   # Max 1-day precipitation
                    'rx5day',   # Max 5-day precipitation
                    'sdii',     # Simple daily intensity index
                    'cdd',      # Consecutive dry days
                    'cwd',      # Consecutive wet days
                    'r10mm',    # Number of heavy precipitation days
                    'r20mm',    # Number of very heavy precipitation days
                    'r95p',     # Very wet days
                    'r99p'      # Extremely wet days
                ],
                'extremes': [
                    'tx90p',    # Warm days
                    'tn90p',    # Warm nights
                    'tx10p',    # Cool days
                    'tn10p',    # Cool nights
                    'wsdi',     # Warm spell duration index
                    'csdi'      # Cold spell duration index
                ],
                'agricultural': [
                    'gsl',      # Growing season length
                    'spi',      # Standardized precipitation index
                    'spei'      # Standardized precipitation evapotranspiration index
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
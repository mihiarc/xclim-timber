"""
Simplified configuration for PRISM Zarr data pipeline.
"""
import yaml
from pathlib import Path
from typing import Dict, Optional


class Config:
    """Minimal configuration for PRISM climate data pipeline."""

    def __init__(self, config_file: Optional[str] = None):
        """Initialize with defaults, optionally override from YAML."""
        # Minimal defaults - just what's actually needed
        self.config = {
            'data': {
                'base_path': '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr',
                'output_path': './outputs',
                'log_path': './logs'
            },
            'indices': self._get_indices_config(),
            'output': {
                'format': 'netcdf',
                'compression': {'complevel': 4}
            }
        }

        # Override with user config if provided
        if config_file and Path(config_file).exists():
            with open(config_file) as f:
                user_config = yaml.safe_load(f)
                # Only update the base path if specified
                if 'base_path' in user_config:
                    self.config['data']['base_path'] = user_config['base_path']
                if 'output_path' in user_config:
                    self.config['data']['output_path'] = user_config['output_path']

    def _get_indices_config(self) -> Dict:
        """Return all 84 climate indices organized by category."""
        return {
            'baseline_period': {'start': 1971, 'end': 2000},
            'temperature': [
                'tg_mean', 'tx_max', 'tn_min', 'daily_temperature_range',
                'daily_temperature_range_variability', 'tropical_nights',
                'frost_days', 'ice_days', 'summer_days', 'hot_days',
                'very_hot_days', 'warm_nights', 'consecutive_frost_days',
                'growing_degree_days', 'heating_degree_days', 'cooling_degree_days'
            ],
            'precipitation': [
                'prcptot', 'rx1day', 'rx5day', 'sdii', 'cdd', 'cwd',
                'r10mm', 'r20mm', 'r95p', 'r99p'
            ],
            'extremes': [
                'tx90p', 'tn90p', 'tx10p', 'tn10p',
                'warm_spell_duration_index', 'cold_spell_duration_index'
            ],
            'humidity': ['dewpoint_temperature', 'relative_humidity'],
            'comfort': ['heat_index', 'humidex'],
            'evapotranspiration': [
                'potential_evapotranspiration', 'reference_evapotranspiration'
            ],
            'agricultural': ['gsl', 'spi', 'spei'],
            'multivariate': [
                'cold_and_dry_days', 'cold_and_wet_days',
                'warm_and_dry_days', 'warm_and_wet_days'
            ]
        }

    def get_zarr_path(self, variable_type: str) -> Path:
        """Get path to specific variable Zarr store."""
        base = Path(self.config['data']['base_path'])

        # Handle different mount points for external drive
        if not base.exists():
            # Try alternative mount points
            for mount in ['/media', '/mnt', '/Volumes']:
                alt_path = Path(mount).glob('*/SSD4TB/data/PRISM/prism.zarr')
                if alt_path:
                    base = next(alt_path)
                    self.config['data']['base_path'] = str(base)
                    break

        # Map variable types to PRISM store subdirectories
        store_map = {
            'temperature': 'temperature',
            'precipitation': 'precipitation',
            'humidity': 'humidity'
        }

        return base / store_map.get(variable_type, variable_type)

    @property
    def output_path(self) -> Path:
        """Get output path."""
        path = Path(self.config['data']['output_path'])
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def log_path(self) -> Path:
        """Get log path."""
        path = Path(self.config['data']['log_path'])
        path.mkdir(parents=True, exist_ok=True)
        return path
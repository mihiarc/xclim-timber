"""
Simplified configuration for NorESM2-LM climate data processing.
"""

from pathlib import Path
from typing import Dict, List, Optional


class NorESMConfig:
    """Configuration specifically for NorESM2-LM data."""
    
    def __init__(self):
        """Initialize NorESM2-LM specific configuration."""
        
        # Base data path
        self.base_path = Path("/media/mihiarc/SSD4TB/data/NorESM2-LM")
        
        # Output paths
        self.output_path = Path("./outputs")
        self.log_path = Path("./logs")
        
        # Available variables in NorESM2-LM
        self.variables = {
            'tas': 'Near-surface air temperature',
            'tasmax': 'Daily maximum near-surface air temperature',
            'tasmin': 'Daily minimum near-surface air temperature',
            'pr': 'Precipitation',
            'hurs': 'Near-surface relative humidity',
            'sfcWind': 'Near-surface wind speed'
        }
        
        # Available scenarios
        self.scenarios = ['historical', 'ssp126', 'ssp245', 'ssp370', 'ssp585']
        
        # Data properties (consistent across NorESM2-LM)
        self.data_properties = {
            'frequency': 'daily',
            'grid': 'gn',  # native grid
            'ensemble_member': 'r1i1p1f1',
            'dimensions': {
                'lat': 600,
                'lon': 1440,
                'time_per_year': 365  # or 366 for leap years
            }
        }
        
        # Processing settings
        self.processing = {
            'chunk_size': {
                'time': 365,  # Process one year at a time
                'lat': 200,   # Smaller chunks for 600x1440 grid
                'lon': 360
            },
            'dask': {
                'n_workers': 4,
                'threads_per_worker': 2,
                'memory_limit': '4GB'
            }
        }
        
        # Climate indices to calculate
        self.indices = {
            'temperature': [
                'annual_mean',     # Annual mean temperature
                'annual_max',      # Annual maximum temperature
                'annual_min',      # Annual minimum temperature
                'frost_days',      # Days with Tmin < 0°C
                'summer_days',     # Days with Tmax > 25°C
                'tropical_nights', # Nights with Tmin > 20°C
                'growing_degree_days'  # GDD base 10°C
            ],
            'precipitation': [
                'annual_total',    # Annual total precipitation
                'wet_days',        # Days with pr > 1mm
                'rx1day',          # Max 1-day precipitation
                'rx5day',          # Max 5-day precipitation
                'cdd',             # Consecutive dry days
                'cwd'              # Consecutive wet days
            ]
        }
        
        # Output settings
        self.output = {
            'format': 'netcdf',
            'compression': {
                'complevel': 4,
                'engine': 'netcdf4'
            },
            'attributes': {
                'source_model': 'NorESM2-LM',
                'institution': 'Norwegian Climate Centre',
                'processing_software': 'xclim-timber'
            }
        }
    
    def get_file_pattern(self, variable: str, scenario: str, year: Optional[int] = None) -> str:
        """
        Get file pattern for NorESM2-LM data.
        
        Parameters:
        -----------
        variable : str
            Variable name (tas, pr, etc.)
        scenario : str
            Scenario name (historical, ssp245, etc.)
        year : int, optional
            Specific year
        
        Returns:
        --------
        str
            File pattern or path
        """
        if year:
            return f"{variable}_day_NorESM2-LM_{scenario}_{self.data_properties['ensemble_member']}_gn_{year}.nc"
        else:
            return f"{variable}_day_NorESM2-LM_{scenario}_{self.data_properties['ensemble_member']}_gn_*.nc"
    
    def get_data_path(self, variable: str, scenario: str) -> Path:
        """
        Get full path to data directory.
        
        Parameters:
        -----------
        variable : str
            Variable name
        scenario : str
            Scenario name
        
        Returns:
        --------
        Path
            Full path to data directory
        """
        return self.base_path / variable / scenario
    
    def list_available_files(self, variable: str, scenario: str) -> List[Path]:
        """
        List all available files for a variable and scenario.
        
        Parameters:
        -----------
        variable : str
            Variable name
        scenario : str
            Scenario name
        
        Returns:
        --------
        List[Path]
            List of available files
        """
        data_path = self.get_data_path(variable, scenario)
        if not data_path.exists():
            return []
        
        pattern = self.get_file_pattern(variable, scenario)
        return sorted(data_path.glob(pattern.replace('*', '[0-9]*')))
    
    def get_year_range(self, scenario: str) -> tuple:
        """
        Get year range for a scenario.
        
        Parameters:
        -----------
        scenario : str
            Scenario name
        
        Returns:
        --------
        tuple
            (start_year, end_year)
        """
        year_ranges = {
            'historical': (1950, 2014),
            'ssp126': (2015, 2100),
            'ssp245': (2015, 2100),
            'ssp370': (2015, 2100),
            'ssp585': (2015, 2100)
        }
        return year_ranges.get(scenario, (None, None))
    
    def validate(self) -> bool:
        """Check if configuration is valid."""
        if not self.base_path.exists():
            print(f"Error: Data path {self.base_path} does not exist")
            return False
        
        # Create output directories
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.log_path.mkdir(parents=True, exist_ok=True)
        
        # Check for at least one variable
        found_vars = []
        for var in self.variables:
            var_path = self.base_path / var
            if var_path.exists():
                found_vars.append(var)
        
        if not found_vars:
            print("Error: No variable directories found")
            return False
        
        print(f"Found variables: {', '.join(found_vars)}")
        return True


# Example usage
if __name__ == "__main__":
    config = NorESMConfig()
    
    if config.validate():
        print("\nConfiguration valid!")
        
        # List available temperature files
        print("\nAvailable historical temperature files:")
        tas_files = config.list_available_files('tas', 'historical')
        for f in tas_files[:5]:  # Show first 5
            print(f"  - {f.name}")
        
        print(f"\nTotal files: {len(tas_files)}")
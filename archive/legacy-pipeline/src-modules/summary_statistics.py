#!/usr/bin/env python
"""
Summary statistics generator for climate indices.
Provides comprehensive statistical analysis and reporting.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
import xarray as xr
import numpy as np
import pandas as pd
from datetime import datetime
import json
import warnings
warnings.filterwarnings('ignore', category=RuntimeWarning)

logger = logging.getLogger(__name__)


class ClimateIndicesStatistics:
    """
    Generate comprehensive statistics for climate indices.
    
    Features:
    - Basic statistics (mean, std, min, max, percentiles)
    - Temporal trends and patterns
    - Spatial statistics
    - Data quality metrics
    - Inter-variable correlations
    """
    
    def __init__(self, dataset_path: Union[str, Path]):
        """
        Initialize with climate indices dataset.
        
        Parameters:
        -----------
        dataset_path : str or Path
            Path to NetCDF file with climate indices
        """
        self.dataset_path = Path(dataset_path)
        self.ds = None
        self.stats = {}
        
    def load_dataset(self) -> xr.Dataset:
        """Load the climate indices dataset."""
        logger.info(f"Loading dataset: {self.dataset_path}")
        self.ds = xr.open_dataset(self.dataset_path)
        logger.info(f"Loaded {len(self.ds.data_vars)} variables")
        return self.ds
    
    def calculate_basic_statistics(self, variable: str) -> Dict:
        """
        Calculate basic statistics for a variable.
        
        Parameters:
        -----------
        variable : str
            Variable name from dataset
            
        Returns:
        --------
        dict
            Statistics dictionary
        """
        if self.ds is None:
            self.load_dataset()
            
        data = self.ds[variable].values
        valid_data = data[~np.isnan(data)]
        
        if len(valid_data) == 0:
            return {'error': 'No valid data'}
        
        stats = {
            'count': len(valid_data),
            'missing': np.isnan(data).sum(),
            'missing_percent': (np.isnan(data).sum() / data.size) * 100,
            'mean': float(np.mean(valid_data)),
            'std': float(np.std(valid_data)),
            'min': float(np.min(valid_data)),
            'max': float(np.max(valid_data)),
            'median': float(np.median(valid_data)),
            'q25': float(np.percentile(valid_data, 25)),
            'q75': float(np.percentile(valid_data, 75)),
            'q05': float(np.percentile(valid_data, 5)),
            'q95': float(np.percentile(valid_data, 95)),
            'iqr': float(np.percentile(valid_data, 75) - np.percentile(valid_data, 25)),
            'range': float(np.max(valid_data) - np.min(valid_data))
        }
        
        # Add physical validity checks
        stats['physical_validity'] = self._check_physical_validity(variable, stats)
        
        return stats
    
    def _check_physical_validity(self, variable: str, stats: Dict) -> Dict:
        """
        Check if statistics are physically valid for the variable.
        
        Parameters:
        -----------
        variable : str
            Variable name
        stats : dict
            Calculated statistics
            
        Returns:
        --------
        dict
            Validity assessment
        """
        validity = {'status': 'valid', 'issues': []}
        
        # Temperature indices checks
        if any(temp in variable for temp in ['temperature', 'temp', 'tas', 'tmean']):
            if stats['min'] < -60:
                validity['issues'].append(f"Minimum temperature too low: {stats['min']:.1f}°C")
            if stats['max'] > 60:
                validity['issues'].append(f"Maximum temperature too high: {stats['max']:.1f}°C")
        
        # Day count indices checks
        day_indices = ['frost_days', 'ice_days', 'summer_days', 'hot_days', 
                      'tropical_nights', 'consecutive_dry_days', 'consecutive_wet_days']
        if any(idx in variable for idx in day_indices):
            if stats['min'] < 0:
                validity['issues'].append(f"Day count cannot be negative: {stats['min']:.1f}")
            if stats['max'] > 366:
                validity['issues'].append(f"Day count exceeds year length: {stats['max']:.1f}")
        
        # Precipitation checks
        if any(prcp in variable for prcp in ['precipitation', 'prcp', 'pr']):
            if stats['min'] < 0:
                validity['issues'].append(f"Precipitation cannot be negative: {stats['min']:.1f}")
        
        # Temperature range checks
        if 'temperature_range' in variable:
            if stats['min'] < 0:
                validity['issues'].append(f"Temperature range cannot be negative: {stats['min']:.1f}")
        
        if validity['issues']:
            validity['status'] = 'invalid'
        
        return validity
    
    def calculate_temporal_statistics(self, variable: str) -> Dict:
        """
        Calculate temporal trends and patterns.
        
        Parameters:
        -----------
        variable : str
            Variable name
            
        Returns:
        --------
        dict
            Temporal statistics
        """
        if self.ds is None:
            self.load_dataset()
        
        if 'time' not in self.ds.dims:
            return {'error': 'No time dimension'}
        
        data = self.ds[variable]
        
        # Calculate annual means if time dimension exists
        temporal_stats = {}
        
        try:
            # Annual statistics
            annual_mean = data.groupby('time.year').mean()
            temporal_stats['annual_means'] = {
                str(year): float(val) 
                for year, val in zip(annual_mean.year.values, annual_mean.mean().values)
            }
            
            # Trend analysis (simple linear regression)
            years = annual_mean.year.values
            values = annual_mean.mean().values
            
            # Remove NaN values
            mask = ~np.isnan(values)
            if mask.sum() > 1:
                years_clean = years[mask]
                values_clean = values[mask]
                
                # Linear trend
                z = np.polyfit(years_clean, values_clean, 1)
                temporal_stats['linear_trend'] = {
                    'slope': float(z[0]),
                    'intercept': float(z[1]),
                    'units': 'per year'
                }
                
                # Calculate trend significance (simplified)
                predicted = np.polyval(z, years_clean)
                residuals = values_clean - predicted
                r_squared = 1 - (np.sum(residuals**2) / np.sum((values_clean - np.mean(values_clean))**2))
                temporal_stats['linear_trend']['r_squared'] = float(r_squared)
        
        except Exception as e:
            logger.warning(f"Could not calculate temporal statistics: {e}")
            temporal_stats['error'] = str(e)
        
        return temporal_stats
    
    def calculate_spatial_statistics(self, variable: str) -> Dict:
        """
        Calculate spatial statistics.
        
        Parameters:
        -----------
        variable : str
            Variable name
            
        Returns:
        --------
        dict
            Spatial statistics
        """
        if self.ds is None:
            self.load_dataset()
        
        spatial_stats = {}
        
        data = self.ds[variable]
        
        # Check for spatial dimensions
        has_lat = 'lat' in self.ds.dims or 'latitude' in self.ds.dims
        has_lon = 'lon' in self.ds.dims or 'longitude' in self.ds.dims
        
        if not (has_lat and has_lon):
            return {'error': 'No spatial dimensions found'}
        
        try:
            # Spatial coverage
            lat_dim = 'lat' if 'lat' in self.ds.dims else 'latitude'
            lon_dim = 'lon' if 'lon' in self.ds.dims else 'longitude'
            
            spatial_stats['spatial_extent'] = {
                'lat_min': float(self.ds[lat_dim].min()),
                'lat_max': float(self.ds[lat_dim].max()),
                'lon_min': float(self.ds[lon_dim].min()),
                'lon_max': float(self.ds[lon_dim].max()),
                'n_lat': len(self.ds[lat_dim]),
                'n_lon': len(self.ds[lon_dim]),
                'total_gridpoints': len(self.ds[lat_dim]) * len(self.ds[lon_dim])
            }
            
            # Calculate spatial mean, std
            spatial_mean = data.mean(dim=['time'] if 'time' in data.dims else [])
            
            spatial_stats['spatial_distribution'] = {
                'mean': float(spatial_mean.mean()),
                'std': float(spatial_mean.std()),
                'min': float(spatial_mean.min()),
                'max': float(spatial_mean.max())
            }
            
            # Data coverage
            total_points = data.size
            valid_points = (~np.isnan(data.values)).sum()
            spatial_stats['data_coverage'] = {
                'total_points': int(total_points),
                'valid_points': int(valid_points),
                'coverage_percent': float((valid_points / total_points) * 100)
            }
            
        except Exception as e:
            logger.warning(f"Could not calculate spatial statistics: {e}")
            spatial_stats['error'] = str(e)
        
        return spatial_stats
    
    def generate_full_report(self) -> Dict:
        """
        Generate comprehensive statistics report for all variables.
        
        Returns:
        --------
        dict
            Complete statistics report
        """
        if self.ds is None:
            self.load_dataset()
        
        report = {
            'metadata': {
                'dataset': str(self.dataset_path),
                'generated': datetime.now().isoformat(),
                'n_variables': len(self.ds.data_vars),
                'variables': list(self.ds.data_vars),
                'dimensions': dict(self.ds.dims),
                'file_size_mb': self.dataset_path.stat().st_size / (1024 * 1024)
            },
            'variables': {}
        }
        
        # Calculate statistics for each variable
        for var in self.ds.data_vars:
            logger.info(f"Processing {var}...")
            
            var_stats = {
                'basic': self.calculate_basic_statistics(var),
                'temporal': self.calculate_temporal_statistics(var),
                'spatial': self.calculate_spatial_statistics(var)
            }
            
            # Add variable metadata
            var_stats['metadata'] = {
                'units': self.ds[var].attrs.get('units', 'unknown'),
                'long_name': self.ds[var].attrs.get('long_name', var),
                'dtype': str(self.ds[var].dtype),
                'shape': list(self.ds[var].shape)
            }
            
            report['variables'][var] = var_stats
        
        # Add summary section
        report['summary'] = self._generate_summary(report)
        
        return report
    
    def _generate_summary(self, report: Dict) -> Dict:
        """
        Generate summary section of report.
        
        Parameters:
        -----------
        report : dict
            Full report dictionary
            
        Returns:
        --------
        dict
            Summary statistics
        """
        summary = {
            'data_quality': {
                'variables_with_issues': [],
                'overall_missing_percent': 0,
                'physical_validity_issues': []
            },
            'key_findings': []
        }
        
        total_missing = 0
        total_points = 0
        
        for var, stats in report['variables'].items():
            # Check for data quality issues
            if 'basic' in stats:
                basic = stats['basic']
                if 'missing_percent' in basic:
                    total_missing += basic.get('missing', 0)
                    total_points += basic.get('count', 0) + basic.get('missing', 0)
                    
                    if basic['missing_percent'] > 50:
                        summary['data_quality']['variables_with_issues'].append(
                            f"{var}: {basic['missing_percent']:.1f}% missing"
                        )
                
                # Check physical validity
                if 'physical_validity' in basic:
                    validity = basic['physical_validity']
                    if validity['status'] == 'invalid':
                        for issue in validity['issues']:
                            summary['data_quality']['physical_validity_issues'].append(
                                f"{var}: {issue}"
                            )
        
        # Calculate overall missing percentage
        if total_points > 0:
            summary['data_quality']['overall_missing_percent'] = (
                (total_missing / total_points) * 100
            )
        
        # Generate key findings
        n_valid_vars = len([v for v in report['variables'] 
                           if 'error' not in report['variables'][v].get('basic', {})])
        summary['key_findings'].append(
            f"Successfully analyzed {n_valid_vars}/{len(report['variables'])} variables"
        )
        
        if summary['data_quality']['physical_validity_issues']:
            summary['key_findings'].append(
                f"Found {len(summary['data_quality']['physical_validity_issues'])} physical validity issues"
            )
        
        return summary
    
    def save_report(self, output_path: Optional[Path] = None, 
                   format: str = 'json') -> Path:
        """
        Save statistics report to file.
        
        Parameters:
        -----------
        output_path : Path, optional
            Output file path
        format : str
            Output format ('json', 'csv', or 'txt')
            
        Returns:
        --------
        Path
            Path to saved report
        """
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = Path(f'climate_indices_stats_{timestamp}.{format}')
        
        report = self.generate_full_report()
        
        if format == 'json':
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
        
        elif format == 'txt':
            with open(output_path, 'w') as f:
                self._write_text_report(f, report)
        
        elif format == 'csv':
            self._save_csv_report(output_path, report)
        
        logger.info(f"Report saved to {output_path}")
        return output_path
    
    def _write_text_report(self, file, report: Dict):
        """Write human-readable text report."""
        file.write("=" * 80 + "\n")
        file.write("CLIMATE INDICES STATISTICAL REPORT\n")
        file.write("=" * 80 + "\n\n")
        
        # Metadata
        file.write(f"Dataset: {report['metadata']['dataset']}\n")
        file.write(f"Generated: {report['metadata']['generated']}\n")
        file.write(f"Variables: {report['metadata']['n_variables']}\n")
        file.write(f"File size: {report['metadata']['file_size_mb']:.1f} MB\n")
        file.write("\n")
        
        # Summary
        file.write("SUMMARY\n")
        file.write("-" * 40 + "\n")
        summary = report['summary']
        for finding in summary['key_findings']:
            file.write(f"• {finding}\n")
        file.write(f"\nOverall missing data: {summary['data_quality']['overall_missing_percent']:.1f}%\n")
        
        if summary['data_quality']['physical_validity_issues']:
            file.write("\nPhysical Validity Issues:\n")
            for issue in summary['data_quality']['physical_validity_issues'][:10]:  # Limit to 10
                file.write(f"  ⚠ {issue}\n")
        
        file.write("\n")
        
        # Variable statistics
        file.write("VARIABLE STATISTICS\n")
        file.write("=" * 80 + "\n")
        
        for var, stats in report['variables'].items():
            file.write(f"\n{var}\n")
            file.write("-" * 40 + "\n")
            
            if 'metadata' in stats:
                meta = stats['metadata']
                file.write(f"  Units: {meta['units']}\n")
                file.write(f"  Shape: {meta['shape']}\n")
            
            if 'basic' in stats and 'error' not in stats['basic']:
                basic = stats['basic']
                file.write(f"  Mean: {basic['mean']:.2f}\n")
                file.write(f"  Std: {basic['std']:.2f}\n")
                file.write(f"  Range: [{basic['min']:.2f}, {basic['max']:.2f}]\n")
                file.write(f"  Missing: {basic['missing_percent']:.1f}%\n")
            
            if 'temporal' in stats and 'linear_trend' in stats['temporal']:
                trend = stats['temporal']['linear_trend']
                file.write(f"  Trend: {trend['slope']:.4f} per year (R²={trend.get('r_squared', 0):.3f})\n")
    
    def _save_csv_report(self, output_path: Path, report: Dict):
        """Save report as CSV with one row per variable."""
        rows = []
        
        for var, stats in report['variables'].items():
            row = {'variable': var}
            
            # Add metadata
            if 'metadata' in stats:
                row.update({f"meta_{k}": v for k, v in stats['metadata'].items()})
            
            # Add basic stats
            if 'basic' in stats and 'error' not in stats['basic']:
                row.update({f"basic_{k}": v for k, v in stats['basic'].items() 
                          if k != 'physical_validity'})
            
            # Add trend if available
            if 'temporal' in stats and 'linear_trend' in stats['temporal']:
                row['trend_slope'] = stats['temporal']['linear_trend']['slope']
                row['trend_r_squared'] = stats['temporal']['linear_trend'].get('r_squared', None)
            
            rows.append(row)
        
        df = pd.DataFrame(rows)
        df.to_csv(output_path, index=False)


def main():
    """Example usage of statistics generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate statistics for climate indices')
    parser.add_argument('dataset', help='Path to NetCDF dataset')
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--format', '-f', choices=['json', 'txt', 'csv'], 
                       default='txt', help='Output format')
    
    args = parser.parse_args()
    
    # Initialize statistics generator
    stats_gen = ClimateIndicesStatistics(args.dataset)
    
    # Generate and save report
    output_path = Path(args.output) if args.output else None
    saved_path = stats_gen.save_report(output_path, format=args.format)
    
    print(f"\n✓ Report saved to {saved_path}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
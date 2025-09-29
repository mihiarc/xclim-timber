#!/usr/bin/env python3
"""
Statistical analysis for extracted climate indices CSV files.
Adapted from the legacy summary_statistics module for CSV format.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import warnings
warnings.filterwarnings('ignore')

class ExtractedIndicesAnalyzer:
    """
    Analyze extracted climate indices from CSV files.
    Provides comprehensive statistics, trends, and data quality checks.
    """

    def __init__(self, csv_path: str):
        """Initialize with extracted indices CSV file."""
        self.csv_path = Path(csv_path)
        self.df = None
        self.climate_cols = None
        self.location_cols = ['saleid', 'parcelid', 'lat', 'lon']

    def load_data(self):
        """Load the CSV file and identify columns."""
        print(f"Loading {self.csv_path}...")
        self.df = pd.read_csv(self.csv_path)

        # Identify climate index columns (exclude ID columns and year)
        exclude_cols = self.location_cols + ['year']
        self.climate_cols = [c for c in self.df.columns if c not in exclude_cols]

        print(f"Loaded {len(self.df):,} rows with {len(self.climate_cols)} climate indices")
        return self.df

    def calculate_basic_statistics(self, column: str) -> dict:
        """Calculate basic statistics for a climate index."""
        data = self.df[column].dropna()

        if len(data) == 0:
            return {'error': 'All values are missing'}

        stats = {
            'count': len(data),
            'missing': self.df[column].isna().sum(),
            'missing_percent': (self.df[column].isna().sum() / len(self.df)) * 100,
            'mean': float(data.mean()),
            'std': float(data.std()),
            'min': float(data.min()),
            'max': float(data.max()),
            'median': float(data.median()),
            'q25': float(data.quantile(0.25)),
            'q75': float(data.quantile(0.75)),
            'q05': float(data.quantile(0.05)),
            'q95': float(data.quantile(0.95)),
            'iqr': float(data.quantile(0.75) - data.quantile(0.25))
        }

        # Physical validity checks
        stats['validity'] = self.check_physical_validity(column, stats)

        return stats

    def check_physical_validity(self, column: str, stats: dict) -> dict:
        """Check if values are physically reasonable."""
        issues = []

        # Temperature checks (in Celsius)
        temp_indices = ['tg_mean', 'tx_max', 'tn_min', 'dewpoint_mean', 'dewpoint_min', 'dewpoint_max']
        if any(idx in column for idx in temp_indices):
            if stats['min'] < -60:
                issues.append(f"Temperature too low: {stats['min']:.1f}°C")
            if stats['max'] > 60:
                issues.append(f"Temperature too high: {stats['max']:.1f}°C")

        # Day count checks
        day_indices = ['frost_days', 'ice_days', 'summer_days', 'hot_days',
                      'tropical_nights', 'humid_days', 'cdd', 'cwd']
        if any(idx in column for idx in day_indices):
            if stats['min'] < 0:
                issues.append(f"Days cannot be negative: {stats['min']}")
            if stats['max'] > 366:
                issues.append(f"Days exceed year length: {stats['max']}")

        # Precipitation checks
        precip_indices = ['prcptot', 'rx1day', 'rx5day', 'sdii']
        if any(idx in column for idx in precip_indices):
            if stats['min'] < 0:
                issues.append(f"Precipitation cannot be negative: {stats['min']:.1f}")

        # VPD checks - distinguish between day counts and actual VPD values
        if 'vpd' in column.lower():
            if '_days' in column:
                # These are day counts, not VPD values
                if stats['min'] < 0:
                    issues.append(f"Days cannot be negative: {stats['min']}")
                if stats['max'] > 366:
                    issues.append(f"Days exceed year length: {stats['max']}")
            elif 'vpdmax_mean' in column or 'vpdmin_mean' in column:
                # Actual VPD values in hPa (divide by 10 for kPa)
                if stats['min'] < 0:
                    issues.append(f"VPD cannot be negative: {stats['min']:.1f}")
                # Note: Values appear to be in hPa, so 30 hPa = 3.0 kPa is reasonable

        return {
            'status': 'invalid' if issues else 'valid',
            'issues': issues
        }

    def calculate_temporal_trends(self, column: str) -> dict:
        """Calculate temporal trends for a climate index."""
        # Group by year and calculate annual means
        annual = self.df.groupby('year')[column].agg(['mean', 'count', 'std'])
        annual = annual[annual['count'] > 100]  # Require at least 100 valid values per year

        if len(annual) < 2:
            return {'error': 'Insufficient data for trend analysis'}

        trends = {
            'annual_means': annual['mean'].to_dict(),
            'annual_counts': annual['count'].to_dict()
        }

        # Linear trend
        years = annual.index.values
        values = annual['mean'].values

        # Remove NaN values
        mask = ~np.isnan(values)
        if mask.sum() > 1:
            years_clean = years[mask]
            values_clean = values[mask]

            # Fit linear trend
            z = np.polyfit(years_clean, values_clean, 1)
            trends['linear_trend'] = {
                'slope': float(z[0]),
                'intercept': float(z[1]),
                'units_per_year': column
            }

            # Calculate R²
            predicted = np.polyval(z, years_clean)
            residuals = values_clean - predicted
            ss_res = np.sum(residuals**2)
            ss_tot = np.sum((values_clean - np.mean(values_clean))**2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            trends['linear_trend']['r_squared'] = float(r_squared)

            # Trend interpretation
            if abs(z[0]) > 0.01:  # Significant slope threshold
                direction = "increasing" if z[0] > 0 else "decreasing"
                trends['interpretation'] = f"{direction} by {abs(z[0]):.3f} per year"

        return trends

    def calculate_spatial_statistics(self, column: str) -> dict:
        """Calculate spatial statistics for a climate index."""
        # Average across all years for each location
        spatial_data = self.df.groupby(['saleid', 'parcelid', 'lat', 'lon'])[column].mean().reset_index()
        spatial_data = spatial_data.dropna()

        if len(spatial_data) == 0:
            return {'error': 'No valid spatial data'}

        stats = {
            'n_locations': len(spatial_data),
            'spatial_mean': float(spatial_data[column].mean()),
            'spatial_std': float(spatial_data[column].std()),
            'spatial_min': float(spatial_data[column].min()),
            'spatial_max': float(spatial_data[column].max()),
            'lat_range': [float(spatial_data['lat'].min()), float(spatial_data['lat'].max())],
            'lon_range': [float(spatial_data['lon'].min()), float(spatial_data['lon'].max())]
        }

        # Calculate spatial gradient (simplified)
        if len(spatial_data) > 10:
            # Latitude gradient
            lat_corr = spatial_data[['lat', column]].corr().iloc[0, 1]
            stats['lat_correlation'] = float(lat_corr)

            # Longitude gradient
            lon_corr = spatial_data[['lon', column]].corr().iloc[0, 1]
            stats['lon_correlation'] = float(lon_corr)

            # Interpretation
            if abs(lat_corr) > 0.3:
                stats['lat_gradient'] = "strong" if abs(lat_corr) > 0.6 else "moderate"
            if abs(lon_corr) > 0.3:
                stats['lon_gradient'] = "strong" if abs(lon_corr) > 0.6 else "moderate"

        return stats

    def generate_full_report(self) -> dict:
        """Generate comprehensive analysis report."""
        if self.df is None:
            self.load_data()

        print("\nGenerating comprehensive report...")

        report = {
            'metadata': {
                'file': str(self.csv_path),
                'generated': datetime.now().isoformat(),
                'total_rows': len(self.df),
                'unique_locations': self.df.groupby(['saleid', 'parcelid']).ngroups,
                'year_range': [int(self.df['year'].min()), int(self.df['year'].max())],
                'n_indices': len(self.climate_cols),
                'file_size_mb': self.csv_path.stat().st_size / (1024 * 1024)
            },
            'geographic_extent': {
                'lat_min': float(self.df['lat'].min()),
                'lat_max': float(self.df['lat'].max()),
                'lon_min': float(self.df['lon'].min()),
                'lon_max': float(self.df['lon'].max())
            },
            'indices': {}
        }

        # Analyze each climate index
        for i, col in enumerate(self.climate_cols, 1):
            print(f"  Analyzing {col} ({i}/{len(self.climate_cols)})...")

            index_stats = {
                'basic': self.calculate_basic_statistics(col),
                'temporal': self.calculate_temporal_trends(col),
                'spatial': self.calculate_spatial_statistics(col)
            }

            report['indices'][col] = index_stats

        # Generate summary
        report['summary'] = self.generate_summary(report)

        print("\n✓ Report generation complete!")
        return report

    def generate_summary(self, report: dict) -> dict:
        """Generate executive summary of findings."""
        summary = {
            'data_quality': {
                'indices_with_issues': [],
                'high_missing_indices': [],
                'validity_issues': []
            },
            'key_trends': [],
            'spatial_patterns': []
        }

        for idx, stats in report['indices'].items():
            # Data quality issues
            if 'basic' in stats:
                missing_pct = stats['basic'].get('missing_percent', 0)
                if missing_pct > 50:
                    summary['data_quality']['high_missing_indices'].append(
                        f"{idx}: {missing_pct:.1f}% missing"
                    )

                validity = stats['basic'].get('validity', {})
                if validity.get('status') == 'invalid':
                    for issue in validity['issues'][:2]:  # Limit to 2 issues per index
                        summary['data_quality']['validity_issues'].append(f"{idx}: {issue}")

            # Significant trends
            if 'temporal' in stats and 'linear_trend' in stats['temporal']:
                trend = stats['temporal']['linear_trend']
                if trend.get('r_squared', 0) > 0.5 and abs(trend['slope']) > 0.01:
                    summary['key_trends'].append({
                        'index': idx,
                        'slope': trend['slope'],
                        'r_squared': trend['r_squared'],
                        'interpretation': stats['temporal'].get('interpretation', '')
                    })

            # Spatial patterns
            if 'spatial' in stats:
                if abs(stats['spatial'].get('lat_correlation', 0)) > 0.4:
                    summary['spatial_patterns'].append({
                        'index': idx,
                        'type': 'latitudinal',
                        'correlation': stats['spatial']['lat_correlation']
                    })

        # Sort trends by R²
        summary['key_trends'] = sorted(
            summary['key_trends'],
            key=lambda x: x['r_squared'],
            reverse=True
        )[:10]  # Top 10 trends

        return summary

    def save_report(self, output_path: str = None, format: str = 'json'):
        """Save the analysis report."""
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_name = self.csv_path.stem
            output_path = f"{base_name}_analysis_{timestamp}.{format}"

        report = self.generate_full_report()

        if format == 'json':
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)

        elif format == 'txt':
            with open(output_path, 'w') as f:
                self.write_text_report(f, report)

        print(f"\n✓ Report saved to {output_path}")
        return output_path

    def write_text_report(self, file, report: dict):
        """Write human-readable text report."""
        file.write("=" * 80 + "\n")
        file.write("EXTRACTED CLIMATE INDICES ANALYSIS REPORT\n")
        file.write("=" * 80 + "\n\n")

        # Metadata
        meta = report['metadata']
        file.write(f"Dataset: {meta['file']}\n")
        file.write(f"Generated: {meta['generated']}\n")
        file.write(f"Total rows: {meta['total_rows']:,}\n")
        file.write(f"Unique locations: {meta['unique_locations']:,}\n")
        file.write(f"Years: {meta['year_range'][0]}-{meta['year_range'][1]}\n")
        file.write(f"Climate indices: {meta['n_indices']}\n")
        file.write(f"File size: {meta['file_size_mb']:.1f} MB\n\n")

        # Geographic extent
        geo = report['geographic_extent']
        file.write("Geographic Coverage:\n")
        file.write(f"  Latitude: {geo['lat_min']:.2f}° to {geo['lat_max']:.2f}°\n")
        file.write(f"  Longitude: {geo['lon_min']:.2f}° to {geo['lon_max']:.2f}°\n\n")

        # Summary
        file.write("=" * 80 + "\n")
        file.write("EXECUTIVE SUMMARY\n")
        file.write("-" * 40 + "\n\n")

        summary = report['summary']

        # Data quality
        file.write("Data Quality Issues:\n")
        if summary['data_quality']['high_missing_indices']:
            file.write("  High missing data:\n")
            for item in summary['data_quality']['high_missing_indices'][:5]:
                file.write(f"    • {item}\n")

        if summary['data_quality']['validity_issues']:
            file.write("  Physical validity concerns:\n")
            for item in summary['data_quality']['validity_issues'][:5]:
                file.write(f"    ⚠ {item}\n")

        # Key trends
        if summary['key_trends']:
            file.write("\nSignificant Temporal Trends:\n")
            for trend in summary['key_trends'][:5]:
                file.write(f"  • {trend['index']}: {trend['interpretation']} (R²={trend['r_squared']:.3f})\n")

        # Spatial patterns
        if summary['spatial_patterns']:
            file.write("\nSpatial Patterns Detected:\n")
            for pattern in summary['spatial_patterns'][:5]:
                direction = "increases" if pattern['correlation'] > 0 else "decreases"
                file.write(f"  • {pattern['index']} {direction} with latitude (r={pattern['correlation']:.2f})\n")

        file.write("\n" + "=" * 80 + "\n")
        file.write("DETAILED STATISTICS BY INDEX\n")
        file.write("=" * 80 + "\n")

        # Detailed statistics for each index
        for idx, stats in report['indices'].items():
            file.write(f"\n{idx}\n")
            file.write("-" * 40 + "\n")

            if 'basic' in stats and 'error' not in stats['basic']:
                basic = stats['basic']
                file.write(f"  Mean: {basic['mean']:.2f}\n")
                file.write(f"  Std Dev: {basic['std']:.2f}\n")
                file.write(f"  Range: [{basic['min']:.2f}, {basic['max']:.2f}]\n")
                file.write(f"  Median: {basic['median']:.2f}\n")
                file.write(f"  IQR: {basic['iqr']:.2f}\n")
                file.write(f"  Missing: {basic['missing_percent']:.1f}%\n")

                if basic['validity']['status'] == 'invalid':
                    file.write(f"  ⚠ Validity: {basic['validity']['issues'][0]}\n")

            if 'temporal' in stats and 'linear_trend' in stats['temporal']:
                trend = stats['temporal']['linear_trend']
                file.write(f"  Trend: {trend['slope']:.4f}/year (R²={trend.get('r_squared', 0):.3f})\n")

            if 'spatial' in stats and stats['spatial'].get('n_locations', 0) > 0:
                spatial = stats['spatial']
                file.write(f"  Spatial mean: {spatial['spatial_mean']:.2f}\n")


def main():
    """Run analysis on extracted indices."""
    import argparse

    parser = argparse.ArgumentParser(description='Analyze extracted climate indices')
    parser.add_argument('input', help='Path to extracted indices CSV file')
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--format', '-f', choices=['json', 'txt'],
                       default='txt', help='Output format (default: txt)')

    args = parser.parse_args()

    # Run analysis
    analyzer = ExtractedIndicesAnalyzer(args.input)
    output_path = analyzer.save_report(args.output, args.format)

    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
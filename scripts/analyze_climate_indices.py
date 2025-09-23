#!/usr/bin/env python
"""
Comprehensive analysis script for climate indices.
Generates statistics reports and visualizations.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
import argparse

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.summary_statistics import ClimateIndicesStatistics
from src.visualizations import ClimateIndicesVisualizer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_comprehensive_analysis(dataset_path: Path, 
                              output_dir: Optional[Path] = None,
                              variables: Optional[List[str]] = None) -> Dict:
    """
    Run complete analysis including statistics and visualizations.
    
    Parameters:
    -----------
    dataset_path : Path
        Path to climate indices dataset
    output_dir : Path, optional
        Output directory for results
    variables : list, optional
        Specific variables to analyze
    
    Returns:
    --------
    dict
        Analysis results summary
    """
    
    print("\n" + "="*80)
    print("CLIMATE INDICES COMPREHENSIVE ANALYSIS")
    print("="*80)
    print(f"Dataset: {dataset_path}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Create output directory
    if output_dir is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path(f'outputs/analysis_{timestamp}')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {
        'dataset': str(dataset_path),
        'timestamp': datetime.now().isoformat(),
        'output_dir': str(output_dir)
    }
    
    # Phase 1: Generate Statistics Report
    print("Phase 1: Generating Statistics Report")
    print("-" * 40)
    
    try:
        stats_gen = ClimateIndicesStatistics(dataset_path)
        
        # Generate reports in multiple formats
        print("  • Generating JSON report...")
        json_report = output_dir / 'statistics_report.json'
        stats_gen.save_report(json_report, format='json')
        
        print("  • Generating text report...")
        text_report = output_dir / 'statistics_report.txt'
        stats_gen.save_report(text_report, format='txt')
        
        print("  • Generating CSV summary...")
        csv_report = output_dir / 'statistics_summary.csv'
        stats_gen.save_report(csv_report, format='csv')
        
        # Get the full report for analysis
        report = stats_gen.generate_full_report()
        results['statistics'] = {
            'n_variables': report['metadata']['n_variables'],
            'reports_generated': ['json', 'txt', 'csv']
        }
        
        # Extract key findings
        if 'summary' in report:
            summary = report['summary']
            results['key_findings'] = summary.get('key_findings', [])
            results['data_quality'] = summary.get('data_quality', {})
        
        print(f"  ✓ Statistics reports saved to {output_dir}")
        
        # Print summary to console
        print("\n  Summary:")
        print(f"    - Variables analyzed: {report['metadata']['n_variables']}")
        print(f"    - File size: {report['metadata']['file_size_mb']:.1f} MB")
        
        if 'data_quality' in summary:
            dq = summary['data_quality']
            print(f"    - Overall missing data: {dq.get('overall_missing_percent', 0):.1f}%")
            if dq.get('physical_validity_issues'):
                print(f"    - Physical validity issues: {len(dq['physical_validity_issues'])}")
        
    except Exception as e:
        logger.error(f"Failed to generate statistics: {e}")
        results['statistics'] = {'error': str(e)}
    
    # Phase 2: Generate Visualizations
    print("\nPhase 2: Generating Visualizations")
    print("-" * 40)
    
    try:
        viz = ClimateIndicesVisualizer(dataset_path)
        viz.output_dir = output_dir / 'visualizations'
        viz.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load dataset to get available variables
        viz.load_dataset()
        
        # Select variables to visualize
        if variables is None:
            # Auto-select interesting variables
            priority_vars = [
                'annual_mean', 'frost_days', 'growing_degree_days',
                'prcptot', 'tx90p', 'daily_temperature_range'
            ]
            available_vars = list(viz.ds.data_vars)
            variables = [v for v in priority_vars if v in available_vars]
            if not variables:
                variables = available_vars[:6]  # Use first 6 if no priority vars found
        
        print(f"  Visualizing {len(variables)} variables: {', '.join(variables[:3])}...")
        
        # Generate different plot types
        plots_generated = []
        
        # 1. Summary dashboard
        print("  • Creating summary dashboard...")
        viz.create_summary_panel(variables[:4])
        plots_generated.append('summary_dashboard')
        
        # 2. Time series for key variables
        print("  • Generating time series plots...")
        for var in variables[:3]:  # Limit to 3 for time
            try:
                viz.plot_time_series(var)
                plots_generated.append(f'{var}_timeseries')
            except Exception as e:
                logger.warning(f"Could not plot time series for {var}: {e}")
        
        # 3. Spatial maps
        print("  • Creating spatial maps...")
        for var in variables[:2]:  # Limit to 2 for time
            try:
                viz.plot_spatial_map(var)
                plots_generated.append(f'{var}_map')
            except Exception as e:
                logger.warning(f"Could not create map for {var}: {e}")
        
        # 4. Distribution comparison
        print("  • Plotting distributions...")
        viz.plot_distribution(variables)
        plots_generated.append('distributions')
        
        # 5. Correlation matrix (if multiple variables)
        if len(variables) > 1:
            print("  • Generating correlation matrix...")
            viz.plot_correlation_matrix(variables[:15])  # Limit for readability
            plots_generated.append('correlation_matrix')
        
        results['visualizations'] = {
            'plots_generated': len(plots_generated),
            'plot_types': list(set([p.split('_')[-1] for p in plots_generated])),
            'output_dir': str(viz.output_dir)
        }
        
        print(f"  ✓ {len(plots_generated)} visualizations saved to {viz.output_dir}")
        
    except Exception as e:
        logger.error(f"Failed to generate visualizations: {e}")
        results['visualizations'] = {'error': str(e)}
    
    # Phase 3: Generate Summary Report
    print("\nPhase 3: Creating Analysis Summary")
    print("-" * 40)
    
    summary_file = output_dir / 'analysis_summary.txt'
    with open(summary_file, 'w') as f:
        f.write("="*80 + "\n")
        f.write("CLIMATE INDICES ANALYSIS SUMMARY\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"Dataset: {dataset_path}\n")
        f.write(f"Analysis Date: {results['timestamp']}\n")
        f.write(f"Output Directory: {output_dir}\n\n")
        
        if 'statistics' in results and 'error' not in results['statistics']:
            f.write("STATISTICS\n")
            f.write("-"*40 + "\n")
            f.write(f"Variables Analyzed: {results['statistics']['n_variables']}\n")
            f.write(f"Reports Generated: {', '.join(results['statistics']['reports_generated'])}\n\n")
        
        if 'key_findings' in results:
            f.write("KEY FINDINGS\n")
            f.write("-"*40 + "\n")
            for finding in results['key_findings']:
                f.write(f"• {finding}\n")
            f.write("\n")
        
        if 'data_quality' in results:
            dq = results['data_quality']
            f.write("DATA QUALITY\n")
            f.write("-"*40 + "\n")
            f.write(f"Overall Missing: {dq.get('overall_missing_percent', 0):.1f}%\n")
            if dq.get('physical_validity_issues'):
                f.write(f"Issues Found: {len(dq['physical_validity_issues'])}\n")
                for issue in dq['physical_validity_issues'][:5]:  # Show first 5
                    f.write(f"  ⚠ {issue}\n")
            f.write("\n")
        
        if 'visualizations' in results and 'error' not in results['visualizations']:
            f.write("VISUALIZATIONS\n")
            f.write("-"*40 + "\n")
            f.write(f"Plots Generated: {results['visualizations']['plots_generated']}\n")
            f.write(f"Plot Types: {', '.join(results['visualizations']['plot_types'])}\n")
            f.write(f"Location: {results['visualizations']['output_dir']}\n")
    
    print(f"  ✓ Analysis summary saved to {summary_file}")
    
    # Print final summary
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)
    print(f"\nResults saved to: {output_dir}")
    print("\nGenerated outputs:")
    print("  • Statistics reports (JSON, TXT, CSV)")
    print(f"  • {results.get('visualizations', {}).get('plots_generated', 0)} visualizations")
    print("  • Analysis summary")
    
    return results


def main():
    """Main function for CLI usage."""
    
    parser = argparse.ArgumentParser(
        description='Comprehensive analysis of climate indices',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze the combined indices file
  python analyze_climate_indices.py outputs/comprehensive_2001_2024/combined_indices.nc
  
  # Analyze specific variables
  python analyze_climate_indices.py data.nc -v frost_days ice_days annual_mean
  
  # Save to specific directory
  python analyze_climate_indices.py data.nc -o outputs/my_analysis
        """
    )
    
    parser.add_argument('dataset', help='Path to climate indices NetCDF file')
    parser.add_argument('-o', '--output', help='Output directory for results')
    parser.add_argument('-v', '--variables', nargs='+', 
                       help='Specific variables to analyze')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check dataset exists
    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        print(f"Error: Dataset not found: {dataset_path}")
        return 1
    
    # Run analysis
    try:
        output_dir = Path(args.output) if args.output else None
        results = run_comprehensive_analysis(
            dataset_path,
            output_dir=output_dir,
            variables=args.variables
        )
        
        return 0 if 'error' not in results else 1
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        return 1


if __name__ == "__main__":
    import sys
    from typing import Dict, List, Optional
    sys.exit(main())
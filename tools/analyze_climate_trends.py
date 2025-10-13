#!/usr/bin/env python3
"""
Analyze and visualize climate indices trends across Pacific Northwest and Southeast regions.

Creates comprehensive visualizations showing:
- Regional climate comparisons
- Temporal trends (1981-2024)
- Extreme events analysis
- Seasonal patterns

Usage:
    python analyze_climate_trends.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging
from scipy import stats

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def load_data():
    """Load both regional datasets."""
    logger.info("Loading Pacific Northwest data...")
    pnw = pd.read_csv('outputs/extractions/temperature_pacific_northwest.csv')
    pnw['region'] = 'Pacific Northwest'

    logger.info("Loading Southeast data...")
    se = pd.read_csv('outputs/extractions/temperature_southeast.csv')
    se['region'] = 'Southeast'

    logger.info(f"PNW: {len(pnw):,} rows, SE: {len(se):,} rows")
    return pnw, se

def compute_regional_means(pnw, se):
    """Compute annual regional mean values."""
    logger.info("Computing regional means by year...")

    # Get climate index columns (exclude metadata)
    meta_cols = ['saleid', 'parcelid', 'lat', 'lon', 'year', 'region']
    climate_cols = [col for col in pnw.columns if col not in meta_cols]

    # Compute annual means for each region
    pnw_means = pnw.groupby('year')[climate_cols].mean()
    pnw_means['region'] = 'Pacific Northwest'

    se_means = se.groupby('year')[climate_cols].mean()
    se_means['region'] = 'Southeast'

    # Combine
    combined = pd.concat([
        pnw_means.reset_index(),
        se_means.reset_index()
    ])

    return combined, climate_cols

def plot_temperature_trends(df, output_dir):
    """Plot temperature trends over time."""
    logger.info("Plotting temperature trends...")

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Temperature Trends (1981-2024): Pacific Northwest vs Southeast',
                 fontsize=16, fontweight='bold', y=0.995)

    indices = [
        ('tg_mean', 'Annual Mean Temperature (°C)', axes[0, 0]),
        ('tx_max', 'Annual Maximum Temperature (°C)', axes[0, 1]),
        ('tn_min', 'Annual Minimum Temperature (°C)', axes[1, 0]),
        ('daily_temperature_range', 'Daily Temperature Range (°C)', axes[1, 1])
    ]

    for idx, title, ax in indices:
        for region in ['Pacific Northwest', 'Southeast']:
            data = df[df['region'] == region]
            ax.plot(data['year'], data[idx], marker='o', linewidth=2,
                   markersize=4, label=region, alpha=0.7)

            # Add trend line
            z = np.polyfit(data['year'], data[idx], 1)
            p = np.poly1d(z)
            ax.plot(data['year'], p(data['year']), '--', linewidth=1.5, alpha=0.5)

            # Calculate trend
            slope, _, _, p_value, _ = stats.linregress(data['year'], data[idx])
            trend_text = f"{region[:3]}: {slope:.3f}°C/year"
            if p_value < 0.05:
                trend_text += "*"
            ax.text(0.02, 0.98 - (0.05 if region == 'Pacific Northwest' else 0),
                   trend_text, transform=ax.transAxes, fontsize=9,
                   verticalalignment='top', bbox=dict(boxstyle='round',
                   facecolor='wheat', alpha=0.3))

        ax.set_xlabel('Year', fontsize=11)
        ax.set_ylabel(title, fontsize=11)
        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.legend(loc='best', fontsize=9)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'temperature_trends.png', dpi=300, bbox_inches='tight')
    logger.info(f"Saved: {output_dir / 'temperature_trends.png'}")
    plt.close()

def plot_extreme_events(df, output_dir):
    """Plot extreme events frequency."""
    logger.info("Plotting extreme events...")

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Extreme Events Trends (1981-2024)', fontsize=16, fontweight='bold', y=0.995)

    events = [
        ('heat_wave_frequency', 'Heat Wave Frequency (events/year)', axes[0, 0]),
        ('hot_days', 'Hot Days >30°C (days/year)', axes[0, 1]),
        ('tropical_nights', 'Tropical Nights >20°C (nights/year)', axes[0, 2]),
        ('frost_days', 'Frost Days <0°C (days/year)', axes[1, 0]),
        ('ice_days', 'Ice Days (max<0°C, days/year)', axes[1, 1]),
        ('consecutive_frost_days', 'Max Consecutive Frost Days', axes[1, 2])
    ]

    for idx, title, ax in events:
        for region in ['Pacific Northwest', 'Southeast']:
            data = df[df['region'] == region]
            ax.plot(data['year'], data[idx], marker='o', linewidth=2,
                   markersize=4, label=region, alpha=0.7)

            # Add trend line
            z = np.polyfit(data['year'], data[idx], 1)
            p = np.poly1d(z)
            ax.plot(data['year'], p(data['year']), '--', linewidth=1.5, alpha=0.5)

            # Calculate trend
            slope, _, _, p_value, _ = stats.linregress(data['year'], data[idx])
            trend = "↑" if slope > 0 else "↓"
            sig = "*" if p_value < 0.05 else ""
            ax.text(0.02, 0.98 - (0.05 if region == 'Pacific Northwest' else 0),
                   f"{region[:3]}: {abs(slope):.2f}/year {trend}{sig}",
                   transform=ax.transAxes, fontsize=9, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        ax.set_xlabel('Year', fontsize=10)
        ylabel = title.split('(')[1].rstrip(')') if '(' in title else 'Value'
        ax.set_ylabel(ylabel, fontsize=10)
        ax.set_title(title.split('(')[0].strip() if '(' in title else title, fontsize=11, fontweight='bold')
        ax.legend(loc='best', fontsize=8)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'extreme_events.png', dpi=300, bbox_inches='tight')
    logger.info(f"Saved: {output_dir / 'extreme_events.png'}")
    plt.close()

def plot_growing_season(df, output_dir):
    """Plot growing season metrics."""
    logger.info("Plotting growing season metrics...")

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Growing Season Metrics (1981-2024)', fontsize=16, fontweight='bold', y=0.995)

    metrics = [
        ('frost_free_season_length', 'Frost-Free Season Length (days)', axes[0, 0]),
        ('growing_degree_days', 'Growing Degree Days (°C·days)', axes[0, 1]),
        ('frost_free_season_start', 'Last Spring Frost (Julian day)', axes[1, 0]),
        ('frost_free_season_end', 'First Fall Frost (Julian day)', axes[1, 1])
    ]

    for idx, title, ax in metrics:
        for region in ['Pacific Northwest', 'Southeast']:
            data = df[df['region'] == region]
            ax.plot(data['year'], data[idx], marker='o', linewidth=2,
                   markersize=4, label=region, alpha=0.7)

            # Add trend line
            z = np.polyfit(data['year'], data[idx], 1)
            p = np.poly1d(z)
            ax.plot(data['year'], p(data['year']), '--', linewidth=1.5, alpha=0.5)

            # Calculate trend
            slope, _, _, p_value, _ = stats.linregress(data['year'], data[idx])
            trend = "↑" if slope > 0 else "↓"
            sig = "*" if p_value < 0.05 else ""
            ax.text(0.02, 0.98 - (0.05 if region == 'Pacific Northwest' else 0),
                   f"{region[:3]}: {abs(slope):.2f}/year {trend}{sig}",
                   transform=ax.transAxes, fontsize=9, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

        ax.set_xlabel('Year', fontsize=11)
        ylabel = title.split('(')[1].rstrip(')') if '(' in title else 'Value'
        ax.set_ylabel(ylabel, fontsize=11)
        ax.set_title(title.split('(')[0].strip() if '(' in title else title, fontsize=12, fontweight='bold')
        ax.legend(loc='best', fontsize=9)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'growing_season.png', dpi=300, bbox_inches='tight')
    logger.info(f"Saved: {output_dir / 'growing_season.png'}")
    plt.close()

def plot_regional_comparison_boxplots(pnw, se, output_dir):
    """Create boxplot comparison of key indices."""
    logger.info("Creating regional comparison boxplots...")

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Regional Climate Comparison (1981-2024 Distributions)',
                 fontsize=16, fontweight='bold', y=0.995)

    indices = [
        ('tg_mean', 'Mean Temperature (°C)', axes[0, 0]),
        ('heat_wave_frequency', 'Heat Wave Frequency', axes[0, 1]),
        ('frost_days', 'Frost Days', axes[0, 2]),
        ('growing_degree_days', 'Growing Degree Days', axes[1, 0]),
        ('tropical_nights', 'Tropical Nights', axes[1, 1]),
        ('frost_free_season_length', 'Frost-Free Season (days)', axes[1, 2])
    ]

    combined = pd.concat([pnw, se], ignore_index=True)

    for idx, title, ax in indices:
        sns.boxplot(data=combined, x='region', y=idx, ax=ax)
        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.set_xlabel('')
        ax.set_ylabel(title.split('(')[1].rstrip(')') if '(' in title else '', fontsize=10)
        ax.tick_params(axis='x', rotation=15)

        # Add mean values as text
        for i, region in enumerate(['Pacific Northwest', 'Southeast']):
            mean_val = combined[combined['region'] == region][idx].mean()
            ax.text(i, ax.get_ylim()[1] * 0.95, f'μ={mean_val:.1f}',
                   ha='center', fontsize=9, bbox=dict(boxstyle='round',
                   facecolor='yellow', alpha=0.3))

    plt.tight_layout()
    plt.savefig(output_dir / 'regional_comparison.png', dpi=300, bbox_inches='tight')
    logger.info(f"Saved: {output_dir / 'regional_comparison.png'}")
    plt.close()

def plot_climate_change_indicators(df, output_dir):
    """Plot key climate change indicators."""
    logger.info("Plotting climate change indicators...")

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Climate Change Indicators (1981-2024)',
                 fontsize=16, fontweight='bold', y=0.995)

    indicators = [
        ('warm_spell_duration_index', 'Warm Spell Duration Index (WSDI)', axes[0, 0]),
        ('cold_spell_duration_index', 'Cold Spell Duration Index (CSDI)', axes[0, 1]),
        ('tx90p', 'Warm Days (>90th percentile)', axes[1, 0]),
        ('tn10p', 'Cool Nights (<10th percentile)', axes[1, 1])
    ]

    for idx, title, ax in indicators:
        for region in ['Pacific Northwest', 'Southeast']:
            data = df[df['region'] == region]
            ax.plot(data['year'], data[idx], marker='o', linewidth=2,
                   markersize=4, label=region, alpha=0.7)

            # Add trend line
            z = np.polyfit(data['year'], data[idx], 1)
            p = np.poly1d(z)
            ax.plot(data['year'], p(data['year']), '--', linewidth=1.5, alpha=0.5)

            # Calculate trend with significance
            slope, _, _, p_value, _ = stats.linregress(data['year'], data[idx])
            sig = "***" if p_value < 0.001 else "**" if p_value < 0.01 else "*" if p_value < 0.05 else ""
            trend = "↑" if slope > 0 else "↓"
            ax.text(0.02, 0.98 - (0.05 if region == 'Pacific Northwest' else 0),
                   f"{region[:3]}: {abs(slope):.2f}/year {trend}{sig}",
                   transform=ax.transAxes, fontsize=9, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='lightcoral' if 'Warm' in title else 'lightblue', alpha=0.4))

        ax.set_xlabel('Year', fontsize=11)
        ax.set_ylabel('Days', fontsize=11)
        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.legend(loc='best', fontsize=9)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'climate_change_indicators.png', dpi=300, bbox_inches='tight')
    logger.info(f"Saved: {output_dir / 'climate_change_indicators.png'}")
    plt.close()

def generate_summary_statistics(pnw, se, df_means, output_dir):
    """Generate summary statistics report."""
    logger.info("Generating summary statistics...")

    report = []
    report.append("=" * 80)
    report.append("CLIMATE INDICES ANALYSIS SUMMARY (1981-2024)")
    report.append("=" * 80)
    report.append("")

    # Regional summaries
    report.append("REGIONAL AVERAGES (44-year means)")
    report.append("-" * 80)

    key_indices = [
        ('tg_mean', 'Mean Temperature', '°C'),
        ('frost_free_season_length', 'Frost-Free Season', 'days'),
        ('growing_degree_days', 'Growing Degree Days', '°C·days'),
        ('heat_wave_frequency', 'Heat Wave Frequency', 'events/yr'),
        ('frost_days', 'Frost Days', 'days/yr'),
        ('tropical_nights', 'Tropical Nights', 'nights/yr')
    ]

    for idx, name, unit in key_indices:
        pnw_mean = pnw[idx].mean()
        se_mean = se[idx].mean()
        diff = se_mean - pnw_mean
        pct_diff = (diff / pnw_mean * 100) if pnw_mean != 0 else 0

        report.append(f"\n{name}:")
        report.append(f"  Pacific Northwest: {pnw_mean:.1f} {unit}")
        report.append(f"  Southeast:         {se_mean:.1f} {unit}")
        report.append(f"  Difference:        {diff:+.1f} {unit} ({pct_diff:+.1f}%)")

    report.append("")
    report.append("")
    report.append("TEMPORAL TRENDS (Linear regression, 1981-2024)")
    report.append("-" * 80)

    trend_indices = [
        ('tg_mean', 'Mean Temperature', '°C/year'),
        ('heat_wave_frequency', 'Heat Wave Frequency', 'events/decade'),
        ('frost_days', 'Frost Days', 'days/decade'),
        ('frost_free_season_length', 'Frost-Free Season', 'days/decade')
    ]

    for idx, name, unit in trend_indices:
        report.append(f"\n{name}:")
        for region in ['Pacific Northwest', 'Southeast']:
            data = df_means[df_means['region'] == region]
            slope, _, _, p_value, _ = stats.linregress(data['year'], data[idx])

            # Convert to per decade if needed
            if 'decade' in unit:
                slope = slope * 10

            sig = "***" if p_value < 0.001 else "**" if p_value < 0.01 else "*" if p_value < 0.05 else "ns"
            direction = "increasing" if slope > 0 else "decreasing"

            report.append(f"  {region:20s}: {slope:+.3f} {unit} ({direction}, p={sig})")

    report.append("")
    report.append("")
    report.append("CLIMATE CHANGE SIGNALS")
    report.append("-" * 80)
    report.append("")
    report.append("Warming signals (1981-2024):")

    # Check for warming
    for region in ['Pacific Northwest', 'Southeast']:
        data = df_means[df_means['region'] == region]
        temp_slope, _, _, temp_p, _ = stats.linregress(data['year'], data['tg_mean'])
        warm_days_slope, _, _, warm_p, _ = stats.linregress(data['year'], data['tx90p'])

        report.append(f"\n{region}:")
        report.append(f"  Temperature increase: {temp_slope * 43:.2f}°C total ({temp_slope:.3f}°C/year)")
        report.append(f"  Statistical significance: p < {temp_p:.4f}")
        report.append(f"  Warm days increase: {warm_days_slope * 43:.1f} days total")

        if temp_p < 0.05:
            report.append(f"  ✓ Significant warming detected")
        else:
            report.append(f"  ✗ No significant warming trend")

    report.append("")
    report.append("=" * 80)

    # Save report
    report_text = "\n".join(report)
    with open(output_dir / 'analysis_summary.txt', 'w') as f:
        f.write(report_text)

    logger.info(f"Saved: {output_dir / 'analysis_summary.txt'}")
    print("\n" + report_text)

def main():
    """Main analysis workflow."""
    logger.info("=" * 60)
    logger.info("CLIMATE INDICES TREND ANALYSIS")
    logger.info("=" * 60)

    # Create output directory
    output_dir = Path('outputs/analysis')
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Load data
    pnw, se = load_data()

    # Compute regional means
    df_means, climate_cols = compute_regional_means(pnw, se)

    # Generate visualizations
    plot_temperature_trends(df_means, output_dir)
    plot_extreme_events(df_means, output_dir)
    plot_growing_season(df_means, output_dir)
    plot_regional_comparison_boxplots(pnw, se, output_dir)
    plot_climate_change_indicators(df_means, output_dir)

    # Generate summary statistics
    generate_summary_statistics(pnw, se, df_means, output_dir)

    logger.info("")
    logger.info("=" * 60)
    logger.info("✓ Analysis complete!")
    logger.info(f"✓ Generated 5 visualizations in {output_dir}")
    logger.info(f"✓ Summary statistics: {output_dir / 'analysis_summary.txt'}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""
Example script showing how to format climate indices CSV data.
"""

from csv_formatter import ClimateCSVFormatter
import pandas as pd

def main():
    """Demonstrate CSV formatting for climate indices data."""

    print("=== Climate Indices CSV Formatting Example ===\n")

    # Initialize formatter for full historical period (1950-2014)
    formatter = ClimateCSVFormatter(historical_years=(1950, 2014))

    # For this example, we'll use the sample data (1950-1955)
    sample_formatter = ClimateCSVFormatter(historical_years=(1950, 1955))

    print("1. Loading sample historical data...")
    try:
        # Load the sample historical data
        df = sample_formatter.load_historical_data('outputs')
        print(f"   ✓ Loaded {len(df)} location-year combinations")
        print(f"   ✓ Years: {sorted(df['year'].unique())}")
        print(f"   ✓ Locations: {len(df.groupby(['lat', 'lon']))}")

    except Exception as e:
        print(f"   ✗ Error loading data: {e}")
        return

    print("\n2. Creating long format (current format)...")
    long_df = sample_formatter.create_long_format(df)
    print(f"   ✓ Long format: {long_df.shape[0]} rows × {long_df.shape[1]} columns")
    print(f"   ✓ Each row represents: unique location-year combination")

    # Show sample
    print("\n   Sample long format data:")
    print(long_df[['saleid', 'parcelid', 'lat', 'lon', 'year', 'annual_mean', 'frost_days']].head(3))

    print("\n3. Creating wide format...")
    wide_df = sample_formatter.create_wide_format(df)
    print(f"   ✓ Wide format: {wide_df.shape[0]} rows × {wide_df.shape[1]} columns")
    print(f"   ✓ Each row represents: unique location")
    print(f"   ✓ Climate indices spread across years as separate columns")

    # Show sample
    print("\n   Sample wide format data (first few columns):")
    sample_cols = ['saleid', 'parcelid', 'lat', 'lon', 'annual_mean_1950', 'annual_mean_1951', 'frost_days_1950', 'frost_days_1951']
    print(wide_df[sample_cols].head(3))

    print("\n4. Saving formatted data...")
    long_file, wide_file = sample_formatter.save_formatted_data(
        long_df, wide_df,
        'outputs/formatted',
        'example_climate_indices'
    )

    print(f"\n=== Summary ===")
    print(f"Historical data period: {sample_formatter.historical_years[0]}-{sample_formatter.historical_years[1]}")
    print(f"")
    print(f"Long format (traditional):")
    print(f"  - Structure: Each row = location-year combination")
    print(f"  - Use case: Time series analysis, statistical modeling")
    print(f"  - File: {long_file}")
    print(f"")
    print(f"Wide format (pivot table style):")
    print(f"  - Structure: Each row = location, columns = index_year")
    print(f"  - Use case: Comparison across years, spatial analysis")
    print(f"  - File: {wide_file}")
    print(f"")
    print(f"Available climate indices:")
    climate_indices = [col for col in long_df.columns if col not in ['saleid', 'parcelid', 'lat', 'lon', 'year']]
    for i, idx in enumerate(climate_indices, 1):
        print(f"  {i:2d}. {idx}")


if __name__ == "__main__":
    main()
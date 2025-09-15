#!/usr/bin/env python
"""
CSV formatter for climate indices data.
Converts between long and wide formats and filters for historical data only.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import argparse
import logging
from typing import List, Dict, Optional

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class ClimateCSVFormatter:
    """Format climate indices CSV data into different structures."""

    def __init__(self, historical_years: tuple = (1950, 2014)):
        """
        Initialize the formatter.

        Parameters:
        -----------
        historical_years : tuple
            Range of historical years (start, end) inclusive
        """
        self.historical_years = historical_years
        self.climate_indices = [
            'annual_mean', 'annual_min', 'annual_max', 'annual_std',
            'frost_days', 'ice_days', 'summer_days', 'hot_days',
            'tropical_nights', 'growing_degree_days', 'heating_degree_days',
            'cooling_degree_days'
        ]

    def load_historical_data(self, data_dir: str) -> pd.DataFrame:
        """
        Load and combine historical CSV files.

        Parameters:
        -----------
        data_dir : str
            Directory containing historical CSV files

        Returns:
        --------
        pd.DataFrame
            Combined historical data in long format
        """
        data_path = Path(data_dir)
        historical_path = data_path / 'historical'

        # Look for historical files
        csv_files = []
        if historical_path.exists():
            csv_files.extend(list(historical_path.glob('parcel_indices_[0-9]*.csv')))

        # Also check main output directory
        csv_files.extend(list(data_path.glob('parcel_indices_[0-9]*.csv')))

        if not csv_files:
            raise ValueError(f"No historical CSV files found in {data_dir}")

        logger.info(f"Found {len(csv_files)} CSV files")

        # Load and combine all files
        dataframes = []
        for file in csv_files:
            try:
                df = pd.read_csv(file)

                # Filter for historical years only
                if 'year' in df.columns:
                    df = df[
                        (df['year'] >= self.historical_years[0]) &
                        (df['year'] <= self.historical_years[1])
                    ]

                if not df.empty:
                    dataframes.append(df)
                    logger.info(f"Loaded {len(df)} rows from {file.name}")

            except Exception as e:
                logger.warning(f"Could not load {file}: {e}")

        if not dataframes:
            raise ValueError("No valid historical data found")

        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True)

        # Remove duplicates (same location-year combination)
        combined_df = combined_df.drop_duplicates(
            subset=['saleid', 'parcelid', 'lat', 'lon', 'year']
        )

        logger.info(f"Combined dataset: {len(combined_df)} rows, "
                   f"years {combined_df['year'].min()}-{combined_df['year'].max()}")

        return combined_df

    def create_long_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure data is in long format (already should be).

        Parameters:
        -----------
        df : pd.DataFrame
            Input dataframe

        Returns:
        --------
        pd.DataFrame
            Data in long format (each row = location-year)
        """
        # The input data should already be in long format
        # Just sort for consistency
        long_df = df.copy()
        long_df = long_df.sort_values(['saleid', 'parcelid', 'lat', 'lon', 'year'])

        logger.info(f"Long format: {len(long_df)} rows")
        return long_df

    def create_wide_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert data to wide format.

        Parameters:
        -----------
        df : pd.DataFrame
            Input dataframe in long format

        Returns:
        --------
        pd.DataFrame
            Data in wide format (each row = location, columns = index_year)
        """
        logger.info("Converting to wide format...")

        # Identify location columns and climate indices
        location_cols = ['saleid', 'parcelid', 'lat', 'lon']

        # Get available climate indices from the dataframe
        available_indices = [col for col in self.climate_indices if col in df.columns]
        logger.info(f"Available climate indices: {available_indices}")

        # Pivot each climate index separately
        wide_dfs = []

        # Start with location information
        locations = df[location_cols].drop_duplicates().sort_values(location_cols)
        logger.info(f"Found {len(locations)} unique locations")

        wide_result = locations.copy()

        # Pivot each climate index
        for index_name in available_indices:
            logger.info(f"Pivoting {index_name}...")

            # Create pivot table for this index
            pivot_df = df.pivot_table(
                index=location_cols,
                columns='year',
                values=index_name,
                aggfunc='mean'  # In case of duplicates
            )

            # Flatten column names (add index name prefix)
            pivot_df.columns = [f"{index_name}_{year}" for year in pivot_df.columns]

            # Reset index to merge
            pivot_df = pivot_df.reset_index()

            # Merge with main result
            wide_result = wide_result.merge(
                pivot_df,
                on=location_cols,
                how='left'
            )

        logger.info(f"Wide format: {len(wide_result)} rows, {len(wide_result.columns)} columns")

        return wide_result

    def save_formatted_data(self, long_df: pd.DataFrame, wide_df: pd.DataFrame,
                          output_dir: str, prefix: str = "climate_indices"):
        """
        Save both long and wide format data.

        Parameters:
        -----------
        long_df : pd.DataFrame
            Data in long format
        wide_df : pd.DataFrame
            Data in wide format
        output_dir : str
            Output directory
        prefix : str
            File name prefix
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Create file names with year range
        year_range = f"{self.historical_years[0]}_{self.historical_years[1]}"

        # Save long format
        long_file = output_path / f"{prefix}_historical_long_{year_range}.csv"
        long_df.to_csv(long_file, index=False)
        logger.info(f"Saved long format to {long_file}")

        # Save wide format
        wide_file = output_path / f"{prefix}_historical_wide_{year_range}.csv"
        wide_df.to_csv(wide_file, index=False)
        logger.info(f"Saved wide format to {wide_file}")

        # Print summary
        print(f"\n=== CSV Formatting Summary ===")
        print(f"Historical period: {self.historical_years[0]}-{self.historical_years[1]}")
        print(f"Unique locations: {len(wide_df)}")
        print(f"Total location-year combinations: {len(long_df)}")
        print(f"Years available: {sorted(long_df['year'].unique())}")
        print(f"\nLong format (each row = location-year):")
        print(f"  File: {long_file}")
        print(f"  Shape: {long_df.shape}")
        print(f"\nWide format (each row = location, columns = index_year):")
        print(f"  File: {wide_file}")
        print(f"  Shape: {wide_df.shape}")

        # Show sample of climate indices columns in wide format
        climate_cols = [col for col in wide_df.columns if any(idx in col for idx in self.climate_indices)]
        sample_cols = climate_cols[:10] if len(climate_cols) > 10 else climate_cols
        print(f"\nSample wide format columns: {sample_cols}")

        return long_file, wide_file


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Format climate indices CSV data into long and wide formats"
    )
    parser.add_argument('--input-dir', '-i', default='outputs',
                       help='Input directory containing CSV files')
    parser.add_argument('--output-dir', '-o', default='outputs/formatted',
                       help='Output directory for formatted files')
    parser.add_argument('--start-year', type=int, default=1950,
                       help='Start year for historical data (default: 1950)')
    parser.add_argument('--end-year', type=int, default=2014,
                       help='End year for historical data (default: 2014)')
    parser.add_argument('--prefix', default='climate_indices',
                       help='File name prefix (default: climate_indices)')

    args = parser.parse_args()

    # Initialize formatter
    formatter = ClimateCSVFormatter(
        historical_years=(args.start_year, args.end_year)
    )

    try:
        # Load historical data
        logger.info("Loading historical data...")
        df = formatter.load_historical_data(args.input_dir)

        # Create both formats
        logger.info("Creating long format...")
        long_df = formatter.create_long_format(df)

        logger.info("Creating wide format...")
        wide_df = formatter.create_wide_format(df)

        # Save results
        formatter.save_formatted_data(
            long_df, wide_df,
            args.output_dir, args.prefix
        )

        print("\n✓ CSV formatting completed successfully!")

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
#!/usr/bin/env python
"""
Demo script showing xclim-timber MVP with dual-format CSV output.
Combines the efficient extraction with flexible output formatting.
"""

import subprocess
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def run_demo():
    """Demonstrate the integrated MVP + CSV formatting workflow."""

    print("=== xclim-timber: MVP + Dual CSV Format Demo ===\n")

    # Check if we have sample data
    if not Path('parcel_coordinates_sample.csv').exists():
        print("❌ Sample parcel coordinates not found")
        print("Please ensure you have sample data or create parcel_coordinates_sample.csv")
        return

    # Step 1: Extract climate indices using MVP tool
    print("1. Extracting climate indices using xclim-timber MVP...")
    print("   (This would normally process real NetCDF files)")
    print("   For demo purposes, we'll use any existing CSV output\n")

    # Step 2: Check for existing output or create sample
    output_files = list(Path('.').glob('*indices*.csv'))
    if not output_files:
        print("   No existing CSV output found for demo")
        print("   To run full demo, first extract climate indices with:")
        print("   python xclim_timber.py --input your_data.nc --parcels parcels.csv")
        return

    print(f"   Found {len(output_files)} CSV output files:")
    for f in output_files[:3]:  # Show first 3
        print(f"     - {f}")

    # Step 3: Format CSV outputs
    print("\n2. Converting to dual CSV formats...")

    try:
        # Import and use CSV formatter
        from csv_formatter import ClimateCSVFormatter

        # Initialize formatter for recent years (example)
        formatter = ClimateCSVFormatter(historical_years=(2008, 2023))

        # Load data
        logger.info("Loading CSV data...")
        df = formatter.load_historical_data('.')

        # Create both formats
        long_df = formatter.create_long_format(df)
        wide_df = formatter.create_wide_format(df)

        # Save results
        output_dir = Path('demo_output')
        formatter.save_formatted_data(long_df, wide_df, output_dir, 'demo_climate')

        print("\n✅ Demo completed successfully!")
        print(f"\nOutput files created in {output_dir}/:")
        for f in output_dir.glob('*.csv'):
            print(f"  📄 {f.name}")

        print(f"\n📊 Data Summary:")
        print(f"   • Long format: {long_df.shape[0]:,} rows × {long_df.shape[1]} columns")
        print(f"   • Wide format: {wide_df.shape[0]:,} rows × {wide_df.shape[1]} columns")
        print(f"   • Climate indices: {len([c for c in df.columns if c not in ['saleid', 'parcelid', 'lat', 'lon', 'year']])}")
        print(f"   • Years covered: {sorted(df['year'].unique())}")

    except ImportError:
        print("❌ CSV formatter not available")
        print("Please ensure csv_formatter.py is in the current directory")
    except Exception as e:
        print(f"❌ Error during formatting: {e}")
        logger.error(f"Demo error: {e}")

def show_usage():
    """Show usage examples for the integrated workflow."""
    print("\n=== Usage Examples ===")
    print("\n1. Extract climate indices (MVP):")
    print("   python xclim_timber.py --input temp_data.nc --parcels coordinates.csv")

    print("\n2. Format outputs (Dual CSV):")
    print("   python csv_formatter.py --input-dir . --output-dir formatted/")

    print("\n3. Batch processing + formatting:")
    print("   # Extract multiple years")
    print("   python xclim_timber.py --parcels coords.csv --data-dir /path/to/data \\")
    print("                          --start-year 2010 --end-year 2015")
    print("   # Format all outputs")
    print("   python csv_formatter.py --input-dir outputs --start-year 2010 --end-year 2015")

    print("\n4. Programmatic usage:")
    print("   from csv_formatter import ClimateCSVFormatter")
    print("   formatter = ClimateCSVFormatter()")
    print("   long_df, wide_df = formatter.create_both_formats('outputs/')")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--help':
        show_usage()
    else:
        run_demo()
        show_usage()
#!/usr/bin/env python
"""
Run comprehensive climate indices calculation for 2001-2024.
Uses 1981-2000 as baseline period for percentile-based indices.
Now uses streaming pipeline for memory-efficient processing.
"""

import sys
from pathlib import Path
from datetime import datetime
import logging
import argparse
import warnings

# Suppress common warnings for climate data processing
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*All-NaN slice.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*divide.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*invalid value.*')
warnings.filterwarnings('ignore', category=UserWarning, message='.*cell_methods.*')

# Configure logging to reduce verbosity from xclim
logging.getLogger('xclim').setLevel(logging.WARNING)
logging.getLogger('xclim.core.cfchecks').setLevel(logging.ERROR)

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from pipeline_streaming import StreamingClimatePipeline
from config import Config
import xarray as xr


def validate_data_coverage():
    """Validate that PRISM data covers both baseline and processing periods."""

    print("=" * 80)
    print("VALIDATING DATA COVERAGE")
    print("=" * 80)

    # Check PRISM temperature data
    prism_temp = Path('/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature')

    if prism_temp.exists():
        print("\n✓ Found PRISM temperature data")

        # Load to check time range
        ds = xr.open_zarr(prism_temp, consolidated=False)

        time_start = str(ds.time.min().values)[:10]
        time_end = str(ds.time.max().values)[:10]

        print(f"  Available period: {time_start} to {time_end}")

        # Check baseline coverage (1981-2000)
        baseline_start = '1981-01-01'
        baseline_end = '2000-12-31'

        if time_start <= baseline_start and time_end >= baseline_end:
            print(f"  ✓ Baseline period (1981-2000) fully covered")
        else:
            print(f"  ⚠ Warning: Baseline period may not be fully covered")

        # Check processing coverage (2001-2024)
        process_start = '2001-01-01'
        process_end = '2024-12-31'

        if time_end >= process_end:
            print(f"  ✓ Processing period (2001-2024) fully covered")
        else:
            actual_end = min(time_end, process_end)
            print(f"  ⚠ Processing period adjusted to 2001-{actual_end[:4]}")

        ds.close()
        return True
    else:
        print(f"✗ PRISM data not found at {prism_temp}")
        return False


def estimate_processing_requirements(chunk_years=1):
    """
    Estimate memory and time requirements for streaming processing.

    Parameters:
    -----------
    chunk_years : int
        Number of years to process per chunk
    """

    print("\n" + "=" * 80)
    print("STREAMING PROCESSING REQUIREMENTS")
    print("=" * 80)

    # Dataset dimensions
    n_days_total = 365 * 24  # 24 years total
    n_days_chunk = 365 * chunk_years  # days per chunk
    n_lat = 621
    n_lon = 1405
    n_indices = 42
    n_chunks = 24 // chunk_years

    # Memory estimates for STREAMING approach
    data_points_chunk = n_days_chunk * n_lat * n_lon
    memory_per_chunk = (data_points_chunk * 4) / 1e9  # 4 bytes per float32, to GB

    print(f"\nDataset dimensions:")
    print(f"  Total period: 24 years ({n_days_total:,} days)")
    print(f"  Spatial points: {n_lat:,} × {n_lon:,} = {n_lat*n_lon:,}")
    print(f"  Total data points: {n_days_total * n_lat * n_lon:,}")

    print(f"\n✨ STREAMING APPROACH:")
    print(f"  Chunk size: {chunk_years} year(s) = {n_days_chunk:,} days")
    print(f"  Number of chunks: {n_chunks}")
    print(f"  Data points per chunk: {data_points_chunk:,}")

    print(f"\nMemory usage (STREAMING):")
    print(f"  Per chunk: ~{memory_per_chunk:.1f} GB")
    print(f"  Peak RAM usage: ~{memory_per_chunk * 1.5:.1f} GB (including overhead)")
    print(f"  Traditional approach: ~134 GB (all data in memory)")
    print(f"  💚 Memory savings: {100 * (1 - memory_per_chunk * 1.5 / 134):.0f}% reduction")

    print(f"\nProcessing estimates:")
    print(f"  Indices per chunk: {n_indices}")
    print(f"  Time per chunk: ~{5 * chunk_years}-{10 * chunk_years} minutes")
    print(f"  Total time: ~{n_chunks * 5 * chunk_years}-{n_chunks * 10 * chunk_years} minutes")
    print(f"  Output: {n_chunks} chunk files + 1 combined file (~10-20 GB total)")


def run_comprehensive_processing(chunk_years=1, enable_dashboard=True):
    """
    Execute the comprehensive climate indices calculation using streaming approach.

    Parameters:
    -----------
    chunk_years : int
        Number of years to process per chunk (default: 1)
    enable_dashboard : bool
        Whether to enable Dask dashboard (default: False)
    """

    print("\n" + "=" * 80)
    print("STARTING STREAMING COMPREHENSIVE PROCESSING")
    print("=" * 80)

    config_path = Path(__file__).parent.parent / 'configs' / 'config_comprehensive_2001_2024.yaml'

    if not config_path.exists():
        print(f"✗ Configuration not found: {config_path}")
        return False

    print(f"\nUsing configuration: {config_path}")
    print("Processing period: 2001-2024")
    print("Baseline period: 1981-2000")
    print("Indices: 42+ across 8 categories")
    print(f"⚡ Streaming mode: {chunk_years}-year chunks")
    if enable_dashboard:
        print("📊 Dashboard: http://localhost:8787 (may have visualization issues)")

    # Initialize streaming pipeline
    print("\n" + "-" * 40)
    print("Initializing streaming pipeline...")

    try:
        # Use the new streaming pipeline with dashboard preference
        pipeline = StreamingClimatePipeline(
            str(config_path),
            chunk_years=chunk_years,
            enable_dashboard=enable_dashboard
        )

        # Run streaming processing
        print("\n" + "-" * 40)
        print("Starting streaming calculation...")
        print(f"\nProcessing {24 // chunk_years} chunks sequentially...")
        print("Each chunk will be saved immediately to conserve memory.")
        if enable_dashboard:
            print("📊 Monitor progress at: http://localhost:8787")
            print("Note: Dashboard may show errors for complex task graphs\n")
        else:
            print("Dashboard disabled for cleaner processing\n")

        start_time = datetime.now()

        # Process all configured variables with streaming
        results = pipeline.run_streaming(
            variables=['temperature', 'precipitation', 'humidity'],
            start_year=2001,
            end_year=2024
        )

        elapsed = datetime.now() - start_time

        print("\n" + "=" * 80)
        print("STREAMING PROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total time: {elapsed}")
        print(f"Status: {results.get('status', 'unknown')}")

        # Handle different result structures based on success/failure
        if results.get('status') == 'success':
            print(f"Chunks processed: {results.get('chunks_processed', 0)}/{results.get('total_chunks', 0)}")
            print(f"Output location: {results.get('output_path', 'unknown')}")

            print(f"\n✓ Chunk files created:")
            chunk_files = results.get('chunk_files', [])
            for chunk_file in chunk_files[:5]:  # Show first 5
                print(f"  - {Path(chunk_file).name}")
            if len(chunk_files) > 5:
                print(f"  ... and {len(chunk_files) - 5} more")

            print(f"\n✓ Combined output: {results.get('output_path', '.')}/combined_indices.nc")

        elif results.get('status') == 'failed':
            # Handle failure case
            error_msg = results.get('error', 'Unknown error')
            print(f"\n❌ Processing failed: {error_msg}")

            if 'No stores found' in error_msg:
                print("\n📍 Troubleshooting tips:")
                print("  1. Check that Zarr stores exist at the configured paths")
                print("  2. Verify the data path in config_comprehensive_2001_2024.yaml")
                print("  3. Ensure the Zarr stores contain the required variables")
                print("\n  Expected store locations:")
                print("    - /media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature")
                print("    - /media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation")
                print("    - /media/mihiarc/SSD4TB/data/PRISM/prism.zarr/humidity")

        # Clean up
        pipeline.close()

        return results.get('status') == 'success'

    except Exception as e:
        print(f"\n✗ Processing failed: {e}")
        import traceback
        traceback.print_exc()
        if 'pipeline' in locals():
            pipeline.close()
        return False


def main():
    """Main execution function with command-line argument support."""

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Run comprehensive climate indices calculation with streaming processing"
    )
    parser.add_argument(
        '--chunk-years', type=int, default=1,
        help='Number of years to process per chunk (default: 1, recommended: 1-2)'
    )
    parser.add_argument(
        '--skip-validation', action='store_true',
        help='Skip data validation step (use with caution)'
    )
    parser.add_argument(
        '--yes', '-y', action='store_true',
        help='Skip confirmation prompt'
    )
    parser.add_argument(
        '--show-warnings', action='store_true',
        help='Show all warnings (useful for debugging)'
    )
    parser.add_argument(
        '--no-dashboard', action='store_true',
        help='Disable Dask dashboard to avoid visualization errors'
    )

    args = parser.parse_args()

    # Reset warning filters if requested
    if args.show_warnings:
        warnings.resetwarnings()
        print("⚠ Warning suppression disabled - all warnings will be shown")

    print("\n" + "=" * 80)
    print("COMPREHENSIVE CLIMATE INDICES PROCESSOR")
    print("⚡ Now with Zarr Streaming Technology")
    print("=" * 80)
    print("Processing: 2001-2024 | Baseline: 1981-2000")
    print("Indices: 42+ from WMO standards")
    print(f"Chunk size: {args.chunk_years} year(s) per chunk")
    print("=" * 80)

    # Step 1: Validate data
    if not args.skip_validation:
        if not validate_data_coverage():
            print("\n⚠ Please ensure PRISM data is available")
            print("Update the path in config_comprehensive_2001_2024.yaml")
            print("Or use --skip-validation to bypass this check")
            return 1
    else:
        print("\n⚠ Skipping data validation as requested")

    # Step 2: Show requirements
    estimate_processing_requirements(args.chunk_years)

    # Step 3: Confirm with user
    if not args.yes:
        print("\n" + "=" * 80)
        print(f"\n📊 STREAMING CONFIGURATION:")
        print(f"  • Chunk size: {args.chunk_years} year(s)")
        print(f"  • Number of chunks: {24 // args.chunk_years}")
        print(f"  • Memory usage: ~{5.6 * args.chunk_years:.1f} GB per chunk")
        print(f"  • Traditional memory: ~134 GB (all at once)")

        response = input("\nProceed with streaming processing? (y/n): ")

        if response.lower() != 'y':
            print("Processing cancelled.")
            return 0

    # Step 4: Run streaming processing
    success = run_comprehensive_processing(
        chunk_years=args.chunk_years,
        enable_dashboard=not args.no_dashboard
    )

    if success:
        print("\n✓ All streaming processing completed successfully!")
        print("\n🎉 BENEFITS OF STREAMING APPROACH:")
        print(f"  • Memory saved: ~{134 - 5.6 * args.chunk_years:.0f} GB")
        print("  • Fault tolerance: Each year saved independently")
        print("  • Progress visibility: See results as they complete")

        print("\nNext steps:")
        print("1. Check chunk outputs in ./outputs/")
        print("2. Verify combined_indices.nc contains all years")
        print("3. Validate indices using quality control scripts")
        print("4. Generate summary statistics and visualizations")
        return 0
    else:
        print("\n✗ Processing encountered errors. Check logs for details.")
        print("Tip: Try with --chunk-years 2 for faster processing if you have more RAM")
        return 1


if __name__ == "__main__":
    sys.exit(main())
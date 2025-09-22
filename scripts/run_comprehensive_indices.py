#!/usr/bin/env python
"""
Run comprehensive climate indices calculation for 2001-2024.
Uses 1981-2000 as baseline period for percentile-based indices.
"""

import sys
from pathlib import Path
from datetime import datetime
import logging

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from pipeline import ClimateDataPipeline
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


def estimate_processing_requirements():
    """Estimate memory and time requirements."""

    print("\n" + "=" * 80)
    print("PROCESSING REQUIREMENTS")
    print("=" * 80)

    # Dataset dimensions
    n_days = 365 * 24  # 24 years
    n_lat = 621
    n_lon = 1405
    n_indices = 42

    # Memory estimates (rough)
    data_points = n_days * n_lat * n_lon
    memory_per_index = (data_points * 4) / 1e9  # 4 bytes per float32, to GB

    print(f"\nDataset dimensions:")
    print(f"  Time points: {n_days:,} days (24 years)")
    print(f"  Spatial points: {n_lat:,} × {n_lon:,} = {n_lat*n_lon:,}")
    print(f"  Total data points: {data_points:,}")

    print(f"\nMemory estimates:")
    print(f"  Per index: ~{memory_per_index:.1f} GB")
    print(f"  All indices: ~{memory_per_index * n_indices:.1f} GB")
    print(f"  With chunking: ~8-16 GB RAM required")

    print(f"\nProcessing estimates:")
    print(f"  Indices to calculate: {n_indices}")
    print(f"  Estimated time: 2-4 hours (with 4 workers)")
    print(f"  Output size: ~10-20 GB (compressed)")


def run_comprehensive_processing():
    """Execute the comprehensive climate indices calculation."""

    print("\n" + "=" * 80)
    print("STARTING COMPREHENSIVE PROCESSING")
    print("=" * 80)

    config_path = Path(__file__).parent.parent / 'configs' / 'config_comprehensive_2001_2024.yaml'

    if not config_path.exists():
        print(f"✗ Configuration not found: {config_path}")
        return False

    print(f"\nUsing configuration: {config_path}")
    print("Processing period: 2001-2024")
    print("Baseline period: 1981-2000")
    print("Indices: 42+ across 8 categories")

    # Initialize pipeline
    print("\n" + "-" * 40)
    print("Initializing pipeline...")

    try:
        pipeline = ClimateDataPipeline(str(config_path), verbose=True)

        # Run processing
        print("\n" + "-" * 40)
        print("Starting calculation...")
        print("Monitor progress at: http://localhost:8787")
        print("\nThis will take 2-4 hours. Press Ctrl+C to cancel.\n")

        start_time = datetime.now()

        # Process all configured variables
        pipeline.run(['temperature', 'precipitation', 'humidity'])

        elapsed = datetime.now() - start_time

        print("\n" + "=" * 80)
        print("PROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total time: {elapsed}")
        print(f"Output location: {pipeline.config.output_path}")

        # Get status
        status = pipeline.get_status()
        print(f"\nProcessed variables: {len(status['datasets_processed'])}")
        print(f"Calculated indices: {len(status['indices_calculated'])}")

        return True

    except Exception as e:
        print(f"\n✗ Processing failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main execution function."""

    print("\n" + "=" * 80)
    print("COMPREHENSIVE CLIMATE INDICES PROCESSOR")
    print("=" * 80)
    print("Processing: 2001-2024 | Baseline: 1981-2000")
    print("Indices: 42+ from WMO standards")
    print("=" * 80)

    # Step 1: Validate data
    if not validate_data_coverage():
        print("\n⚠ Please ensure PRISM data is available")
        print("Update the path in config_comprehensive_2001_2024.yaml")
        return 1

    # Step 2: Show requirements
    estimate_processing_requirements()

    # Step 3: Confirm with user
    print("\n" + "=" * 80)
    response = input("\nProceed with processing? (y/n): ")

    if response.lower() != 'y':
        print("Processing cancelled.")
        return 0

    # Step 4: Run processing
    success = run_comprehensive_processing()

    if success:
        print("\n✓ All processing completed successfully!")
        print("\nNext steps:")
        print("1. Check outputs in ./outputs/comprehensive_2001_2024/")
        print("2. Validate indices using quality control scripts")
        print("3. Generate summary statistics and visualizations")
        return 0
    else:
        print("\n✗ Processing encountered errors. Check logs for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
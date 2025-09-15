#!/usr/bin/env python
"""
Benchmark script to compare performance of efficient_extraction.py vs fast_point_extraction.py
Tests both scripts with varying numbers of parcels to determine optimal approach.
"""

import time
import sys
import os
import tempfile
import tracemalloc
from pathlib import Path
import pandas as pd
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from datetime import datetime
import json

# Import extraction method
sys.path.insert(0, str(Path(__file__).parent))
from xclim_timber import process_year as xclim_process

# For benchmark comparison, we'll test the same function with different settings
# Since we removed fast_point_extraction.py, this benchmark is now historical reference only


def create_test_parcels(n_parcels: int, output_file: str) -> str:
    """Create a test parcels CSV with random coordinates."""
    np.random.seed(42)  # For reproducibility

    # Generate random coordinates within typical ranges
    # Using ranges that match typical climate data grids
    lats = np.random.uniform(-60, 70, n_parcels)
    lons = np.random.uniform(-180, 180, n_parcels)

    df = pd.DataFrame({
        'saleid': range(1, n_parcels + 1),
        'parcelid': [f'P{i:05d}' for i in range(1, n_parcels + 1)],
        'parcel_level_latitude': lats,
        'parcel_level_longitude': lons
    })

    df.to_csv(output_file, index=False)
    return output_file


def measure_performance(func, *args, **kwargs):
    """Measure execution time and memory usage of a function."""
    # Start memory tracking
    tracemalloc.start()
    start_memory = tracemalloc.get_traced_memory()[0]

    # Measure execution time
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    end_time = time.perf_counter()

    # Get peak memory usage
    peak_memory = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()

    execution_time = end_time - start_time
    memory_used = (peak_memory - start_memory) / (1024 * 1024)  # Convert to MB

    return execution_time, memory_used, result


def benchmark_extraction_methods(nc_file: str, parcel_counts: list = None):
    """
    Benchmark both extraction methods with different parcel counts.

    Parameters:
    -----------
    nc_file : str
        Path to NetCDF file for testing
    parcel_counts : list
        List of parcel counts to test
    """
    if parcel_counts is None:
        parcel_counts = [100, 500, 1000, 2000, 5000]

    results = {
        'efficient': {'times': [], 'memory': [], 'counts': []},
        'fast': {'times': [], 'memory': [], 'counts': []}
    }

    print("\n" + "="*60)
    print("CLIMATE DATA EXTRACTION BENCHMARK")
    print("="*60)
    print(f"Testing with NetCDF file: {Path(nc_file).name}")
    print(f"Parcel counts to test: {parcel_counts}")
    print("-"*60)

    # Create temporary directory for outputs
    with tempfile.TemporaryDirectory() as tmpdir:
        for n_parcels in parcel_counts:
            print(f"\n📊 Testing with {n_parcels} parcels...")

            # Create test parcels
            parcels_csv = os.path.join(tmpdir, f'parcels_{n_parcels}.csv')
            create_test_parcels(n_parcels, parcels_csv)

            # Test efficient_extraction.py
            output_efficient = os.path.join(tmpdir, f'output_efficient_{n_parcels}.csv')
            print(f"  Testing efficient_extraction.py...", end="")
            try:
                time_eff, mem_eff, _ = measure_performance(
                    efficient_process,
                    nc_file, parcels_csv, output_efficient, 2023
                )
                results['efficient']['times'].append(time_eff)
                results['efficient']['memory'].append(mem_eff)
                results['efficient']['counts'].append(n_parcels)
                print(f" ✓ {time_eff:.2f}s, {mem_eff:.1f}MB")
            except Exception as e:
                print(f" ✗ Error: {e}")
                results['efficient']['times'].append(None)
                results['efficient']['memory'].append(None)
                results['efficient']['counts'].append(n_parcels)

            # Test fast_point_extraction.py
            output_fast = os.path.join(tmpdir, f'output_fast_{n_parcels}.csv')
            print(f"  Testing fast_point_extraction.py...", end="")
            try:
                time_fast, mem_fast, _ = measure_performance(
                    fast_process,
                    nc_file, parcels_csv, output_fast, 2023
                )
                results['fast']['times'].append(time_fast)
                results['fast']['memory'].append(mem_fast)
                results['fast']['counts'].append(n_parcels)
                print(f" ✓ {time_fast:.2f}s, {mem_fast:.1f}MB")
            except Exception as e:
                print(f" ✗ Error: {e}")
                results['fast']['times'].append(None)
                results['fast']['memory'].append(None)
                results['fast']['counts'].append(n_parcels)

            # Compare results
            if results['efficient']['times'][-1] and results['fast']['times'][-1]:
                speedup = results['fast']['times'][-1] / results['efficient']['times'][-1]
                if speedup > 1:
                    print(f"  → efficient_extraction is {speedup:.2f}x faster")
                else:
                    print(f"  → fast_point_extraction is {1/speedup:.2f}x faster")

    return results


def plot_benchmark_results(results: dict, output_file: str = 'benchmark_results.png'):
    """Create visualization of benchmark results."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Plot execution times
    ax1.plot(results['efficient']['counts'], results['efficient']['times'],
             'b-o', label='efficient_extraction.py', linewidth=2)
    ax1.plot(results['fast']['counts'], results['fast']['times'],
             'r-s', label='fast_point_extraction.py', linewidth=2)
    ax1.set_xlabel('Number of Parcels')
    ax1.set_ylabel('Execution Time (seconds)')
    ax1.set_title('Execution Time Comparison')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # Plot memory usage
    ax2.plot(results['efficient']['counts'], results['efficient']['memory'],
             'b-o', label='efficient_extraction.py', linewidth=2)
    ax2.plot(results['fast']['counts'], results['fast']['memory'],
             'r-s', label='fast_point_extraction.py', linewidth=2)
    ax2.set_xlabel('Number of Parcels')
    ax2.set_ylabel('Memory Usage (MB)')
    ax2.set_title('Memory Usage Comparison')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.suptitle('Climate Data Extraction Performance Benchmark', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_file, dpi=100, bbox_inches='tight')
    print(f"\n📈 Benchmark plot saved to: {output_file}")

    return fig


def generate_report(results: dict):
    """Generate a detailed performance report."""
    print("\n" + "="*60)
    print("BENCHMARK RESULTS SUMMARY")
    print("="*60)

    # Calculate statistics
    eff_times = [t for t in results['efficient']['times'] if t is not None]
    fast_times = [t for t in results['fast']['times'] if t is not None]
    eff_memory = [m for m in results['efficient']['memory'] if m is not None]
    fast_memory = [m for m in results['fast']['memory'] if m is not None]

    if eff_times and fast_times:
        print("\n⏱️  EXECUTION TIME:")
        print(f"  efficient_extraction.py:")
        print(f"    • Average: {np.mean(eff_times):.2f}s")
        print(f"    • Range: {min(eff_times):.2f}s - {max(eff_times):.2f}s")
        print(f"  fast_point_extraction.py:")
        print(f"    • Average: {np.mean(fast_times):.2f}s")
        print(f"    • Range: {min(fast_times):.2f}s - {max(fast_times):.2f}s")

        avg_speedup = np.mean(fast_times) / np.mean(eff_times)
        if avg_speedup > 1:
            print(f"\n  🏆 Winner: efficient_extraction.py ({avg_speedup:.2f}x faster on average)")
        else:
            print(f"\n  🏆 Winner: fast_point_extraction.py ({1/avg_speedup:.2f}x faster on average)")

    if eff_memory and fast_memory:
        print("\n💾 MEMORY USAGE:")
        print(f"  efficient_extraction.py:")
        print(f"    • Average: {np.mean(eff_memory):.1f} MB")
        print(f"    • Peak: {max(eff_memory):.1f} MB")
        print(f"  fast_point_extraction.py:")
        print(f"    • Average: {np.mean(fast_memory):.1f} MB")
        print(f"    • Peak: {max(fast_memory):.1f} MB")

        mem_ratio = np.mean(eff_memory) / np.mean(fast_memory)
        if mem_ratio < 1:
            print(f"\n  🏆 Winner: efficient_extraction.py ({1/mem_ratio:.2f}x less memory)")
        else:
            print(f"\n  🏆 Winner: fast_point_extraction.py ({mem_ratio:.2f}x less memory)")

    # Scalability analysis
    if len(results['efficient']['counts']) > 2:
        print("\n📈 SCALABILITY ANALYSIS:")

        # Calculate scaling factor (time increase per 1000 parcels)
        counts = np.array(results['efficient']['counts'])

        if eff_times:
            eff_scaling = np.polyfit(counts, eff_times, 1)[0] * 1000
            print(f"  efficient_extraction.py: +{eff_scaling:.2f}s per 1000 parcels")

        if fast_times:
            fast_scaling = np.polyfit(counts, fast_times, 1)[0] * 1000
            print(f"  fast_point_extraction.py: +{fast_scaling:.2f}s per 1000 parcels")

        if eff_times and fast_times:
            if eff_scaling < fast_scaling:
                print(f"\n  🏆 Better scalability: efficient_extraction.py")
            else:
                print(f"\n  🏆 Better scalability: fast_point_extraction.py")

    # Save results to JSON
    with open('benchmark_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print("\n📄 Detailed results saved to: benchmark_results.json")


def main():
    """Main benchmark function."""
    import argparse

    parser = argparse.ArgumentParser(description='Benchmark climate data extraction methods')
    parser.add_argument('--input', '-i', required=True,
                       help='NetCDF file to use for testing')
    parser.add_argument('--parcels', nargs='+', type=int,
                       default=[100, 500, 1000, 2000, 5000],
                       help='Number of parcels to test (default: 100 500 1000 2000 5000)')
    parser.add_argument('--plot', action='store_true',
                       help='Generate performance plots')

    args = parser.parse_args()

    # Check if input file exists
    if not Path(args.input).exists():
        print(f"Error: NetCDF file not found: {args.input}")
        sys.exit(1)

    # Run benchmarks
    results = benchmark_extraction_methods(args.input, args.parcels)

    # Generate report
    generate_report(results)

    # Create plots if requested
    if args.plot:
        plot_benchmark_results(results)

    print("\n✅ Benchmark complete!\n")


if __name__ == "__main__":
    main()
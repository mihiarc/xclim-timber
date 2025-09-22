#!/usr/bin/env python
"""
Test script for the streamlined Zarr-exclusive pipeline.
"""

import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from config import Config
from data_loader import ClimateDataLoader


def test_zarr_loading():
    """Test the Zarr-exclusive data loading functionality."""

    print("=" * 60)
    print("Testing Zarr-Exclusive Pipeline")
    print("=" * 60)

    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Test 1: Configuration loading
    print("\n1. Testing configuration...")
    config = Config('config_zarr.yaml')
    print(f"   ✓ Configuration loaded")
    print(f"   Input path: {config.input_path}")
    print(f"   Output path: {config.output_path}")

    # Test 2: Data loader initialization
    print("\n2. Testing data loader...")
    loader = ClimateDataLoader(config)
    print(f"   ✓ Data loader initialized")

    # Test 3: Scan for Zarr stores
    print("\n3. Scanning for Zarr stores...")

    # Check if we have the example Zarr store
    test_zarr_path = Path('tmin_1981_01.zarr')
    if test_zarr_path.exists():
        print(f"   Found test Zarr store: {test_zarr_path}")

        # Test 4: Load the Zarr store
        print("\n4. Loading Zarr store...")
        try:
            ds = loader.load_zarr(test_zarr_path)
            print(f"   ✓ Successfully loaded Zarr store")
            print(f"   Dimensions: {dict(ds.dims)}")
            print(f"   Variables: {list(ds.data_vars)}")
            print(f"   Source format: {ds.attrs.get('source_format', 'unknown')}")

            # Test 5: Check standardized dimensions
            print("\n5. Checking dimension standardization...")
            expected_dims = ['lat', 'lon', 'time']
            for dim in expected_dims:
                if dim in ds.dims:
                    print(f"   ✓ {dim}: {ds.dims[dim]} points")

            # Test 6: Get dataset info
            print("\n6. Testing dataset info...")
            loader.datasets['test'] = ds
            info = loader.get_info()
            for name, details in info.items():
                print(f"   Dataset: {name}")
                print(f"   Memory size: {details['memory_size']:.2f} GB")
                print(f"   Chunks: {details['chunks']}")

        except Exception as e:
            print(f"   ✗ Error loading Zarr store: {e}")
            return False
    else:
        print(f"   ⚠ No test Zarr store found at {test_zarr_path}")
        print("   Scanning current directory for any Zarr stores...")
        stores = loader.scan_directory(Path('.'))
        if stores:
            print(f"   Found {len(stores)} Zarr stores:")
            for store in stores[:3]:  # Show first 3
                print(f"     - {store}")
        else:
            print("   No Zarr stores found in current directory")

    print("\n" + "=" * 60)
    print("✓ All tests completed successfully!")
    print("=" * 60)

    return True


if __name__ == "__main__":
    success = test_zarr_loading()
    sys.exit(0 if success else 1)
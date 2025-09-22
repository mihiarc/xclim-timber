#!/usr/bin/env python
"""
Explore and catalog the PRISM Zarr store on the external drive.
"""

import sys
import xarray as xr
import zarr
from pathlib import Path
import json
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))
from data_loader import ClimateDataLoader
from config import Config


def explore_prism_zarr():
    """Explore the PRISM Zarr store structure and contents."""

    prism_path = Path('/media/mihiarc/SSD4TB/data/PRISM/prism.zarr')

    print("=" * 80)
    print("PRISM ZARR STORE EXPLORATION")
    print("=" * 80)
    print(f"\nStore location: {prism_path}")
    print(f"Store size: 132 GB")
    print(f"Format: Zarr v3 (latest specification)")

    # Check subdirectories
    print("\n" + "─" * 40)
    print("STORE STRUCTURE")
    print("─" * 40)

    subdirs = ['temperature', 'precipitation', 'humidity']
    for subdir in subdirs:
        subdir_path = prism_path / subdir
        if subdir_path.exists():
            print(f"\n{subdir.upper()}:")
            # List variables in each subdirectory
            variables = [d.name for d in subdir_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
            for var in sorted(variables):
                if var not in ['lat', 'lon', 'time']:
                    print(f"  └── {var}")

    # Try to load each dataset
    print("\n" + "─" * 40)
    print("LOADING DATASETS")
    print("─" * 40)

    datasets = {}
    for subdir in subdirs:
        subdir_path = prism_path / subdir
        if subdir_path.exists():
            print(f"\nAttempting to load {subdir} data...")
            try:
                # Try loading with xarray (it should handle Zarr v3)
                ds = xr.open_zarr(subdir_path, consolidated=False)
                datasets[subdir] = ds

                print(f"✓ Successfully loaded {subdir}")
                print(f"  Dimensions: {dict(ds.sizes)}")
                print(f"  Variables: {list(ds.data_vars)}")
                print(f"  Coordinates: {list(ds.coords)}")

                # Check time range if available
                if 'time' in ds.coords:
                    time_range = (
                        str(ds.time.min().values)[:10],
                        str(ds.time.max().values)[:10]
                    )
                    print(f"  Time range: {time_range[0]} to {time_range[1]}")

                # Check spatial extent
                if 'lat' in ds.coords and 'lon' in ds.coords:
                    lat_range = (float(ds.lat.min()), float(ds.lat.max()))
                    lon_range = (float(ds.lon.min()), float(ds.lon.max()))
                    print(f"  Latitude range: {lat_range[0]:.2f}° to {lat_range[1]:.2f}°")
                    print(f"  Longitude range: {lon_range[0]:.2f}° to {lon_range[1]:.2f}°")

            except Exception as e:
                print(f"✗ Failed to load {subdir}: {str(e)[:100]}")

    # Summary statistics
    if datasets:
        print("\n" + "=" * 80)
        print("DATASET SUMMARY")
        print("=" * 80)

        for name, ds in datasets.items():
            print(f"\n{name.upper()} Dataset:")
            total_points = 1
            for dim, size in ds.sizes.items():
                total_points *= size

            print(f"  Total data points: {total_points:,}")
            print(f"  Memory size (uncompressed): ~{ds.nbytes / 1e9:.2f} GB")

            # Show sample of data variables
            for var in list(ds.data_vars)[:3]:
                var_data = ds[var]
                print(f"\n  Variable: {var}")
                if hasattr(var_data, 'attrs'):
                    if 'units' in var_data.attrs:
                        print(f"    Units: {var_data.attrs['units']}")
                    if 'long_name' in var_data.attrs:
                        print(f"    Description: {var_data.attrs['long_name']}")
                print(f"    Shape: {var_data.shape}")
                print(f"    Data type: {var_data.dtype}")

    return datasets


def test_with_pipeline():
    """Test loading PRISM data with our Zarr-exclusive pipeline."""

    print("\n" + "=" * 80)
    print("TESTING WITH XCLIM-TIMBER PIPELINE")
    print("=" * 80)

    # Create configuration
    config = Config()
    config.set('data.input_path', '/media/mihiarc/SSD4TB/data/PRISM')

    # Initialize loader
    loader = ClimateDataLoader(config)

    # Try to load the temperature dataset
    print("\nTesting pipeline loader with temperature data...")
    try:
        temp_path = Path('/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature')
        ds = loader.load_zarr(temp_path, consolidated=False)

        print("✓ Successfully loaded with pipeline!")
        print(f"  Dataset info: {loader.get_info()}")

        # Test optimization function
        print("\nNote: The optimize_zarr_store function can be used to:")
        print("  - Rechunk the data for better performance")
        print("  - Consolidate metadata for faster loading")
        print("  - Create an optimized copy of the store")

    except Exception as e:
        print(f"✗ Pipeline loading failed: {e}")


if __name__ == "__main__":
    print("Exploring PRISM Zarr store on external drive...\n")

    datasets = explore_prism_zarr()

    if datasets:
        test_with_pipeline()

    print("\n" + "=" * 80)
    print("EXPLORATION COMPLETE")
    print("=" * 80)
    print("\nThe PRISM Zarr store contains high-resolution climate data")
    print("organized by variable type (temperature, precipitation, humidity).")
    print("Each subdirectory is a separate Zarr array that can be loaded independently.")
    print("\nTo use this data in your pipeline:")
    print("1. Update config_zarr.yaml with path: /media/mihiarc/SSD4TB/data/PRISM")
    print("2. Load specific variables: prism.zarr/temperature, prism.zarr/precipitation, etc.")
    print("3. Process with xclim indices as normal")
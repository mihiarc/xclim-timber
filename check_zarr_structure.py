#!/usr/bin/env python3
"""Check the structure of PRISM Zarr stores."""

import xarray as xr
from pathlib import Path

base_path = Path('/media/mihiarc/SSD4TB/data/PRISM/prism.zarr')

print("=" * 60)
print("PRISM ZARR STORE STRUCTURE ANALYSIS")
print("=" * 60)

for subdir in ['temperature', 'precipitation', 'humidity']:
    zarr_path = base_path / subdir
    if zarr_path.exists():
        print(f"\n{subdir.upper()} Store:")
        print("-" * 40)
        try:
            ds = xr.open_zarr(zarr_path)
            print(f"  Dimensions: {dict(ds.dims)}")
            print(f"  Variables: {list(ds.data_vars)}")
            print(f"  Coordinates: {list(ds.coords)}")

            # Check first variable details
            if ds.data_vars:
                var_name = list(ds.data_vars)[0]
                var = ds[var_name]
                print(f"\n  {var_name} details:")
                print(f"    Shape: {var.shape}")
                print(f"    Units: {var.attrs.get('units', 'not specified')}")
                print(f"    Dtype: {var.dtype}")
                if 'time' in ds.dims:
                    print(f"    Time range: {ds.time.min().values} to {ds.time.max().values}")
        except Exception as e:
            print(f"  Error loading: {e}")

print("\n" + "=" * 60)
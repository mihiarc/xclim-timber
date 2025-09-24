#!/usr/bin/env python
"""
Inspect data quality and visualize missing value patterns.
Helps diagnose coordinate issues and data alignment problems.
"""

import sys
from pathlib import Path
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


def inspect_zarr_store(zarr_path: str, variable_name: str = None):
    """
    Inspect a Zarr store and visualize data quality.
    
    Parameters:
    -----------
    zarr_path : str
        Path to Zarr store
    variable_name : str
        Name of variable to inspect (if store contains multiple)
    """
    print(f"\nInspecting: {zarr_path}")
    print("=" * 80)
    
    # Open the Zarr store
    ds = xr.open_zarr(zarr_path, consolidated=False)
    
    # Get the data variable
    if variable_name and variable_name in ds:
        data = ds[variable_name]
    elif len(ds.data_vars) == 1:
        data = ds[list(ds.data_vars)[0]]
    else:
        print(f"Available variables: {list(ds.data_vars)}")
        data = ds[list(ds.data_vars)[0]]
        print(f"Using first variable: {data.name}")
    
    # Print basic info
    print(f"\nDataset Info:")
    print(f"  Variable: {data.name}")
    print(f"  Shape: {data.shape}")
    print(f"  Dimensions: {data.dims}")
    print(f"  Coordinates:")
    for coord in data.coords:
        coord_data = data.coords[coord]
        if len(coord_data) > 5:
            try:
                print(f"    {coord}: [{float(coord_data.values[0]):.2f} ... {float(coord_data.values[-1]):.2f}] (n={len(coord_data)})")
            except:
                print(f"    {coord}: {coord_data.values[0]} ... {coord_data.values[-1]} (n={len(coord_data)})")
        else:
            print(f"    {coord}: {coord_data.values}")
    
    # Analyze missing values over time
    print(f"\nMissing Value Analysis:")
    
    # Sample different time slices
    time_slices = [
        '2001-01-15',
        '2001-07-15', 
        '2010-01-15',
        '2020-01-15',
        '2024-01-15'
    ]
    
    for time_str in time_slices:
        try:
            time_slice = data.sel(time=time_str, method='nearest')
            total_points = time_slice.size
            missing_points = np.isnan(time_slice.values).sum()
            missing_pct = (missing_points / total_points) * 100
            print(f"  {time_str}: {missing_pct:.1f}% missing ({missing_points:,}/{total_points:,})")
        except:
            print(f"  {time_str}: Could not access")
    
    # Create visualization
    create_missing_data_visualization(data)
    
    return ds


def create_missing_data_visualization(data):
    """
    Create comprehensive visualization of missing data patterns.
    """
    print("\nCreating visualizations...")
    
    fig = plt.figure(figsize=(16, 12))
    
    # Get a sample time slice (first available time)
    if 'time' in data.dims:
        sample_data = data.isel(time=0)
        time_label = str(data.time[0].values)[:10]
    else:
        sample_data = data
        time_label = 'No time dimension'
    
    # 1. Spatial pattern of missing values
    ax1 = plt.subplot(2, 3, 1)
    missing_mask = np.isnan(sample_data.values).astype(float)
    im1 = ax1.imshow(missing_mask, cmap='RdYlBu_r', vmin=0, vmax=1, aspect='auto')
    ax1.set_title(f'Missing Data Pattern (1=missing)\n{time_label}')
    ax1.set_xlabel('Longitude index')
    ax1.set_ylabel('Latitude index')
    plt.colorbar(im1, ax=ax1, label='Missing')
    
    # Add percentage annotation
    missing_pct = np.mean(missing_mask) * 100
    ax1.text(0.02, 0.98, f'Missing: {missing_pct:.1f}%', 
            transform=ax1.transAxes, va='top', 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    # 2. Actual data values (where not missing)
    ax2 = plt.subplot(2, 3, 2)
    vmin, vmax = np.nanpercentile(sample_data.values, [1, 99])
    im2 = ax2.imshow(sample_data.values, cmap='viridis', vmin=vmin, vmax=vmax, aspect='auto')
    ax2.set_title(f'Data Values\n{time_label}')
    ax2.set_xlabel('Longitude index')
    ax2.set_ylabel('Latitude index')
    plt.colorbar(im2, ax=ax2, label=data.attrs.get('units', 'Value'))
    
    # 3. Missing data by latitude
    ax3 = plt.subplot(2, 3, 3)
    missing_by_lat = np.mean(np.isnan(sample_data.values), axis=1) * 100
    lat_indices = np.arange(len(missing_by_lat))
    ax3.plot(missing_by_lat, lat_indices)
    ax3.set_xlabel('Missing %')
    ax3.set_ylabel('Latitude index')
    ax3.set_title('Missing Data by Latitude')
    ax3.grid(True, alpha=0.3)
    ax3.set_xlim(0, 100)
    ax3.invert_yaxis()  # Match image orientation
    
    # 4. Missing data by longitude
    ax4 = plt.subplot(2, 3, 4)
    missing_by_lon = np.mean(np.isnan(sample_data.values), axis=0) * 100
    ax4.plot(missing_by_lon)
    ax4.set_xlabel('Longitude index')
    ax4.set_ylabel('Missing %')
    ax4.set_title('Missing Data by Longitude')
    ax4.grid(True, alpha=0.3)
    ax4.set_ylim(0, 100)
    
    # 5. Histogram of data values
    ax5 = plt.subplot(2, 3, 5)
    valid_data = sample_data.values[~np.isnan(sample_data.values)]
    if len(valid_data) > 0:
        ax5.hist(valid_data, bins=50, edgecolor='black', alpha=0.7)
        ax5.set_xlabel(f'Values ({data.attrs.get("units", "units")})')
        ax5.set_ylabel('Frequency')
        ax5.set_title(f'Value Distribution\nn={len(valid_data):,}')
        
        # Add statistics
        stats_text = f'Mean: {np.mean(valid_data):.2f}\nStd: {np.std(valid_data):.2f}\nMin: {np.min(valid_data):.2f}\nMax: {np.max(valid_data):.2f}'
        ax5.text(0.98, 0.98, stats_text, transform=ax5.transAxes, 
                va='top', ha='right',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    # 6. Coordinate inspection
    ax6 = plt.subplot(2, 3, 6)
    ax6.axis('off')
    
    # Get coordinate info
    info_text = "Coordinate Ranges:\n\n"
    
    if 'lat' in sample_data.coords or 'latitude' in sample_data.coords:
        lat_coord = sample_data.coords.get('lat', sample_data.coords.get('latitude'))
        info_text += f"Latitude:\n"
        info_text += f"  Min: {float(lat_coord.min()):.4f}\n"
        info_text += f"  Max: {float(lat_coord.max()):.4f}\n"
        info_text += f"  Points: {len(lat_coord)}\n\n"
    
    if 'lon' in sample_data.coords or 'longitude' in sample_data.coords:
        lon_coord = sample_data.coords.get('lon', sample_data.coords.get('longitude'))
        info_text += f"Longitude:\n"
        info_text += f"  Min: {float(lon_coord.min()):.4f}\n"
        info_text += f"  Max: {float(lon_coord.max()):.4f}\n"
        info_text += f"  Points: {len(lon_coord)}\n\n"
    
    # Check for common coordinate issues
    issues = []
    if 'lon' in sample_data.coords or 'longitude' in sample_data.coords:
        lon_coord = sample_data.coords.get('lon', sample_data.coords.get('longitude'))
        if float(lon_coord.min()) > 0:
            issues.append("⚠ Longitude might be 0-360 instead of -180-180")
        if float(lon_coord.max()) > 180:
            issues.append("⚠ Longitude is in 0-360 format")
    
    if issues:
        info_text += "Potential Issues:\n"
        for issue in issues:
            info_text += f"  {issue}\n"
    
    ax6.text(0.1, 0.9, info_text, transform=ax6.transAxes, va='top', fontsize=10,
            fontfamily='monospace')
    
    plt.suptitle(f'Data Quality Inspection: {data.name}', fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    # Save figure
    output_dir = Path('outputs/diagnostics')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f'data_quality_{data.name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png'
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"Saved visualization to: {output_file}")
    
    plt.show()
    
    return missing_pct


def diagnose_coordinate_mismatch(zarr_path1: str, zarr_path2: str):
    """
    Compare coordinates between two datasets to diagnose mismatches.
    """
    print("\nComparing coordinates between datasets...")
    print("=" * 80)
    
    ds1 = xr.open_zarr(zarr_path1, consolidated=False)
    ds2 = xr.open_zarr(zarr_path2, consolidated=False)
    
    # Compare dimensions
    print("\nDimensions:")
    print(f"  Dataset 1: {dict(ds1.dims)}")
    print(f"  Dataset 2: {dict(ds2.dims)}")
    
    # Compare coordinates
    for coord in ['lat', 'latitude', 'lon', 'longitude']:
        if coord in ds1.coords and coord in ds2.coords:
            coord1 = ds1.coords[coord]
            coord2 = ds2.coords[coord]
            
            print(f"\n{coord}:")
            print(f"  Dataset 1: [{coord1.min().values:.4f}, {coord1.max().values:.4f}] n={len(coord1)}")
            print(f"  Dataset 2: [{coord2.min().values:.4f}, {coord2.max().values:.4f}] n={len(coord2)}")
            
            if not np.allclose(coord1.values, coord2.values, rtol=1e-5):
                print(f"  ⚠ MISMATCH DETECTED!")
                diff = np.abs(coord1.values[:min(5, len(coord1))] - coord2.values[:min(5, len(coord2))])
                print(f"  First 5 differences: {diff}")


def main():
    """Main function to inspect data quality."""
    
    print("\n" + "=" * 80)
    print("DATA QUALITY INSPECTION TOOL")
    print("=" * 80)
    
    # Inspect temperature data
    temp_path = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'
    if Path(temp_path).exists():
        ds_temp = inspect_zarr_store(temp_path)
    else:
        print(f"Temperature data not found at: {temp_path}")
    
    # Inspect precipitation data
    precip_path = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation'
    if Path(precip_path).exists():
        ds_precip = inspect_zarr_store(precip_path)
        
        # Compare coordinates if both exist
        if Path(temp_path).exists():
            diagnose_coordinate_mismatch(temp_path, precip_path)
    
    print("\n" + "=" * 80)
    print("Inspection complete. Check visualizations in outputs/diagnostics/")
    print("=" * 80)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
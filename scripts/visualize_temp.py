#!/usr/bin/env python
"""
Visualize annual mean temperature map for validation.
"""

import sys
import logging
from pathlib import Path
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import matplotlib.colors as colors
from matplotlib import cm
import warnings
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def load_temperature_data(file_path):
    """Load temperature data from NetCDF file."""
    logger.info(f"Loading data from: {file_path}")
    ds = xr.open_dataset(file_path)
    
    # Find temperature variable
    temp_vars = ['annual_mean_temperature', 'tas', 'temperature', 'tg_mean']
    temp_data = None
    
    for var in temp_vars:
        if var in ds.data_vars:
            temp_data = ds[var]
            logger.info(f"Found temperature variable: {var}")
            break
    
    if temp_data is None:
        # Use first variable
        var_name = list(ds.data_vars)[0]
        temp_data = ds[var_name]
        logger.info(f"Using variable: {var_name}")
    
    return temp_data


def create_temperature_map(temp_data, output_file='temperature_map.png', title=None):
    """
    Create a global temperature map.
    
    Parameters:
    -----------
    temp_data : xr.DataArray
        Temperature data
    output_file : str
        Output file path
    title : str
        Map title
    """
    logger.info("Creating temperature map...")
    
    # If there's a time dimension, select first time step
    if 'time' in temp_data.dims:
        temp_data = temp_data.isel(time=0)
        time_str = str(temp_data.time.values)[:10]
    else:
        time_str = ""
    
    # Create figure
    fig, ax = plt.subplots(1, 1, figsize=(15, 8))
    
    # Prepare data
    lon = temp_data.lon.values
    lat = temp_data.lat.values
    data = temp_data.values.copy()  # Make a copy to avoid read-only issues
    
    # Handle NaN values
    data = np.ma.masked_invalid(data)
    
    # Create meshgrid
    lon_grid, lat_grid = np.meshgrid(lon, lat)
    
    # Determine color scale based on data range
    # Convert to regular array for percentile calculation
    data_flat = data.flatten()
    data_valid = data_flat[~np.isnan(data_flat)]
    vmin = np.percentile(data_valid, 1)  # 1st percentile
    vmax = np.percentile(data_valid, 99)  # 99th percentile
    
    # Create custom colormap for temperature
    if vmin < 0 and vmax > 0:
        # Use diverging colormap for data spanning zero
        cmap = plt.cm.RdBu_r
        # Make zero white
        vcenter = 0
        norm = colors.TwoSlopeNorm(vmin=vmin, vcenter=vcenter, vmax=vmax)
    else:
        # Use sequential colormap
        cmap = plt.cm.YlOrRd
        norm = colors.Normalize(vmin=vmin, vmax=vmax)
    
    # Plot data
    im = ax.pcolormesh(lon_grid, lat_grid, data, 
                       cmap=cmap, norm=norm, 
                       shading='auto', rasterized=True)
    
    # Add coastlines (simple approach without cartopy)
    ax.set_xlim(lon.min(), lon.max())
    ax.set_ylim(lat.min(), lat.max())
    
    # Add grid
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    
    # Labels
    ax.set_xlabel('Longitude (°E)', fontsize=12)
    ax.set_ylabel('Latitude (°N)', fontsize=12)
    
    # Title
    if title is None:
        title = f'Annual Mean Temperature {time_str}'
    ax.set_title(title, fontsize=14, fontweight='bold')
    
    # Add equator and prime meridian lines
    ax.axhline(y=0, color='gray', linestyle='-', linewidth=0.8, alpha=0.5)
    ax.axvline(x=0, color='gray', linestyle='-', linewidth=0.8, alpha=0.5)
    
    # Colorbar
    cbar = plt.colorbar(im, ax=ax, orientation='horizontal', 
                       pad=0.08, aspect=40, shrink=0.8)
    cbar.set_label('Temperature (°C)', fontsize=12)
    cbar.ax.tick_params(labelsize=10)
    
    # Add statistics as text
    mean_val = float(np.nanmean(data))
    std_val = float(np.nanstd(data))
    min_val = float(np.nanmin(data))
    max_val = float(np.nanmax(data))
    
    stats_text = (f'Mean: {mean_val:.1f}°C\n'
                 f'Std: {std_val:.1f}°C\n'
                 f'Min: {min_val:.1f}°C\n'
                 f'Max: {max_val:.1f}°C')
    
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
           fontsize=10, verticalalignment='top',
           bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    # Adjust layout
    plt.tight_layout()
    
    # Save figure
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    logger.info(f"Map saved to: {output_path}")
    
    # Also save as lower resolution for quick viewing
    preview_path = output_path.parent / f"{output_path.stem}_preview.png"
    plt.savefig(preview_path, dpi=72, bbox_inches='tight')
    logger.info(f"Preview saved to: {preview_path}")
    
    plt.close()
    
    return output_path


def create_zonal_mean_plot(temp_data, output_file='zonal_mean.png'):
    """
    Create zonal mean temperature plot.
    
    Parameters:
    -----------
    temp_data : xr.DataArray
        Temperature data
    output_file : str
        Output file path
    """
    logger.info("Creating zonal mean plot...")
    
    # If there's a time dimension, select first time step
    if 'time' in temp_data.dims:
        temp_data = temp_data.isel(time=0)
    
    # Calculate zonal mean (average across longitude)
    zonal_mean = temp_data.mean(dim='lon')
    
    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Plot 1: Zonal mean profile
    ax1.plot(zonal_mean.values, zonal_mean.lat.values, 'b-', linewidth=2)
    ax1.axvline(x=0, color='gray', linestyle='--', alpha=0.5)
    ax1.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
    ax1.grid(True, alpha=0.3)
    ax1.set_xlabel('Temperature (°C)', fontsize=12)
    ax1.set_ylabel('Latitude (°N)', fontsize=12)
    ax1.set_title('Zonal Mean Temperature', fontsize=14, fontweight='bold')
    ax1.set_ylim(-90, 90)
    
    # Add latitude bands
    ax1.axhspan(-90, -66.5, alpha=0.1, color='blue', label='Antarctic')
    ax1.axhspan(-23.5, 23.5, alpha=0.1, color='red', label='Tropical')
    ax1.axhspan(66.5, 90, alpha=0.1, color='blue', label='Arctic')
    
    # Plot 2: Histogram of temperature values
    data_flat = temp_data.values.flatten()
    data_flat = data_flat[~np.isnan(data_flat)]
    
    ax2.hist(data_flat, bins=50, edgecolor='black', alpha=0.7)
    ax2.axvline(x=np.mean(data_flat), color='red', linestyle='--', 
                linewidth=2, label=f'Mean: {np.mean(data_flat):.1f}°C')
    ax2.set_xlabel('Temperature (°C)', fontsize=12)
    ax2.set_ylabel('Frequency', fontsize=12)
    ax2.set_title('Temperature Distribution', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    
    plt.tight_layout()
    
    # Save figure
    output_path = Path(output_file)
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    logger.info(f"Zonal mean plot saved to: {output_path}")
    
    plt.close()
    
    return output_path


def main(input_file, output_dir='outputs/maps'):
    """
    Main function to create visualizations.
    
    Parameters:
    -----------
    input_file : str
        Path to NetCDF file with temperature data
    output_dir : str
        Directory for output maps
    """
    # Load data
    temp_data = load_temperature_data(input_file)
    
    # Extract year from filename or data
    input_path = Path(input_file)
    if 'annual_mean' in input_path.stem:
        year = input_path.stem.split('_')[-1]
        if not year.isdigit():
            year = "data"
    else:
        # Try to extract from filename pattern
        parts = input_path.stem.split('_')
        year = parts[-1] if parts[-1].isdigit() else "data"
    
    # Create output directory
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create global map
    map_file = output_dir / f"temperature_map_{year}.png"
    create_temperature_map(temp_data, map_file, 
                          title=f'Annual Mean Temperature {year}')
    
    # Create zonal mean plot
    zonal_file = output_dir / f"zonal_mean_{year}.png"
    create_zonal_mean_plot(temp_data, zonal_file)
    
    logger.info("=" * 50)
    logger.info("Visualization complete!")
    logger.info(f"Maps saved in: {output_dir}")
    logger.info("=" * 50)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Visualize temperature data')
    parser.add_argument('input', help='Input NetCDF file')
    parser.add_argument('--output-dir', '-o', default='outputs/maps',
                       help='Output directory for maps')
    
    args = parser.parse_args()
    
    try:
        main(args.input, args.output_dir)
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
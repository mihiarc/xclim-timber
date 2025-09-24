#!/usr/bin/env python
"""
Visualization module for climate indices.
Generates plots and maps for analysis.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.gridspec import GridSpec
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

# Optional imports
try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    HAS_CARTOPY = True
except ImportError:
    HAS_CARTOPY = False

try:
    import seaborn as sns
    sns.set_style('whitegrid')
except ImportError:
    pass

logger = logging.getLogger(__name__)

# Set style
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 150
plt.rcParams['axes.grid'] = True
plt.rcParams['grid.alpha'] = 0.3


class ClimateIndicesVisualizer:
    """
    Generate visualizations for climate indices.
    
    Features:
    - Time series plots
    - Spatial maps
    - Distribution histograms
    - Correlation matrices
    - Multi-panel summaries
    """
    
    def __init__(self, dataset_path: Union[str, Path]):
        """
        Initialize visualizer with dataset.
        
        Parameters:
        -----------
        dataset_path : str or Path
            Path to NetCDF file with climate indices
        """
        self.dataset_path = Path(dataset_path)
        self.ds = None
        self.output_dir = Path('outputs/visualizations')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_dataset(self) -> xr.Dataset:
        """Load the climate indices dataset."""
        logger.info(f"Loading dataset: {self.dataset_path}")
        self.ds = xr.open_dataset(self.dataset_path)
        logger.info(f"Loaded {len(self.ds.data_vars)} variables")
        return self.ds
    
    def plot_time_series(self, variable: str, 
                        region: Optional[Dict] = None,
                        save: bool = True) -> plt.Figure:
        """
        Plot time series for a variable.
        
        Parameters:
        -----------
        variable : str
            Variable name
        region : dict, optional
            Region bounds {'lat': [min, max], 'lon': [min, max]}
        save : bool
            Whether to save the figure
            
        Returns:
        --------
        matplotlib.figure.Figure
            The generated figure
        """
        if self.ds is None:
            self.load_dataset()
        
        data = self.ds[variable]
        
        # Apply regional selection if provided
        if region:
            if 'lat' in region:
                data = data.sel(lat=slice(region['lat'][0], region['lat'][1]))
            if 'lon' in region:
                data = data.sel(lon=slice(region['lon'][0], region['lon'][1]))
        
        # Calculate spatial mean for time series
        if 'time' in data.dims:
            ts_data = data.mean(dim=[d for d in data.dims if d != 'time'])
        else:
            logger.warning(f"No time dimension in {variable}")
            return None
        
        # Create figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        
        # Plot 1: Full time series
        ax1.plot(ts_data.time, ts_data.values, 'b-', linewidth=1.5, alpha=0.8)
        ax1.set_title(f'{variable} - Time Series', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Time')
        ax1.set_ylabel(data.attrs.get('units', 'Value'))
        ax1.grid(True, alpha=0.3)
        
        # Add trend line
        if len(ts_data.time) > 1:
            x_numeric = np.arange(len(ts_data.time))
            valid_mask = ~np.isnan(ts_data.values)
            if valid_mask.sum() > 1:
                z = np.polyfit(x_numeric[valid_mask], ts_data.values[valid_mask], 1)
                trend_line = np.polyval(z, x_numeric)
                ax1.plot(ts_data.time, trend_line, 'r--', alpha=0.5, 
                        label=f'Trend: {z[0]:.3f}/time')
                ax1.legend()
        
        # Plot 2: Annual cycle or distribution
        if 'time' in data.dims and len(data.time) > 12:
            # Try to create annual cycle
            try:
                monthly = data.groupby('time.month').mean()
                monthly_mean = monthly.mean(dim=[d for d in monthly.dims if d != 'month'])
                
                ax2.plot(monthly_mean.month, monthly_mean.values, 'go-', linewidth=2)
                ax2.set_title('Annual Cycle (Monthly Means)', fontsize=12)
                ax2.set_xlabel('Month')
                ax2.set_ylabel(data.attrs.get('units', 'Value'))
                ax2.set_xticks(range(1, 13))
                ax2.set_xticklabels(['J','F','M','A','M','J','J','A','S','O','N','D'])
                ax2.grid(True, alpha=0.3)
            except:
                # If monthly grouping fails, show distribution
                ax2.hist(ts_data.values[~np.isnan(ts_data.values)], bins=30, 
                        color='skyblue', edgecolor='black', alpha=0.7)
                ax2.set_title('Value Distribution', fontsize=12)
                ax2.set_xlabel(data.attrs.get('units', 'Value'))
                ax2.set_ylabel('Frequency')
        
        plt.tight_layout()
        
        if save:
            output_file = self.output_dir / f'{variable}_timeseries.png'
            plt.savefig(output_file, bbox_inches='tight')
            logger.info(f"Saved time series plot to {output_file}")
        
        return fig
    
    def plot_spatial_map(self, variable: str,
                        time_slice: Optional[str] = None,
                        projection: Optional[object] = None,
                        save: bool = True) -> plt.Figure:
        """
        Plot spatial map for a variable.
        
        Parameters:
        -----------
        variable : str
            Variable name
        time_slice : str, optional
            Time to plot (e.g., '2020-01-01')
        projection : cartopy.crs.Projection, optional
            Map projection
        save : bool
            Whether to save the figure
            
        Returns:
        --------
        matplotlib.figure.Figure
            The generated figure
        """
        if self.ds is None:
            self.load_dataset()
        
        data = self.ds[variable]
        
        # Select time slice or mean
        if 'time' in data.dims:
            if time_slice:
                data = data.sel(time=time_slice, method='nearest')
            else:
                data = data.mean(dim='time')
        
        # Check for spatial dimensions
        lat_dim = 'lat' if 'lat' in data.dims else 'latitude'
        lon_dim = 'lon' if 'lon' in data.dims else 'longitude'
        
        if lat_dim not in data.dims or lon_dim not in data.dims:
            logger.warning(f"No spatial dimensions found for {variable}")
            return None
        
        # Create figure
        fig = plt.figure(figsize=(12, 8))

        if HAS_CARTOPY and projection is not None:
            # Use cartopy if available
            if projection is None:
                projection = ccrs.PlateCarree()
            ax = fig.add_subplot(111, projection=projection)

            # Add map features
            ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax.add_feature(cfeature.BORDERS, linewidth=0.5, alpha=0.5)
            ax.add_feature(cfeature.OCEAN, color='lightblue', alpha=0.3)
            ax.add_feature(cfeature.LAND, color='beige', alpha=0.3)
        else:
            # Simple matplotlib plot without cartopy
            ax = fig.add_subplot(111)
        
        # Plot data
        lons = self.ds[lon_dim].values
        lats = self.ds[lat_dim].values
        
        # Create appropriate colormap
        cmap = self._get_colormap(variable)
        vmin, vmax = self._get_color_limits(variable, data.values)
        
        # Plot with pcolormesh
        if HAS_CARTOPY and projection is not None:
            im = ax.pcolormesh(lons, lats, data.values,
                              transform=ccrs.PlateCarree(),
                              cmap=cmap, vmin=vmin, vmax=vmax)
            # Set extent
            ax.set_extent([lons.min(), lons.max(), lats.min(), lats.max()],
                         ccrs.PlateCarree())
            # Add gridlines
            ax.gridlines(draw_labels=True, linewidth=0.5, alpha=0.5)
        else:
            # Simple matplotlib without transform
            im = ax.pcolormesh(lons, lats, data.values,
                              cmap=cmap, vmin=vmin, vmax=vmax)
            ax.set_xlim(lons.min(), lons.max())
            ax.set_ylim(lats.min(), lats.max())
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            ax.grid(True, alpha=0.3)

        # Add colorbar
        cbar = plt.colorbar(im, ax=ax, orientation='horizontal',
                           pad=0.05, fraction=0.046)
        cbar.set_label(data.attrs.get('units', 'Value'))
        
        # Title
        title = f'{variable}'
        if time_slice:
            title += f' - {time_slice}'
        else:
            title += ' - Temporal Mean'
        ax.set_title(title, fontsize=14, fontweight='bold')
        
        if save:
            output_file = self.output_dir / f'{variable}_map.png'
            plt.savefig(output_file, bbox_inches='tight')
            logger.info(f"Saved spatial map to {output_file}")
        
        return fig
    
    def plot_distribution(self, variables: List[str],
                         save: bool = True) -> plt.Figure:
        """
        Plot distribution comparison for multiple variables.
        
        Parameters:
        -----------
        variables : list
            List of variable names
        save : bool
            Whether to save the figure
            
        Returns:
        --------
        matplotlib.figure.Figure
            The generated figure
        """
        if self.ds is None:
            self.load_dataset()
        
        n_vars = len(variables)
        n_cols = min(3, n_vars)
        n_rows = (n_vars + n_cols - 1) // n_cols
        
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(5*n_cols, 4*n_rows))
        
        if n_vars == 1:
            axes = [axes]
        else:
            axes = axes.flatten()
        
        for i, var in enumerate(variables):
            if var not in self.ds.data_vars:
                continue
            
            ax = axes[i]
            data = self.ds[var].values.flatten()
            valid_data = data[~np.isnan(data)]
            
            if len(valid_data) > 0:
                # Histogram
                ax.hist(valid_data, bins=50, color='steelblue', 
                       edgecolor='black', alpha=0.7)
                
                # Add statistics
                mean = np.mean(valid_data)
                median = np.median(valid_data)
                std = np.std(valid_data)
                
                ax.axvline(mean, color='red', linestyle='--', linewidth=2,
                          label=f'Mean: {mean:.2f}')
                ax.axvline(median, color='green', linestyle='--', linewidth=2,
                          label=f'Median: {median:.2f}')
                
                ax.set_title(f'{var}', fontsize=12, fontweight='bold')
                ax.set_xlabel(self.ds[var].attrs.get('units', 'Value'))
                ax.set_ylabel('Frequency')
                ax.legend(loc='upper right', fontsize=9)
                
                # Add text with stats
                stats_text = f'Std: {std:.2f}\nN: {len(valid_data):,}'
                ax.text(0.98, 0.75, stats_text, transform=ax.transAxes,
                       fontsize=9, ha='right',
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # Remove extra subplots
        for i in range(n_vars, len(axes)):
            fig.delaxes(axes[i])
        
        plt.suptitle('Climate Indices Distributions', fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        if save:
            output_file = self.output_dir / 'distributions.png'
            plt.savefig(output_file, bbox_inches='tight')
            logger.info(f"Saved distributions plot to {output_file}")
        
        return fig
    
    def plot_correlation_matrix(self, variables: Optional[List[str]] = None,
                               save: bool = True) -> plt.Figure:
        """
        Plot correlation matrix between variables.
        
        Parameters:
        -----------
        variables : list, optional
            List of variables (default: all)
        save : bool
            Whether to save the figure
            
        Returns:
        --------
        matplotlib.figure.Figure
            The generated figure
        """
        if self.ds is None:
            self.load_dataset()
        
        if variables is None:
            variables = list(self.ds.data_vars)[:20]  # Limit to 20 for readability
        
        # Calculate correlations
        corr_data = {}
        for var in variables:
            if var in self.ds.data_vars:
                data = self.ds[var].values.flatten()
                corr_data[var] = data
        
        # Create correlation matrix
        import pandas as pd
        df = pd.DataFrame(corr_data)
        corr_matrix = df.corr()
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # Plot heatmap
        im = ax.imshow(corr_matrix, cmap='RdBu_r', vmin=-1, vmax=1, aspect='auto')
        
        # Set ticks
        ax.set_xticks(np.arange(len(variables)))
        ax.set_yticks(np.arange(len(variables)))
        ax.set_xticklabels(variables, rotation=45, ha='right')
        ax.set_yticklabels(variables)
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Correlation Coefficient')
        
        # Add grid
        ax.set_xticks(np.arange(len(variables)+1)-.5, minor=True)
        ax.set_yticks(np.arange(len(variables)+1)-.5, minor=True)
        ax.grid(which='minor', color='gray', linestyle='-', linewidth=0.5)
        
        # Add title
        ax.set_title('Climate Indices Correlation Matrix', 
                    fontsize=14, fontweight='bold', pad=20)
        
        plt.tight_layout()
        
        if save:
            output_file = self.output_dir / 'correlation_matrix.png'
            plt.savefig(output_file, bbox_inches='tight')
            logger.info(f"Saved correlation matrix to {output_file}")
        
        return fig
    
    def create_summary_panel(self, variables: List[str],
                           save: bool = True) -> plt.Figure:
        """
        Create a multi-panel summary figure.
        
        Parameters:
        -----------
        variables : list
            Variables to include (max 4)
        save : bool
            Whether to save the figure
            
        Returns:
        --------
        matplotlib.figure.Figure
            The generated figure
        """
        if self.ds is None:
            self.load_dataset()
        
        variables = variables[:4]  # Limit to 4 variables
        
        fig = plt.figure(figsize=(16, 12))
        gs = GridSpec(3, 4, figure=fig, hspace=0.3, wspace=0.3)
        
        # Panel 1: Time series for first variable
        ax1 = fig.add_subplot(gs[0, :])
        if variables and variables[0] in self.ds.data_vars:
            data = self.ds[variables[0]]
            if 'time' in data.dims:
                ts = data.mean(dim=[d for d in data.dims if d != 'time'])
                ax1.plot(ts.time, ts.values, 'b-', linewidth=1.5)
                ax1.set_title(f'{variables[0]} Time Series', fontweight='bold')
                ax1.set_ylabel(data.attrs.get('units', 'Value'))
                ax1.grid(True, alpha=0.3)
        
        # Panels 2-5: Spatial maps for each variable
        for i, var in enumerate(variables):
            if var not in self.ds.data_vars:
                continue
            
            ax = fig.add_subplot(gs[1, i])
            data = self.ds[var]
            
            if 'time' in data.dims:
                data = data.mean(dim='time')
            
            if 'lat' in data.dims and 'lon' in data.dims:
                im = ax.imshow(data.values, cmap=self._get_colormap(var),
                              aspect='auto')
                ax.set_title(var, fontsize=10)
                ax.axis('off')
                plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        
        # Panel 6: Statistics table
        ax6 = fig.add_subplot(gs[2, :2])
        ax6.axis('tight')
        ax6.axis('off')
        
        # Create statistics table
        table_data = []
        for var in variables:
            if var in self.ds.data_vars:
                data = self.ds[var].values
                valid = data[~np.isnan(data)]
                if len(valid) > 0:
                    table_data.append([
                        var[:15],
                        f"{np.mean(valid):.2f}",
                        f"{np.std(valid):.2f}",
                        f"{np.min(valid):.2f}",
                        f"{np.max(valid):.2f}"
                    ])
        
        if table_data:
            table = ax6.table(cellText=table_data,
                            colLabels=['Variable', 'Mean', 'Std', 'Min', 'Max'],
                            cellLoc='center',
                            loc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.scale(1.2, 1.5)
            ax6.set_title('Summary Statistics', fontweight='bold', pad=20)
        
        # Panel 7: Distribution comparison
        ax7 = fig.add_subplot(gs[2, 2:])
        for var in variables:
            if var in self.ds.data_vars:
                data = self.ds[var].values.flatten()
                valid = data[~np.isnan(data)]
                if len(valid) > 0:
                    ax7.hist(valid, bins=30, alpha=0.5, label=var)
        
        ax7.set_xlabel('Value')
        ax7.set_ylabel('Frequency')
        ax7.set_title('Distribution Comparison', fontweight='bold')
        ax7.legend()
        ax7.grid(True, alpha=0.3)
        
        # Overall title
        fig.suptitle('Climate Indices Summary Dashboard', 
                    fontsize=16, fontweight='bold')
        
        if save:
            output_file = self.output_dir / 'summary_dashboard.png'
            plt.savefig(output_file, bbox_inches='tight', dpi=150)
            logger.info(f"Saved summary dashboard to {output_file}")
        
        return fig
    
    def _get_colormap(self, variable: str) -> str:
        """Get appropriate colormap for variable."""
        # Temperature-related
        if any(t in variable.lower() for t in ['temp', 'tas', 'degree']):
            return 'RdYlBu_r'
        # Precipitation
        elif any(p in variable.lower() for p in ['prec', 'prcp', 'rain']):
            return 'YlGnBu'
        # Day counts
        elif 'days' in variable.lower():
            return 'viridis'
        # Default
        return 'coolwarm'
    
    def _get_color_limits(self, variable: str, data: np.ndarray) -> Tuple[float, float]:
        """Get appropriate color limits for variable."""
        valid = data[~np.isnan(data)]
        if len(valid) == 0:
            return 0, 1
        
        # Use percentiles to avoid outliers
        vmin = np.percentile(valid, 2)
        vmax = np.percentile(valid, 98)
        
        return vmin, vmax


def main():
    """Example usage of visualizer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Visualize climate indices')
    parser.add_argument('dataset', help='Path to NetCDF dataset')
    parser.add_argument('--variables', '-v', nargs='+', 
                       help='Variables to plot')
    parser.add_argument('--plot-type', '-t', 
                       choices=['timeseries', 'map', 'distribution', 'correlation', 'summary'],
                       default='summary', help='Type of plot to generate')
    
    args = parser.parse_args()
    
    # Initialize visualizer
    viz = ClimateIndicesVisualizer(args.dataset)
    
    # Generate plots based on type
    if args.plot_type == 'timeseries' and args.variables:
        for var in args.variables:
            viz.plot_time_series(var)
    
    elif args.plot_type == 'map' and args.variables:
        for var in args.variables:
            viz.plot_spatial_map(var)
    
    elif args.plot_type == 'distribution':
        vars_to_plot = args.variables or list(viz.ds.data_vars)[:6]
        viz.plot_distribution(vars_to_plot)
    
    elif args.plot_type == 'correlation':
        viz.plot_correlation_matrix(args.variables)
    
    elif args.plot_type == 'summary':
        vars_to_plot = args.variables or list(viz.ds.data_vars)[:4]
        viz.create_summary_panel(vars_to_plot)
    
    print(f"\n✓ Visualizations saved to {viz.output_dir}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
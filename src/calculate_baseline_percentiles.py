#!/usr/bin/env python
"""
Pre-calculate baseline percentiles for extreme temperature indices.

This module calculates day-of-year percentiles using the full baseline period
(1981-2000) and saves them for use in chunked processing. This solves the
problem of calculating percentile indices when processing data in small chunks.
"""

import logging
import xarray as xr
import numpy as np
from pathlib import Path
from typing import Dict, Optional
from xclim.core.calendar import percentile_doy
import warnings
import dask

# Suppress expected warnings
warnings.filterwarnings('ignore', category=RuntimeWarning, message='All-NaN slice encountered')
warnings.filterwarnings('ignore', message='Increasing number of chunks')
warnings.filterwarnings('ignore', message='PerformanceWarning')

logger = logging.getLogger(__name__)


class BaselinePercentileCalculator:
    """Calculate and cache baseline percentiles for extreme indices."""

    def __init__(self, baseline_start: int = 1981, baseline_end: int = 2000):
        """
        Initialize calculator with baseline period.

        Parameters:
        -----------
        baseline_start : int
            Start year of baseline period
        baseline_end : int
            End year of baseline period
        """
        self.baseline_start = baseline_start
        self.baseline_end = baseline_end
        self.percentiles = {}

    def calculate_baseline_percentiles(self,
                                      data_path: str,
                                      save_path: Optional[str] = None) -> Dict[str, xr.DataArray]:
        """
        Calculate all required baseline percentiles for extreme indices.

        Parameters:
        -----------
        data_path : str
            Path to temperature data (Zarr store or NetCDF)
        save_path : str, optional
            Path to save calculated percentiles

        Returns:
        --------
        dict
            Dictionary of calculated percentiles
        """
        logger.info(f"Calculating baseline percentiles for {self.baseline_start}-{self.baseline_end}")

        # Load temperature data for baseline period
        logger.info(f"Loading data from {data_path}")

        if Path(data_path).suffix == '.zarr' or Path(data_path).is_dir():
            ds = xr.open_zarr(data_path, consolidated=False)
        else:
            ds = xr.open_dataset(data_path)

        # Select baseline period
        baseline_slice = slice(f"{self.baseline_start}-01-01", f"{self.baseline_end}-12-31")
        ds_baseline = ds.sel(time=baseline_slice)

        # Check we have enough years
        n_years = len(ds_baseline.groupby('time.year'))
        if n_years < 10:
            raise ValueError(
                f"Only {n_years} years in baseline period {self.baseline_start}-{self.baseline_end}. "
                f"At least 10 years recommended for robust percentiles."
            )
        logger.info(f"Using {n_years} years of baseline data")

        # Standardize units
        for var in ['tmax', 'tasmax', 'tmin', 'tasmin']:
            if var in ds_baseline:
                data = ds_baseline[var]
                if 'units' in data.attrs:
                    if data.attrs['units'] in ['degrees_celsius', 'celsius', 'C']:
                        data.attrs['units'] = 'degC'

        # Calculate percentiles for each variable
        # Note: We calculate each percentile separately to avoid extra dimensions
        percentile_configs = [
            ('tx90p_threshold', 'tmax', 90, "90th percentile of daily maximum temperature"),
            ('tx10p_threshold', 'tmax', 10, "10th percentile of daily maximum temperature"),
            ('tn90p_threshold', 'tmin', 90, "90th percentile of daily minimum temperature"),
            ('tn10p_threshold', 'tmin', 10, "10th percentile of daily minimum temperature"),
        ]

        results = {}

        for name, var_name, percentile, description in percentile_configs:
            # Check if variable exists (also try alternate names)
            if var_name not in ds_baseline:
                alt_names = {'tmax': 'tasmax', 'tmin': 'tasmin'}
                if var_name in alt_names and alt_names[var_name] in ds_baseline:
                    var_name = alt_names[var_name]
                else:
                    logger.warning(f"Variable '{var_name}' not found for {name}")
                    continue

            data = ds_baseline[var_name]
            logger.info(f"Calculating {name}: {description}")

            # Optimize chunking for percentile calculation
            # Load time dimension fully but chunk spatially
            data_rechunked = data.chunk({'time': -1, 'lat': 100, 'lon': 100})

            # Calculate day-of-year percentiles
            # Window of 5 days is standard for climate extremes
            # Pass single percentile value to avoid extra dimension
            with dask.config.set(scheduler='threads'):
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore')
                    doy_percentile = percentile_doy(data_rechunked, window=5, per=percentile)

                    # Force computation
                    logger.info(f"  Computing percentiles (this may take a few minutes)...")
                    doy_percentile = doy_percentile.compute()

            # If percentile_doy returns multiple percentiles, select the right one
            if 'percentiles' in doy_percentile.dims:
                doy_percentile = doy_percentile.sel(percentiles=percentile)
                # Drop the now-unnecessary percentiles coordinate
                if 'percentiles' in doy_percentile.coords:
                    doy_percentile = doy_percentile.drop_vars('percentiles')

            # Ensure units are preserved
            if 'units' in data.attrs:
                doy_percentile.attrs['units'] = data.attrs['units']

            doy_percentile.attrs['description'] = description
            doy_percentile.attrs['baseline_period'] = f"{self.baseline_start}-{self.baseline_end}"
            doy_percentile.attrs['baseline_years'] = n_years

            results[name] = doy_percentile

            # Log some statistics
            valid_data = doy_percentile.values[~np.isnan(doy_percentile.values)]
            if len(valid_data) > 0:
                mean_threshold = float(np.mean(valid_data))
                logger.info(f"  Mean threshold: {mean_threshold:.2f} {doy_percentile.attrs.get('units', '')}")
            logger.info(f"  Shape: {doy_percentile.shape} (should be 3D: lat × lon × dayofyear)")

        # Save if requested
        if save_path:
            logger.info(f"Saving baseline percentiles to {save_path}")

            # Combine into a dataset
            ds_percentiles = xr.Dataset(results)
            ds_percentiles.attrs['baseline_period'] = f"{self.baseline_start}-{self.baseline_end}"
            ds_percentiles.attrs['description'] = "Pre-calculated baseline percentiles for extreme temperature indices"

            # Save as NetCDF
            ds_percentiles.to_netcdf(save_path, engine='netcdf4', encoding={
                var: {'zlib': True, 'complevel': 4} for var in results.keys()
            })

            logger.info(f"Saved {len(results)} percentiles to {save_path}")

        self.percentiles = results
        return results

    def load_percentiles(self, path: str) -> Dict[str, xr.DataArray]:
        """
        Load pre-calculated percentiles from file.

        Parameters:
        -----------
        path : str
            Path to percentiles file

        Returns:
        --------
        dict
            Dictionary of percentiles
        """
        logger.info(f"Loading baseline percentiles from {path}")
        ds = xr.open_dataset(path)

        self.percentiles = {var: ds[var] for var in ds.data_vars}

        # Log what was loaded
        for name, data in self.percentiles.items():
            logger.info(f"  Loaded {name}: shape={data.shape}")

        return self.percentiles


def main():
    """Calculate baseline percentiles for PRISM data."""
    import sys
    import time

    # Setup logging with cleaner format
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )

    # Paths
    temp_data_path = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'
    output_dir = Path('data/baselines')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / 'baseline_percentiles_1981_2000.nc'

    # Check if data exists
    if not Path(temp_data_path).exists():
        logger.error(f"Temperature data not found at {temp_data_path}")
        return 1

    # Calculate percentiles
    calculator = BaselinePercentileCalculator(
        baseline_start=1981,
        baseline_end=2000
    )

    print("\n" + "="*70)
    print("BASELINE PERCENTILE CALCULATOR FOR EXTREME INDICES")
    print("="*70)
    print(f"\nThis will calculate day-of-year percentiles using the")
    print(f"baseline period 1981-2000 (20 years of data).")
    print(f"\n⚠️  This is a one-time calculation that may take 10-20 minutes.")
    print(f"   The results will be saved and reused for all future processing.")
    print("\n" + "-"*70)

    try:
        start_time = time.time()

        percentiles = calculator.calculate_baseline_percentiles(
            data_path=temp_data_path,
            save_path=output_path
        )

        elapsed = time.time() - start_time

        print("\n" + "="*70)
        print(f"✅ SUCCESS! Calculated {len(percentiles)} baseline percentiles")
        print(f"   Time taken: {elapsed/60:.1f} minutes")
        print(f"   Saved to: {output_path}")
        print(f"   File size: {Path(output_path).stat().st_size / 1e6:.1f} MB")
        print("\nCalculated percentiles:")
        for name in percentiles:
            print(f"  - {name}")
        print("\nThese percentiles can now be used for chunked processing")
        print("without recalculation.")
        print("="*70 + "\n")

        return 0

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
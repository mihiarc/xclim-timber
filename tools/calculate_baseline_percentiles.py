#!/usr/bin/env python
"""
Pre-calculate baseline percentiles for extreme temperature and precipitation indices.

This module calculates day-of-year percentiles using the full baseline period
(1981-2000) and saves them for use in chunked processing. This solves the
problem of calculating percentile indices when processing data in small chunks.

Key differences:
- Temperature: percentiles calculated on ALL days
- Precipitation: percentiles calculated on WET DAYS ONLY (pr ≥ 1mm) per WMO standards
"""

import logging
import xarray as xr
import numpy as np
from pathlib import Path
from typing import Dict, Optional
from xclim.core.calendar import percentile_doy
import warnings
import dask
import gc

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
                                      temp_data_path: str,
                                      precip_data_path: Optional[str] = None,
                                      save_path: Optional[str] = None) -> Dict[str, xr.DataArray]:
        """
        Calculate all required baseline percentiles for extreme indices.

        Parameters:
        -----------
        temp_data_path : str
            Path to temperature data (Zarr store or NetCDF)
        precip_data_path : str, optional
            Path to precipitation data (Zarr store or NetCDF)
        save_path : str, optional
            Path to save calculated percentiles

        Returns:
        --------
        dict
            Dictionary of calculated percentiles
        """
        logger.info(f"Calculating baseline percentiles for {self.baseline_start}-{self.baseline_end}")

        # Load temperature data for baseline period
        logger.info(f"Loading temperature data from {temp_data_path}")

        if Path(temp_data_path).suffix == '.zarr' or Path(temp_data_path).is_dir():
            ds_temp = xr.open_zarr(temp_data_path, consolidated=False)
        else:
            ds_temp = xr.open_dataset(temp_data_path)

        # Select baseline period
        baseline_slice = slice(f"{self.baseline_start}-01-01", f"{self.baseline_end}-12-31")
        ds_temp_baseline = ds_temp.sel(time=baseline_slice)

        # Load precipitation data if provided
        ds_precip_baseline = None
        if precip_data_path:
            logger.info(f"Loading precipitation data from {precip_data_path}")
            if Path(precip_data_path).suffix == '.zarr' or Path(precip_data_path).is_dir():
                ds_precip = xr.open_zarr(precip_data_path, consolidated=False)
            else:
                ds_precip = xr.open_dataset(precip_data_path)
            ds_precip_baseline = ds_precip.sel(time=baseline_slice)

        # Check we have enough years
        n_years = len(ds_temp_baseline.groupby('time.year'))
        if n_years < 10:
            raise ValueError(
                f"Only {n_years} years in baseline period {self.baseline_start}-{self.baseline_end}. "
                f"At least 10 years recommended for robust percentiles."
            )
        logger.info(f"Using {n_years} years of baseline data")

        # Standardize units for temperature
        for var in ['tmax', 'tasmax', 'tmin', 'tasmin', 'tmean', 'tas']:
            if var in ds_temp_baseline:
                data = ds_temp_baseline[var]
                if 'units' in data.attrs:
                    if data.attrs['units'] in ['degrees_celsius', 'celsius', 'C']:
                        data.attrs['units'] = 'degC'

        # Standardize units for precipitation
        if ds_precip_baseline is not None:
            for var in ['pr', 'ppt', 'precip']:
                if var in ds_precip_baseline:
                    data = ds_precip_baseline[var]
                    if 'units' in data.attrs:
                        # Daily precipitation should be mm/day for xclim compatibility
                        if data.attrs['units'] in ['millimeter', 'millimeters', 'mm']:
                            data.attrs['units'] = 'mm/day'

        # Calculate percentiles for each variable
        # Note: We calculate each percentile separately to avoid extra dimensions
        percentile_configs = [
            # Temperature percentiles (calculated on ALL days)
            ('tx90p_threshold', 'tmax', 90, "90th percentile of daily maximum temperature", 'temperature', None),
            ('tx10p_threshold', 'tmax', 10, "10th percentile of daily maximum temperature", 'temperature', None),
            ('tn90p_threshold', 'tmin', 90, "90th percentile of daily minimum temperature", 'temperature', None),
            ('tn10p_threshold', 'tmin', 10, "10th percentile of daily minimum temperature", 'temperature', None),
            # Precipitation percentiles (calculated on WET DAYS ONLY: pr ≥ 1mm)
            ('pr95p_threshold', 'pr', 95, "95th percentile of wet day precipitation", 'precipitation', 1.0),
            ('pr99p_threshold', 'pr', 99, "99th percentile of wet day precipitation", 'precipitation', 1.0),
            # Multivariate indices percentiles (for compound extremes)
            ('tas_25p_threshold', 'tmean', 25, "25th percentile of daily mean temperature (for cold thresholds)", 'temperature', None),
            ('tas_75p_threshold', 'tmean', 75, "75th percentile of daily mean temperature (for warm thresholds)", 'temperature', None),
            ('pr_25p_threshold', 'pr', 25, "25th percentile of wet day precipitation (for dry thresholds)", 'precipitation', 1.0),
            ('pr_75p_threshold', 'pr', 75, "75th percentile of wet day precipitation (for wet thresholds)", 'precipitation', 1.0),
        ]

        results = {}

        for name, var_name, percentile, description, data_type, wet_day_threshold in percentile_configs:
            # Select the appropriate dataset
            if data_type == 'temperature':
                ds_baseline = ds_temp_baseline
                alt_names = {'tmax': 'tasmax', 'tmin': 'tasmin', 'tmean': 'tas'}
            elif data_type == 'precipitation':
                if ds_precip_baseline is None:
                    logger.warning(f"Skipping {name}: precipitation data not provided")
                    continue
                ds_baseline = ds_precip_baseline
                alt_names = {'pr': 'ppt', 'ppt': 'pr'}
            else:
                logger.warning(f"Unknown data type '{data_type}' for {name}")
                continue

            # Check if variable exists (also try alternate names)
            if var_name not in ds_baseline:
                if var_name in alt_names and alt_names[var_name] in ds_baseline:
                    var_name = alt_names[var_name]
                else:
                    logger.warning(f"Variable '{var_name}' not found for {name}")
                    continue

            data = ds_baseline[var_name]
            logger.info(f"Calculating {name}: {description}")

            # Apply wet-day filtering for precipitation (WMO standard)
            if wet_day_threshold is not None:
                logger.info(f"  Filtering to wet days (>= {wet_day_threshold} mm)")
                # Replace dry days with NaN so they're excluded from percentile calculation
                data = data.where(data >= wet_day_threshold)
                # Count wet days for logging
                wet_days = (data >= wet_day_threshold).sum().values
                total_days = data.sizes['time']
                logger.info(f"  Wet days: {wet_days:,} / {total_days:,} ({100*wet_days/total_days:.1f}%)")

            # Optimize chunking for percentile calculation
            # Load time dimension fully but chunk spatially (smaller chunks for memory efficiency with 10 percentiles)
            data_rechunked = data.chunk({'time': -1, 'lat': 50, 'lon': 50})

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
            if wet_day_threshold is not None:
                doy_percentile.attrs['wet_day_threshold'] = f"{wet_day_threshold} mm"

            results[name] = doy_percentile

            # Log some statistics
            valid_data = doy_percentile.values[~np.isnan(doy_percentile.values)]
            if len(valid_data) > 0:
                mean_threshold = float(np.mean(valid_data))
                logger.info(f"  Mean threshold: {mean_threshold:.2f} {doy_percentile.attrs.get('units', '')}")
            logger.info(f"  Shape: {doy_percentile.shape} (should be 3D: lat × lon × dayofyear)")

            # Clean up intermediate data to free memory before next percentile
            del data, data_rechunked
            gc.collect()

        # Save if requested
        if save_path:
            logger.info(f"Saving baseline percentiles to {save_path}")

            # Combine into a dataset
            ds_percentiles = xr.Dataset(results)
            ds_percentiles.attrs['baseline_period'] = f"{self.baseline_start}-{self.baseline_end}"
            ds_percentiles.attrs['description'] = "Pre-calculated baseline percentiles for extreme temperature and precipitation indices"
            ds_percentiles.attrs['note'] = "Precipitation percentiles calculated on wet days only (pr >= 1mm) per WMO standards"

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
    """Calculate baseline percentiles for PRISM data (temperature and precipitation)."""
    import sys
    import time

    # Setup logging with cleaner format
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )

    # Paths
    temp_data_path = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'
    precip_data_path = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/precipitation'
    output_dir = Path('data/baselines')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / 'baseline_percentiles_1981_2000.nc'

    # Check if data exists
    if not Path(temp_data_path).exists():
        logger.error(f"Temperature data not found at {temp_data_path}")
        return 1

    if not Path(precip_data_path).exists():
        logger.error(f"Precipitation data not found at {precip_data_path}")
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
    print(f"\nCalculating percentiles for:")
    print(f"  • Temperature: 6 percentiles (tx90p, tx10p, tn90p, tn10p, tas25p, tas75p)")
    print(f"  • Precipitation: 4 percentiles (pr95p, pr99p, pr25p, pr75p on wet days only)")
    print(f"\n⚠️  This is a one-time calculation that may take 25-35 minutes.")
    print(f"   The results will be saved and reused for all future processing.")
    print(f"   Using smaller chunks (50x50) for memory efficiency.")
    print("\n" + "-"*70)

    try:
        start_time = time.time()

        percentiles = calculator.calculate_baseline_percentiles(
            temp_data_path=temp_data_path,
            precip_data_path=precip_data_path,
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
        print("\n📝 Note: Precipitation percentiles calculated on wet days only")
        print("   (pr >= 1mm) following WMO standards.")
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
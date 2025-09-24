#!/usr/bin/env python
"""
Example of how to use pre-calculated baseline percentiles for extreme indices.

This shows how to load the baseline thresholds and apply them to calculate
percentile-based indices for any time period.
"""

import xarray as xr
import numpy as np
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def load_baseline_percentiles(baseline_path: str = 'data/baselines/baseline_percentiles_1981_2000.nc'):
    """
    Load pre-calculated baseline percentiles.

    Parameters:
    -----------
    baseline_path : str
        Path to baseline percentiles file

    Returns:
    --------
    dict
        Dictionary with threshold arrays
    """
    if not Path(baseline_path).exists():
        raise FileNotFoundError(f"Baseline percentiles not found at {baseline_path}. "
                              "Run calculate_baseline_percentiles.py first.")

    ds = xr.open_dataset(baseline_path)

    thresholds = {
        'tx90p': ds['tx90p_threshold'],
        'tx10p': ds['tx10p_threshold'],
        'tn90p': ds['tn90p_threshold'],
        'tn10p': ds['tn10p_threshold']
    }

    logger.info(f"Loaded baseline percentiles from {baseline_path}")
    for name, data in thresholds.items():
        logger.info(f"  {name}: shape={data.shape}")

    return thresholds


def calculate_percentile_indices_with_baseline(
    tasmax: xr.DataArray,
    tasmin: xr.DataArray,
    baseline_thresholds: dict,
    freq: str = 'YS'
) -> dict:
    """
    Calculate percentile indices using pre-calculated baseline thresholds.

    Parameters:
    -----------
    tasmax : xr.DataArray
        Daily maximum temperature
    tasmin : xr.DataArray
        Daily minimum temperature
    baseline_thresholds : dict
        Pre-calculated thresholds from load_baseline_percentiles()
    freq : str
        Resampling frequency (default: 'YS' for annual)

    Returns:
    --------
    dict
        Calculated indices
    """
    from xclim.indicators import atmos

    indices = {}

    # TX90p: Warm days
    if 'tx90p' in baseline_thresholds and tasmax is not None:
        try:
            indices['tx90p'] = atmos.tx90p(
                tasmax,
                tasmax_per=baseline_thresholds['tx90p'],
                freq=freq
            )
            logger.info("Calculated TX90p using baseline thresholds")
        except Exception as e:
            logger.error(f"Error calculating tx90p: {e}")

    # TX10p: Cool days
    if 'tx10p' in baseline_thresholds and tasmax is not None:
        try:
            indices['tx10p'] = atmos.tx10p(
                tasmax,
                tasmax_per=baseline_thresholds['tx10p'],
                freq=freq
            )
            logger.info("Calculated TX10p using baseline thresholds")
        except Exception as e:
            logger.error(f"Error calculating tx10p: {e}")

    # TN90p: Warm nights
    if 'tn90p' in baseline_thresholds and tasmin is not None:
        try:
            indices['tn90p'] = atmos.tn90p(
                tasmin,
                tasmin_per=baseline_thresholds['tn90p'],
                freq=freq
            )
            logger.info("Calculated TN90p using baseline thresholds")
        except Exception as e:
            logger.error(f"Error calculating tn90p: {e}")

    # TN10p: Cool nights
    if 'tn10p' in baseline_thresholds and tasmin is not None:
        try:
            indices['tn10p'] = atmos.tn10p(
                tasmin,
                tasmin_per=baseline_thresholds['tn10p'],
                freq=freq
            )
            logger.info("Calculated TN10p using baseline thresholds")
        except Exception as e:
            logger.error(f"Error calculating tn10p: {e}")

    return indices


def example_usage():
    """
    Example of calculating percentile indices for a specific year.
    """
    import warnings
    warnings.filterwarnings('ignore')

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    print("\n" + "="*70)
    print("EXAMPLE: Using Pre-calculated Baseline Percentiles")
    print("="*70)

    # Step 1: Load baseline thresholds
    print("\n1. Loading baseline percentiles...")
    thresholds = load_baseline_percentiles()

    # Step 2: Load temperature data for a specific period
    print("\n2. Loading temperature data (example: 2023)...")

    # Example with PRISM data
    temp_path = '/media/mihiarc/SSD4TB/data/PRISM/prism.zarr/temperature'

    if Path(temp_path).exists():
        ds = xr.open_zarr(temp_path, consolidated=False)

        # Select a specific year for demonstration
        ds_2023 = ds.sel(time=slice('2023-01-01', '2023-12-31'))

        tasmax = ds_2023['tmax'] if 'tmax' in ds_2023 else None
        tasmin = ds_2023['tmin'] if 'tmin' in ds_2023 else None

        # Ensure units
        if tasmax is not None:
            tasmax.attrs['units'] = 'degC'
        if tasmin is not None:
            tasmin.attrs['units'] = 'degC'

        print(f"  Loaded data shape: {tasmax.shape if tasmax is not None else 'N/A'}")

        # Step 3: Calculate indices
        print("\n3. Calculating percentile indices...")
        indices = calculate_percentile_indices_with_baseline(
            tasmax, tasmin, thresholds
        )

        # Step 4: Analyze results
        print("\n4. Results for 2023:")
        print("-"*70)

        for name, data in indices.items():
            if data is not None:
                # Convert timedelta to days if needed
                if 'timedelta' in str(data.dtype):
                    values = (data / np.timedelta64(1, 'D')).values
                else:
                    values = data.values

                valid = values[~np.isnan(values)]
                if len(valid) > 0:
                    mean_days = np.mean(valid)
                    print(f"  {name}: {mean_days:.1f} days/year")

                    # Compare to expected baseline (36.5 days)
                    if '90p' in name:
                        diff = mean_days - 36.5
                        if diff > 0:
                            print(f"    → {diff:.1f} days above baseline (warming signal)")
                        else:
                            print(f"    → {abs(diff):.1f} days below baseline")
    else:
        print(f"  Temperature data not found at {temp_path}")
        print("  Using synthetic data for demonstration...")

        # Create synthetic data
        import pandas as pd
        times = pd.date_range('2023-01-01', '2023-12-31', freq='D')
        lats = np.linspace(25, 50, 100)
        lons = np.linspace(-125, -66, 100)

        # Create temperature with warming trend
        temp_base = 20 + 10 * np.sin(np.arange(len(times)) * 2 * np.pi / 365)
        warming = 2  # 2°C warming relative to baseline

        temp_3d = temp_base[:, np.newaxis, np.newaxis] + warming
        temp_3d += np.random.randn(len(times), len(lats), len(lons)) * 2

        tasmax = xr.DataArray(
            temp_3d + 5,
            dims=['time', 'lat', 'lon'],
            coords={'time': times, 'lat': lats, 'lon': lons},
            name='tasmax'
        )
        tasmax.attrs['units'] = 'degC'

        print(f"  Created synthetic data with {warming}°C warming")

    print("\n" + "="*70)
    print("COMPLETE: Percentile indices calculated using baseline thresholds")
    print("="*70)


if __name__ == '__main__':
    example_usage()
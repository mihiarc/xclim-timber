#!/usr/bin/env python3
"""
Integration test fixtures for xclim-timber v5.0 parallel spatial tiling.

Provides realistic test datasets and fixtures for end-to-end pipeline testing.
"""

import pytest
import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import tempfile
import shutil


@pytest.fixture(scope="session")
def test_zarr_store_temperature(tmp_path_factory):
    """
    Create realistic test Zarr store for temperature data.

    Generates a small PRISM-like dataset (100x100 grid, 2 years = 730 days)
    with realistic temperature patterns.

    Returns:
        Path to Zarr store directory
    """
    zarr_dir = tmp_path_factory.mktemp("test_data") / "test_temp.zarr"

    # Create time coordinate (2 years of daily data)
    start_date = datetime(2022, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(730)]

    # Create spatial coordinates (100x100 grid)
    lat = np.linspace(40, 45, 100)  # Typical US latitude range
    lon = np.linspace(-120, -115, 100)  # Typical US longitude range

    # Generate realistic temperature data with seasonal patterns
    np.random.seed(42)  # Reproducible test data

    # Base temperature with seasonal cycle
    day_of_year = np.array([d.timetuple().tm_yday for d in dates])
    seasonal_cycle = 15 * np.sin(2 * np.pi * (day_of_year - 80) / 365)  # Peak in summer

    # Generate temperatures
    tas_base = 15 + seasonal_cycle[:, np.newaxis, np.newaxis]
    tas = tas_base + np.random.randn(730, 100, 100) * 3  # Add noise

    tasmax_base = 22 + seasonal_cycle[:, np.newaxis, np.newaxis]
    tasmax = tasmax_base + np.random.randn(730, 100, 100) * 4

    tasmin_base = 8 + seasonal_cycle[:, np.newaxis, np.newaxis]
    tasmin = tasmin_base + np.random.randn(730, 100, 100) * 3

    # Ensure tasmax > tas > tasmin
    tasmax = np.maximum(tasmax, tas + 2)
    tasmin = np.minimum(tasmin, tas - 2)

    # Create dataset
    ds = xr.Dataset(
        {
            'tas': (['time', 'lat', 'lon'], tas.astype(np.float32)),
            'tasmax': (['time', 'lat', 'lon'], tasmax.astype(np.float32)),
            'tasmin': (['time', 'lat', 'lon'], tasmin.astype(np.float32)),
        },
        coords={
            'time': dates,
            'lat': lat,
            'lon': lon,
        }
    )

    # Add CF-compliant attributes
    ds['tas'].attrs = {'units': 'degC', 'standard_name': 'air_temperature', 'long_name': 'Mean Temperature'}
    ds['tasmax'].attrs = {'units': 'degC', 'standard_name': 'air_temperature', 'long_name': 'Maximum Temperature'}
    ds['tasmin'].attrs = {'units': 'degC', 'standard_name': 'air_temperature', 'long_name': 'Minimum Temperature'}

    # Save to Zarr with chunking
    ds.chunk({'time': 365, 'lat': 50, 'lon': 50}).to_zarr(zarr_dir, mode='w')

    return zarr_dir


@pytest.fixture(scope="session")
def test_zarr_store_precipitation(tmp_path_factory):
    """
    Create realistic test Zarr store for precipitation data.

    Generates a small PRISM-like dataset (100x100 grid, 2 years = 730 days)
    with realistic precipitation patterns.

    Returns:
        Path to Zarr store directory
    """
    zarr_dir = tmp_path_factory.mktemp("test_data") / "test_precip.zarr"

    # Create time coordinate (2 years of daily data)
    start_date = datetime(2022, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(730)]

    # Create spatial coordinates (100x100 grid)
    lat = np.linspace(40, 45, 100)
    lon = np.linspace(-120, -115, 100)

    # Generate realistic precipitation data (mostly dry days with occasional rain)
    np.random.seed(42)

    # 70% dry days, 30% wet days
    wet_day_mask = np.random.rand(730, 100, 100) < 0.3

    # Wet days: log-normal distribution (realistic precipitation)
    precip_amounts = np.random.lognormal(mean=1.5, sigma=1.0, size=(730, 100, 100))
    precip_amounts = np.clip(precip_amounts, 0, 150)  # Cap at 150mm

    pr = np.where(wet_day_mask, precip_amounts, 0.0).astype(np.float32)

    # Create dataset
    ds = xr.Dataset(
        {
            'pr': (['time', 'lat', 'lon'], pr),
        },
        coords={
            'time': dates,
            'lat': lat,
            'lon': lon,
        }
    )

    # Add CF-compliant attributes
    ds['pr'].attrs = {'units': 'mm d-1', 'standard_name': 'precipitation_flux', 'long_name': 'Precipitation'}

    # Save to Zarr with chunking
    ds.chunk({'time': 365, 'lat': 50, 'lon': 50}).to_zarr(zarr_dir, mode='w')

    return zarr_dir


@pytest.fixture(scope="session")
def test_baseline_percentiles(tmp_path_factory):
    """
    Create test baseline percentiles file for extreme indices.

    Generates a small baseline percentiles file matching test data dimensions.

    Returns:
        Path to baseline percentiles NetCDF file
    """
    baseline_dir = tmp_path_factory.mktemp("test_data") / "baselines"
    baseline_dir.mkdir(exist_ok=True)
    baseline_file = baseline_dir / "baseline_percentiles_test.nc"

    # Create spatial coordinates matching test data
    lat = np.linspace(40, 45, 100)
    lon = np.linspace(-120, -115, 100)
    dayofyear = np.arange(1, 366)  # 365 days

    # Generate realistic baseline thresholds
    np.random.seed(42)

    # Temperature percentiles (seasonal cycle)
    seasonal_cycle = 15 * np.sin(2 * np.pi * (dayofyear - 80) / 365)

    tx90p = 25 + seasonal_cycle[:, np.newaxis, np.newaxis] + np.random.randn(365, 100, 100) * 2
    tx10p = 5 + seasonal_cycle[:, np.newaxis, np.newaxis] + np.random.randn(365, 100, 100) * 2
    tn90p = 18 + seasonal_cycle[:, np.newaxis, np.newaxis] + np.random.randn(365, 100, 100) * 2
    tn10p = -2 + seasonal_cycle[:, np.newaxis, np.newaxis] + np.random.randn(365, 100, 100) * 2

    # Precipitation percentiles (constant across days, spatially variable)
    pr95p = 20 + np.random.randn(365, 100, 100) * 3
    pr95p = np.maximum(pr95p, 5)  # Minimum 5mm threshold

    pr99p = 40 + np.random.randn(365, 100, 100) * 5
    pr99p = np.maximum(pr99p, 15)  # Minimum 15mm threshold

    # Create dataset
    ds = xr.Dataset(
        {
            'tx90p_threshold': (['dayofyear', 'lat', 'lon'], tx90p.astype(np.float32)),
            'tx10p_threshold': (['dayofyear', 'lat', 'lon'], tx10p.astype(np.float32)),
            'tn90p_threshold': (['dayofyear', 'lat', 'lon'], tn90p.astype(np.float32)),
            'tn10p_threshold': (['dayofyear', 'lat', 'lon'], tn10p.astype(np.float32)),
            'pr95p_threshold': (['dayofyear', 'lat', 'lon'], pr95p.astype(np.float32)),
            'pr99p_threshold': (['dayofyear', 'lat', 'lon'], pr99p.astype(np.float32)),
        },
        coords={
            'dayofyear': dayofyear,
            'lat': lat,
            'lon': lon,
        }
    )

    # Add attributes
    for var in ds.data_vars:
        ds[var].attrs = {'units': 'degC' if var.startswith('t') else 'mm d-1'}

    # Save to NetCDF
    ds.to_netcdf(baseline_file)

    return baseline_file


@pytest.fixture
def tmp_output_dir(tmp_path):
    """
    Create temporary output directory for each test.

    Returns:
        Path to temporary output directory
    """
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def mock_pipeline_config(test_zarr_store_temperature, test_zarr_store_precipitation, test_baseline_percentiles, monkeypatch):
    """
    Mock PipelineConfig to use test data paths instead of production paths.

    Args:
        test_zarr_store_temperature: Fixture providing test temperature Zarr
        test_zarr_store_precipitation: Fixture providing test precipitation Zarr
        test_baseline_percentiles: Fixture providing test baseline file
        monkeypatch: pytest monkeypatch fixture
    """
    from core.config import PipelineConfig

    # Mock Zarr paths
    monkeypatch.setattr(PipelineConfig, 'TEMP_ZARR', str(test_zarr_store_temperature))
    monkeypatch.setattr(PipelineConfig, 'PRECIP_ZARR', str(test_zarr_store_precipitation))
    monkeypatch.setattr(PipelineConfig, 'BASELINE_FILE', str(test_baseline_percentiles))

    # Mock chunk config for smaller test data
    monkeypatch.setattr(PipelineConfig, 'DEFAULT_CHUNKS', {
        'time': 365,
        'lat': 50,
        'lon': 50
    })


@pytest.fixture
def small_test_dataset():
    """
    Create a minimal in-memory test dataset for quick unit tests.

    Returns:
        xarray.Dataset with minimal temperature data
    """
    dates = [datetime(2023, 1, 1) + timedelta(days=i) for i in range(365)]
    lat = np.linspace(40, 45, 50)
    lon = np.linspace(-120, -115, 50)

    np.random.seed(42)
    tas = 15 + np.random.randn(365, 50, 50) * 5
    tasmax = tas + 5 + np.random.randn(365, 50, 50) * 2
    tasmin = tas - 5 + np.random.randn(365, 50, 50) * 2

    ds = xr.Dataset(
        {
            'tas': (['time', 'lat', 'lon'], tas.astype(np.float32)),
            'tasmax': (['time', 'lat', 'lon'], tasmax.astype(np.float32)),
            'tasmin': (['time', 'lat', 'lon'], tasmin.astype(np.float32)),
        },
        coords={'time': dates, 'lat': lat, 'lon': lon}
    )

    for var in ds.data_vars:
        ds[var].attrs = {'units': 'degC', 'standard_name': 'air_temperature'}

    return ds


@pytest.fixture
def cleanup_temp_files():
    """
    Cleanup fixture to ensure temporary tile files are removed after tests.

    Yields and then cleans up any tile_*.nc files in the outputs directory.
    """
    yield

    # Cleanup after test
    output_dir = Path('./outputs')
    if output_dir.exists():
        for tile_file in output_dir.glob('tile_*.nc'):
            try:
                tile_file.unlink()
            except Exception:
                pass  # Best effort cleanup

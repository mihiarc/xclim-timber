"""
Shared pytest fixtures and test utilities for xclim-timber test suite.

Provides reusable test data generators, mock objects, and common fixtures
for all unit and integration tests.
"""

import pytest
import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple
from datetime import datetime, timedelta


# ==================== Test Data Generators ====================

def create_test_temperature_dataset(
    n_time: int = 365,
    n_lat: int = 10,
    n_lon: int = 10,
    start_date: str = '2020-01-01',
    seed: int = 42
) -> xr.Dataset:
    """
    Create a small test temperature dataset with realistic values.

    Args:
        n_time: Number of time steps (default: 365 for 1 year)
        n_lat: Number of latitude points
        n_lon: Number of longitude points
        start_date: Start date for time coordinate
        seed: Random seed for reproducibility

    Returns:
        xarray Dataset with tas, tasmax, tasmin variables
    """
    np.random.seed(seed)

    # Create coordinate arrays
    time = pd.date_range(start_date, periods=n_time, freq='D')
    lat = np.linspace(37.0, 49.0, n_lat)  # CONUS-like latitude range
    lon = np.linspace(-124.0, -67.0, n_lon)  # CONUS-like longitude range

    # Generate realistic temperature data (in Celsius)
    # Mean temperature: 15°C with seasonal variation
    seasonal_cycle = 10 * np.sin(2 * np.pi * np.arange(n_time) / 365)
    tas_base = 15 + seasonal_cycle[:, np.newaxis, np.newaxis]
    tas_data = tas_base + np.random.randn(n_time, n_lat, n_lon) * 2

    # Maximum temperature: ~5°C above mean
    tasmax_data = tas_data + 5 + np.random.randn(n_time, n_lat, n_lon) * 1

    # Minimum temperature: ~5°C below mean
    tasmin_data = tas_data - 5 + np.random.randn(n_time, n_lat, n_lon) * 1

    # Create dataset
    ds = xr.Dataset(
        {
            'tas': (['time', 'lat', 'lon'], tas_data),
            'tasmax': (['time', 'lat', 'lon'], tasmax_data),
            'tasmin': (['time', 'lat', 'lon'], tasmin_data)
        },
        coords={
            'time': time,
            'lat': lat,
            'lon': lon
        }
    )

    # Add CF-compliant attributes
    ds['tas'].attrs = {
        'units': 'degC',
        'long_name': 'Mean air temperature',
        'standard_name': 'air_temperature'
    }
    ds['tasmax'].attrs = {
        'units': 'degC',
        'long_name': 'Maximum air temperature',
        'standard_name': 'air_temperature'
    }
    ds['tasmin'].attrs = {
        'units': 'degC',
        'long_name': 'Minimum air temperature',
        'standard_name': 'air_temperature'
    }

    return ds


def create_test_precipitation_dataset(
    n_time: int = 365,
    n_lat: int = 10,
    n_lon: int = 10,
    start_date: str = '2020-01-01',
    seed: int = 42
) -> xr.Dataset:
    """
    Create a small test precipitation dataset with realistic values.

    Args:
        n_time: Number of time steps (default: 365 for 1 year)
        n_lat: Number of latitude points
        n_lon: Number of longitude points
        start_date: Start date for time coordinate
        seed: Random seed for reproducibility

    Returns:
        xarray Dataset with pr variable
    """
    np.random.seed(seed)

    # Create coordinate arrays
    time = pd.date_range(start_date, periods=n_time, freq='D')
    lat = np.linspace(37.0, 49.0, n_lat)
    lon = np.linspace(-124.0, -67.0, n_lon)

    # Generate realistic precipitation data (in mm/day)
    # Use exponential distribution for realistic precipitation (many dry days, few wet days)
    pr_data = np.random.exponential(scale=3.0, size=(n_time, n_lat, n_lon))
    # 70% of days are dry (< 1mm)
    dry_days = np.random.rand(n_time, n_lat, n_lon) > 0.3
    pr_data[dry_days] = 0.0

    # Create dataset
    ds = xr.Dataset(
        {
            'pr': (['time', 'lat', 'lon'], pr_data)
        },
        coords={
            'time': time,
            'lat': lat,
            'lon': lon
        }
    )

    # Add CF-compliant attributes
    ds['pr'].attrs = {
        'units': 'mm d-1',
        'long_name': 'Precipitation',
        'standard_name': 'precipitation_flux'
    }

    return ds


def create_test_baseline_percentiles(
    n_lat: int = 10,
    n_lon: int = 10,
    seed: int = 42
) -> xr.Dataset:
    """
    Create test baseline percentiles for extreme indices.

    Args:
        n_lat: Number of latitude points
        n_lon: Number of longitude points
        seed: Random seed for reproducibility

    Returns:
        xarray Dataset with baseline percentile thresholds
    """
    np.random.seed(seed)

    # Create coordinate arrays
    dayofyear = np.arange(1, 367)  # 366 days (including leap day)
    lat = np.linspace(37.0, 49.0, n_lat)
    lon = np.linspace(-124.0, -67.0, n_lon)

    # Generate realistic percentile thresholds
    # Temperature percentiles (in Celsius)
    tx90p = 25 + np.random.randn(366, n_lat, n_lon) * 3  # 90th percentile of tasmax
    tx10p = 5 + np.random.randn(366, n_lat, n_lon) * 3   # 10th percentile of tasmax
    tn90p = 15 + np.random.randn(366, n_lat, n_lon) * 3  # 90th percentile of tasmin
    tn10p = -5 + np.random.randn(366, n_lat, n_lon) * 3  # 10th percentile of tasmin

    # Precipitation percentiles (in mm/day)
    pr95p = 20 + np.random.randn(366, n_lat, n_lon) * 5  # 95th percentile
    pr99p = 40 + np.random.randn(366, n_lat, n_lon) * 10  # 99th percentile
    pr75p = 10 + np.random.randn(366, n_lat, n_lon) * 3  # 75th percentile

    # Multivariate percentiles
    tas25p = 5 + np.random.randn(366, n_lat, n_lon) * 3   # 25th percentile of tas
    tas75p = 20 + np.random.randn(366, n_lat, n_lon) * 3  # 75th percentile of tas
    pr25p = 2 + np.random.randn(366, n_lat, n_lon) * 1    # 25th percentile of pr

    # Create dataset
    ds = xr.Dataset(
        {
            'tx90p_threshold': (['dayofyear', 'lat', 'lon'], tx90p),
            'tx10p_threshold': (['dayofyear', 'lat', 'lon'], tx10p),
            'tn90p_threshold': (['dayofyear', 'lat', 'lon'], tn90p),
            'tn10p_threshold': (['dayofyear', 'lat', 'lon'], tn10p),
            'pr95p_threshold': (['dayofyear', 'lat', 'lon'], pr95p),
            'pr99p_threshold': (['dayofyear', 'lat', 'lon'], pr99p),
            'pr_75p_threshold': (['dayofyear', 'lat', 'lon'], pr75p),
            'tas_25p_threshold': (['dayofyear', 'lat', 'lon'], tas25p),
            'tas_75p_threshold': (['dayofyear', 'lat', 'lon'], tas75p),
            'pr_25p_threshold': (['dayofyear', 'lat', 'lon'], pr25p)
        },
        coords={
            'dayofyear': dayofyear,
            'lat': lat,
            'lon': lon
        }
    )

    # Add global attributes
    ds.attrs['baseline_period'] = '1981-2000'
    ds.attrs['creation_date'] = datetime.now().isoformat()
    ds.attrs['description'] = 'Test baseline percentiles for xclim-timber unit tests'

    return ds


def create_test_zarr_store(tmp_path: Path, dataset: xr.Dataset, store_name: str) -> str:
    """
    Create a temporary Zarr store for testing.

    Args:
        tmp_path: Pytest tmp_path fixture
        dataset: Dataset to save as Zarr
        store_name: Name of the Zarr store

    Returns:
        Path to the created Zarr store
    """
    zarr_path = tmp_path / store_name
    dataset.to_zarr(zarr_path, mode='w')
    return str(zarr_path)


def create_known_temperature_dataset() -> Tuple[xr.Dataset, Dict[str, float]]:
    """
    Create a temperature dataset with known expected values for verification testing.

    Returns:
        Tuple of (dataset, expected_values_dict)
    """
    # Create 5 days with specific temperature values
    time = pd.date_range('2020-01-01', periods=5, freq='D')
    lat = np.array([40.0])
    lon = np.array([-100.0])

    # Specific temperature values (in Celsius)
    tasmin_values = np.array([-5, -2, 0, 2, 5])[:, np.newaxis, np.newaxis]
    tasmax_values = np.array([5, 8, 10, 12, 15])[:, np.newaxis, np.newaxis]
    tas_values = (tasmin_values + tasmax_values) / 2

    ds = xr.Dataset(
        {
            'tas': (['time', 'lat', 'lon'], tas_values),
            'tasmax': (['time', 'lat', 'lon'], tasmax_values),
            'tasmin': (['time', 'lat', 'lon'], tasmin_values)
        },
        coords={
            'time': time,
            'lat': lat,
            'lon': lon
        }
    )

    # Add CF-compliant attributes
    for var in ['tas', 'tasmax', 'tasmin']:
        ds[var].attrs = {
            'units': 'degC',
            'standard_name': 'air_temperature'
        }

    # Expected values for various indices
    expected = {
        'frost_days': 3,  # Days with tasmin < 0°C
        'ice_days': 0,    # Days with tasmax < 0°C
        'summer_days': 0, # Days with tasmax > 25°C
        'mean_tas': 5.0   # Mean temperature across all days
    }

    return ds, expected


def create_known_precipitation_dataset() -> Tuple[xr.Dataset, Dict[str, float]]:
    """
    Create a precipitation dataset with known expected values for verification testing.

    Returns:
        Tuple of (dataset, expected_values_dict)
    """
    # Create 10 days with specific precipitation values
    time = pd.date_range('2020-01-01', periods=10, freq='D')
    lat = np.array([40.0])
    lon = np.array([-100.0])

    # Specific precipitation values (in mm/day)
    # Pattern: 5 dry days, 3 wet days, 2 dry days
    pr_values = np.array([0, 0, 0, 0, 0, 10, 20, 15, 0, 0])[:, np.newaxis, np.newaxis]

    ds = xr.Dataset(
        {
            'pr': (['time', 'lat', 'lon'], pr_values)
        },
        coords={
            'time': time,
            'lat': lat,
            'lon': lon
        }
    )

    ds['pr'].attrs = {
        'units': 'mm d-1',
        'standard_name': 'precipitation_flux'
    }

    # Expected values for various indices
    expected = {
        'prcptot': 45.0,  # Total precipitation
        'cwd': 3,         # Maximum consecutive wet days (>= 1mm)
        'cdd': 5,         # Maximum consecutive dry days (< 1mm)
        'r10mm': 3,       # Days with >= 10mm
        'wet_days': 3     # Total wet days
    }

    return ds, expected


# ==================== Pytest Fixtures ====================

@pytest.fixture
def sample_temperature_dataset():
    """Fixture providing a small temperature dataset for testing."""
    return create_test_temperature_dataset()


@pytest.fixture
def sample_precipitation_dataset():
    """Fixture providing a small precipitation dataset for testing."""
    return create_test_precipitation_dataset()


@pytest.fixture
def sample_baseline_percentiles():
    """Fixture providing baseline percentiles for testing."""
    return create_test_baseline_percentiles()


@pytest.fixture
def known_temperature_data():
    """Fixture providing temperature data with known expected values."""
    return create_known_temperature_dataset()


@pytest.fixture
def known_precipitation_data():
    """Fixture providing precipitation data with known expected values."""
    return create_known_precipitation_dataset()


@pytest.fixture
def temp_zarr_store(tmp_path, sample_temperature_dataset):
    """Fixture providing a temporary temperature Zarr store."""
    return create_test_zarr_store(tmp_path, sample_temperature_dataset, 'temperature.zarr')


@pytest.fixture
def precip_zarr_store(tmp_path, sample_precipitation_dataset):
    """Fixture providing a temporary precipitation Zarr store."""
    return create_test_zarr_store(tmp_path, sample_precipitation_dataset, 'precipitation.zarr')


@pytest.fixture
def baseline_file(tmp_path, sample_baseline_percentiles):
    """Fixture providing a temporary baseline percentiles file."""
    baseline_path = tmp_path / 'baseline_percentiles_test.nc'
    sample_baseline_percentiles.to_netcdf(baseline_path)
    return baseline_path


@pytest.fixture
def mock_pipeline_config(monkeypatch, temp_zarr_store, precip_zarr_store, baseline_file):
    """Fixture that patches PipelineConfig to use test data paths."""
    from core.config import PipelineConfig

    monkeypatch.setattr(PipelineConfig, 'TEMP_ZARR', temp_zarr_store)
    monkeypatch.setattr(PipelineConfig, 'PRECIP_ZARR', precip_zarr_store)
    monkeypatch.setattr(PipelineConfig, 'BASELINE_FILE', str(baseline_file))

    return PipelineConfig


@pytest.fixture
def temp_output_dir(tmp_path):
    """Fixture providing a temporary output directory."""
    output_dir = tmp_path / 'outputs'
    output_dir.mkdir()
    return output_dir


@pytest.fixture
def mock_logger(mocker):
    """Fixture providing a mock logger for testing logging calls."""
    return mocker.patch('logging.Logger')


# ==================== Test Utilities ====================

def assert_dataarray_valid(da: xr.DataArray, name: str):
    """
    Assert that a DataArray is valid for climate index results.

    Args:
        da: DataArray to validate
        name: Name of the index (for error messages)
    """
    assert isinstance(da, xr.DataArray), f"{name} must be a DataArray"
    assert not da.isnull().all(), f"{name} contains all NaN values"
    assert 'time' in da.dims, f"{name} must have time dimension"
    assert 'lat' in da.dims, f"{name} must have lat dimension"
    assert 'lon' in da.dims, f"{name} must have lon dimension"
    assert 'units' in da.attrs, f"{name} must have units attribute"


def assert_dataset_has_indices(ds: xr.Dataset, expected_indices: list):
    """
    Assert that a dataset contains all expected climate indices.

    Args:
        ds: Dataset to validate
        expected_indices: List of expected variable names
    """
    for idx_name in expected_indices:
        assert idx_name in ds.data_vars, f"Missing expected index: {idx_name}"
        assert_dataarray_valid(ds[idx_name], idx_name)


def assert_netcdf_file_valid(file_path: Path):
    """
    Assert that a NetCDF file is valid and can be opened.

    Args:
        file_path: Path to NetCDF file
    """
    assert file_path.exists(), f"NetCDF file does not exist: {file_path}"
    assert file_path.stat().st_size > 0, f"NetCDF file is empty: {file_path}"

    # Try to open and validate structure
    ds = xr.open_dataset(file_path)
    assert len(ds.data_vars) > 0, f"NetCDF file has no data variables: {file_path}"
    assert 'creation_date' in ds.attrs, "NetCDF file missing creation_date metadata"
    ds.close()

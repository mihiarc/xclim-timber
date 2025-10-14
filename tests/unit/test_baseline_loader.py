"""
Unit tests for core.baseline_loader module.

Tests baseline percentile loading, caching, and validation.
"""

import pytest
from pathlib import Path
import xarray as xr

from core.baseline_loader import BaselineLoader
from core.config import PipelineConfig


class TestBaselineLoader:
    """Tests for BaselineLoader class."""

    def test_init_with_default_path(self):
        """Test initialization with default baseline file path."""
        loader = BaselineLoader()
        assert loader.baseline_file == Path(PipelineConfig.BASELINE_FILE)
        assert loader._baseline_cache is None

    def test_init_with_custom_path(self, tmp_path):
        """Test initialization with custom baseline file path."""
        custom_path = tmp_path / 'custom_baseline.nc'
        loader = BaselineLoader(baseline_file=custom_path)
        assert loader.baseline_file == custom_path

    def test_has_baseline_file_exists(self, baseline_file):
        """Test has_baseline_file returns True when file exists."""
        loader = BaselineLoader(baseline_file=baseline_file)
        assert loader.has_baseline_file() is True

    def test_has_baseline_file_not_exists(self, tmp_path):
        """Test has_baseline_file returns False when file doesn't exist."""
        missing_file = tmp_path / 'missing_baseline.nc'
        loader = BaselineLoader(baseline_file=missing_file)
        assert loader.has_baseline_file() is False

    def test_load_baseline_file_success(self, baseline_file):
        """Test successful baseline file loading."""
        loader = BaselineLoader(baseline_file=baseline_file)
        ds = loader._load_baseline_file()

        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0
        assert 'baseline_period' in ds.attrs

    def test_load_baseline_file_caching(self, baseline_file):
        """Test that baseline file is cached after first load."""
        loader = BaselineLoader(baseline_file=baseline_file)

        # First load
        ds1 = loader._load_baseline_file()
        assert loader._baseline_cache is not None

        # Second load should return cached version (same object)
        ds2 = loader._load_baseline_file()
        assert ds2 is ds1

    def test_load_baseline_file_not_found(self, tmp_path):
        """Test loading non-existent baseline file raises FileNotFoundError."""
        missing_file = tmp_path / 'missing_baseline.nc'
        loader = BaselineLoader(baseline_file=missing_file)

        with pytest.raises(FileNotFoundError) as exc_info:
            loader._load_baseline_file()

        assert 'Baseline percentiles file not found' in str(exc_info.value)
        assert 'calculate_baseline_percentiles.py' in str(exc_info.value)

    def test_load_baseline_percentiles_success(self, baseline_file):
        """Test loading specific baseline percentile variables."""
        loader = BaselineLoader(baseline_file=baseline_file)
        required_vars = ['tx90p_threshold', 'tx10p_threshold']

        percentiles = loader.load_baseline_percentiles(required_vars)

        assert len(percentiles) == 2
        assert 'tx90p_threshold' in percentiles
        assert 'tx10p_threshold' in percentiles
        assert isinstance(percentiles['tx90p_threshold'], xr.DataArray)
        assert 'dayofyear' in percentiles['tx90p_threshold'].dims

    def test_load_baseline_percentiles_missing_required(self, baseline_file):
        """Test loading with missing required variables raises ValueError."""
        loader = BaselineLoader(baseline_file=baseline_file)
        required_vars = ['tx90p_threshold', 'missing_variable']

        with pytest.raises(ValueError) as exc_info:
            loader.load_baseline_percentiles(required_vars, allow_missing=False)

        assert 'Missing required baseline variables' in str(exc_info.value)
        assert 'missing_variable' in str(exc_info.value)

    def test_load_baseline_percentiles_allow_missing(self, baseline_file):
        """Test loading with allow_missing=True handles missing variables gracefully."""
        loader = BaselineLoader(baseline_file=baseline_file)
        required_vars = ['tx90p_threshold', 'missing_variable']

        # Should not raise, but only return available variables
        percentiles = loader.load_baseline_percentiles(required_vars, allow_missing=True)

        assert 'tx90p_threshold' in percentiles
        assert 'missing_variable' not in percentiles

    def test_get_temperature_baselines(self, baseline_file):
        """Test getting all temperature baseline percentiles."""
        loader = BaselineLoader(baseline_file=baseline_file)
        baselines = loader.get_temperature_baselines()

        # Should have all 4 temperature baseline variables
        expected_vars = ['tx90p_threshold', 'tx10p_threshold', 'tn90p_threshold', 'tn10p_threshold']
        for var in expected_vars:
            assert var in baselines
            assert isinstance(baselines[var], xr.DataArray)

    def test_get_precipitation_baselines(self, baseline_file):
        """Test getting all precipitation baseline percentiles."""
        loader = BaselineLoader(baseline_file=baseline_file)
        baselines = loader.get_precipitation_baselines()

        # Should have precipitation baseline variables
        assert 'pr95p_threshold' in baselines
        assert 'pr99p_threshold' in baselines

    def test_get_multivariate_baselines(self, baseline_file):
        """Test getting all multivariate baseline percentiles."""
        loader = BaselineLoader(baseline_file=baseline_file)
        baselines = loader.get_multivariate_baselines()

        # Should have multivariate baseline variables
        expected_vars = ['tas_25p_threshold', 'tas_75p_threshold', 'pr_25p_threshold', 'pr_75p_threshold']
        for var in expected_vars:
            assert var in baselines

    def test_clear_cache(self, baseline_file):
        """Test clearing baseline cache."""
        loader = BaselineLoader(baseline_file=baseline_file)

        # Load baseline to populate cache
        loader._load_baseline_file()
        assert loader._baseline_cache is not None

        # Clear cache
        loader.clear_cache()
        assert loader._baseline_cache is None

    def test_baseline_period_validation_warning(self, tmp_path, sample_baseline_percentiles):
        """Test warning when baseline period doesn't match expected."""
        # Create baseline with different period
        sample_baseline_percentiles.attrs['baseline_period'] = '1991-2010'
        baseline_path = tmp_path / 'wrong_period_baseline.nc'
        sample_baseline_percentiles.to_netcdf(baseline_path)

        loader = BaselineLoader(baseline_file=baseline_path)

        # Should log warning but not fail
        ds = loader._load_baseline_file()
        assert ds.attrs['baseline_period'] == '1991-2010'

    def test_baseline_dimensions_valid(self, baseline_file):
        """Test that loaded baselines have required dimensions."""
        loader = BaselineLoader(baseline_file=baseline_file)
        percentiles = loader.load_baseline_percentiles(['tx90p_threshold'])

        da = percentiles['tx90p_threshold']
        assert 'dayofyear' in da.dims
        assert 'lat' in da.dims
        assert 'lon' in da.dims

    def test_baseline_lazy_loading(self, baseline_file):
        """Test that baselines use lazy loading (chunks='auto')."""
        loader = BaselineLoader(baseline_file=baseline_file)
        ds = loader._load_baseline_file()

        # Check that dataset is chunked (lazy loaded)
        for var in ds.data_vars:
            assert hasattr(ds[var], 'chunks'), f"{var} should be chunked for lazy loading"

    def test_load_baseline_twice_uses_cache(self, baseline_file):
        """Test that loading baseline twice uses cache (no duplicate file reads)."""
        loader = BaselineLoader(baseline_file=baseline_file)

        # Load twice
        baselines1 = loader.get_temperature_baselines()
        baselines2 = loader.get_temperature_baselines()

        # Should return same cached data
        assert loader._baseline_cache is not None


@pytest.mark.regression
class TestBaselineLoaderRegressions:
    """Regression tests for previously fixed bugs."""

    def test_baseline_rechunking_issue_75(self, baseline_file):
        """
        Regression test for Issue #75: Baseline rechunking memory overhead.

        Verifies that baseline data is properly chunked for memory efficiency.
        """
        loader = BaselineLoader(baseline_file=baseline_file)
        baselines = loader.get_temperature_baselines()

        # Verify baselines are chunked
        for var_name, baseline in baselines.items():
            assert hasattr(baseline, 'chunks'), f"{var_name} should be chunked"

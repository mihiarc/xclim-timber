"""
Unit tests for precipitation index calculations.

Tests individual precipitation indices using known input/output pairs.
"""

import pytest
import xarray as xr
import numpy as np
import xclim.indicators.atmos as atmos


class TestPrecipitationIndices:
    """Tests for basic precipitation indices."""

    def test_prcptot_calculation(self, known_precipitation_data):
        """Test total precipitation calculation with known values."""
        ds, expected = known_precipitation_data

        result = atmos.prcptot(ds.pr, freq='YS')

        # Expected: 45.0 mm total
        assert np.isclose(result.values[0], expected['prcptot'], atol=0.1), \
            f"Expected {expected['prcptot']} mm, got {result.values[0]} mm"

    def test_cwd_calculation(self, known_precipitation_data):
        """Test consecutive wet days calculation with known values."""
        ds, expected = known_precipitation_data

        result = atmos.maximum_consecutive_wet_days(ds.pr, thresh='1 mm d-1', freq='YS')

        # Expected: 3 consecutive wet days
        assert result.values[0] == expected['cwd'], \
            f"Expected {expected['cwd']} consecutive wet days, got {result.values[0]}"

    def test_cdd_calculation(self, known_precipitation_data):
        """Test consecutive dry days calculation with known values."""
        ds, expected = known_precipitation_data

        result = atmos.maximum_consecutive_dry_days(ds.pr, thresh='1 mm d-1', freq='YS')

        # Expected: 5 consecutive dry days
        assert result.values[0] == expected['cdd'], \
            f"Expected {expected['cdd']} consecutive dry days, got {result.values[0]}"

    def test_r10mm_calculation(self, known_precipitation_data):
        """Test days with >= 10mm precipitation."""
        ds, expected = known_precipitation_data

        result = atmos.wetdays(ds.pr, thresh='10 mm d-1', freq='YS')

        # Expected: 3 days with >= 10mm
        assert result.values[0] == expected['r10mm'], \
            f"Expected {expected['r10mm']} days, got {result.values[0]}"

    def test_wetdays_calculation(self, known_precipitation_data):
        """Test total wet days calculation."""
        ds, expected = known_precipitation_data

        result = atmos.wetdays(ds.pr, thresh='1 mm d-1', freq='YS')

        # Expected: 3 wet days
        assert result.values[0] == expected['wet_days'], \
            f"Expected {expected['wet_days']} wet days, got {result.values[0]}"

    def test_sdii_calculation(self, sample_precipitation_dataset):
        """Test simple daily intensity index (SDII)."""
        result = atmos.daily_pr_intensity(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_rx1day_calculation(self, sample_precipitation_dataset):
        """Test maximum 1-day precipitation."""
        result = atmos.max_1day_precipitation_amount(sample_precipitation_dataset.pr, freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.attrs['units'] == 'mm d-1'
        assert result.values[0] >= 0

    def test_rx5day_calculation(self, sample_precipitation_dataset):
        """Test maximum 5-day precipitation."""
        result = atmos.max_n_day_precipitation_amount(sample_precipitation_dataset.pr, window=5, freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0


class TestExtremePrecipitationIndices:
    """Tests for percentile-based extreme precipitation indices."""

    def test_r95p_calculation(self, sample_precipitation_dataset, sample_baseline_percentiles):
        """Test very wet days (r95p) calculation."""
        result = atmos.days_over_precip_thresh(
            pr=sample_precipitation_dataset.pr,
            per=sample_baseline_percentiles['pr95p_threshold'],
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0
        assert result.values[0] <= 365

    def test_r99p_calculation(self, sample_precipitation_dataset, sample_baseline_percentiles):
        """Test extremely wet days (r99p) calculation."""
        result = atmos.days_over_precip_thresh(
            pr=sample_precipitation_dataset.pr,
            per=sample_baseline_percentiles['pr99p_threshold'],
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0
        assert result.values[0] <= 365

    def test_r95ptot_calculation(self, sample_precipitation_dataset, sample_baseline_percentiles):
        """Test precipitation from very wet days (r95ptot)."""
        result = atmos.fraction_over_precip_thresh(
            pr=sample_precipitation_dataset.pr,
            per=sample_baseline_percentiles['pr95p_threshold'],
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_r99ptot_calculation(self, sample_precipitation_dataset, sample_baseline_percentiles):
        """Test precipitation from extremely wet days (r99ptot)."""
        result = atmos.fraction_over_precip_thresh(
            pr=sample_precipitation_dataset.pr,
            per=sample_baseline_percentiles['pr99p_threshold'],
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0


class TestPrecipitationThresholdIndices:
    """Tests for precipitation threshold-based indices."""

    def test_r1mm_calculation(self, sample_precipitation_dataset):
        """Test days with >= 1mm precipitation."""
        result = atmos.wetdays(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0
        assert result.values[0] <= 365

    def test_r20mm_calculation(self, sample_precipitation_dataset):
        """Test days with >= 20mm precipitation."""
        result = atmos.wetdays(sample_precipitation_dataset.pr, thresh='20 mm d-1', freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_r50mm_calculation(self, sample_precipitation_dataset):
        """Test days with >= 50mm precipitation."""
        result = atmos.wetdays(sample_precipitation_dataset.pr, thresh='50 mm d-1', freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0


class TestPrecipitationIndicesValidation:
    """Validation tests for precipitation indices."""

    def test_indices_have_required_attributes(self, sample_precipitation_dataset):
        """Test that calculated indices have required attributes."""
        indices_to_test = [
            ('prcptot', atmos.prcptot(sample_precipitation_dataset.pr, freq='YS')),
            ('cwd', atmos.maximum_consecutive_wet_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS')),
            ('cdd', atmos.maximum_consecutive_dry_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS'))
        ]

        for name, result in indices_to_test:
            assert 'units' in result.attrs, f"{name} missing units attribute"
            assert isinstance(result, xr.DataArray), f"{name} should be DataArray"

    def test_indices_have_correct_dimensions(self, sample_precipitation_dataset):
        """Test that indices have correct dimensions."""
        result = atmos.prcptot(sample_precipitation_dataset.pr, freq='YS')

        assert 'time' in result.dims
        assert 'lat' in result.dims
        assert 'lon' in result.dims

    def test_count_indices_are_non_negative(self, sample_precipitation_dataset):
        """Test that count-based indices return non-negative values."""
        count_indices = [
            atmos.wetdays(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS'),
            atmos.maximum_consecutive_wet_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS'),
            atmos.maximum_consecutive_dry_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS')
        ]

        for result in count_indices:
            assert (result >= 0).all(), "Count indices must be non-negative"

    def test_total_precipitation_non_negative(self, sample_precipitation_dataset):
        """Test that total precipitation is non-negative."""
        result = atmos.prcptot(sample_precipitation_dataset.pr, freq='YS')

        assert (result >= 0).all(), "Total precipitation must be non-negative"

    def test_intensity_non_negative(self, sample_precipitation_dataset):
        """Test that precipitation intensity is non-negative."""
        result = atmos.daily_pr_intensity(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS')

        assert (result >= 0).all(), "Precipitation intensity must be non-negative"

    def test_max_precipitation_non_negative(self, sample_precipitation_dataset):
        """Test that max precipitation values are non-negative."""
        result = atmos.max_1day_precipitation_amount(sample_precipitation_dataset.pr, freq='YS')

        assert (result >= 0).all(), "Max precipitation must be non-negative"


class TestPrecipitationEdgeCases:
    """Tests for edge cases in precipitation calculations."""

    def test_all_dry_days(self):
        """Test indices with dataset containing all dry days."""
        # Create dataset with all zeros
        time = pd.date_range('2020-01-01', periods=365, freq='D')
        ds = xr.Dataset({
            'pr': (['time', 'lat', 'lon'], np.zeros((365, 1, 1)))
        }, coords={'time': time, 'lat': [40.0], 'lon': [-100.0]})
        ds['pr'].attrs['units'] = 'mm d-1'

        # All these should work with all-dry data
        prcptot = atmos.prcptot(ds.pr, freq='YS')
        assert prcptot.values[0] == 0

        wetdays = atmos.wetdays(ds.pr, thresh='1 mm d-1', freq='YS')
        assert wetdays.values[0] == 0

    def test_all_wet_days(self):
        """Test indices with dataset containing all wet days."""
        # Create dataset with constant precipitation
        time = pd.date_range('2020-01-01', periods=365, freq='D')
        ds = xr.Dataset({
            'pr': (['time', 'lat', 'lon'], np.ones((365, 1, 1)) * 10)
        }, coords={'time': time, 'lat': [40.0], 'lon': [-100.0]})
        ds['pr'].attrs['units'] = 'mm d-1'

        cdd = atmos.maximum_consecutive_dry_days(ds.pr, thresh='1 mm d-1', freq='YS')
        assert cdd.values[0] == 0

        wetdays = atmos.wetdays(ds.pr, thresh='1 mm d-1', freq='YS')
        assert wetdays.values[0] == 365

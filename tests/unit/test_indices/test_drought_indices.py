"""
Unit tests for drought index calculations.

Tests drought-related indices including SPI, dry spell calculations.
"""

import pytest
import xarray as xr
import numpy as np
import pandas as pd
import xclim.indicators.atmos as atmos


class TestDroughtIndices:
    """Tests for drought-related indices."""

    def test_dry_days_calculation(self, sample_precipitation_dataset):
        """Test dry days (< 1mm) calculation."""
        # Create inverse of wetdays for dry days count
        result = atmos.dry_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0
        assert result.values[0] <= 365

    def test_maximum_dry_spell_calculation(self, sample_precipitation_dataset):
        """Test maximum dry spell length calculation."""
        result = atmos.maximum_consecutive_dry_days(
            sample_precipitation_dataset.pr,
            thresh='1 mm d-1',
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0
        assert result.values[0] <= 365

    def test_dry_spell_frequency_calculation(self, sample_precipitation_dataset):
        """Test dry spell frequency calculation."""
        result = atmos.dry_spell_frequency(
            sample_precipitation_dataset.pr,
            thresh='1 mm d-1',
            window=5,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_dry_spell_total_length_calculation(self, sample_precipitation_dataset):
        """Test total length of dry spells."""
        result = atmos.dry_spell_total_length(
            sample_precipitation_dataset.pr,
            thresh='1 mm d-1',
            window=5,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0


class TestDroughtIndicesWithKnownValues:
    """Tests for drought indices with known input/output values."""

    def test_dry_spell_with_known_pattern(self):
        """Test dry spell calculation with known precipitation pattern."""
        # Create specific pattern: 10 dry days, 3 wet days, 7 dry days
        time = pd.date_range('2020-01-01', periods=20, freq='D')
        pr_values = np.zeros((20, 1, 1))
        pr_values[10:13, 0, 0] = 5.0  # 3 wet days in the middle

        ds = xr.Dataset({
            'pr': (['time', 'lat', 'lon'], pr_values)
        }, coords={'time': time, 'lat': [40.0], 'lon': [-100.0]})
        ds['pr'].attrs['units'] = 'mm d-1'

        # Maximum consecutive dry days should be 10
        result = atmos.maximum_consecutive_dry_days(ds.pr, thresh='1 mm d-1', freq='YS')
        assert result.values[0] == 10, f"Expected 10 dry days, got {result.values[0]}"

    def test_wet_spell_with_known_pattern(self):
        """Test wet spell calculation with known precipitation pattern."""
        # Create specific pattern: 5 wet days, 10 dry days, 8 wet days
        time = pd.date_range('2020-01-01', periods=23, freq='D')
        pr_values = np.zeros((23, 1, 1))
        pr_values[0:5, 0, 0] = 5.0   # 5 wet days
        pr_values[15:23, 0, 0] = 3.0  # 8 wet days

        ds = xr.Dataset({
            'pr': (['time', 'lat', 'lon'], pr_values)
        }, coords={'time': time, 'lat': [40.0], 'lon': [-100.0]})
        ds['pr'].attrs['units'] = 'mm d-1'

        # Maximum consecutive wet days should be 8
        result = atmos.maximum_consecutive_wet_days(ds.pr, thresh='1 mm d-1', freq='YS')
        assert result.values[0] == 8, f"Expected 8 wet days, got {result.values[0]}"


class TestDroughtIndicesValidation:
    """Validation tests for drought indices."""

    def test_drought_indices_have_required_attributes(self, sample_precipitation_dataset):
        """Test that drought indices have required attributes."""
        indices_to_test = [
            ('cdd', atmos.maximum_consecutive_dry_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS')),
            ('dry_days', atmos.dry_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS'))
        ]

        for name, result in indices_to_test:
            assert 'units' in result.attrs, f"{name} missing units attribute"
            assert isinstance(result, xr.DataArray), f"{name} should be DataArray"

    def test_drought_indices_non_negative(self, sample_precipitation_dataset):
        """Test that drought indices return non-negative values."""
        indices = [
            atmos.maximum_consecutive_dry_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS'),
            atmos.dry_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS'),
            atmos.dry_spell_frequency(sample_precipitation_dataset.pr, thresh='1 mm d-1', window=5, freq='YS')
        ]

        for result in indices:
            assert (result >= 0).all(), "Drought indices must be non-negative"

    def test_drought_indices_within_year_bounds(self, sample_precipitation_dataset):
        """Test that drought day counts don't exceed days in year."""
        indices = [
            atmos.maximum_consecutive_dry_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS'),
            atmos.dry_days(sample_precipitation_dataset.pr, thresh='1 mm d-1', freq='YS')
        ]

        for result in indices:
            assert (result <= 366).all(), "Drought day counts can't exceed 366 days"


class TestDroughtIndexEdgeCases:
    """Tests for edge cases in drought calculations."""

    def test_no_dry_spells(self):
        """Test with dataset having no dry spells (all wet)."""
        time = pd.date_range('2020-01-01', periods=365, freq='D')
        ds = xr.Dataset({
            'pr': (['time', 'lat', 'lon'], np.ones((365, 1, 1)) * 10)
        }, coords={'time': time, 'lat': [40.0], 'lon': [-100.0]})
        ds['pr'].attrs['units'] = 'mm d-1'

        result = atmos.maximum_consecutive_dry_days(ds.pr, thresh='1 mm d-1', freq='YS')
        assert result.values[0] == 0, "Should have no dry days"

    def test_all_dry(self):
        """Test with dataset that is completely dry."""
        time = pd.date_range('2020-01-01', periods=365, freq='D')
        ds = xr.Dataset({
            'pr': (['time', 'lat', 'lon'], np.zeros((365, 1, 1)))
        }, coords={'time': time, 'lat': [40.0], 'lon': [-100.0]})
        ds['pr'].attrs['units'] = 'mm d-1'

        result = atmos.maximum_consecutive_dry_days(ds.pr, thresh='1 mm d-1', freq='YS')
        assert result.values[0] == 365, "All days should be dry"

        dry_days = atmos.dry_days(ds.pr, thresh='1 mm d-1', freq='YS')
        assert dry_days.values[0] == 365, "All days should count as dry"

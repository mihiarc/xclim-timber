"""
Unit tests for temperature index calculations.

Tests individual temperature indices using known input/output pairs.
"""

import pytest
import xarray as xr
import numpy as np
import pandas as pd
import xclim.indicators.atmos as atmos


class TestTemperatureIndices:
    """Tests for basic temperature indices."""

    def test_frost_days_calculation(self, known_temperature_data):
        """Test frost days calculation with known values."""
        ds, expected = known_temperature_data

        result = atmos.frost_days(ds.tasmin, freq='YS')

        # Expected: 3 days with tasmin < 0°C (days 1, 2, 3)
        assert result.values[0] == expected['frost_days'], \
            f"Expected {expected['frost_days']} frost days, got {result.values[0]}"

    def test_ice_days_calculation(self, known_temperature_data):
        """Test ice days calculation with known values."""
        ds, expected = known_temperature_data

        result = atmos.ice_days(ds.tasmax, freq='YS')

        # Expected: 0 days with tasmax < 0°C
        assert result.values[0] == expected['ice_days'], \
            f"Expected {expected['ice_days']} ice days, got {result.values[0]}"

    def test_summer_days_calculation(self, known_temperature_data):
        """Test summer days calculation with known values."""
        ds, expected = known_temperature_data

        result = atmos.tx_days_above(ds.tasmax, thresh='25 degC', freq='YS')

        # Expected: 0 days with tasmax > 25°C
        assert result.values[0] == expected['summer_days'], \
            f"Expected {expected['summer_days']} summer days, got {result.values[0]}"

    def test_tg_mean_calculation(self, known_temperature_data):
        """Test annual mean temperature calculation."""
        ds, expected = known_temperature_data

        result = atmos.tg_mean(ds.tas, freq='YS')

        # Expected: mean of 5 days = 5.0°C
        assert np.isclose(result.values[0], expected['mean_tas'], atol=0.1), \
            f"Expected mean tas {expected['mean_tas']}, got {result.values[0]}"

    def test_tx_max_calculation(self, sample_temperature_dataset):
        """Test maximum temperature calculation."""
        result = atmos.tx_max(sample_temperature_dataset.tasmax, freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.attrs['units'] == 'degC'
        assert len(result) == 1  # One year
        assert result.values[0] > 0  # Should be positive

    def test_tn_min_calculation(self, sample_temperature_dataset):
        """Test minimum temperature calculation."""
        result = atmos.tn_min(sample_temperature_dataset.tasmin, freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.attrs['units'] == 'degC'
        assert len(result) == 1

    def test_tropical_nights_calculation(self, sample_temperature_dataset):
        """Test tropical nights calculation."""
        result = atmos.tropical_nights(sample_temperature_dataset.tasmin, freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0  # Count should be non-negative

    def test_hot_days_calculation(self, sample_temperature_dataset):
        """Test hot days (>30°C) calculation."""
        result = atmos.tx_days_above(sample_temperature_dataset.tasmax, thresh='30 degC', freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_consecutive_frost_days(self, sample_temperature_dataset):
        """Test consecutive frost days calculation."""
        result = atmos.consecutive_frost_days(sample_temperature_dataset.tasmin, freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_growing_degree_days(self, sample_temperature_dataset):
        """Test growing degree days calculation."""
        result = atmos.growing_degree_days(
            sample_temperature_dataset.tas,
            thresh='10 degC',
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.attrs['units'] == 'K d'
        assert result.values[0] >= 0

    def test_heating_degree_days(self, sample_temperature_dataset):
        """Test heating degree days calculation."""
        result = atmos.heating_degree_days(
            sample_temperature_dataset.tas,
            thresh='17 degC',
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_cooling_degree_days(self, sample_temperature_dataset):
        """Test cooling degree days calculation."""
        result = atmos.cooling_degree_days(
            sample_temperature_dataset.tas,
            thresh='18 degC',
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_freezing_degree_days(self, sample_temperature_dataset):
        """Test freezing degree days calculation."""
        result = atmos.freezing_degree_days(sample_temperature_dataset.tas, freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_daily_temperature_range(self, sample_temperature_dataset):
        """Test daily temperature range calculation."""
        result = atmos.daily_temperature_range(
            sample_temperature_dataset.tasmin,
            sample_temperature_dataset.tasmax,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] > 0  # Range should be positive

    def test_extreme_temperature_range(self, sample_temperature_dataset):
        """Test extreme temperature range calculation."""
        result = atmos.extreme_temperature_range(
            sample_temperature_dataset.tasmin,
            sample_temperature_dataset.tasmax,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] > 0

    def test_frost_season_length(self, sample_temperature_dataset):
        """Test frost season length calculation."""
        result = atmos.frost_season_length(sample_temperature_dataset.tasmin, freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_frost_free_season_length(self, sample_temperature_dataset):
        """Test frost-free season length calculation."""
        result = atmos.frost_free_season_length(sample_temperature_dataset.tasmin, freq='YS')

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0


class TestExtremeTemperatureIndices:
    """Tests for percentile-based extreme temperature indices."""

    def test_tx90p_warm_days(self, sample_temperature_dataset, sample_baseline_percentiles):
        """Test warm days (tx90p) calculation."""
        result = atmos.tx90p(
            tasmax=sample_temperature_dataset.tasmax,
            tasmax_per=sample_baseline_percentiles['tx90p_threshold'],
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0
        assert result.values[0] <= 365  # Can't exceed days in year

    def test_tx10p_cool_days(self, sample_temperature_dataset, sample_baseline_percentiles):
        """Test cool days (tx10p) calculation."""
        result = atmos.tx10p(
            tasmax=sample_temperature_dataset.tasmax,
            tasmax_per=sample_baseline_percentiles['tx10p_threshold'],
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0
        assert result.values[0] <= 365

    def test_tn90p_warm_nights(self, sample_temperature_dataset, sample_baseline_percentiles):
        """Test warm nights (tn90p) calculation."""
        result = atmos.tn90p(
            tasmin=sample_temperature_dataset.tasmin,
            tasmin_per=sample_baseline_percentiles['tn90p_threshold'],
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0
        assert result.values[0] <= 365

    def test_tn10p_cool_nights(self, sample_temperature_dataset, sample_baseline_percentiles):
        """Test cool nights (tn10p) calculation."""
        result = atmos.tn10p(
            tasmin=sample_temperature_dataset.tasmin,
            tasmin_per=sample_baseline_percentiles['tn10p_threshold'],
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0
        assert result.values[0] <= 365

    def test_warm_spell_duration_index(self, sample_temperature_dataset, sample_baseline_percentiles):
        """Test warm spell duration index (WSDI) calculation."""
        result = atmos.warm_spell_duration_index(
            tasmax=sample_temperature_dataset.tasmax,
            tasmax_per=sample_baseline_percentiles['tx90p_threshold'],
            window=6,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_cold_spell_duration_index(self, sample_temperature_dataset, sample_baseline_percentiles):
        """Test cold spell duration index (CSDI) calculation."""
        result = atmos.cold_spell_duration_index(
            tasmin=sample_temperature_dataset.tasmin,
            tasmin_per=sample_baseline_percentiles['tn10p_threshold'],
            window=6,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0


class TestAdvancedTemperatureIndices:
    """Tests for advanced temperature indices (Phase 7 & 9)."""

    def test_growing_season_start(self, sample_temperature_dataset):
        """Test growing season start calculation."""
        result = atmos.growing_season_start(
            tas=sample_temperature_dataset.tas,
            thresh='5 degC',
            window=5,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)

    def test_growing_season_end(self, sample_temperature_dataset):
        """Test growing season end calculation."""
        result = atmos.growing_season_end(
            tas=sample_temperature_dataset.tas,
            thresh='5 degC',
            window=5,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)

    def test_cold_spell_frequency(self, sample_temperature_dataset):
        """Test cold spell frequency calculation."""
        result = atmos.cold_spell_frequency(
            tas=sample_temperature_dataset.tas,
            thresh='-10 degC',
            window=5,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_hot_spell_frequency(self, sample_temperature_dataset):
        """Test hot spell frequency calculation."""
        result = atmos.hot_spell_frequency(
            tasmax=sample_temperature_dataset.tasmax,
            thresh='30 degC',
            window=3,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_heat_wave_frequency(self, sample_temperature_dataset):
        """Test heat wave frequency calculation."""
        result = atmos.heat_wave_frequency(
            tasmin=sample_temperature_dataset.tasmin,
            tasmax=sample_temperature_dataset.tasmax,
            thresh_tasmin='22 degC',
            thresh_tasmax='30 degC',
            window=3,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_last_spring_frost(self, sample_temperature_dataset):
        """Test last spring frost calculation."""
        result = atmos.last_spring_frost(
            tasmin=sample_temperature_dataset.tasmin,
            thresh='0 degC',
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)

    def test_daily_temperature_range_variability(self, sample_temperature_dataset):
        """Test daily temperature range variability calculation."""
        result = atmos.daily_temperature_range_variability(
            tasmin=sample_temperature_dataset.tasmin,
            tasmax=sample_temperature_dataset.tasmax,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0

    def test_heat_wave_index(self, sample_temperature_dataset):
        """Test heat wave index calculation."""
        result = atmos.heat_wave_index(
            tasmax=sample_temperature_dataset.tasmax,
            thresh='25 degC',
            window=5,
            freq='YS'
        )

        assert isinstance(result, xr.DataArray)
        assert result.values[0] >= 0


class TestTemperatureIndicesValidation:
    """Validation tests for temperature indices."""

    def test_indices_have_required_attributes(self, sample_temperature_dataset):
        """Test that calculated indices have required attributes."""
        indices_to_test = [
            ('frost_days', atmos.frost_days(sample_temperature_dataset.tasmin, freq='YS')),
            ('tx_max', atmos.tx_max(sample_temperature_dataset.tasmax, freq='YS')),
            ('tg_mean', atmos.tg_mean(sample_temperature_dataset.tas, freq='YS'))
        ]

        for name, result in indices_to_test:
            assert 'units' in result.attrs, f"{name} missing units attribute"
            assert isinstance(result, xr.DataArray), f"{name} should be DataArray"

    def test_indices_have_correct_dimensions(self, sample_temperature_dataset):
        """Test that indices have correct dimensions."""
        result = atmos.frost_days(sample_temperature_dataset.tasmin, freq='YS')

        assert 'time' in result.dims
        assert 'lat' in result.dims
        assert 'lon' in result.dims

    def test_count_indices_are_non_negative(self, sample_temperature_dataset):
        """Test that count-based indices return non-negative values."""
        count_indices = [
            atmos.frost_days(sample_temperature_dataset.tasmin, freq='YS'),
            atmos.ice_days(sample_temperature_dataset.tasmax, freq='YS'),
            atmos.tx_days_above(sample_temperature_dataset.tasmax, thresh='25 degC', freq='YS')
        ]

        for result in count_indices:
            assert (result >= 0).all(), "Count indices must be non-negative"

    def test_degree_day_indices_are_non_negative(self, sample_temperature_dataset):
        """Test that degree day indices return non-negative values."""
        dd_indices = [
            atmos.growing_degree_days(sample_temperature_dataset.tas, thresh='10 degC', freq='YS'),
            atmos.heating_degree_days(sample_temperature_dataset.tas, thresh='17 degC', freq='YS'),
            atmos.cooling_degree_days(sample_temperature_dataset.tas, thresh='18 degC', freq='YS')
        ]

        for result in dd_indices:
            assert (result >= 0).all(), "Degree day indices must be non-negative"

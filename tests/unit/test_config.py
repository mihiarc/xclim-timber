"""
Unit tests for core.config module.

Tests configuration constants, paths, and utility methods.
"""

import pytest
from core.config import PipelineConfig


class TestPipelineConfig:
    """Tests for PipelineConfig class."""

    def test_default_chunks_structure(self):
        """Test that default chunk configuration has expected keys and values."""
        chunks = PipelineConfig.DEFAULT_CHUNKS

        assert 'time' in chunks
        assert 'lat' in chunks
        assert 'lon' in chunks

        assert chunks['time'] == 365, "Time chunks should be 365 (one year)"
        assert chunks['lat'] == 103, "Lat chunks should be 103"
        assert chunks['lon'] == 201, "Lon chunks should be 201"

    def test_temp_rename_map(self):
        """Test temperature variable rename mappings."""
        rename_map = PipelineConfig.TEMP_RENAME_MAP

        assert 'tmean' in rename_map
        assert rename_map['tmean'] == 'tas'
        assert rename_map['tmax'] == 'tasmax'
        assert rename_map['tmin'] == 'tasmin'

    def test_precip_rename_map(self):
        """Test precipitation variable rename mappings."""
        rename_map = PipelineConfig.PRECIP_RENAME_MAP

        assert 'ppt' in rename_map
        assert rename_map['ppt'] == 'pr'

    def test_humidity_rename_map(self):
        """Test humidity variable rename mappings."""
        rename_map = PipelineConfig.HUMIDITY_RENAME_MAP

        assert 'tdmean' in rename_map
        assert rename_map['tdmean'] == 'tdew'
        assert rename_map['vpdmax'] == 'vpdmax'
        assert rename_map['vpdmin'] == 'vpdmin'

    def test_temp_unit_fixes(self):
        """Test temperature unit fix mappings."""
        unit_fixes = PipelineConfig.TEMP_UNIT_FIXES

        assert unit_fixes['tas'] == 'degC'
        assert unit_fixes['tasmax'] == 'degC'
        assert unit_fixes['tasmin'] == 'degC'

    def test_precip_unit_fixes(self):
        """Test precipitation unit fix mappings."""
        unit_fixes = PipelineConfig.PRECIP_UNIT_FIXES

        assert unit_fixes['pr'] == 'mm d-1', "Precipitation units should be CF-compliant"

    def test_humidity_unit_fixes(self):
        """Test humidity unit fix mappings."""
        unit_fixes = PipelineConfig.HUMIDITY_UNIT_FIXES

        assert unit_fixes['tdew'] == 'degC'
        assert unit_fixes['vpdmax'] == 'kPa'
        assert unit_fixes['vpdmin'] == 'kPa'

    def test_cf_standard_names(self):
        """Test CF standard name mappings."""
        cf_names = PipelineConfig.CF_STANDARD_NAMES

        assert cf_names['tas'] == 'air_temperature'
        assert cf_names['pr'] == 'precipitation_flux'
        assert cf_names['tdew'] == 'dew_point_temperature'

    def test_baseline_configuration(self):
        """Test baseline percentile configuration."""
        assert PipelineConfig.BASELINE_PERIOD == '1981-2000'
        assert 'baseline_percentiles' in PipelineConfig.BASELINE_FILE

        # Test baseline variable lists
        temp_vars = PipelineConfig.TEMP_BASELINE_VARS
        assert 'tx90p_threshold' in temp_vars
        assert 'tx10p_threshold' in temp_vars
        assert 'tn90p_threshold' in temp_vars
        assert 'tn10p_threshold' in temp_vars

        precip_vars = PipelineConfig.PRECIP_BASELINE_VARS
        assert 'pr95p_threshold' in precip_vars
        assert 'pr99p_threshold' in precip_vars

        multivar_vars = PipelineConfig.MULTIVARIATE_BASELINE_VARS
        assert 'tas_25p_threshold' in multivar_vars
        assert 'tas_75p_threshold' in multivar_vars
        assert 'pr_25p_threshold' in multivar_vars
        assert 'pr_75p_threshold' in multivar_vars

    def test_default_processing_options(self):
        """Test default processing configuration values."""
        assert PipelineConfig.DEFAULT_CHUNK_YEARS == 1
        assert PipelineConfig.DEFAULT_OUTPUT_DIR == './outputs'
        assert PipelineConfig.DEFAULT_START_YEAR == 1981
        assert PipelineConfig.DEFAULT_END_YEAR == 2024

    def test_default_encoding(self):
        """Test default NetCDF encoding configuration."""
        encoding = PipelineConfig.default_encoding()

        assert encoding['zlib'] is True
        assert encoding['complevel'] == 4
        assert 'chunksizes' in encoding
        assert len(encoding['chunksizes']) == 3  # (time, lat, lon)

    def test_default_encoding_custom_chunks(self):
        """Test default encoding with custom chunk sizes."""
        custom_chunks = (5, 100, 200)
        encoding = PipelineConfig.default_encoding(chunksizes=custom_chunks)

        assert encoding['chunksizes'] == custom_chunks

    def test_setup_warning_filters(self):
        """Test that warning filter setup doesn't raise exceptions."""
        # Should not raise any exceptions
        PipelineConfig.setup_warning_filters()

    def test_zarr_paths_exist(self):
        """Test that Zarr path constants are defined."""
        assert hasattr(PipelineConfig, 'TEMP_ZARR')
        assert hasattr(PipelineConfig, 'PRECIP_ZARR')
        assert hasattr(PipelineConfig, 'HUMIDITY_ZARR')

        # Paths should be strings
        assert isinstance(PipelineConfig.TEMP_ZARR, str)
        assert isinstance(PipelineConfig.PRECIP_ZARR, str)
        assert isinstance(PipelineConfig.HUMIDITY_ZARR, str)

    def test_all_rename_maps_are_dicts(self):
        """Test that all rename maps are dictionaries."""
        assert isinstance(PipelineConfig.TEMP_RENAME_MAP, dict)
        assert isinstance(PipelineConfig.PRECIP_RENAME_MAP, dict)
        assert isinstance(PipelineConfig.HUMIDITY_RENAME_MAP, dict)

    def test_all_unit_fixes_are_dicts(self):
        """Test that all unit fix maps are dictionaries."""
        assert isinstance(PipelineConfig.TEMP_UNIT_FIXES, dict)
        assert isinstance(PipelineConfig.PRECIP_UNIT_FIXES, dict)
        assert isinstance(PipelineConfig.HUMIDITY_UNIT_FIXES, dict)

    def test_baseline_vars_are_lists(self):
        """Test that all baseline variable lists are lists."""
        assert isinstance(PipelineConfig.TEMP_BASELINE_VARS, list)
        assert isinstance(PipelineConfig.PRECIP_BASELINE_VARS, list)
        assert isinstance(PipelineConfig.MULTIVARIATE_BASELINE_VARS, list)

        # Lists should not be empty
        assert len(PipelineConfig.TEMP_BASELINE_VARS) > 0
        assert len(PipelineConfig.PRECIP_BASELINE_VARS) > 0
        assert len(PipelineConfig.MULTIVARIATE_BASELINE_VARS) > 0

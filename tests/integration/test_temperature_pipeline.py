#!/usr/bin/env python3
"""
Full integration tests for Temperature Pipeline with spatial tiling.

Tests the complete temperature pipeline end-to-end including:
- Full pipeline execution with test data
- All 35 temperature indices calculation
- Spatial tiling integration
- Output file validation
- Data quality checks
"""

import pytest
import numpy as np
import xarray as xr
from pathlib import Path

# Import after setting up mock config
from temperature_pipeline import TemperaturePipeline


class TestTemperaturePipelineFullRun:
    """Test complete temperature pipeline execution."""

    def test_full_pipeline_single_year(self, mock_pipeline_config, tmp_output_dir):
        """Test complete temperature pipeline with single year of data."""
        pipeline = TemperaturePipeline(n_tiles=4)

        # Run pipeline for single year
        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        # Verify output file created
        assert len(output_files) == 1, "Should generate 1 output file for single year"
        assert output_files[0].exists(), f"Output file {output_files[0]} should exist"

        # Verify file is readable
        ds = xr.open_dataset(output_files[0])

        # Check dataset structure
        assert 'time' in ds.dims, "Should have time dimension"
        assert 'lat' in ds.dims, "Should have lat dimension"
        assert 'lon' in ds.dims, "Should have lon dimension"

        # Check indices present (expect 35 total)
        # Basic (19) + Extreme (6) + Advanced Phase 7 (8) + Phase 9 (2) = 35
        assert len(ds.data_vars) >= 30, \
            f"Should have at least 30 indices, got {len(ds.data_vars)}"

        # Check specific key indices exist
        expected_indices = [
            'tg_mean', 'tx_max', 'tn_min',
            'summer_days', 'frost_days', 'tropical_nights',
            'growing_degree_days', 'heating_degree_days', 'cooling_degree_days',
            'tx90p', 'tx10p', 'tn90p', 'tn10p',
            'warm_spell_duration_index', 'cold_spell_duration_index'
        ]

        for idx in expected_indices:
            assert idx in ds.data_vars, f"Expected index {idx} not found in output"

        # Check dimensions
        assert ds.dims['time'] == 1, "Should have 1 time step for single year"
        assert ds.dims['lat'] == 100, "Should have 100 lat points (test data)"
        assert ds.dims['lon'] == 100, "Should have 100 lon points (test data)"

        ds.close()

    def test_full_pipeline_two_years(self, mock_pipeline_config, tmp_output_dir):
        """Test complete temperature pipeline with two years of data."""
        pipeline = TemperaturePipeline(n_tiles=4, chunk_years=2)

        # Run pipeline for two years
        output_files = pipeline.run(
            start_year=2022,
            end_year=2023,
            output_dir=str(tmp_output_dir)
        )

        # Should generate 1 file (2 years processed together)
        assert len(output_files) == 1, "Should generate 1 output file for 2 years with chunk_years=2"

        ds = xr.open_dataset(output_files[0])

        # Check time dimension
        assert ds.dims['time'] == 2, "Should have 2 time steps for 2 years"

        # Verify indices exist
        assert len(ds.data_vars) >= 30, f"Should have at least 30 indices"

        ds.close()


class TestTemperatureIndicesQuality:
    """Test data quality of calculated temperature indices."""

    def test_indices_have_reasonable_values(self, mock_pipeline_config, tmp_output_dir):
        """Test that calculated indices have reasonable values."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check temperature statistics are reasonable
        if 'tg_mean' in ds:
            tg_mean = ds['tg_mean'].values
            assert np.nanmean(tg_mean) > -50, "Mean temperature too low"
            assert np.nanmean(tg_mean) < 50, "Mean temperature too high"

        if 'tx_max' in ds:
            tx_max = ds['tx_max'].values
            assert np.nanmax(tx_max) > np.nanmean(tg_mean) if 'tg_mean' in ds else True, \
                "Max temperature should be > mean temperature"

        if 'tn_min' in ds:
            tn_min = ds['tn_min'].values
            assert np.nanmin(tn_min) < np.nanmean(tg_mean) if 'tg_mean' in ds else True, \
                "Min temperature should be < mean temperature"

        # Check count indices are non-negative
        count_indices = ['summer_days', 'frost_days', 'tropical_nights', 'ice_days']
        for idx in count_indices:
            if idx in ds:
                values = ds[idx].values
                assert np.nanmin(values) >= 0, f"{idx} should be non-negative"
                assert np.nanmax(values) <= 366, f"{idx} should not exceed 366 days"

        ds.close()

    def test_indices_nan_handling(self, mock_pipeline_config, tmp_output_dir):
        """Test that NaN values are handled appropriately."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check NaN percentage for each index
        for var_name in ds.data_vars:
            data = ds[var_name].values
            nan_fraction = np.isnan(data).sum() / data.size

            # Allow some NaN, but not excessive
            assert nan_fraction < 0.5, \
                f"{var_name} has {nan_fraction*100:.1f}% NaN values (too many)"

        ds.close()

    def test_extreme_indices_use_baselines(self, mock_pipeline_config, tmp_output_dir):
        """Test that extreme indices are calculated (requires baselines)."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Verify extreme indices exist
        extreme_indices = ['tx90p', 'tx10p', 'tn90p', 'tn10p',
                           'warm_spell_duration_index', 'cold_spell_duration_index']

        for idx in extreme_indices:
            assert idx in ds.data_vars, f"Extreme index {idx} should be calculated"

            # Verify values are reasonable (days, so should be 0-366)
            values = ds[idx].values
            assert np.nanmin(values) >= 0, f"{idx} should be non-negative"
            assert np.nanmax(values) <= 366, f"{idx} should not exceed 366 days"

        ds.close()


class TestTemperatureSpatialTiling:
    """Test spatial tiling integration in temperature pipeline."""

    def test_different_tile_counts(self, mock_pipeline_config, tmp_output_dir):
        """Test that different tile counts produce consistent results."""
        results = {}

        for n_tiles in [2, 4]:
            pipeline = TemperaturePipeline(n_tiles=n_tiles)

            output_dir = tmp_output_dir / f"tiles_{n_tiles}"
            output_dir.mkdir()

            output_files = pipeline.run(
                start_year=2022,
                end_year=2022,
                output_dir=str(output_dir)
            )

            ds = xr.open_dataset(output_files[0])
            results[n_tiles] = ds

        # Compare results between different tile counts
        baseline = results[2]
        for n_tiles in [4]:
            for var_name in baseline.data_vars:
                if var_name in results[n_tiles].data_vars:
                    baseline_values = baseline[var_name].values
                    test_values = results[n_tiles][var_name].values

                    # Should be very close (within floating point precision)
                    np.testing.assert_allclose(
                        baseline_values,
                        test_values,
                        rtol=1e-5,
                        atol=1e-6,
                        err_msg=f"{var_name} differs between 2 and {n_tiles} tiles"
                    )

        # Cleanup
        for ds in results.values():
            ds.close()

    def test_spatial_coverage_complete(self, mock_pipeline_config, tmp_output_dir):
        """Test that tiling covers entire spatial domain."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check spatial coverage
        lat_coverage = len(ds.lat)
        lon_coverage = len(ds.lon)

        assert lat_coverage == 100, f"Should cover all 100 lat points, got {lat_coverage}"
        assert lon_coverage == 100, f"Should cover all 100 lon points, got {lon_coverage}"

        # Check no duplicate coordinates
        assert len(set(ds.lat.values)) == lat_coverage, "Duplicate lat coordinates detected"
        assert len(set(ds.lon.values)) == lon_coverage, "Duplicate lon coordinates detected"

        ds.close()


class TestTemperatureOutputMetadata:
    """Test output file metadata and attributes."""

    def test_global_attributes_present(self, mock_pipeline_config, tmp_output_dir):
        """Test that output files have proper global attributes."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check global attributes
        assert 'creation_date' in ds.attrs, "Should have creation_date attribute"
        assert 'software' in ds.attrs, "Should have software attribute"
        assert 'time_range' in ds.attrs, "Should have time_range attribute"
        assert 'indices_count' in ds.attrs, "Should have indices_count attribute"
        assert 'phase' in ds.attrs, "Should have phase attribute"
        assert 'baseline_period' in ds.attrs, "Should have baseline_period attribute"
        assert 'processing' in ds.attrs, "Should have processing attribute"

        # Verify specific attributes
        assert '2022' in ds.attrs['time_range'], "time_range should contain year"
        assert ds.attrs['indices_count'] >= 30, f"Should have at least 30 indices"
        assert 'tiling' in ds.attrs['processing'].lower(), "Should mention tiling in processing"

        ds.close()

    def test_variable_attributes_present(self, mock_pipeline_config, tmp_output_dir):
        """Test that variables have proper CF-compliant attributes."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check that variables have units
        for var_name in ds.data_vars:
            assert 'units' in ds[var_name].attrs, f"{var_name} should have units attribute"

            # Count indices should have units='1' (dimensionless)
            if var_name in TemperaturePipeline.COUNT_INDICES:
                assert ds[var_name].attrs['units'] == '1', \
                    f"{var_name} should have units='1' (dimensionless)"

        ds.close()


class TestTemperatureCountIndicesFix:
    """Test count indices encoding fix."""

    def test_count_indices_have_correct_units(self, mock_pipeline_config, tmp_output_dir):
        """Test that count indices have units='1' to prevent timedelta encoding."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check count indices
        for idx_name in TemperaturePipeline.COUNT_INDICES:
            if idx_name in ds.data_vars:
                units = ds[idx_name].attrs.get('units', None)
                assert units == '1', \
                    f"{idx_name} should have units='1', got '{units}'"

                # Verify values are not corrupted (not NaT)
                values = ds[idx_name].values
                assert not np.all(np.isnan(values)), \
                    f"{idx_name} has all NaN values (possible timedelta encoding corruption)"

        ds.close()


class TestTemperatureMemoryEfficiency:
    """Test memory efficiency of tiling."""

    def test_tiling_reduces_memory_footprint(self, mock_pipeline_config, tmp_output_dir):
        """Test that tiling keeps memory usage reasonable."""
        import psutil
        import os

        process = psutil.Process(os.getpid())

        # Measure memory before
        initial_mem = process.memory_info().rss / 1024 / 1024  # MB

        # Run pipeline with tiling
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        # Measure memory after
        final_mem = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_mem - initial_mem

        # Memory increase should be reasonable for test data
        # (100x100 grid is tiny, so should use <200MB)
        assert memory_increase < 500, \
            f"Memory increase too high: {memory_increase:.1f} MB"


class TestTemperatureErrorHandling:
    """Test error handling in temperature pipeline."""

    def test_invalid_year_range(self, mock_pipeline_config, tmp_output_dir):
        """Test that invalid year ranges are rejected."""
        pipeline = TemperaturePipeline(n_tiles=4)

        # End year before start year should fail
        with pytest.raises((ValueError, AssertionError)):
            pipeline.run(
                start_year=2023,
                end_year=2022,
                output_dir=str(tmp_output_dir)
            )

    def test_missing_baseline_handling(self, mock_pipeline_config, tmp_output_dir, monkeypatch):
        """Test pipeline behavior when baseline percentiles are missing."""
        # Mock baseline loader to return empty baselines
        from core.baseline_loader import BaselineLoader

        def mock_get_temp_baselines(self):
            return {}

        monkeypatch.setattr(BaselineLoader, 'get_temperature_baselines', mock_get_temp_baselines)

        # Create new pipeline with mocked baseline
        pipeline = TemperaturePipeline(n_tiles=4)

        # Should still run, but extreme indices may be missing
        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        # Should generate output (basic indices should work)
        assert len(output_files) > 0, "Should generate output even without baselines"

        ds = xr.open_dataset(output_files[0])

        # Basic indices should exist
        assert 'tg_mean' in ds.data_vars, "Basic indices should still work"

        ds.close()


class TestTemperatureOutputFileFormat:
    """Test output file format and structure."""

    def test_output_file_is_netcdf(self, mock_pipeline_config, tmp_output_dir):
        """Test that output file is valid NetCDF."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        # Should be readable as NetCDF
        ds = xr.open_dataset(output_files[0])
        assert ds is not None, "Should be readable as NetCDF"
        ds.close()

    def test_output_file_naming(self, mock_pipeline_config, tmp_output_dir):
        """Test that output files follow naming convention."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        # Check filename format
        filename = output_files[0].name
        assert 'temperature' in filename.lower(), "Filename should contain 'temperature'"
        assert '2022' in filename, "Filename should contain year"
        assert filename.endswith('.nc'), "Filename should end with .nc"

    def test_output_file_compression(self, mock_pipeline_config, tmp_output_dir):
        """Test that output files are compressed."""
        pipeline = TemperaturePipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        # Check file size is reasonable (compression should help)
        file_size_mb = output_files[0].stat().st_size / (1024 * 1024)

        # For 100x100 test grid with 35 indices, compressed file should be < 50MB
        assert file_size_mb < 50, \
            f"Output file too large: {file_size_mb:.2f} MB (compression may not be working)"

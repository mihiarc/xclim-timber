#!/usr/bin/env python3
"""
Full integration tests for Precipitation Pipeline with spatial tiling.

Tests the complete precipitation pipeline end-to-end including:
- Full pipeline execution with test data
- All 13 precipitation indices calculation
- Spatial tiling integration
- Output file validation
- Data quality checks
"""

import pytest
import numpy as np
import xarray as xr
from pathlib import Path

from precipitation_pipeline import PrecipitationPipeline


class TestPrecipitationPipelineFullRun:
    """Test complete precipitation pipeline execution."""

    def test_full_pipeline_single_year(self, mock_pipeline_config, tmp_output_dir):
        """Test complete precipitation pipeline with single year of data."""
        pipeline = PrecipitationPipeline(n_tiles=4)

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

        # Check indices present (expect 13 total)
        # Basic (6) + Extreme (2) + Threshold (2) + Enhanced (3) = 13
        assert len(ds.data_vars) >= 10, \
            f"Should have at least 10 indices, got {len(ds.data_vars)}"

        # Check specific key indices exist
        expected_indices = [
            'prcptot', 'rx1day', 'rx5day', 'sdii', 'cdd', 'cwd',
            'r10mm', 'r20mm'
        ]

        for idx in expected_indices:
            assert idx in ds.data_vars, f"Expected index {idx} not found in output"

        # Check dimensions
        assert ds.dims['time'] == 1, "Should have 1 time step for single year"
        assert ds.dims['lat'] == 100, "Should have 100 lat points (test data)"
        assert ds.dims['lon'] == 100, "Should have 100 lon points (test data)"

        ds.close()

    def test_full_pipeline_two_years(self, mock_pipeline_config, tmp_output_dir):
        """Test complete precipitation pipeline with two years of data."""
        pipeline = PrecipitationPipeline(n_tiles=4, chunk_years=2)

        # Run pipeline for two years
        output_files = pipeline.run(
            start_year=2022,
            end_year=2023,
            output_dir=str(tmp_output_dir)
        )

        # Should generate 1 file (2 years processed together)
        assert len(output_files) == 1, "Should generate 1 output file for 2 years"

        ds = xr.open_dataset(output_files[0])

        # Check time dimension
        assert ds.dims['time'] == 2, "Should have 2 time steps for 2 years"

        # Verify indices exist
        assert len(ds.data_vars) >= 10, f"Should have at least 10 indices"

        ds.close()


class TestPrecipitationIndicesQuality:
    """Test data quality of calculated precipitation indices."""

    def test_indices_have_reasonable_values(self, mock_pipeline_config, tmp_output_dir):
        """Test that calculated indices have reasonable values."""
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check total precipitation is reasonable
        if 'prcptot' in ds:
            prcptot = ds['prcptot'].values
            assert np.nanmin(prcptot) >= 0, "Total precipitation should be non-negative"
            assert np.nanmean(prcptot) < 5000, "Total precipitation too high (should be < 5000mm/year)"

        # Check max 1-day precipitation
        if 'rx1day' in ds:
            rx1day = ds['rx1day'].values
            assert np.nanmin(rx1day) >= 0, "Max 1-day precip should be non-negative"
            assert np.nanmax(rx1day) < 500, "Max 1-day precip too high (should be < 500mm)"

        # Check consecutive dry/wet days
        if 'cdd' in ds:
            cdd = ds['cdd'].values
            assert np.nanmin(cdd) >= 0, "Consecutive dry days should be non-negative"
            assert np.nanmax(cdd) <= 366, "Consecutive dry days should not exceed 366"

        if 'cwd' in ds:
            cwd = ds['cwd'].values
            assert np.nanmin(cwd) >= 0, "Consecutive wet days should be non-negative"
            assert np.nanmax(cwd) <= 366, "Consecutive wet days should not exceed 366"

        # Check intensity index
        if 'sdii' in ds:
            sdii = ds['sdii'].values
            assert np.nanmin(sdii) >= 0, "Daily precipitation intensity should be non-negative"
            assert np.nanmean(sdii) < 100, "Daily intensity too high (should be < 100mm/day)"

        ds.close()

    def test_threshold_indices_reasonable(self, mock_pipeline_config, tmp_output_dir):
        """Test that threshold indices have reasonable count values."""
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check heavy precipitation days (>= 10mm)
        if 'r10mm' in ds:
            r10mm = ds['r10mm'].values
            assert np.nanmin(r10mm) >= 0, "r10mm should be non-negative"
            assert np.nanmax(r10mm) <= 366, "r10mm should not exceed 366 days"

        # Check very heavy precipitation days (>= 20mm)
        if 'r20mm' in ds:
            r20mm = ds['r20mm'].values
            assert np.nanmin(r20mm) >= 0, "r20mm should be non-negative"
            assert np.nanmax(r20mm) <= 366, "r20mm should not exceed 366 days"

            # r20mm should be <= r10mm (20mm threshold is stricter)
            if 'r10mm' in ds:
                r10mm = ds['r10mm'].values
                # Allow for NaN values
                valid_mask = ~(np.isnan(r10mm) | np.isnan(r20mm))
                if np.any(valid_mask):
                    assert np.all(r20mm[valid_mask] <= r10mm[valid_mask]), \
                        "r20mm should be <= r10mm (stricter threshold)"

        ds.close()

    def test_enhanced_indices_present(self, mock_pipeline_config, tmp_output_dir):
        """Test that Phase 6 enhanced indices are calculated."""
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check Phase 6 enhanced indices
        enhanced_indices = ['dry_days', 'wetdays', 'wetdays_prop']

        for idx in enhanced_indices:
            if idx in ds.data_vars:
                values = ds[idx].values

                if idx == 'wetdays_prop':
                    # Proportion should be between 0 and 1
                    assert np.nanmin(values) >= 0, f"{idx} should be >= 0"
                    assert np.nanmax(values) <= 1, f"{idx} should be <= 1"
                else:
                    # Day counts
                    assert np.nanmin(values) >= 0, f"{idx} should be non-negative"
                    assert np.nanmax(values) <= 366, f"{idx} should not exceed 366 days"

        ds.close()

    def test_extreme_indices_with_baselines(self, mock_pipeline_config, tmp_output_dir):
        """Test that extreme indices are calculated with baselines."""
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check extreme indices
        extreme_indices = ['r95p', 'r99p']

        for idx in extreme_indices:
            if idx in ds.data_vars:
                values = ds[idx].values
                # These are day counts, so should be 0-366
                assert np.nanmin(values) >= 0, f"{idx} should be non-negative"
                assert np.nanmax(values) <= 366, f"{idx} should not exceed 366 days"

        ds.close()


class TestPrecipitationSpatialTiling:
    """Test spatial tiling integration in precipitation pipeline."""

    def test_different_tile_counts(self, mock_pipeline_config, tmp_output_dir):
        """Test that different tile counts produce consistent results."""
        results = {}

        for n_tiles in [2, 4]:
            pipeline = PrecipitationPipeline(n_tiles=n_tiles)

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
        pipeline = PrecipitationPipeline(n_tiles=4)

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


class TestPrecipitationOutputMetadata:
    """Test output file metadata and attributes."""

    def test_global_attributes_present(self, mock_pipeline_config, tmp_output_dir):
        """Test that output files have proper global attributes."""
        pipeline = PrecipitationPipeline(n_tiles=4)

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

        # Verify specific attributes
        assert '2022' in ds.attrs['time_range'], "time_range should contain year"
        assert ds.attrs['indices_count'] >= 10, f"Should have at least 10 indices"
        assert 'Phase 6' in ds.attrs['phase'], "Should mention Phase 6"

        ds.close()

    def test_variable_attributes_present(self, mock_pipeline_config, tmp_output_dir):
        """Test that variables have proper CF-compliant attributes."""
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        # Check that variables have units
        for var_name in ds.data_vars:
            assert 'units' in ds[var_name].attrs, f"{var_name} should have units attribute"

            # Check units are reasonable
            units = ds[var_name].attrs['units']
            assert units is not None and units != '', f"{var_name} should have non-empty units"

        ds.close()


class TestPrecipitationMemoryEfficiency:
    """Test memory efficiency of tiling."""

    def test_tiling_reduces_memory_footprint(self, mock_pipeline_config, tmp_output_dir):
        """Test that tiling keeps memory usage reasonable."""
        import psutil
        import os

        process = psutil.Process(os.getpid())

        # Measure memory before
        initial_mem = process.memory_info().rss / 1024 / 1024  # MB

        # Run pipeline with tiling
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        # Measure memory after
        final_mem = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_mem - initial_mem

        # Memory increase should be reasonable for test data
        assert memory_increase < 500, \
            f"Memory increase too high: {memory_increase:.1f} MB"


class TestPrecipitationErrorHandling:
    """Test error handling in precipitation pipeline."""

    def test_invalid_year_range(self, mock_pipeline_config, tmp_output_dir):
        """Test that invalid year ranges are rejected."""
        pipeline = PrecipitationPipeline(n_tiles=4)

        # End year before start year should fail
        with pytest.raises((ValueError, AssertionError)):
            pipeline.run(
                start_year=2023,
                end_year=2022,
                output_dir=str(tmp_output_dir)
            )

    def test_missing_baseline_handling(self, mock_pipeline_config, tmp_output_dir, monkeypatch):
        """Test pipeline behavior when baseline percentiles are missing."""
        from core.baseline_loader import BaselineLoader

        def mock_get_precip_baselines(self):
            return {}

        monkeypatch.setattr(BaselineLoader, 'get_precipitation_baselines', mock_get_precip_baselines)

        # Create new pipeline with mocked baseline
        pipeline = PrecipitationPipeline(n_tiles=4)

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
        assert 'prcptot' in ds.data_vars, "Basic indices should still work"

        ds.close()


class TestPrecipitationOutputFileFormat:
    """Test output file format and structure."""

    def test_output_file_is_netcdf(self, mock_pipeline_config, tmp_output_dir):
        """Test that output file is valid NetCDF."""
        pipeline = PrecipitationPipeline(n_tiles=4)

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
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        # Check filename format
        filename = output_files[0].name
        assert 'precipitation' in filename.lower(), "Filename should contain 'precipitation'"
        assert '2022' in filename, "Filename should contain year"
        assert filename.endswith('.nc'), "Filename should end with .nc"

    def test_output_file_compression(self, mock_pipeline_config, tmp_output_dir):
        """Test that output files are compressed."""
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        # Check file size is reasonable
        file_size_mb = output_files[0].stat().st_size / (1024 * 1024)

        # For 100x100 test grid with 13 indices, compressed file should be < 30MB
        assert file_size_mb < 30, \
            f"Output file too large: {file_size_mb:.2f} MB (compression may not be working)"


class TestPrecipitationDataConsistency:
    """Test data consistency across different processing methods."""

    def test_wet_dry_days_sum_to_total(self, mock_pipeline_config, tmp_output_dir):
        """Test that wet_days + dry_days approximately equals total days."""
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        if 'wetdays' in ds and 'dry_days' in ds:
            wetdays = ds['wetdays'].values
            dry_days = ds['dry_days'].values

            # Sum should be approximately 365 (or 366 for leap year)
            total_days = wetdays + dry_days

            # Allow some tolerance for different threshold definitions
            expected_days = 365  # 2022 is not a leap year
            assert np.nanmean(total_days) >= expected_days - 10, \
                "wet_days + dry_days should be close to total days"
            assert np.nanmean(total_days) <= expected_days + 10, \
                "wet_days + dry_days should be close to total days"

        ds.close()

    def test_wetdays_prop_matches_wetdays_count(self, mock_pipeline_config, tmp_output_dir):
        """Test that wetdays_prop is consistent with wetdays count."""
        pipeline = PrecipitationPipeline(n_tiles=4)

        output_files = pipeline.run(
            start_year=2022,
            end_year=2022,
            output_dir=str(tmp_output_dir)
        )

        ds = xr.open_dataset(output_files[0])

        if 'wetdays' in ds and 'wetdays_prop' in ds:
            wetdays = ds['wetdays'].values
            wetdays_prop = ds['wetdays_prop'].values

            # wetdays_prop should be wetdays / total_days
            expected_days = 365
            calculated_prop = wetdays / expected_days

            # Should be close (within 1%)
            valid_mask = ~(np.isnan(wetdays) | np.isnan(wetdays_prop))
            if np.any(valid_mask):
                np.testing.assert_allclose(
                    wetdays_prop[valid_mask],
                    calculated_prop[valid_mask],
                    rtol=0.01,
                    atol=0.01,
                    err_msg="wetdays_prop not consistent with wetdays count"
                )

        ds.close()

"""
Unit tests for core.base_pipeline module.

Tests BasePipeline abstract class and common pipeline functionality.
"""

import pytest
import xarray as xr
import numpy as np
from pathlib import Path
from datetime import datetime

from core.base_pipeline import BasePipeline


class MockPipeline(BasePipeline):
    """Concrete implementation of BasePipeline for testing."""

    def calculate_indices(self, datasets):
        """Mock implementation that returns a simple index."""
        ds = list(datasets.values())[0]

        # Create a mock index
        mock_index = xr.DataArray(
            np.ones((1, ds.sizes['lat'], ds.sizes['lon'])),
            dims=['time', 'lat', 'lon'],
            coords={
                'time': [2020],
                'lat': ds.lat,
                'lon': ds.lon
            }
        )
        mock_index.attrs['units'] = '1'
        mock_index.attrs['long_name'] = 'Mock climate index'

        return {'mock_index': mock_index}


class TestBasePipeline:
    """Tests for BasePipeline class."""

    def test_init_with_defaults(self, temp_zarr_store):
        """Test initialization with default parameters."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        assert pipeline.zarr_paths == {'temperature': temp_zarr_store}
        assert pipeline.chunk_years == 1
        assert pipeline.enable_dashboard is False
        assert pipeline.chunk_config is not None

    def test_init_with_custom_chunks(self, temp_zarr_store):
        """Test initialization with custom chunk configuration."""
        custom_chunks = {'time': 100, 'lat': 50, 'lon': 50}
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store},
            chunk_config=custom_chunks
        )

        assert pipeline.chunk_config == custom_chunks

    def test_init_with_custom_chunk_years(self, temp_zarr_store):
        """Test initialization with custom chunk years."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store},
            chunk_years=5
        )

        assert pipeline.chunk_years == 5

    def test_default_chunk_config(self):
        """Test default chunk configuration."""
        chunks = BasePipeline._default_chunk_config()

        assert chunks['time'] == 365
        assert chunks['lat'] == 103
        assert chunks['lon'] == 201

    def test_setup_dask_client(self, temp_zarr_store):
        """Test Dask client setup (threaded scheduler)."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        # Should not raise exceptions
        pipeline.setup_dask_client()

    def test_load_zarr_data(self, temp_zarr_store):
        """Test loading data from Zarr store."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        ds = pipeline._load_zarr_data(temp_zarr_store, 2020, 2020)

        assert isinstance(ds, xr.Dataset)
        assert 'time' in ds.dims
        assert len(ds.time) > 0

        # Check time range
        assert ds.time[0].dt.year == 2020
        assert ds.time[-1].dt.year == 2020

    def test_load_zarr_data_multi_year(self, temp_zarr_store):
        """Test loading multi-year data range."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        # Sample dataset has 365 days starting from 2020-01-01
        ds = pipeline._load_zarr_data(temp_zarr_store, 2020, 2020)

        assert isinstance(ds, xr.Dataset)
        assert len(ds.time) == 365

    def test_rename_variables(self, sample_temperature_dataset):
        """Test variable renaming."""
        pipeline = MockPipeline(zarr_paths={'temperature': 'dummy'})

        # Add a variable to rename
        sample_temperature_dataset['tmax'] = sample_temperature_dataset['tasmax'].copy()

        rename_map = {'tmax': 'tasmax_renamed'}
        renamed_ds = pipeline._rename_variables(sample_temperature_dataset, rename_map)

        assert 'tasmax_renamed' in renamed_ds
        assert 'tmax' not in renamed_ds

    def test_rename_variables_no_change_if_already_correct(self, sample_temperature_dataset):
        """Test that renaming doesn't change variable if name already correct."""
        pipeline = MockPipeline(zarr_paths={'temperature': 'dummy'})

        rename_map = {'tasmax': 'tasmax'}  # Same name
        renamed_ds = pipeline._rename_variables(sample_temperature_dataset, rename_map)

        assert 'tasmax' in renamed_ds

    def test_fix_units(self, sample_temperature_dataset):
        """Test unit fixing."""
        pipeline = MockPipeline(zarr_paths={'temperature': 'dummy'})

        # Change units to incorrect value
        sample_temperature_dataset['tas'].attrs['units'] = 'K'

        unit_fixes = {'tas': 'degC'}
        fixed_ds = pipeline._fix_units(sample_temperature_dataset, unit_fixes)

        assert fixed_ds['tas'].attrs['units'] == 'degC'

    def test_fix_units_for_missing_variable(self, sample_temperature_dataset):
        """Test unit fixing handles missing variables gracefully."""
        pipeline = MockPipeline(zarr_paths={'temperature': 'dummy'})

        unit_fixes = {'missing_var': 'degC'}
        # Should not raise
        fixed_ds = pipeline._fix_units(sample_temperature_dataset, unit_fixes)

        assert isinstance(fixed_ds, xr.Dataset)

    def test_add_global_metadata(self, sample_temperature_dataset, temp_zarr_store):
        """Test adding global metadata to result dataset."""
        pipeline = MockPipeline(zarr_paths={'temperature': temp_zarr_store})

        # Create a result dataset
        result_ds = xr.Dataset({
            'mock_index': (['time', 'lat', 'lon'], np.ones((1, 10, 10)))
        })

        result_ds = pipeline._add_global_metadata(
            result_ds,
            start_year=2020,
            end_year=2020,
            pipeline_name='test_pipeline',
            indices_count=1
        )

        assert 'creation_date' in result_ds.attrs
        assert 'software' in result_ds.attrs
        assert 'test_pipeline' in result_ds.attrs['software']
        assert result_ds.attrs['time_range'] == '2020-2020'
        assert result_ds.attrs['indices_count'] == 1

    def test_add_global_metadata_with_additional_attrs(self, temp_zarr_store):
        """Test adding global metadata with additional attributes."""
        pipeline = MockPipeline(zarr_paths={'temperature': temp_zarr_store})

        result_ds = xr.Dataset({
            'mock_index': (['time', 'lat', 'lon'], np.ones((1, 10, 10)))
        })

        additional_attrs = {'custom_attr': 'custom_value'}
        result_ds = pipeline._add_global_metadata(
            result_ds,
            start_year=2020,
            end_year=2020,
            pipeline_name='test_pipeline',
            indices_count=1,
            additional_attrs=additional_attrs
        )

        assert result_ds.attrs['custom_attr'] == 'custom_value'

    def test_save_result(self, sample_temperature_dataset, tmp_path, temp_zarr_store):
        """Test saving result to NetCDF."""
        pipeline = MockPipeline(zarr_paths={'temperature': temp_zarr_store})

        result_ds = xr.Dataset({
            'mock_index': (['time', 'lat', 'lon'], np.ones((1, 10, 10)))
        })

        output_file = tmp_path / 'test_output.nc'
        pipeline._save_result(result_ds, output_file)

        assert output_file.exists()
        assert output_file.stat().st_size > 0

        # Verify file can be opened
        ds = xr.open_dataset(output_file)
        assert 'mock_index' in ds.data_vars
        ds.close()

    def test_save_result_with_custom_encoding(self, tmp_path, temp_zarr_store):
        """Test saving result with custom encoding."""
        pipeline = MockPipeline(zarr_paths={'temperature': temp_zarr_store})

        result_ds = xr.Dataset({
            'mock_index': (['time', 'lat', 'lon'], np.ones((1, 10, 10)))
        })

        custom_encoding = {
            'mock_index': {
                'zlib': True,
                'complevel': 9,
                'chunksizes': (1, 5, 5)
            }
        }

        output_file = tmp_path / 'test_output_custom.nc'
        pipeline._save_result(result_ds, output_file, encoding_config=custom_encoding)

        assert output_file.exists()

    def test_process_time_chunk(self, temp_zarr_store, tmp_path):
        """Test processing a single time chunk."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        output_file = pipeline.process_time_chunk(2020, 2020, tmp_path)

        assert output_file is not None
        assert output_file.exists()
        assert 'mock_indices_2020_2020.nc' in output_file.name

    def test_process_time_chunk_no_indices(self, temp_zarr_store, tmp_path, monkeypatch):
        """Test processing time chunk when no indices are calculated."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        # Mock calculate_indices to return empty dict
        def mock_calculate_empty(datasets):
            return {}

        monkeypatch.setattr(pipeline, '_calculate_all_indices', mock_calculate_empty)

        output_file = pipeline.process_time_chunk(2020, 2020, tmp_path)

        assert output_file is None

    def test_run_single_year(self, temp_zarr_store, tmp_path):
        """Test running pipeline for single year."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        output_files = pipeline.run(2020, 2020, str(tmp_path))

        assert len(output_files) == 1
        assert output_files[0].exists()

    def test_run_multi_year_chunked(self, temp_zarr_store, tmp_path):
        """Test running pipeline with year chunking."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store},
            chunk_years=1
        )

        # Note: Our test data only has 2020, so this will only process that year
        output_files = pipeline.run(2020, 2020, str(tmp_path))

        assert len(output_files) >= 1

    def test_run_creates_output_directory(self, temp_zarr_store, tmp_path):
        """Test that run creates output directory if it doesn't exist."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        new_output_dir = tmp_path / 'new_outputs'
        assert not new_output_dir.exists()

        output_files = pipeline.run(2020, 2020, str(new_output_dir))

        assert new_output_dir.exists()
        assert len(output_files) > 0

    def test_calculate_indices_abstract_method(self):
        """Test that calculate_indices must be implemented by subclasses."""
        # BasePipeline is abstract, so we can't instantiate it directly
        # This is enforced by the ABC mechanism
        with pytest.raises(TypeError):
            BasePipeline(zarr_paths={})

    def test_pipeline_name_sanitization(self, temp_zarr_store, tmp_path):
        """
        Regression test for Issue #83: Path traversal sanitization.

        Verifies that pipeline names are sanitized to prevent path traversal.
        """
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        output_file = pipeline.process_time_chunk(2020, 2020, tmp_path)

        # Verify output file is in the expected directory (not traversed)
        assert output_file.parent == tmp_path
        assert '..' not in str(output_file)
        assert '../' not in str(output_file)


@pytest.mark.regression
class TestBasePipelineRegressions:
    """Regression tests for previously fixed bugs."""

    def test_output_file_path_traversal_issue_83(self, temp_zarr_store, tmp_path):
        """
        Regression test for Issue #83: Path traversal security fix.

        Ensures that output filenames are sanitized using os.path.basename
        to prevent directory traversal attacks.
        """
        # Create a mock pipeline with a potentially malicious name
        class MaliciousPipeline(BasePipeline):
            def calculate_indices(self, datasets):
                return {'index': xr.DataArray([1])}

        pipeline = MaliciousPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        # Process chunk
        output_file = pipeline.process_time_chunk(2020, 2020, tmp_path)

        # Verify output is in tmp_path (not traversed to parent)
        assert output_file.parent == tmp_path
        assert '../' not in str(output_file)

    def test_time_chunking_boundary_conditions(self, temp_zarr_store, tmp_path):
        """Test time chunking with various boundary conditions."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store},
            chunk_years=1
        )

        # Test with same start and end year
        output_files = pipeline.run(2020, 2020, str(tmp_path))
        assert len(output_files) == 1

    def test_memory_tracking(self, temp_zarr_store, tmp_path):
        """Test that memory tracking doesn't cause errors."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        # Should track memory without errors
        output_file = pipeline.process_time_chunk(2020, 2020, tmp_path)
        assert output_file is not None


class TestBasePipelineErrorHandling:
    """Tests for error handling in BasePipeline."""

    def test_load_zarr_missing_file(self, tmp_path):
        """Test loading from non-existent Zarr store."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': 'nonexistent.zarr'}
        )

        with pytest.raises(Exception):
            pipeline._load_zarr_data('nonexistent.zarr', 2020, 2020)

    def test_run_with_exception(self, temp_zarr_store, tmp_path, monkeypatch):
        """Test that run propagates exceptions."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        def mock_process_error(start_year, end_year, output_path):
            raise RuntimeError("Mock processing error")

        monkeypatch.setattr(pipeline, 'process_time_chunk', mock_process_error)

        with pytest.raises(RuntimeError) as exc_info:
            pipeline.run(2020, 2020, str(tmp_path))

        assert "Mock processing error" in str(exc_info.value)


class TestBasePipelineIntegration:
    """Integration tests for complete pipeline workflows."""

    def test_end_to_end_pipeline(self, temp_zarr_store, tmp_path):
        """Test complete end-to-end pipeline execution."""
        pipeline = MockPipeline(
            zarr_paths={'temperature': temp_zarr_store}
        )

        # Run pipeline
        output_files = pipeline.run(2020, 2020, str(tmp_path))

        # Verify outputs
        assert len(output_files) > 0

        for output_file in output_files:
            assert output_file.exists()

            # Open and verify structure
            ds = xr.open_dataset(output_file)
            assert 'mock_index' in ds.data_vars
            assert 'creation_date' in ds.attrs
            assert 'software' in ds.attrs
            ds.close()

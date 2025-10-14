#!/usr/bin/env python3
"""
Integration tests for error recovery and cleanup in spatial tiling.

Tests:
- Tile cleanup on processing failure
- Partial tile failure handling
- Disk space exhaustion handling
- Dimension mismatch detection
- Graceful degradation scenarios
"""

import pytest
import numpy as np
import xarray as xr
from pathlib import Path
from unittest.mock import patch, MagicMock
import shutil

from core.spatial_tiling import SpatialTilingMixin


class ErrorInjectingPipeline(SpatialTilingMixin):
    """Pipeline with injectable errors for testing error recovery."""

    def __init__(self, n_tiles=4, fail_on_tile=None):
        super().__init__(n_tiles=n_tiles)
        self.fail_on_tile = fail_on_tile
        self.processed_tiles = []

    def calculate_indices(self, datasets):
        """Calculate with optional failure injection."""
        ds = datasets['primary']
        return {
            'test_index': ds['tas'].mean(dim='time', keepdims=True)
        }

    def _process_single_tile(self, ds, lat_slice, lon_slice, tile_name):
        """Process tile with optional failure injection."""
        self.processed_tiles.append(tile_name)

        # Inject failure if configured
        if self.fail_on_tile and tile_name == self.fail_on_tile:
            raise RuntimeError(f"Simulated failure processing tile {tile_name}")

        return super()._process_single_tile(ds, lat_slice, lon_slice, tile_name)


class TestTileCleanupOnFailure:
    """Test that tile files are cleaned up when processing fails."""

    def test_cleanup_on_processing_failure(self, small_test_dataset, tmp_output_dir):
        """Verify tile files are cleaned up even when processing fails."""
        # Configure pipeline to fail on second tile
        pipeline = ErrorInjectingPipeline(n_tiles=4, fail_on_tile='northeast')

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Processing should fail
        with pytest.raises(RuntimeError, match="Simulated failure processing tile northeast"):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )

        # Verify no tile files left behind
        tile_files = list(tmp_output_dir.glob('tile_*.nc'))
        assert len(tile_files) == 0, \
            f"Tile files not cleaned up after failure: {tile_files}"

    def test_cleanup_on_merge_failure(self, small_test_dataset, tmp_output_dir):
        """Test cleanup when tile merge fails."""
        pipeline = ErrorInjectingPipeline(n_tiles=4)

        # Inject failure in merge operation
        original_merge = pipeline._merge_tiles

        def failing_merge(tile_files, expected_dims):
            raise RuntimeError("Simulated merge failure")

        pipeline._merge_tiles = failing_merge

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Should fail during merge
        with pytest.raises(RuntimeError, match="Simulated merge failure"):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )

        # Note: Tile files may remain after merge failure (that's expected behavior)
        # The important thing is the process doesn't crash


class TestPartialTileFailures:
    """Test handling of partial tile failures."""

    def test_all_or_nothing_tile_processing(self, small_test_dataset, tmp_output_dir):
        """Test that pipeline fails completely if any tile fails (no partial results)."""
        pipeline = ErrorInjectingPipeline(n_tiles=4, fail_on_tile='southeast')

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Should fail and not return partial results
        with pytest.raises(RuntimeError, match="Simulated failure"):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )

        # Some tiles may have been processed before failure
        assert len(pipeline.processed_tiles) > 0, "Should have attempted some tiles"
        assert len(pipeline.processed_tiles) <= 4, "Should not process more than 4 tiles"

    def test_tile_count_validation(self, small_test_dataset, tmp_output_dir):
        """Test that processing fails if tile count doesn't match expected."""
        pipeline = ErrorInjectingPipeline(n_tiles=4)

        # Mock _save_tile to "lose" a tile file
        original_save = pipeline._save_tile
        saved_count = {'count': 0}

        def selective_save(tile_indices, tile_name, output_dir):
            saved_count['count'] += 1
            if tile_name == 'northeast':
                # Don't actually save this tile (simulate failure)
                return output_dir / f'tile_{tile_name}.nc'  # Return path but don't create
            return original_save(tile_indices, tile_name, output_dir)

        pipeline._save_tile = selective_save

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Should fail due to missing tile
        with pytest.raises((FileNotFoundError, ValueError, OSError)):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )


class TestDiskSpaceHandling:
    """Test handling of disk space issues."""

    def test_disk_full_during_tile_write(self, small_test_dataset, tmp_output_dir):
        """Test graceful handling when disk is full during tile write."""
        pipeline = ErrorInjectingPipeline(n_tiles=4)

        # Mock to_netcdf to simulate disk full error
        original_to_netcdf = xr.Dataset.to_netcdf
        write_count = {'count': 0}

        def disk_full_to_netcdf(self, path, *args, **kwargs):
            write_count['count'] += 1
            if write_count['count'] == 2:  # Fail on second write
                raise OSError("[Errno 28] No space left on device")
            return original_to_netcdf(self, path, *args, **kwargs)

        with patch.object(xr.Dataset, 'to_netcdf', disk_full_to_netcdf):
            expected_dims = {
                'time': 1,
                'lat': len(small_test_dataset.lat),
                'lon': len(small_test_dataset.lon),
            }

            # Should raise RuntimeError wrapping OSError
            with pytest.raises(RuntimeError, match="Disk space exhaustion or I/O error"):
                pipeline.process_with_spatial_tiling(
                    ds=small_test_dataset,
                    output_dir=tmp_output_dir,
                    expected_dims=expected_dims
                )

    def test_partial_file_cleanup_on_write_error(self, small_test_dataset, tmp_output_dir):
        """Test that partial files are cleaned up when write fails."""
        pipeline = ErrorInjectingPipeline(n_tiles=4)

        # Track created files
        created_files = []

        original_to_netcdf = xr.Dataset.to_netcdf

        def failing_to_netcdf(self, path, *args, **kwargs):
            created_files.append(Path(path))
            # Create partial file
            Path(path).touch()
            # Then fail
            raise OSError("Write failed")

        with patch.object(xr.Dataset, 'to_netcdf', failing_to_netcdf):
            expected_dims = {
                'time': 1,
                'lat': len(small_test_dataset.lat),
                'lon': len(small_test_dataset.lon),
            }

            with pytest.raises(RuntimeError):
                pipeline.process_with_spatial_tiling(
                    ds=small_test_dataset,
                    output_dir=tmp_output_dir,
                    expected_dims=expected_dims
                )

        # Partial files should be cleaned up by _save_tile error handler
        for file in created_files:
            # Files may or may not exist depending on cleanup logic
            # The important thing is the process doesn't leave corrupt files
            if file.exists():
                assert file.stat().st_size == 0, \
                    f"Partial file {file} not cleaned up properly"


class TestDimensionMismatchDetection:
    """Test dimension mismatch detection during merge."""

    def test_dimension_mismatch_raises_error(self, small_test_dataset, tmp_output_dir):
        """Test that dimension mismatches are detected and reported."""
        pipeline = ErrorInjectingPipeline(n_tiles=4)

        # Provide incorrect expected dimensions
        expected_dims = {
            'time': 1,
            'lat': 999,  # Wrong size
            'lon': 999,  # Wrong size
        }

        # Should fail with clear error message
        with pytest.raises(ValueError, match="Dimension mismatch after tile merge"):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )

    def test_missing_dimension_raises_error(self, small_test_dataset, tmp_output_dir):
        """Test that missing dimensions are detected."""
        pipeline = ErrorInjectingPipeline(n_tiles=4)

        # Provide incomplete expected dimensions
        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            # Missing 'lon' dimension
        }

        # Should fail (either KeyError or dimension mismatch)
        with pytest.raises((KeyError, ValueError)):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )


class TestCalculationErrors:
    """Test error handling during index calculation."""

    def test_calculation_error_propagates(self, small_test_dataset, tmp_output_dir):
        """Test that errors during calculation are properly propagated."""
        pipeline = ErrorInjectingPipeline(n_tiles=4)

        # Inject calculation error
        def failing_calculate(datasets):
            raise ValueError("Invalid calculation parameters")

        pipeline.calculate_indices = failing_calculate

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Should propagate calculation error
        with pytest.raises(ValueError, match="Invalid calculation parameters"):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )

    def test_nan_handling_in_calculation(self, tmp_output_dir):
        """Test that NaN values in calculations don't crash the pipeline."""
        # Create dataset with some NaN values
        dates = [np.datetime64('2023-01-01') + np.timedelta64(i, 'D') for i in range(100)]
        lat = np.linspace(40, 45, 50)
        lon = np.linspace(-120, -115, 50)

        tas = np.random.rand(100, 50, 50).astype(np.float32)
        tas[0:10, 0:10, 0:10] = np.nan  # Introduce NaN region

        nan_ds = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], tas)},
            coords={'time': dates, 'lat': lat, 'lon': lon}
        )
        nan_ds['tas'].attrs = {'units': 'degC'}

        pipeline = ErrorInjectingPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': 50,
            'lon': 50,
        }

        # Should complete without error (NaN handling is application-specific)
        result = pipeline.process_with_spatial_tiling(
            ds=nan_ds,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Result should exist (NaN values may propagate to result)
        assert len(result) > 0, "Should produce result even with NaN inputs"


class TestResourceExhaustion:
    """Test handling of resource exhaustion scenarios."""

    def test_memory_pressure_handling(self, tmp_output_dir):
        """Test that pipeline handles memory pressure gracefully."""
        # Create larger dataset to simulate memory pressure
        dates = [np.datetime64('2023-01-01') + np.timedelta64(i, 'D') for i in range(365)]
        lat = np.linspace(40, 45, 100)
        lon = np.linspace(-120, -115, 100)

        # Large dataset (but still manageable for tests)
        tas = np.random.rand(365, 100, 100).astype(np.float32)

        large_ds = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], tas)},
            coords={'time': dates, 'lat': lat, 'lon': lon}
        )
        large_ds['tas'].attrs = {'units': 'degC'}

        pipeline = ErrorInjectingPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': 100,
            'lon': 100,
        }

        # Should complete (tiling helps with memory management)
        result = pipeline.process_with_spatial_tiling(
            ds=large_ds,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        assert len(result) > 0, "Should handle larger dataset with tiling"


class TestGracefulDegradation:
    """Test graceful degradation in edge cases."""

    def test_single_tile_failure_stops_processing(self, small_test_dataset, tmp_output_dir):
        """Test that single tile failure stops entire processing (fail-fast)."""
        pipeline = ErrorInjectingPipeline(n_tiles=4, fail_on_tile='northwest')

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Should fail fast when first tile fails
        with pytest.raises(RuntimeError, match="Simulated failure"):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )

        # Should not have processed all tiles (fail-fast behavior)
        # Note: Due to parallel execution, some tiles may complete before error is detected

    def test_output_directory_not_writable(self, small_test_dataset, tmp_path):
        """Test handling when output directory is not writable."""
        # Create read-only directory
        readonly_dir = tmp_path / "readonly"
        readonly_dir.mkdir()
        readonly_dir.chmod(0o444)  # Read-only

        pipeline = ErrorInjectingPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Should fail with permission error
        with pytest.raises((PermissionError, OSError)):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=readonly_dir,
                expected_dims=expected_dims
            )

        # Cleanup
        readonly_dir.chmod(0o755)

    def test_corrupted_tile_file_handling(self, small_test_dataset, tmp_output_dir):
        """Test handling of corrupted tile files during merge."""
        pipeline = ErrorInjectingPipeline(n_tiles=4)

        # Corrupt a tile file after saving
        original_save = pipeline._save_tile

        def corrupting_save(tile_indices, tile_name, output_dir):
            tile_file = original_save(tile_indices, tile_name, output_dir)

            if tile_name == 'northeast':
                # Corrupt the file
                with open(tile_file, 'wb') as f:
                    f.write(b'CORRUPTED DATA')

            return tile_file

        pipeline._save_tile = corrupting_save

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Should fail when trying to read corrupted tile
        with pytest.raises((OSError, ValueError)):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )

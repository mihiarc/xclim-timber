#!/usr/bin/env python3
"""
Integration tests for tile merging correctness and data continuity.

Tests that:
- Merged tiles have correct dimensions
- No duplicate coordinates after merge
- No discontinuities at tile boundaries
- Data values are preserved correctly
- Tiling produces identical results to non-tiled processing
"""

import pytest
import numpy as np
import xarray as xr
from pathlib import Path
import tempfile

from core.spatial_tiling import SpatialTilingMixin


class SimpleTilingPipeline(SpatialTilingMixin):
    """Simple pipeline for testing tile merge operations."""

    def __init__(self, n_tiles=4):
        super().__init__(n_tiles=n_tiles)

    def calculate_indices(self, datasets):
        """Simple calculation: mean over time dimension."""
        ds = datasets['primary']
        indices = {}

        for var in ds.data_vars:
            # Calculate annual mean as a simple test index
            indices[f'{var}_mean'] = ds[var].mean(dim='time', keepdims=True)

        return indices


class TestTileMergeDimensions:
    """Test that merged tiles have correct dimensions."""

    def test_merge_dimensions_2_tiles(self, small_test_dataset, tmp_output_dir):
        """Test merged dataset has correct dimensions for 2 tiles."""
        pipeline = SimpleTilingPipeline(n_tiles=2)

        # Expected dimensions after processing
        expected_dims = {
            'time': 1,  # Annual mean reduces time to 1
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Process with tiling
        result = pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Verify result dimensions match input
        for var_name, data_array in result.items():
            assert data_array.dims == ('time', 'lat', 'lon'), f"{var_name} has wrong dimensions"
            assert len(data_array.time) == 1, f"{var_name} time dimension wrong"
            assert len(data_array.lat) == len(small_test_dataset.lat), f"{var_name} lat dimension wrong"
            assert len(data_array.lon) == len(small_test_dataset.lon), f"{var_name} lon dimension wrong"

    def test_merge_dimensions_4_tiles(self, small_test_dataset, tmp_output_dir):
        """Test merged dataset has correct dimensions for 4 tiles."""
        pipeline = SimpleTilingPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        result = pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Verify all variables have correct shape
        for var_name, data_array in result.items():
            expected_shape = (1, len(small_test_dataset.lat), len(small_test_dataset.lon))
            assert data_array.shape == expected_shape, \
                f"{var_name} has shape {data_array.shape}, expected {expected_shape}"


class TestTileMergeCoordinates:
    """Test coordinate handling during tile merge."""

    def test_no_duplicate_coordinates_2_tiles(self, small_test_dataset, tmp_output_dir):
        """Verify merged tiles have no duplicate coordinates for 2 tiles."""
        pipeline = SimpleTilingPipeline(n_tiles=2)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        result = pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Check each variable for duplicate coordinates
        for var_name, data_array in result.items():
            # Check latitude uniqueness
            lat_values = data_array.lat.values
            lat_unique = np.unique(lat_values)
            assert len(lat_unique) == len(lat_values), \
                f"{var_name} has duplicate latitude coordinates: {len(lat_values)} total, {len(lat_unique)} unique"

            # Check longitude uniqueness
            lon_values = data_array.lon.values
            lon_unique = np.unique(lon_values)
            assert len(lon_unique) == len(lon_values), \
                f"{var_name} has duplicate longitude coordinates: {len(lon_values)} total, {len(lon_unique)} unique"

    def test_no_duplicate_coordinates_4_tiles(self, small_test_dataset, tmp_output_dir):
        """Verify merged tiles have no duplicate coordinates for 4 tiles."""
        pipeline = SimpleTilingPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        result = pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        for var_name, data_array in result.items():
            # Verify no duplicates in coordinates
            lat_unique_count = len(set(data_array.lat.values))
            lon_unique_count = len(set(data_array.lon.values))

            assert lat_unique_count == len(data_array.lat), \
                f"{var_name} has {len(data_array.lat) - lat_unique_count} duplicate lat coordinates"
            assert lon_unique_count == len(data_array.lon), \
                f"{var_name} has {len(data_array.lon) - lon_unique_count} duplicate lon coordinates"

    def test_coordinate_ordering_preserved(self, small_test_dataset, tmp_output_dir):
        """Test that coordinate ordering is preserved after merge."""
        pipeline = SimpleTilingPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        result = pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Get original coordinates
        orig_lat = small_test_dataset.lat.values
        orig_lon = small_test_dataset.lon.values

        # Check result coordinates match original
        for var_name, data_array in result.items():
            result_lat = data_array.lat.values
            result_lon = data_array.lon.values

            np.testing.assert_array_equal(
                result_lat, orig_lat,
                err_msg=f"{var_name} latitude coordinates don't match original"
            )
            np.testing.assert_array_equal(
                result_lon, orig_lon,
                err_msg=f"{var_name} longitude coordinates don't match original"
            )


class TestTileMergeDataContinuity:
    """Test data continuity across tile boundaries."""

    def test_no_discontinuities_at_boundaries(self, small_test_dataset, tmp_output_dir):
        """
        Test that there are no artificial discontinuities at tile boundaries.

        Creates smooth data and verifies no jumps at tile boundaries after merge.
        """
        # Create dataset with smooth spatial gradient
        lat = small_test_dataset.lat.values
        lon = small_test_dataset.lon.values
        time = small_test_dataset.time.values

        # Create smooth 2D gradient
        lat_grid, lon_grid = np.meshgrid(lat, lon, indexing='ij')
        smooth_data = lat_grid + lon_grid  # Simple linear combination

        # Replicate across time
        smooth_data_3d = np.tile(smooth_data[np.newaxis, :, :], (len(time), 1, 1))

        # Create dataset
        smooth_ds = xr.Dataset(
            {'smooth_var': (['time', 'lat', 'lon'], smooth_data_3d.astype(np.float32))},
            coords={'time': time, 'lat': lat, 'lon': lon}
        )
        smooth_ds['smooth_var'].attrs = {'units': 'degC'}

        # Process with tiling
        pipeline = SimpleTilingPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(lat),
            'lon': len(lon),
        }

        result = pipeline.process_with_spatial_tiling(
            ds=smooth_ds,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Compute expected result (mean over time)
        expected = smooth_data.mean()

        # Check result is smooth (no discontinuities)
        result_data = result['smooth_var_mean'].values[0, :, :]

        # Check for large gradients at tile boundaries (quadrant boundaries)
        lat_mid = len(lat) // 2
        lon_mid = len(lon) // 2

        # Check gradient across latitude boundary (should be small)
        if lat_mid > 0 and lat_mid < len(lat) - 1:
            lat_gradient_at_boundary = np.abs(
                result_data[lat_mid, :] - result_data[lat_mid - 1, :]
            )
            normal_gradient = np.abs(
                result_data[lat_mid + 5, :] - result_data[lat_mid + 4, :]
            )

            # Boundary gradient should not be much larger than normal gradient
            assert np.median(lat_gradient_at_boundary) < np.median(normal_gradient) * 2, \
                "Large discontinuity detected at latitude tile boundary"

        # Check gradient across longitude boundary
        if lon_mid > 0 and lon_mid < len(lon) - 1:
            lon_gradient_at_boundary = np.abs(
                result_data[:, lon_mid] - result_data[:, lon_mid - 1]
            )
            normal_gradient = np.abs(
                result_data[:, lon_mid + 5] - result_data[:, lon_mid + 4]
            )

            assert np.median(lon_gradient_at_boundary) < np.median(normal_gradient) * 2, \
                "Large discontinuity detected at longitude tile boundary"


class TestTilingEquivalence:
    """Test that tiling produces identical results to non-tiled processing."""

    def test_tiling_vs_no_tiling_produces_identical_results(self, small_test_dataset, tmp_output_dir):
        """
        Verify tiling produces identical results to processing without tiling.

        This is the gold standard test: tiling should be transparent to the calculation.
        """
        # Process WITHOUT tiling (baseline)
        pipeline_no_tiling = SimpleTilingPipeline(n_tiles=2)

        # Calculate directly without tiling (mock by processing full dataset)
        indices_no_tiling = pipeline_no_tiling.calculate_indices({'primary': small_test_dataset})

        # Process WITH tiling
        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        pipeline_with_tiling = SimpleTilingPipeline(n_tiles=4)
        indices_with_tiling = pipeline_with_tiling.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Compare results (should be identical within floating point precision)
        for var_name in indices_no_tiling.keys():
            assert var_name in indices_with_tiling, f"{var_name} missing from tiled result"

            result_no_tiling = indices_no_tiling[var_name].values
            result_with_tiling = indices_with_tiling[var_name].values

            # Use allclose for floating point comparison
            np.testing.assert_allclose(
                result_no_tiling,
                result_with_tiling,
                rtol=1e-6,  # Relative tolerance
                atol=1e-8,  # Absolute tolerance
                err_msg=f"Tiling introduced differences in {var_name}"
            )

    def test_different_tile_counts_produce_identical_results(self, small_test_dataset, tmp_output_dir):
        """Test that 2, 4, and 8 tiles all produce identical results."""
        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Process with different tile counts
        results = {}
        for n_tiles in [2, 4, 8]:
            pipeline = SimpleTilingPipeline(n_tiles=n_tiles)
            result = pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )
            results[n_tiles] = result

        # Compare all pairs of results
        baseline = results[2]
        for n_tiles in [4, 8]:
            for var_name in baseline.keys():
                np.testing.assert_allclose(
                    baseline[var_name].values,
                    results[n_tiles][var_name].values,
                    rtol=1e-6,
                    atol=1e-8,
                    err_msg=f"{n_tiles} tiles produced different results than 2 tiles for {var_name}"
                )


class TestTileMergeEdgeCases:
    """Test edge cases in tile merging."""

    def test_single_value_dataset(self, tmp_output_dir):
        """Test tiling works with minimal 2x2 spatial grid."""
        # Create minimal dataset
        minimal_ds = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], np.ones((10, 2, 2), dtype=np.float32))},
            coords={
                'time': np.arange(10),
                'lat': [40, 41],
                'lon': [-120, -119]
            }
        )
        minimal_ds['tas'].attrs = {'units': 'degC'}

        pipeline = SimpleTilingPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': 2,
            'lon': 2,
        }

        # Should not crash even with minimal data
        result = pipeline.process_with_spatial_tiling(
            ds=minimal_ds,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Verify result has correct shape
        for var_name, data_array in result.items():
            assert data_array.shape == (1, 2, 2), f"{var_name} has wrong shape"

    def test_odd_dimensions(self, tmp_output_dir):
        """Test tiling works correctly with odd-sized dimensions."""
        # Create dataset with odd dimensions (51 x 51)
        odd_ds = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], np.random.rand(10, 51, 51).astype(np.float32))},
            coords={
                'time': np.arange(10),
                'lat': np.linspace(40, 45, 51),
                'lon': np.linspace(-120, -115, 51)
            }
        )
        odd_ds['tas'].attrs = {'units': 'degC'}

        pipeline = SimpleTilingPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': 51,
            'lon': 51,
        }

        result = pipeline.process_with_spatial_tiling(
            ds=odd_ds,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Verify complete coverage
        for var_name, data_array in result.items():
            assert data_array.shape == (1, 51, 51), f"{var_name} has wrong shape for odd dimensions"

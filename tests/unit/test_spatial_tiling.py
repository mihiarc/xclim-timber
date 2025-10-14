"""
Unit tests for core.spatial_tiling module.

Tests spatial tiling logic, tile processing, and merge operations.
"""

import pytest
import xarray as xr
import numpy as np
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from core.spatial_tiling import SpatialTilingMixin


class MockPipelineWithTiling(SpatialTilingMixin):
    """Mock pipeline class for testing SpatialTilingMixin."""

    def __init__(self, n_tiles: int = 4):
        SpatialTilingMixin.__init__(self, n_tiles=n_tiles)
        self.indices_calculated = []

    def calculate_indices(self, datasets: dict) -> dict:
        """Mock index calculation."""
        ds = datasets['primary']
        # Create a simple mock index
        result = {
            'mock_index': xr.DataArray(
                np.ones((1, ds.sizes['lat'], ds.sizes['lon'])),
                dims=['time', 'lat', 'lon'],
                coords={'time': [2020], 'lat': ds.lat, 'lon': ds.lon}
            )
        }
        result['mock_index'].attrs['units'] = '1'
        self.indices_calculated.append(result)
        return result


class TestSpatialTilingMixin:
    """Tests for SpatialTilingMixin class."""

    def test_init_with_2_tiles(self):
        """Test initialization with 2 tiles."""
        mixin = MockPipelineWithTiling(n_tiles=2)
        assert mixin.n_tiles == 2
        assert mixin.use_spatial_tiling is True

    def test_init_with_4_tiles(self):
        """Test initialization with 4 tiles."""
        mixin = MockPipelineWithTiling(n_tiles=4)
        assert mixin.n_tiles == 4
        assert mixin.use_spatial_tiling is True

    def test_init_with_8_tiles(self):
        """Test initialization with 8 tiles."""
        mixin = MockPipelineWithTiling(n_tiles=8)
        assert mixin.n_tiles == 8
        assert mixin.use_spatial_tiling is True

    def test_init_with_invalid_tiles(self):
        """Test initialization with invalid number of tiles raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            MockPipelineWithTiling(n_tiles=3)

        assert 'n_tiles must be 2, 4, or 8' in str(exc_info.value)

    def test_get_spatial_tiles_2_tiles(self, sample_temperature_dataset):
        """Test spatial tile calculation for 2 tiles (east/west)."""
        mixin = MockPipelineWithTiling(n_tiles=2)
        tiles = mixin._get_spatial_tiles(sample_temperature_dataset)

        assert len(tiles) == 2
        assert tiles[0][2] == 'west'
        assert tiles[1][2] == 'east'

        # Verify slices cover entire dataset
        lat_slice_west, lon_slice_west, _ = tiles[0]
        lat_slice_east, lon_slice_east, _ = tiles[1]

        assert lat_slice_west == slice(None), "Should cover all latitudes"
        assert lat_slice_east == slice(None), "Should cover all latitudes"

    def test_get_spatial_tiles_4_tiles(self, sample_temperature_dataset):
        """Test spatial tile calculation for 4 tiles (quadrants)."""
        mixin = MockPipelineWithTiling(n_tiles=4)
        tiles = mixin._get_spatial_tiles(sample_temperature_dataset)

        assert len(tiles) == 4
        tile_names = [tile[2] for tile in tiles]
        assert 'northwest' in tile_names
        assert 'northeast' in tile_names
        assert 'southwest' in tile_names
        assert 'southeast' in tile_names

    def test_get_spatial_tiles_8_tiles(self, sample_temperature_dataset):
        """Test spatial tile calculation for 8 tiles (octants)."""
        mixin = MockPipelineWithTiling(n_tiles=8)
        tiles = mixin._get_spatial_tiles(sample_temperature_dataset)

        assert len(tiles) == 8
        tile_names = [tile[2] for tile in tiles]
        assert 'nw1' in tile_names
        assert 'nw2' in tile_names
        assert 'ne1' in tile_names
        assert 'ne2' in tile_names
        assert 'sw1' in tile_names
        assert 'sw2' in tile_names
        assert 'se1' in tile_names
        assert 'se2' in tile_names

    def test_get_spatial_tiles_missing_lat_dimension(self):
        """Test error when dataset missing lat dimension."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        # Create dataset without lat dimension
        ds = xr.Dataset({
            'temp': (['time', 'lon'], np.random.rand(10, 10))
        })

        with pytest.raises(ValueError) as exc_info:
            mixin._get_spatial_tiles(ds)

        assert "Dataset must have 'lat' dimension" in str(exc_info.value)

    def test_get_spatial_tiles_missing_lon_dimension(self):
        """Test error when dataset missing lon dimension."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        # Create dataset without lon dimension
        ds = xr.Dataset({
            'temp': (['time', 'lat'], np.random.rand(10, 10))
        })

        with pytest.raises(ValueError) as exc_info:
            mixin._get_spatial_tiles(ds)

        assert "Dataset must have 'lon' dimension" in str(exc_info.value)

    def test_get_spatial_tiles_empty_dimensions(self):
        """Test error when dimensions are empty."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        # Create dataset with empty dimensions
        ds = xr.Dataset({
            'temp': (['time', 'lat', 'lon'], np.array([]).reshape(0, 0, 0))
        })

        with pytest.raises(ValueError) as exc_info:
            mixin._get_spatial_tiles(ds)

        assert 'dimension is empty' in str(exc_info.value)

    def test_process_single_tile(self, sample_temperature_dataset):
        """Test processing a single tile."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        lat_slice = slice(0, 5)
        lon_slice = slice(0, 5)
        tile_name = 'test_tile'

        indices = mixin._process_single_tile(
            sample_temperature_dataset,
            lat_slice,
            lon_slice,
            tile_name
        )

        assert isinstance(indices, dict)
        assert 'mock_index' in indices
        assert isinstance(indices['mock_index'], xr.DataArray)

    def test_save_tile(self, sample_temperature_dataset, tmp_path):
        """Test saving a tile to NetCDF."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        # Create mock indices
        tile_indices = {
            'mock_index': xr.DataArray(
                np.ones((1, 5, 5)),
                dims=['time', 'lat', 'lon'],
                attrs={'units': '1'}
            )
        }

        tile_file = mixin._save_tile(tile_indices, 'test_tile', tmp_path)

        assert tile_file.exists()
        assert tile_file.name == 'tile_test_tile.nc'

        # Verify file can be opened
        ds = xr.open_dataset(tile_file)
        assert 'mock_index' in ds.data_vars
        ds.close()

    def test_merge_tiles_2_tiles(self, tmp_path):
        """Test merging 2 tiles back into single dataset."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        # Create two tile files
        west_ds = xr.Dataset({
            'mock_index': (['time', 'lat', 'lon'], np.ones((1, 10, 5)))
        }, coords={'time': [2020], 'lat': range(10), 'lon': range(5)})

        east_ds = xr.Dataset({
            'mock_index': (['time', 'lat', 'lon'], np.ones((1, 10, 5)))
        }, coords={'time': [2020], 'lat': range(10), 'lon': range(5, 10)})

        west_file = tmp_path / 'tile_west.nc'
        east_file = tmp_path / 'tile_east.nc'
        west_ds.to_netcdf(west_file)
        east_ds.to_netcdf(east_file)

        # Merge tiles
        expected_dims = {'time': 1, 'lat': 10, 'lon': 10}
        merged = mixin._merge_tiles([west_file, east_file], expected_dims)

        assert merged.sizes['time'] == 1
        assert merged.sizes['lat'] == 10
        assert merged.sizes['lon'] == 10
        assert 'mock_index' in merged.data_vars

    def test_merge_tiles_4_tiles(self, tmp_path):
        """Test merging 4 tiles (quadrants) back into single dataset."""
        mixin = MockPipelineWithTiling(n_tiles=4)

        # Create four quadrant tile files
        tile_files = []
        for i, name in enumerate(['northwest', 'northeast', 'southwest', 'southeast']):
            lat_start = 0 if i < 2 else 5
            lon_start = 0 if i % 2 == 0 else 5

            ds = xr.Dataset({
                'mock_index': (['time', 'lat', 'lon'], np.ones((1, 5, 5)))
            }, coords={
                'time': [2020],
                'lat': range(lat_start, lat_start + 5),
                'lon': range(lon_start, lon_start + 5)
            })

            tile_file = tmp_path / f'tile_{name}.nc'
            ds.to_netcdf(tile_file)
            tile_files.append(tile_file)

        # Merge tiles
        expected_dims = {'time': 1, 'lat': 10, 'lon': 10}
        merged = mixin._merge_tiles(tile_files, expected_dims)

        assert merged.sizes['time'] == 1
        assert merged.sizes['lat'] == 10
        assert merged.sizes['lon'] == 10

    def test_merge_tiles_dimension_mismatch(self, tmp_path):
        """Test that dimension mismatch raises ValueError."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        # Create two tile files with wrong dimensions
        west_ds = xr.Dataset({
            'mock_index': (['time', 'lat', 'lon'], np.ones((1, 10, 5)))
        }, coords={'time': [2020], 'lat': range(10), 'lon': range(5)})

        east_ds = xr.Dataset({
            'mock_index': (['time', 'lat', 'lon'], np.ones((1, 10, 3)))  # Wrong lon size
        }, coords={'time': [2020], 'lat': range(10), 'lon': range(5, 8)})

        west_file = tmp_path / 'tile_west.nc'
        east_file = tmp_path / 'tile_east.nc'
        west_ds.to_netcdf(west_file)
        east_ds.to_netcdf(east_file)

        # Expected dimensions don't match actual
        expected_dims = {'time': 1, 'lat': 10, 'lon': 10}

        with pytest.raises(ValueError) as exc_info:
            mixin._merge_tiles([west_file, east_file], expected_dims)

        assert 'Dimension mismatch' in str(exc_info.value)

    def test_cleanup_tile_files(self, tmp_path):
        """Test cleanup of temporary tile files."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        # Create dummy tile files
        tile_files = []
        for i in range(2):
            tile_file = tmp_path / f'tile_{i}.nc'
            tile_file.touch()
            tile_files.append(tile_file)

        # Verify files exist
        for tile_file in tile_files:
            assert tile_file.exists()

        # Cleanup
        mixin._cleanup_tile_files(tile_files)

        # Verify files are deleted
        for tile_file in tile_files:
            assert not tile_file.exists()

    def test_get_ordered_tile_files_2_tiles(self):
        """Test getting ordered tile files for 2 tiles."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        tile_files_dict = {
            'west': Path('/tmp/tile_west.nc'),
            'east': Path('/tmp/tile_east.nc')
        }

        ordered = mixin._get_ordered_tile_files(tile_files_dict)

        assert len(ordered) == 2
        assert ordered[0] == tile_files_dict['west']
        assert ordered[1] == tile_files_dict['east']

    def test_get_ordered_tile_files_4_tiles(self):
        """Test getting ordered tile files for 4 tiles."""
        mixin = MockPipelineWithTiling(n_tiles=4)

        tile_files_dict = {
            'northwest': Path('/tmp/tile_nw.nc'),
            'northeast': Path('/tmp/tile_ne.nc'),
            'southwest': Path('/tmp/tile_sw.nc'),
            'southeast': Path('/tmp/tile_se.nc')
        }

        ordered = mixin._get_ordered_tile_files(tile_files_dict)

        assert len(ordered) == 4
        assert ordered[0] == tile_files_dict['northwest']
        assert ordered[1] == tile_files_dict['northeast']
        assert ordered[2] == tile_files_dict['southwest']
        assert ordered[3] == tile_files_dict['southeast']

    def test_process_with_spatial_tiling_integration(self, sample_temperature_dataset, tmp_path):
        """Integration test for complete spatial tiling workflow."""
        mixin = MockPipelineWithTiling(n_tiles=2)

        # Use small dataset for testing
        small_ds = sample_temperature_dataset.isel(time=slice(0, 10), lat=slice(0, 10), lon=slice(0, 10))

        expected_dims = {
            'time': 1,  # After aggregation
            'lat': 10,
            'lon': 10
        }

        # Process with spatial tiling
        all_indices = mixin.process_with_spatial_tiling(
            small_ds,
            tmp_path,
            expected_dims
        )

        assert isinstance(all_indices, dict)
        assert 'mock_index' in all_indices
        assert all_indices['mock_index'].sizes['lat'] == 10
        assert all_indices['mock_index'].sizes['lon'] == 10


@pytest.mark.slow
class TestSpatialTilingThreadSafety:
    """Tests for thread safety in spatial tiling."""

    def test_parallel_tile_processing(self, sample_temperature_dataset, tmp_path):
        """Test that parallel tile processing is thread-safe."""
        mixin = MockPipelineWithTiling(n_tiles=4)

        # Use small dataset
        small_ds = sample_temperature_dataset.isel(time=slice(0, 10), lat=slice(0, 20), lon=slice(0, 20))

        expected_dims = {
            'time': 1,
            'lat': 20,
            'lon': 20
        }

        # This should not raise any threading errors
        all_indices = mixin.process_with_spatial_tiling(
            small_ds,
            tmp_path,
            expected_dims
        )

        assert isinstance(all_indices, dict)
        assert len(mixin.indices_calculated) == 4  # Should have processed 4 tiles


@pytest.mark.regression
class TestSpatialTilingRegressions:
    """Regression tests for previously fixed bugs."""

    def test_tile_coordinate_alignment(self, sample_temperature_dataset, tmp_path):
        """
        Regression test: Ensure tiles have proper coordinate alignment after merge.

        This tests that concatenated tiles maintain proper coordinate continuity.
        """
        mixin = MockPipelineWithTiling(n_tiles=2)

        small_ds = sample_temperature_dataset.isel(time=slice(0, 10), lat=slice(0, 10), lon=slice(0, 10))

        expected_dims = {
            'time': 1,
            'lat': 10,
            'lon': 10
        }

        all_indices = mixin.process_with_spatial_tiling(small_ds, tmp_path, expected_dims)

        # Verify coordinates are continuous
        mock_index = all_indices['mock_index']
        assert len(mock_index.lon) == 10
        assert len(mock_index.lat) == 10

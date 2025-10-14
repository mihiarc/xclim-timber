#!/usr/bin/env python3
"""
End-to-end integration tests for spatial tiling functionality.

Tests the complete spatial tiling workflow including:
- Tile boundary calculation
- Spatial coverage verification (no gaps, no overlaps)
- Coordinate correctness
- Tile ordering
"""

import pytest
import numpy as np
import xarray as xr
from pathlib import Path

from core.spatial_tiling import SpatialTilingMixin


class TestSpatialTilingBoundaries:
    """Test tile boundary calculation and validation."""

    def test_tile_boundaries_2_tiles(self, small_test_dataset):
        """Test that 2 tiles (east/west) cover entire spatial domain correctly."""
        mixin = SpatialTilingMixin(n_tiles=2)
        tiles = mixin._get_spatial_tiles(small_test_dataset)

        assert len(tiles) == 2, "Should create 2 tiles"

        # Verify tile names
        tile_names = [tile[2] for tile in tiles]
        assert 'west' in tile_names, "Should have west tile"
        assert 'east' in tile_names, "Should have east tile"

        # Verify longitude coverage (should split at midpoint)
        lon_size = len(small_test_dataset.lon)
        expected_mid = lon_size // 2

        west_tile = [t for t in tiles if t[2] == 'west'][0]
        east_tile = [t for t in tiles if t[2] == 'east'][0]

        # West should be [0, mid)
        assert west_tile[1].start is None or west_tile[1].start == 0
        assert west_tile[1].stop == expected_mid

        # East should be [mid, None)
        assert east_tile[1].start == expected_mid
        assert east_tile[1].stop is None

        # Latitude should be full for both
        assert west_tile[0] == slice(None)
        assert east_tile[0] == slice(None)

    def test_tile_boundaries_4_tiles(self, small_test_dataset):
        """Test that 4 tiles (quadrants) cover entire spatial domain correctly."""
        mixin = SpatialTilingMixin(n_tiles=4)
        tiles = mixin._get_spatial_tiles(small_test_dataset)

        assert len(tiles) == 4, "Should create 4 tiles"

        # Verify tile names
        tile_names = [tile[2] for tile in tiles]
        expected_names = {'northwest', 'northeast', 'southwest', 'southeast'}
        assert set(tile_names) == expected_names, f"Expected {expected_names}, got {set(tile_names)}"

        # Calculate midpoints
        lat_size = len(small_test_dataset.lat)
        lon_size = len(small_test_dataset.lon)
        lat_mid = lat_size // 2
        lon_mid = lon_size // 2

        # Verify each quadrant has correct boundaries
        tile_dict = {tile[2]: tile for tile in tiles}

        # Northwest: [0, lat_mid) x [0, lon_mid)
        nw = tile_dict['northwest']
        assert nw[0].start == 0 and nw[0].stop == lat_mid
        assert nw[1].start == 0 and nw[1].stop == lon_mid

        # Northeast: [0, lat_mid) x [lon_mid, None)
        ne = tile_dict['northeast']
        assert ne[0].start == 0 and ne[0].stop == lat_mid
        assert ne[1].start == lon_mid and ne[1].stop is None

        # Southwest: [lat_mid, None) x [0, lon_mid)
        sw = tile_dict['southwest']
        assert sw[0].start == lat_mid and sw[0].stop is None
        assert sw[1].start == 0 and sw[1].stop == lon_mid

        # Southeast: [lat_mid, None) x [lon_mid, None)
        se = tile_dict['southeast']
        assert se[0].start == lat_mid and se[0].stop is None
        assert se[1].start == lon_mid and se[1].stop is None

    def test_tile_boundaries_8_tiles(self, small_test_dataset):
        """Test that 8 tiles (octants) cover entire spatial domain correctly."""
        mixin = SpatialTilingMixin(n_tiles=8)
        tiles = mixin._get_spatial_tiles(small_test_dataset)

        assert len(tiles) == 8, "Should create 8 tiles"

        # Verify tile names
        tile_names = [tile[2] for tile in tiles]
        expected_names = {'nw1', 'nw2', 'ne1', 'ne2', 'sw1', 'sw2', 'se1', 'se2'}
        assert set(tile_names) == expected_names

        # Calculate split points
        lat_size = len(small_test_dataset.lat)
        lon_size = len(small_test_dataset.lon)
        lat_mid = lat_size // 2
        lon_quarter = lon_size // 4

        # Verify longitude is split into 4 sections
        tile_dict = {tile[2]: tile for tile in tiles}

        # Check first row (north)
        assert tile_dict['nw1'][1].start == 0 and tile_dict['nw1'][1].stop == lon_quarter
        assert tile_dict['nw2'][1].start == lon_quarter and tile_dict['nw2'][1].stop == 2 * lon_quarter
        assert tile_dict['ne1'][1].start == 2 * lon_quarter and tile_dict['ne1'][1].stop == 3 * lon_quarter
        assert tile_dict['ne2'][1].start == 3 * lon_quarter and tile_dict['ne2'][1].stop is None


class TestSpatialCoverage:
    """Test that tiles provide complete spatial coverage without gaps or overlaps."""

    def test_no_gaps_no_overlaps_2_tiles(self, small_test_dataset):
        """Verify 2 tiles cover entire domain with no gaps or overlaps."""
        mixin = SpatialTilingMixin(n_tiles=2)
        tiles = mixin._get_spatial_tiles(small_test_dataset)

        lon_size = len(small_test_dataset.lon)
        lon_coverage = []

        for lat_slice, lon_slice, tile_name in tiles:
            lon_start = lon_slice.start if lon_slice.start is not None else 0
            lon_stop = lon_slice.stop if lon_slice.stop is not None else lon_size
            lon_coverage.extend(range(lon_start, lon_stop))

        # Check complete coverage
        assert len(lon_coverage) == lon_size, "Should cover all longitude points"

        # Check no overlaps (all unique)
        assert len(set(lon_coverage)) == lon_size, "Should have no overlapping longitude points"

        # Check sequential coverage
        assert sorted(lon_coverage) == list(range(lon_size)), "Should cover all points sequentially"

    def test_no_gaps_no_overlaps_4_tiles(self, small_test_dataset):
        """Verify 4 tiles cover entire domain with no gaps or overlaps."""
        mixin = SpatialTilingMixin(n_tiles=4)
        tiles = mixin._get_spatial_tiles(small_test_dataset)

        lat_size = len(small_test_dataset.lat)
        lon_size = len(small_test_dataset.lon)

        # Track coverage for each dimension
        lat_coverage = []
        lon_coverage = []

        for lat_slice, lon_slice, tile_name in tiles:
            lat_start = lat_slice.start if lat_slice.start is not None else 0
            lat_stop = lat_slice.stop if lat_slice.stop is not None else lat_size
            lon_start = lon_slice.start if lon_slice.start is not None else 0
            lon_stop = lon_slice.stop if lon_slice.stop is not None else lon_size

            # Each tile covers a rectangular region
            for lat_idx in range(lat_start, lat_stop):
                for lon_idx in range(lon_start, lon_stop):
                    lat_coverage.append(lat_idx)
                    lon_coverage.append(lon_idx)

        # Check complete coverage
        total_points = lat_size * lon_size
        assert len(lat_coverage) == total_points, f"Should cover all {total_points} spatial points"

        # Verify no overlaps by checking unique (lat, lon) pairs
        coverage_pairs = set(zip(lat_coverage, lon_coverage))
        assert len(coverage_pairs) == total_points, "Should have no overlapping spatial points"

    def test_no_gaps_no_overlaps_8_tiles(self, small_test_dataset):
        """Verify 8 tiles cover entire domain with no gaps or overlaps."""
        mixin = SpatialTilingMixin(n_tiles=8)
        tiles = mixin._get_spatial_tiles(small_test_dataset)

        lat_size = len(small_test_dataset.lat)
        lon_size = len(small_test_dataset.lon)

        # Track all covered grid points
        covered_points = set()

        for lat_slice, lon_slice, tile_name in tiles:
            lat_start = lat_slice.start if lat_slice.start is not None else 0
            lat_stop = lat_slice.stop if lat_slice.stop is not None else lat_size
            lon_start = lon_slice.start if lon_slice.start is not None else 0
            lon_stop = lon_slice.stop if lon_slice.stop is not None else lon_size

            # Add all points in this tile
            for lat_idx in range(lat_start, lat_stop):
                for lon_idx in range(lon_start, lon_stop):
                    point = (lat_idx, lon_idx)
                    assert point not in covered_points, f"Point {point} covered by multiple tiles (overlap detected)"
                    covered_points.add(point)

        # Verify complete coverage
        total_points = lat_size * lon_size
        assert len(covered_points) == total_points, f"Should cover all {total_points} points, got {len(covered_points)}"


class TestTileOrderingAndNames:
    """Test tile ordering for correct concatenation."""

    def test_tile_ordering_2_tiles(self):
        """Test _get_ordered_tile_files returns correct order for 2 tiles."""
        mixin = SpatialTilingMixin(n_tiles=2)

        tile_files_dict = {
            'west': Path('tile_west.nc'),
            'east': Path('tile_east.nc'),
        }

        ordered = mixin._get_ordered_tile_files(tile_files_dict)

        # Order should be: west, east (longitude concatenation)
        assert len(ordered) == 2
        assert ordered[0].name == 'tile_west.nc'
        assert ordered[1].name == 'tile_east.nc'

    def test_tile_ordering_4_tiles(self):
        """Test _get_ordered_tile_files returns correct order for 4 tiles."""
        mixin = SpatialTilingMixin(n_tiles=4)

        tile_files_dict = {
            'northwest': Path('tile_nw.nc'),
            'northeast': Path('tile_ne.nc'),
            'southwest': Path('tile_sw.nc'),
            'southeast': Path('tile_se.nc'),
        }

        ordered = mixin._get_ordered_tile_files(tile_files_dict)

        # Order should be: NW, NE, SW, SE (concat lon first, then lat)
        assert len(ordered) == 4
        assert ordered[0].name == 'tile_nw.nc'
        assert ordered[1].name == 'tile_ne.nc'
        assert ordered[2].name == 'tile_sw.nc'
        assert ordered[3].name == 'tile_se.nc'

    def test_tile_ordering_8_tiles(self):
        """Test _get_ordered_tile_files returns correct order for 8 tiles."""
        mixin = SpatialTilingMixin(n_tiles=8)

        tile_files_dict = {
            'nw1': Path('tile_nw1.nc'),
            'nw2': Path('tile_nw2.nc'),
            'ne1': Path('tile_ne1.nc'),
            'ne2': Path('tile_ne2.nc'),
            'sw1': Path('tile_sw1.nc'),
            'sw2': Path('tile_sw2.nc'),
            'se1': Path('tile_se1.nc'),
            'se2': Path('tile_se2.nc'),
        }

        ordered = mixin._get_ordered_tile_files(tile_files_dict)

        # Order should be: nw1, nw2, ne1, ne2, sw1, sw2, se1, se2
        assert len(ordered) == 8
        expected_order = ['tile_nw1.nc', 'tile_nw2.nc', 'tile_ne1.nc', 'tile_ne2.nc',
                          'tile_sw1.nc', 'tile_sw2.nc', 'tile_se1.nc', 'tile_se2.nc']
        actual_order = [f.name for f in ordered]
        assert actual_order == expected_order, f"Expected {expected_order}, got {actual_order}"


class TestTileConfigurationValidation:
    """Test configuration validation for spatial tiling."""

    def test_invalid_n_tiles_raises_error(self):
        """Test that invalid n_tiles values raise ValueError."""
        with pytest.raises(ValueError, match="n_tiles must be 2, 4, or 8"):
            SpatialTilingMixin(n_tiles=3)

        with pytest.raises(ValueError, match="n_tiles must be 2, 4, or 8"):
            SpatialTilingMixin(n_tiles=16)

    def test_empty_dataset_raises_error(self):
        """Test that empty datasets raise ValueError."""
        mixin = SpatialTilingMixin(n_tiles=4)

        # Dataset with empty lat dimension
        ds_empty_lat = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], np.array([]).reshape(1, 0, 10))},
            coords={'time': [0], 'lat': [], 'lon': np.arange(10)}
        )

        with pytest.raises(ValueError, match="lat dimension is empty"):
            mixin._get_spatial_tiles(ds_empty_lat)

        # Dataset with empty lon dimension
        ds_empty_lon = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], np.array([]).reshape(1, 10, 0))},
            coords={'time': [0], 'lat': np.arange(10), 'lon': []}
        )

        with pytest.raises(ValueError, match="lon dimension is empty"):
            mixin._get_spatial_tiles(ds_empty_lon)

    def test_missing_dimensions_raises_error(self):
        """Test that datasets missing lat or lon dimensions raise ValueError."""
        mixin = SpatialTilingMixin(n_tiles=4)

        # Dataset missing lat dimension
        ds_no_lat = xr.Dataset(
            {'tas': (['time', 'lon'], np.random.rand(10, 50))},
            coords={'time': np.arange(10), 'lon': np.arange(50)}
        )

        with pytest.raises(ValueError, match="Dataset must have 'lat' dimension"):
            mixin._get_spatial_tiles(ds_no_lat)

        # Dataset missing lon dimension
        ds_no_lon = xr.Dataset(
            {'tas': (['time', 'lat'], np.random.rand(10, 50))},
            coords={'time': np.arange(10), 'lat': np.arange(50)}
        )

        with pytest.raises(ValueError, match="Dataset must have 'lon' dimension"):
            mixin._get_spatial_tiles(ds_no_lon)

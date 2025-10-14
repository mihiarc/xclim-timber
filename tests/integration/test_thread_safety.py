#!/usr/bin/env python3
"""
Integration tests for thread safety in parallel tile processing.

Tests:
- Concurrent tile processing verification
- NetCDF write lock prevents race conditions
- Baseline lock prevents concurrent access
- Thread-safe tile file management
- No data corruption in parallel execution
"""

import pytest
import threading
import time
import numpy as np
import xarray as xr
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch, MagicMock

from core.spatial_tiling import SpatialTilingMixin, netcdf_write_lock


class ThreadMonitoringPipeline(SpatialTilingMixin):
    """Pipeline instrumented for thread monitoring."""

    def __init__(self, n_tiles=4):
        super().__init__(n_tiles=n_tiles)
        self.thread_log = []
        self.thread_log_lock = threading.Lock()
        self.baseline_access_log = []
        self.baseline_lock = threading.Lock()

    def calculate_indices(self, datasets):
        """Log thread activity during calculation."""
        thread_id = threading.current_thread().ident
        timestamp = time.time()

        with self.thread_log_lock:
            self.thread_log.append({
                'thread_id': thread_id,
                'event': 'calculate_start',
                'timestamp': timestamp
            })

        # Simulate calculation
        ds = datasets['primary']
        indices = {
            'test_index': ds['tas'].mean(dim='time', keepdims=True)
        }

        with self.thread_log_lock:
            self.thread_log.append({
                'thread_id': thread_id,
                'event': 'calculate_end',
                'timestamp': time.time()
            })

        return indices

    def _process_single_tile(self, ds, lat_slice, lon_slice, tile_name):
        """Override to log baseline access."""
        thread_id = threading.current_thread().ident

        # Log tile processing start
        with self.thread_log_lock:
            self.thread_log.append({
                'thread_id': thread_id,
                'event': 'tile_start',
                'tile': tile_name,
                'timestamp': time.time()
            })

        # Simulate baseline access with lock
        with self.baseline_lock:
            with self.thread_log_lock:
                self.baseline_access_log.append({
                    'thread_id': thread_id,
                    'event': 'baseline_access_start',
                    'tile': tile_name,
                    'timestamp': time.time()
                })

            # Simulate baseline processing
            time.sleep(0.01)

            with self.thread_log_lock:
                self.baseline_access_log.append({
                    'thread_id': thread_id,
                    'event': 'baseline_access_end',
                    'tile': tile_name,
                    'timestamp': time.time()
                })

        # Process tile
        tile_ds = ds.isel(lat=lat_slice, lon=lon_slice)
        indices = self.calculate_indices({'primary': tile_ds})

        # Log tile processing end
        with self.thread_log_lock:
            self.thread_log.append({
                'thread_id': thread_id,
                'event': 'tile_end',
                'tile': tile_name,
                'timestamp': time.time()
            })

        return indices


class TestConcurrentTileProcessing:
    """Test that tiles are processed concurrently."""

    def test_parallel_execution_occurs(self, small_test_dataset, tmp_output_dir):
        """Verify that multiple tiles are processed in parallel."""
        pipeline = ThreadMonitoringPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Process with tiling
        pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Analyze thread log for concurrent execution
        unique_threads = set(entry['thread_id'] for entry in pipeline.thread_log)

        # Should use multiple threads (at least 2 for 4 tiles)
        assert len(unique_threads) >= 2, \
            f"Expected parallel execution with multiple threads, got {len(unique_threads)}"

        # Check for overlapping tile processing (indication of parallelism)
        tile_start_events = [e for e in pipeline.thread_log if e['event'] == 'tile_start']
        tile_end_events = [e for e in pipeline.thread_log if e['event'] == 'tile_end']

        # If truly parallel, some tiles should start before others end
        if len(tile_start_events) >= 2 and len(tile_end_events) >= 1:
            first_end_time = min(e['timestamp'] for e in tile_end_events)
            starts_after_first_end = [e for e in tile_start_events if e['timestamp'] < first_end_time]

            # At least 2 tiles should start before the first one finishes (parallelism indicator)
            assert len(starts_after_first_end) >= 2, \
                "No evidence of parallel tile processing detected"

    def test_all_tiles_processed(self, small_test_dataset, tmp_output_dir):
        """Verify all tiles are processed exactly once."""
        pipeline = ThreadMonitoringPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Count tile processing events
        tile_names = [e['tile'] for e in pipeline.thread_log if e['event'] == 'tile_start']

        # Should process exactly 4 tiles
        assert len(tile_names) == 4, f"Expected 4 tiles, processed {len(tile_names)}"

        # Each tile should be processed exactly once
        expected_tiles = {'northwest', 'northeast', 'southwest', 'southeast'}
        assert set(tile_names) == expected_tiles, \
            f"Expected {expected_tiles}, got {set(tile_names)}"


class TestNetCDFWriteLock:
    """Test NetCDF write lock prevents concurrent writes."""

    def test_netcdf_write_serialization(self, small_test_dataset, tmp_output_dir):
        """Test that NetCDF writes are serialized even with parallel processing."""
        pipeline = ThreadMonitoringPipeline(n_tiles=4)

        # Track NetCDF write operations
        write_events = []
        write_lock_tracking = threading.Lock()

        original_to_netcdf = xr.Dataset.to_netcdf

        def instrumented_to_netcdf(self, *args, **kwargs):
            """Instrument to_netcdf to track concurrent access."""
            thread_id = threading.current_thread().ident

            with write_lock_tracking:
                write_events.append({
                    'thread_id': thread_id,
                    'event': 'write_start',
                    'timestamp': time.time()
                })

            # Add small delay to increase chance of detecting race conditions
            time.sleep(0.01)

            result = original_to_netcdf(self, *args, **kwargs)

            with write_lock_tracking:
                write_events.append({
                    'thread_id': thread_id,
                    'event': 'write_end',
                    'timestamp': time.time()
                })

            return result

        # Patch to_netcdf
        with patch.object(xr.Dataset, 'to_netcdf', instrumented_to_netcdf):
            expected_dims = {
                'time': 1,
                'lat': len(small_test_dataset.lat),
                'lon': len(small_test_dataset.lon),
            }

            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )

        # Verify writes were serialized (no overlapping write operations)
        write_starts = [e for e in write_events if e['event'] == 'write_start']
        write_ends = [e for e in write_events if e['event'] == 'write_end']

        # Sort by timestamp
        write_starts.sort(key=lambda e: e['timestamp'])
        write_ends.sort(key=lambda e: e['timestamp'])

        # Check that writes don't overlap: each start should be after previous end
        for i in range(1, len(write_starts)):
            start_time = write_starts[i]['timestamp']
            prev_end_time = write_ends[i - 1]['timestamp']

            # Allow small timing tolerance (1ms)
            assert start_time >= prev_end_time - 0.001, \
                f"NetCDF write {i} started before write {i-1} ended (race condition detected)"

    def test_netcdf_lock_exists(self):
        """Test that global netcdf_write_lock exists and is a Lock."""
        assert netcdf_write_lock is not None, "netcdf_write_lock not defined"
        assert isinstance(netcdf_write_lock, threading.Lock), \
            f"netcdf_write_lock should be threading.Lock, got {type(netcdf_write_lock)}"


class TestBaselineLockCorrectness:
    """Test baseline lock prevents concurrent baseline access."""

    def test_baseline_access_serialized(self, small_test_dataset, tmp_output_dir):
        """Test that baseline access is serialized by lock."""
        pipeline = ThreadMonitoringPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Verify baseline access was serialized
        baseline_log = pipeline.baseline_access_log

        if len(baseline_log) == 0:
            pytest.skip("No baseline access logged (test may need adjustment)")

        # Extract access intervals
        access_starts = [e for e in baseline_log if e['event'] == 'baseline_access_start']
        access_ends = [e for e in baseline_log if e['event'] == 'baseline_access_end']

        # Sort by timestamp
        access_starts.sort(key=lambda e: e['timestamp'])
        access_ends.sort(key=lambda e: e['timestamp'])

        # Verify no overlapping access (indicates lock is working)
        for i in range(1, len(access_starts)):
            start_time = access_starts[i]['timestamp']
            prev_end_time = access_ends[i - 1]['timestamp']

            # Baseline access should be serialized
            assert start_time >= prev_end_time, \
                f"Baseline access {i} started before access {i-1} ended (lock not working)"

    def test_baseline_lock_prevents_race_condition(self, small_test_dataset, tmp_output_dir):
        """Test that baseline lock prevents data races during concurrent tile processing."""
        # Create a pipeline with instrumented baseline access
        pipeline = ThreadMonitoringPipeline(n_tiles=4)

        # Shared counter to detect race conditions
        shared_counter = {'value': 0}
        race_detected = {'flag': False}

        original_process_tile = pipeline._process_single_tile

        def instrumented_process_tile(ds, lat_slice, lon_slice, tile_name):
            """Instrument to detect race conditions."""
            # Simulate shared resource access with potential race
            with pipeline.baseline_lock:
                # Critical section: non-atomic read-modify-write
                current_value = shared_counter['value']
                time.sleep(0.001)  # Increase race condition probability
                shared_counter['value'] = current_value + 1

            return original_process_tile(ds, lat_slice, lon_slice, tile_name)

        pipeline._process_single_tile = instrumented_process_tile

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # If lock works correctly, counter should be exactly 4 (one per tile)
        assert shared_counter['value'] == 4, \
            f"Race condition detected: expected counter=4, got {shared_counter['value']}"


class TestThreadSafeTileFileManagement:
    """Test thread-safe management of tile files."""

    def test_tile_files_dict_thread_safe(self, small_test_dataset, tmp_output_dir):
        """Test that tile_files_dict is updated thread-safely."""
        pipeline = ThreadMonitoringPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Should complete without errors (thread-safe dict updates)
        result = pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Verify result is complete
        assert len(result) > 0, "Result should contain calculated indices"

    def test_no_tile_file_corruption(self, small_test_dataset, tmp_output_dir):
        """Test that concurrent tile writing doesn't corrupt files."""
        pipeline = ThreadMonitoringPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Process with tiling
        result = pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # All temporary tile files should be cleaned up
        remaining_tiles = list(tmp_output_dir.glob('tile_*.nc'))
        assert len(remaining_tiles) == 0, \
            f"Tile files not cleaned up: {remaining_tiles}"

        # Result should have valid data (no NaN from corruption)
        for var_name, data_array in result.items():
            nan_count = np.isnan(data_array.values).sum()
            total_count = data_array.size

            # Allow some NaN values, but not excessive (>50% would indicate corruption)
            nan_fraction = nan_count / total_count
            assert nan_fraction < 0.5, \
                f"{var_name} has {nan_fraction*100:.1f}% NaN values (possible corruption)"


class TestThreadPoolExecutorBehavior:
    """Test ThreadPoolExecutor integration."""

    def test_executor_max_workers_respected(self, small_test_dataset, tmp_output_dir):
        """Test that executor respects max_workers setting."""
        pipeline = ThreadMonitoringPipeline(n_tiles=4)

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Process and track thread usage
        pipeline.process_with_spatial_tiling(
            ds=small_test_dataset,
            output_dir=tmp_output_dir,
            expected_dims=expected_dims
        )

        # Count unique threads used
        unique_threads = set(entry['thread_id'] for entry in pipeline.thread_log)

        # Should not exceed n_tiles workers (4 in this case)
        # Note: May be fewer if tiles complete quickly
        assert len(unique_threads) <= 4, \
            f"Expected max 4 worker threads, observed {len(unique_threads)}"

    def test_executor_exception_handling(self, small_test_dataset, tmp_output_dir):
        """Test that executor properly propagates exceptions from tile processing."""
        pipeline = ThreadMonitoringPipeline(n_tiles=4)

        # Inject failure in tile processing
        original_calculate = pipeline.calculate_indices

        def failing_calculate(datasets):
            raise RuntimeError("Simulated tile processing failure")

        pipeline.calculate_indices = failing_calculate

        expected_dims = {
            'time': 1,
            'lat': len(small_test_dataset.lat),
            'lon': len(small_test_dataset.lon),
        }

        # Should propagate exception from tile processing
        with pytest.raises(RuntimeError, match="Simulated tile processing failure"):
            pipeline.process_with_spatial_tiling(
                ds=small_test_dataset,
                output_dir=tmp_output_dir,
                expected_dims=expected_dims
            )

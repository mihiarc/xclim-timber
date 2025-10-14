#!/usr/bin/env python3
"""
Simple test runner for integration tests.
Bypasses pytest plugin issues by running tests directly.
"""

import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_imports():
    """Test that all integration test modules can be imported."""
    logger.info("Testing integration test imports...")

    try:
        from tests.integration import test_spatial_tiling_e2e
        logger.info("  - test_spatial_tiling_e2e: OK")
    except Exception as e:
        logger.error(f"  - test_spatial_tiling_e2e: FAILED - {e}")
        return False

    try:
        from tests.integration import test_tile_merge
        logger.info("  - test_tile_merge: OK")
    except Exception as e:
        logger.error(f"  - test_tile_merge: FAILED - {e}")
        return False

    try:
        from tests.integration import test_thread_safety
        logger.info("  - test_thread_safety: OK")
    except Exception as e:
        logger.error(f"  - test_thread_safety: FAILED - {e}")
        return False

    try:
        from tests.integration import test_error_recovery
        logger.info("  - test_error_recovery: OK")
    except Exception as e:
        logger.error(f"  - test_error_recovery: FAILED - {e}")
        return False

    try:
        from tests.integration import test_temperature_pipeline
        logger.info("  - test_temperature_pipeline: OK")
    except Exception as e:
        logger.error(f"  - test_temperature_pipeline: FAILED - {e}")
        return False

    try:
        from tests.integration import test_precipitation_pipeline
        logger.info("  - test_precipitation_pipeline: OK")
    except Exception as e:
        logger.error(f"  - test_precipitation_pipeline: FAILED - {e}")
        return False

    return True


def test_basic_functionality():
    """Test basic tiling functionality without full pytest."""
    logger.info("\nTesting basic tiling functionality...")

    try:
        from core.spatial_tiling import SpatialTilingMixin
        import xarray as xr
        import numpy as np

        # Create test dataset
        dates = [np.datetime64('2023-01-01') + np.timedelta64(i, 'D') for i in range(100)]
        lat = np.linspace(40, 45, 50)
        lon = np.linspace(-120, -115, 50)

        tas = 15 + np.random.randn(100, 50, 50) * 5

        ds = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], tas.astype(np.float32))},
            coords={'time': dates, 'lat': lat, 'lon': lon}
        )
        ds['tas'].attrs = {'units': 'degC'}

        # Test tile generation
        mixin = SpatialTilingMixin(n_tiles=4)
        tiles = mixin._get_spatial_tiles(ds)

        assert len(tiles) == 4, f"Expected 4 tiles, got {len(tiles)}"
        logger.info(f"  - Tile generation: OK (created {len(tiles)} tiles)")

        # Test tile coverage
        tile_names = [tile[2] for tile in tiles]
        expected_names = {'northwest', 'northeast', 'southwest', 'southeast'}
        assert set(tile_names) == expected_names, f"Unexpected tile names: {tile_names}"
        logger.info(f"  - Tile names: OK ({', '.join(tile_names)})")

        # Test tile ordering
        tile_files_dict = {
            'northwest': Path('tile_nw.nc'),
            'northeast': Path('tile_ne.nc'),
            'southwest': Path('tile_sw.nc'),
            'southeast': Path('tile_se.nc'),
        }
        ordered = mixin._get_ordered_tile_files(tile_files_dict)
        assert len(ordered) == 4, "Tile ordering failed"
        logger.info(f"  - Tile ordering: OK")

        return True

    except Exception as e:
        logger.error(f"  - Basic functionality test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def count_test_classes():
    """Count test classes in integration tests."""
    logger.info("\nCounting test classes...")

    from tests.integration import test_spatial_tiling_e2e
    from tests.integration import test_tile_merge
    from tests.integration import test_thread_safety
    from tests.integration import test_error_recovery
    from tests.integration import test_temperature_pipeline
    from tests.integration import test_precipitation_pipeline

    modules = [
        ('test_spatial_tiling_e2e', test_spatial_tiling_e2e),
        ('test_tile_merge', test_tile_merge),
        ('test_thread_safety', test_thread_safety),
        ('test_error_recovery', test_error_recovery),
        ('test_temperature_pipeline', test_temperature_pipeline),
        ('test_precipitation_pipeline', test_precipitation_pipeline),
    ]

    total_classes = 0
    for module_name, module in modules:
        classes = [name for name in dir(module) if name.startswith('Test')]
        logger.info(f"  - {module_name}: {len(classes)} test classes")
        total_classes += len(classes)

    logger.info(f"\nTotal test classes: {total_classes}")
    return total_classes


def main():
    """Main test runner."""
    logger.info("=" * 60)
    logger.info("Integration Test Suite Validation")
    logger.info("=" * 60)

    success = True

    # Test 1: Import all test modules
    if not test_imports():
        logger.error("\nTest import FAILED")
        success = False
    else:
        logger.info("\nTest imports: PASSED")

    # Test 2: Basic functionality
    if not test_basic_functionality():
        logger.error("\nBasic functionality test FAILED")
        success = False
    else:
        logger.info("\nBasic functionality: PASSED")

    # Test 3: Count test classes
    try:
        count = count_test_classes()
        logger.info(f"\nTest class counting: PASSED ({count} classes)")
    except Exception as e:
        logger.error(f"\nTest class counting FAILED: {e}")
        success = False

    # Summary
    logger.info("\n" + "=" * 60)
    if success:
        logger.info("INTEGRATION TEST SUITE VALIDATION: PASSED")
        logger.info("=" * 60)
        logger.info("\nAll integration test modules are correctly structured.")
        logger.info("To run full test suite with pytest:")
        logger.info("  pytest tests/integration/ -v -p no:cov")
        return 0
    else:
        logger.error("INTEGRATION TEST SUITE VALIDATION: FAILED")
        logger.error("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())

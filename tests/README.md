# xclim-timber Test Suite

Comprehensive test infrastructure for xclim-timber climate index calculation pipelines.

## Overview

This test suite provides:
- **Unit tests** for core modules (base_pipeline, spatial_tiling, baseline_loader, config)
- **Index calculation tests** for temperature, precipitation, and drought indices
- **Regression tests** for previously fixed bugs
- **Test fixtures** for reusable test data generation
- **Coverage reporting** to track test effectiveness

## Directory Structure

```
tests/
├── __init__.py
├── conftest.py                    # Shared fixtures and test utilities
├── README.md                      # This file
├── unit/
│   ├── __init__.py
│   ├── test_base_pipeline.py      # Core pipeline functionality
│   ├── test_spatial_tiling.py     # Spatial tiling logic
│   ├── test_baseline_loader.py    # Baseline percentile loading
│   ├── test_config.py              # Configuration module
│   └── test_indices/
│       ├── __init__.py
│       ├── test_temperature_indices.py
│       ├── test_precipitation_indices.py
│       └── test_drought_indices.py
└── fixtures/                       # Test data files (generated on-the-fly)
```

## Installation

Install test dependencies using uv:

```bash
uv pip install -r requirements-test.txt
```

This installs:
- `pytest` - Test framework
- `pytest-cov` - Coverage reporting
- `pytest-xdist` - Parallel test execution
- `pytest-mock` - Mocking utilities
- `hypothesis` - Property-based testing
- `freezegun` - Time mocking

## Running Tests

### Run All Tests

```bash
pytest
```

### Run with Coverage Report

```bash
pytest --cov=core --cov=temperature_pipeline --cov-report=html
```

View the HTML coverage report:
```bash
open htmlcov/index.html
```

### Run Specific Test Files

```bash
# Test base pipeline only
pytest tests/unit/test_base_pipeline.py

# Test temperature indices only
pytest tests/unit/test_indices/test_temperature_indices.py

# Test specific test class
pytest tests/unit/test_config.py::TestPipelineConfig

# Test specific test function
pytest tests/unit/test_base_pipeline.py::TestBasePipeline::test_init_with_defaults
```

### Run Tests by Marker

```bash
# Run only unit tests
pytest -m unit

# Run only regression tests
pytest -m regression

# Run only slow tests
pytest -m slow

# Skip slow tests
pytest -m "not slow"
```

### Run Tests in Parallel

```bash
# Use all CPU cores
pytest -n auto

# Use specific number of workers
pytest -n 4
```

### Run Tests with Verbose Output

```bash
# Show test names as they run
pytest -v

# Show full output (including print statements)
pytest -s

# Show both
pytest -vs
```

### Run Tests with Specific Warnings

```bash
# Show all warnings
pytest -W all

# Fail on warnings
pytest -W error
```

## Test Organization

### Unit Tests

**test_config.py** (20 tests)
- Configuration constant validation
- Variable renaming maps
- Unit fix mappings
- CF standard names
- Baseline configuration
- Default encoding settings

**test_baseline_loader.py** (15 tests)
- Baseline file loading and caching
- Variable selection and validation
- Temperature/precipitation/multivariate baselines
- Missing file error handling
- Baseline period validation
- Thread-safe baseline access

**test_spatial_tiling.py** (18 tests)
- Tile boundary calculation (2, 4, 8 tiles)
- Tile processing and saving
- Tile merging and concatenation
- Dimension validation
- Thread-safe parallel processing
- Coordinate alignment

**test_base_pipeline.py** (25 tests)
- Pipeline initialization
- Zarr data loading
- Variable renaming and unit fixing
- Metadata addition
- NetCDF output with compression
- Time chunking logic
- Error handling
- Path traversal security (regression)

### Index Calculation Tests

**test_temperature_indices.py** (35+ tests)
- Basic indices: frost_days, ice_days, summer_days, growing_degree_days
- Extreme indices: tx90p, tn90p, warm/cold spell duration
- Advanced indices: growing season timing, spell frequency
- Known value validation
- Attribute and dimension validation

**test_precipitation_indices.py** (30+ tests)
- Basic indices: prcptot, cdd, cwd, wetdays
- Extreme indices: r95p, r99p, r95ptot
- Threshold indices: r1mm, r10mm, r20mm
- Intensity and max precipitation
- Edge cases (all dry, all wet)

**test_drought_indices.py** (15+ tests)
- Dry spell calculations
- Dry spell frequency
- Maximum consecutive dry days
- Known pattern validation
- Edge case handling

## Test Fixtures

### Data Generation Fixtures

Located in `conftest.py`:

- **sample_temperature_dataset**: Small temperature dataset (365 days, 10x10 grid)
- **sample_precipitation_dataset**: Small precipitation dataset
- **sample_baseline_percentiles**: Test baseline percentiles
- **known_temperature_data**: Dataset with known expected values
- **known_precipitation_data**: Dataset with known expected values
- **temp_zarr_store**: Temporary Zarr store for testing
- **baseline_file**: Temporary baseline NetCDF file

### Utility Functions

- `create_test_temperature_dataset()`: Generate test temperature data
- `create_test_precipitation_dataset()`: Generate test precipitation data
- `create_test_baseline_percentiles()`: Generate test baselines
- `assert_dataarray_valid()`: Validate DataArray structure
- `assert_dataset_has_indices()`: Validate index presence
- `assert_netcdf_file_valid()`: Validate NetCDF output

## Coverage Targets

### Current Coverage (Baseline)

Run this command to see current coverage:
```bash
pytest --cov=core --cov=temperature_pipeline --cov-report=term-missing
```

### Coverage Goals

- **Core modules** (base_pipeline, spatial_tiling, baseline_loader): **80%+**
- **Index calculations**: **70%+**
- **Configuration**: **60%+**

## Writing New Tests

### Test-Driven Development (TDD) Workflow

1. **Write Failing Test First**
   ```python
   def test_new_feature():
       """Test description of expected behavior."""
       result = new_feature(input_data)
       assert result == expected_value
   ```

2. **Run Test (Should Fail)**
   ```bash
   pytest tests/unit/test_module.py::test_new_feature
   ```

3. **Implement Minimal Code**
   Write just enough code to make the test pass.

4. **Run Test Again (Should Pass)**
   ```bash
   pytest tests/unit/test_module.py::test_new_feature
   ```

5. **Refactor with Confidence**
   Improve code while tests ensure correctness.

### Test Naming Conventions

- Test files: `test_<module_name>.py`
- Test classes: `Test<ComponentName>`
- Test functions: `test_<behavior>_<condition>`

Examples:
```python
class TestSpatialTiling:
    def test_get_spatial_tiles_2_tiles(self):
        """Test spatial tile calculation for 2 tiles (east/west)."""
        pass

    def test_merge_tiles_dimension_mismatch(self):
        """Test that dimension mismatch raises ValueError."""
        pass
```

### Using Fixtures

```python
def test_with_fixture(sample_temperature_dataset):
    """Test using fixture-provided data."""
    assert 'tas' in sample_temperature_dataset
    assert len(sample_temperature_dataset.time) == 365
```

### Testing Exceptions

```python
def test_raises_exception():
    """Test that invalid input raises appropriate exception."""
    with pytest.raises(ValueError) as exc_info:
        function_that_should_fail(invalid_input)

    assert "expected error message" in str(exc_info.value)
```

### Parametrized Tests

```python
@pytest.mark.parametrize("n_tiles,expected_count", [
    (2, 2),
    (4, 4),
    (8, 8)
])
def test_tile_counts(n_tiles, expected_count):
    """Test tile creation with different configurations."""
    tiles = create_tiles(n_tiles)
    assert len(tiles) == expected_count
```

## Regression Tests

Regression tests verify that previously fixed bugs don't reappear.

### Marking Regression Tests

```python
@pytest.mark.regression
class TestSpatialTilingRegressions:
    def test_tile_coordinate_alignment(self):
        """
        Regression test: Ensure tiles have proper coordinate alignment after merge.
        """
        pass
```

### Run Only Regression Tests

```bash
pytest -m regression
```

## Continuous Integration

Tests are designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: pytest --cov=core --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
```

## Performance Considerations

### Test Speed

- **Unit tests should run fast** (< 5 seconds total)
- Use small test datasets (10x10 grids, 365 days)
- Mock external dependencies where appropriate
- Mark slow tests with `@pytest.mark.slow`

### Memory Usage

- Test fixtures use small datasets to minimize memory
- Cleanup temporary files after tests
- Use `tmp_path` fixture for temporary files

## Troubleshooting

### Tests Fail with Import Errors

Ensure you're running tests from the repository root:
```bash
cd /path/to/xclim-timber
pytest
```

### Tests Fail with Missing Dependencies

Install test requirements:
```bash
uv pip install -r requirements-test.txt
```

### Tests Pass Locally but Fail in CI

Check for:
- Platform-specific behavior
- Hardcoded paths
- Timezone dependencies
- Random seed issues

### Coverage Report Not Generated

Ensure pytest-cov is installed:
```bash
uv pip install pytest-cov
```

## Contributing

When adding new features:

1. Write tests first (TDD approach)
2. Ensure tests pass: `pytest`
3. Check coverage: `pytest --cov=core --cov-report=term-missing`
4. Run regression tests: `pytest -m regression`
5. Document complex test scenarios in docstrings

## Contact

For questions about the test suite, see the main project README or open an issue.

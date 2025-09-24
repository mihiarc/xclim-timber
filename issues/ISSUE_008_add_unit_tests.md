# Issue #008: Add unit tests for climate indices calculations

## Problem Description
The timedelta and temperature range issues could have been caught earlier with proper unit tests. We need comprehensive test coverage for the indices calculator to prevent regression and ensure reliability.

## Test Coverage Required

### 1. Data Type Tests
```python
def test_day_count_indices_return_numeric():
    """Ensure day count indices never return timedelta."""
    # Test frost_days, ice_days, tropical_nights, etc.
    result = calculator.calculate_frost_days(test_data)
    assert not np.issubdtype(result.dtype, np.timedelta64)
    assert np.issubdtype(result.dtype, np.number)
```

### 2. Value Range Tests
```python
def test_temperature_range_always_positive():
    """Temperature range must always be positive."""
    tmax = xr.DataArray([20, 25, 30])
    tmin = xr.DataArray([10, 15, 20])
    dtr = calculator.calculate_daily_temp_range(tmin, tmax)
    assert (dtr >= 0).all()
```

### 3. Unit Conversion Tests
```python
def test_temperature_unit_conversions():
    """Test all temperature unit conversions."""
    # Test Kelvin to Celsius
    # Test degrees_celsius to degC
    # Test that differences don't get offset
```

### 4. Edge Case Tests
```python
def test_missing_data_handling():
    """Test NaN propagation and handling."""
    data_with_nan = xr.DataArray([np.nan, 10, 20, np.nan])
    result = calculator.calculate_mean_temp(data_with_nan)
    # Verify NaN handling is correct
```

### 5. Index Relationship Tests
```python
def test_index_relationships():
    """Test physical constraints between indices."""
    # frost_days >= ice_days
    # tmin <= tmean <= tmax
    # 0 <= day_counts <= 365
```

## Test Organization

```
tests/
├── test_indices_calculator.py
│   ├── TestDataTypes
│   ├── TestValueRanges
│   ├── TestUnitConversions
│   ├── TestIndexRelationships
│   └── TestEdgeCases
├── test_data_loader.py
├── test_preprocessor.py
└── fixtures/
    ├── sample_temperature_data.nc
    ├── sample_precipitation_data.nc
    └── expected_outputs.nc
```

## Implementation Plan

### Phase 1: Critical Tests (Immediate)
- Day count data type tests
- Temperature range positivity tests
- Basic unit conversion tests

### Phase 2: Comprehensive Coverage
- All indices calculation tests
- Edge case handling
- Performance tests

### Phase 3: Integration Tests
- Full pipeline tests
- Multi-year processing tests
- Error recovery tests

## Testing Framework

```python
import pytest
import xarray as xr
import numpy as np
from src.indices_calculator import ClimateIndicesCalculator

class TestIndicesCalculator:

    @pytest.fixture
    def calculator(self):
        """Create calculator instance for tests."""
        config = Config('test_config.yaml')
        return ClimateIndicesCalculator(config)

    @pytest.fixture
    def sample_temperature_data(self):
        """Generate sample temperature data."""
        time = pd.date_range('2020-01-01', periods=365)
        lat = np.linspace(30, 50, 10)
        lon = np.linspace(-120, -100, 10)

        tmax = 20 + 10 * np.random.random((365, 10, 10))
        tmin = tmax - 5 - 5 * np.random.random((365, 10, 10))

        return xr.Dataset({
            'tmax': (['time', 'lat', 'lon'], tmax),
            'tmin': (['time', 'lat', 'lon'], tmin),
        }, coords={'time': time, 'lat': lat, 'lon': lon})

    def test_frost_days_returns_numeric(self, calculator, sample_temperature_data):
        """Frost days must return numeric, not timedelta."""
        result = calculator.calculate_temperature_indices(sample_temperature_data)
        assert 'frost_days' in result
        assert not 'timedelta' in str(result['frost_days'].dtype)
        assert result['frost_days'].min() >= 0
        assert result['frost_days'].max() <= 365
```

## Success Criteria
- ✅ All critical tests passing
- ✅ >80% code coverage for indices_calculator.py
- ✅ Tests run automatically in CI/CD
- ✅ Clear test documentation

## Benefits
- Prevent regression of fixed issues
- Catch bugs before production
- Document expected behavior
- Improve confidence in results

## Priority
**MEDIUM-HIGH** - Essential for long-term reliability

## Estimated Effort
- Phase 1: 2-3 hours
- Phase 2: 4-6 hours
- Phase 3: 2-3 hours

## Dependencies
- pytest framework
- sample test data
- CI/CD setup (optional)

## References
- pytest documentation
- xarray testing guide
- Climate indices validation standards
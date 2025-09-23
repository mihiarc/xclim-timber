# Issue #005: Unit conversion errors in temperature processing chain

## Problem Description
Multiple unit conversion issues are occurring throughout the temperature processing chain, causing incorrect values in climate indices. The pipeline handles units inconsistently between data loading, preprocessing, and index calculation stages.

## Identified Problems

### 1. Input Unit Detection
- PRISM data uses `degrees_celsius` but xclim expects `degC`
- Fixed in `_ensure_temperature_units()` but may have gaps

### 2. Temperature Difference Units
- Temperature differences (ΔT) don't need K→C conversion
- 10K difference = 10°C difference (no offset needed)
- Current code may be applying -273.15 offset to differences

### 3. Output Unit Inconsistency
```
Current outputs show mixed units:
- tg_mean: degC ✓
- frost_days: timedelta64[ns] ✗
- daily_temperature_range: K or incorrect C ✗
- growing_degree_days: K·day or C·day?
```

## Complete Unit Handling Audit

### Data Flow:
```
PRISM Zarr → Load → Preprocess → Calculate → Save
  ↓          ↓         ↓           ↓         ↓
deg_celsius  ?      degC?      varies    mixed units
```

### Required Fixes by Stage:

#### Stage 1: Data Loading (data_loader.py)
```python
def load_zarr(self, store_path):
    ds = xr.open_zarr(store_path)

    # Standardize units immediately
    for var in ds.data_vars:
        if 'temperature' in var or var in ['tmin', 'tmax', 'tmean']:
            if ds[var].attrs.get('units') == 'degrees_celsius':
                ds[var].attrs['units'] = 'degC'

    return ds
```

#### Stage 2: Preprocessing (preprocessor.py)
```python
def standardize_units(self, ds):
    """Ensure all temperature variables use standard units."""

    temp_vars = ['tas', 'tasmin', 'tasmax', 'tmin', 'tmax', 'tmean']

    for var in ds.data_vars:
        if var in temp_vars:
            # Map all temperature unit variations
            unit_corrections = {
                'degrees_celsius': 'degC',
                'degree_celsius': 'degC',
                'degrees_Celsius': 'degC',
                'celsius': 'degC',
                'Celsius': 'degC',
                'C': 'degC',
                'degree_C': 'degC',
                'deg_C': 'degC'
            }

            current_units = ds[var].attrs.get('units', '')

            if current_units in unit_corrections:
                ds[var].attrs['units'] = unit_corrections[current_units]
                logger.debug(f"Corrected units for {var}: {current_units} → degC")

    return ds
```

#### Stage 3: Index Calculation (indices_calculator.py)
```python
def calculate_temperature_indices(self, ds):
    # For difference-based indices
    if 'daily_temperature_range' in configured_indices:
        # DTR is a difference, not absolute temperature
        dtr = tasmax - tasmin  # This is already in correct units

        # Don't convert differences from K to C (no offset)
        dtr.attrs['units'] = 'degC'  # It's a Δ, same in K or C

        indices['daily_temperature_range'] = dtr

    # For count-based indices (days)
    if 'frost_days' in configured_indices:
        result = atmos.frost_days(tasmin, freq='YS')

        # Convert timedelta to numeric days
        if 'timedelta' in str(result.dtype):
            result = result / np.timedelta64(1, 'D')
            result.attrs['units'] = 'days'

        indices['frost_days'] = result

    # For degree-day indices
    if 'growing_degree_days' in configured_indices:
        gdd = atmos.growing_degree_days(tas, thresh='10 degC', freq='YS')

        # Ensure output is in degC·days
        if gdd.attrs.get('units') == 'K days':
            # Don't subtract 273.15 from degree-days!
            gdd.attrs['units'] = 'degC days'

        indices['growing_degree_days'] = gdd
```

## Unit Conversion Rules

### Rule 1: Absolute Temperatures
- Require offset: K ↔ °C uses ±273.15
- Require offset: °F ↔ °C uses formula

### Rule 2: Temperature Differences
- NO offset: ΔK = Δ°C
- Scale only: Δ°F = 1.8 × Δ°C

### Rule 3: Accumulated Degree-Days
- Same as differences: K·days = °C·days numerically
- Just change unit label, not values

### Rule 4: Day Counts
- Convert timedelta64 to float/int
- Always report as "days" unit

## Comprehensive Test Suite Needed

```python
def test_unit_conversions():
    # Test absolute temperature
    assert convert_temperature(0, 'degC', 'K') == 273.15

    # Test temperature difference
    assert convert_difference(10, 'K', 'degC') == 10  # No offset!

    # Test degree-days
    assert convert_degree_days(100, 'K days', 'degC days') == 100

    # Test day counts
    td = np.timedelta64(10, 'D')
    assert convert_to_days(td) == 10
```

## Priority
**HIGH** - Unit errors affect multiple indices and can make results completely wrong or unusable.

## Impact of Fixing
- ✅ Daily temperature range becomes positive
- ✅ Frost days become numeric values
- ✅ All indices have consistent, correct units
- ✅ Results match expected climatology

## Files Requiring Updates
1. `src/data_loader.py` - Standardize on load
2. `src/preprocessor.py` - Complete unit mapping
3. `src/indices_calculator.py` - Fix all index calculations
4. `tests/test_units.py` - NEW: Comprehensive unit tests

## Validation After Fix
Run QA/QC and verify:
- No negative temperature ranges
- No timedelta data types in output
- All values in physically plausible ranges
- Units clearly documented in attributes

## References
- CF Conventions on units: https://cfconventions.org/
- xclim unit handling: https://xclim.readthedocs.io/en/stable/notebooks/units.html
- NumPy datetime64: https://numpy.org/doc/stable/reference/arrays.datetime.html
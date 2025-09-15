"""
Corrected implementations of climate indices for xclim-timber.
These corrections address scientific errors found in the original implementation.
"""

import numpy as np
import pandas as pd


def calculate_gdd_with_cap(data, base_temp, cap_temp=None):
    """
    Calculate Growing Degree Days with proper temperature capping.

    Parameters:
    -----------
    data : ndarray
        Temperature data (n_parcels, n_days) in Celsius
    base_temp : float
        Base temperature below which growth doesn't occur
    cap_temp : float, optional
        Maximum temperature cap (growth doesn't increase above this)

    Returns:
    --------
    ndarray : GDD sum for each parcel
    """
    if cap_temp is not None:
        # Cap the temperature first, then subtract base
        effective_temp = np.minimum(data, cap_temp)
        gdd = np.maximum(effective_temp - base_temp, 0)
    else:
        gdd = np.maximum(data - base_temp, 0)

    return np.nansum(gdd, axis=1)


def calculate_corn_gdd(data):
    """
    Calculate Corn Growing Degree Days with correct formula.
    Base: 10°C, Cap: 30°C (standard for corn/maize)

    Parameters:
    -----------
    data : ndarray
        Temperature data (n_parcels, n_days) in Celsius

    Returns:
    --------
    ndarray : Corn GDD sum for each parcel
    """
    # First cap temperature at 30°C, then subtract base of 10°C
    effective_temp = np.minimum(data, 30)
    corn_gdd = np.maximum(effective_temp - 10, 0)
    return np.nansum(corn_gdd, axis=1)


def calculate_freeze_thaw_cycles(data):
    """
    Calculate freeze-thaw cycles (crossings of 0°C).

    Parameters:
    -----------
    data : ndarray
        Temperature data (n_parcels, n_days) in Celsius

    Returns:
    --------
    ndarray : Number of freeze-thaw cycles for each parcel
    """
    n_parcels = data.shape[0]
    cycles = np.zeros(n_parcels)

    for i in range(n_parcels):
        temps = data[i]
        # Remove NaN values for clean calculation
        temps_clean = temps[~np.isnan(temps)]

        if len(temps_clean) > 1:
            # Find where temperature crosses 0°C
            above_zero = temps_clean > 0
            # Count transitions from False to True (thaw) or True to False (freeze)
            transitions = np.diff(above_zero.astype(int))
            # Each crossing (either direction) is a transition
            cycles[i] = np.sum(np.abs(transitions))

    return cycles


def calculate_bioclim_variables(data):
    """
    Calculate proper BIOCLIM variables from daily temperature data.
    Note: Full implementation requires monthly aggregation.

    Parameters:
    -----------
    data : ndarray
        Temperature data (n_parcels, n_days) in Celsius

    Returns:
    --------
    dict : Dictionary of bioclim variables
    """
    n_parcels, n_days = data.shape
    results = {}

    # BIO1: Annual Mean Temperature
    results['bio1_annual_mean_temp'] = np.nanmean(data, axis=1)

    # BIO2: Mean Diurnal Range (requires min/max data - approximation here)
    # Cannot be properly calculated from daily means alone
    results['bio2_diurnal_range'] = np.full(n_parcels, np.nan)  # Mark as unavailable

    # BIO4: Temperature Seasonality (using monthly means)
    # Approximate by reshaping to 12 months (assumes 365 days)
    if n_days >= 365:
        monthly_means = np.zeros((n_parcels, 12))
        days_per_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        day_idx = 0
        for month, days in enumerate(days_per_month):
            if day_idx + days <= n_days:
                monthly_means[:, month] = np.nanmean(data[:, day_idx:day_idx+days], axis=1)
                day_idx += days

        # Temperature seasonality = std dev of monthly means * 100
        results['bio4_temp_seasonality'] = np.nanstd(monthly_means, axis=1) * 100
    else:
        results['bio4_temp_seasonality'] = np.nanstd(data, axis=1) * 100  # Fallback

    # BIO5: Max Temperature of Warmest Month (using monthly max)
    # BIO6: Min Temperature of Coldest Month (using monthly min)
    # These require proper monthly aggregation
    if n_days >= 365:
        monthly_max = np.zeros((n_parcels, 12))
        monthly_min = np.zeros((n_parcels, 12))
        day_idx = 0
        for month, days in enumerate(days_per_month):
            if day_idx + days <= n_days:
                monthly_max[:, month] = np.nanmax(data[:, day_idx:day_idx+days], axis=1)
                monthly_min[:, month] = np.nanmin(data[:, day_idx:day_idx+days], axis=1)
                day_idx += days

        results['bio5_max_temp_warmest_month'] = np.nanmax(monthly_max, axis=1)
        results['bio6_min_temp_coldest_month'] = np.nanmin(monthly_min, axis=1)

    # BIO7: Temperature Annual Range
    results['bio7_annual_temp_range'] = np.nanmax(data, axis=1) - np.nanmin(data, axis=1)

    return results


def calculate_location_specific_extremes(data):
    """
    Calculate cold and warm spells using location-specific percentiles.

    Parameters:
    -----------
    data : ndarray
        Temperature data (n_parcels, n_days) in Celsius

    Returns:
    --------
    dict : Dictionary with cold and warm spell metrics
    """
    n_parcels = data.shape[0]
    results = {
        'cold_spell_max_duration': np.zeros(n_parcels),
        'warm_spell_max_duration': np.zeros(n_parcels),
        'cold_spell_days': np.zeros(n_parcels),
        'warm_spell_days': np.zeros(n_parcels)
    }

    for i in range(n_parcels):
        parcel_data = data[i]
        # Calculate location-specific percentiles
        p10 = np.nanpercentile(parcel_data, 10)
        p90 = np.nanpercentile(parcel_data, 90)

        # Find cold spell days (below 10th percentile)
        cold_days = parcel_data < p10
        warm_days = parcel_data > p90

        # Count total days
        results['cold_spell_days'][i] = np.sum(cold_days)
        results['warm_spell_days'][i] = np.sum(warm_days)

        # Find maximum consecutive duration
        max_cold_consecutive = 0
        max_warm_consecutive = 0
        current_cold = 0
        current_warm = 0

        for j in range(len(parcel_data)):
            if not np.isnan(parcel_data[j]):
                if cold_days[j]:
                    current_cold += 1
                    max_cold_consecutive = max(max_cold_consecutive, current_cold)
                else:
                    current_cold = 0

                if warm_days[j]:
                    current_warm += 1
                    max_warm_consecutive = max(max_warm_consecutive, current_warm)
                else:
                    current_warm = 0

        results['cold_spell_max_duration'][i] = max_cold_consecutive
        results['warm_spell_max_duration'][i] = max_warm_consecutive

    return results


def calculate_growing_season_metrics(data):
    """
    Calculate growing season length and frost-free period.

    Parameters:
    -----------
    data : ndarray
        Temperature data (n_parcels, n_days) in Celsius

    Returns:
    --------
    dict : Dictionary with growing season metrics
    """
    n_parcels = data.shape[0]
    results = {
        'growing_season_length': np.zeros(n_parcels),
        'frost_free_period': np.zeros(n_parcels),
        'last_spring_frost': np.zeros(n_parcels),
        'first_fall_frost': np.zeros(n_parcels)
    }

    for i in range(n_parcels):
        temps = data[i]

        # Find frost-free period
        frost_days = np.where(temps < 0)[0]
        if len(frost_days) > 0:
            # Assuming year starts Jan 1
            mid_year = len(temps) // 2

            # Last spring frost (before mid-year)
            spring_frosts = frost_days[frost_days < mid_year]
            if len(spring_frosts) > 0:
                results['last_spring_frost'][i] = spring_frosts[-1]

            # First fall frost (after mid-year)
            fall_frosts = frost_days[frost_days >= mid_year]
            if len(fall_frosts) > 0:
                results['first_fall_frost'][i] = fall_frosts[0]

                if len(spring_frosts) > 0:
                    results['frost_free_period'][i] = fall_frosts[0] - spring_frosts[-1]

        # Growing season length (days with mean temp > 5°C)
        # Using consecutive days above 5°C
        above_5 = temps > 5
        if np.any(above_5):
            # Find longest consecutive period
            consecutive = 0
            max_consecutive = 0
            for val in above_5:
                if val:
                    consecutive += 1
                    max_consecutive = max(max_consecutive, consecutive)
                else:
                    consecutive = 0
            results['growing_season_length'][i] = max_consecutive

    return results


def calculate_timber_specific_indices(temp_data, precip_data=None):
    """
    Calculate indices specifically relevant for timber/forestry applications.

    Parameters:
    -----------
    temp_data : ndarray
        Temperature data (n_parcels, n_days) in Celsius
    precip_data : ndarray, optional
        Precipitation data (n_parcels, n_days) in mm/day

    Returns:
    --------
    dict : Dictionary with timber-specific indices
    """
    n_parcels = temp_data.shape[0]
    results = {}

    # Heat stress accumulation (degree-days above 35°C, critical for many tree species)
    heat_stress = np.maximum(temp_data - 35, 0)
    results['heat_stress_degree_days'] = np.nansum(heat_stress, axis=1)

    # Optimal growth temperature range (species-specific, using 15-25°C as default)
    optimal_days = np.sum((temp_data >= 15) & (temp_data <= 25), axis=1)
    results['optimal_growth_days'] = optimal_days

    # Photosynthetically active period (days > 10°C)
    results['photosynthetic_days'] = np.sum(temp_data > 10, axis=1)

    # Early/late frost risk (frost after day 90 or before day 300)
    early_frost_risk = np.zeros(n_parcels)
    late_frost_risk = np.zeros(n_parcels)

    for i in range(n_parcels):
        temps = temp_data[i]
        # Early growing season frost (days 90-150)
        if len(temps) > 150:
            early_frost_risk[i] = np.sum(temps[90:150] < 0)
        # Late growing season frost (days 250-300)
        if len(temps) > 300:
            late_frost_risk[i] = np.sum(temps[250:300] < 0)

    results['early_frost_risk_days'] = early_frost_risk
    results['late_frost_risk_days'] = late_frost_risk

    # Chilling portions (Dynamic Model - simplified)
    # Using Utah Model as approximation
    chilling_units = np.zeros(n_parcels)
    for i in range(n_parcels):
        temps = temp_data[i]
        # Utah chill units (simplified)
        # 0-2.4°C: 0.5 units
        # 2.5-9.1°C: 1 unit
        # 9.2-12.4°C: 0.5 units
        # 12.5-15.9°C: 0 units
        # 16-18°C: -0.5 units
        # >18°C: -1 unit

        units = np.zeros_like(temps)
        units[(temps > 0) & (temps <= 2.4)] = 0.5
        units[(temps > 2.4) & (temps <= 9.1)] = 1.0
        units[(temps > 9.1) & (temps <= 12.4)] = 0.5
        units[(temps > 12.4) & (temps <= 15.9)] = 0.0
        units[(temps > 15.9) & (temps <= 18)] = -0.5
        units[temps > 18] = -1.0

        chilling_units[i] = np.nansum(units)

    results['utah_chill_units'] = chilling_units

    # If precipitation data is available
    if precip_data is not None:
        # Simple water stress index (high temp + low precip)
        # Days with temp > 25°C and precip < 1mm
        water_stress = (temp_data > 25) & (precip_data < 1)
        results['water_stress_days'] = np.sum(water_stress, axis=1)

        # Drought stress index (consecutive dry hot days)
        drought_stress = np.zeros(n_parcels)
        for i in range(n_parcels):
            stress_days = water_stress[i]
            consecutive = 0
            max_consecutive = 0
            for val in stress_days:
                if val:
                    consecutive += 1
                    max_consecutive = max(max_consecutive, consecutive)
                else:
                    consecutive = 0
            drought_stress[i] = max_consecutive

        results['max_consecutive_drought_stress'] = drought_stress

    return results
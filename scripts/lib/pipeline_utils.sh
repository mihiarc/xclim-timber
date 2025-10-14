#!/bin/bash
# Pipeline utilities for production orchestration
# Provides common functions for managing pipeline execution

# Source logging utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/logging_utils.sh"

# Pipeline configuration
declare -A PIPELINE_SCRIPTS=(
    ["temperature"]="temperature_pipeline.py"
    ["precipitation"]="precipitation_pipeline.py"
    ["drought"]="drought_pipeline.py"
    ["agricultural"]="agricultural_pipeline.py"
    ["humidity"]="humidity_pipeline.py"
    ["human_comfort"]="human_comfort_pipeline.py"
    ["multivariate"]="multivariate_pipeline.py"
)

declare -A PIPELINE_OUTPUT_DIRS=(
    ["temperature"]="outputs/production/temperature"
    ["precipitation"]="outputs/production/precipitation"
    ["drought"]="outputs/production/drought"
    ["agricultural"]="outputs/production/agricultural"
    ["humidity"]="outputs/production/humidity"
    ["human_comfort"]="outputs/production/human_comfort"
    ["multivariate"]="outputs/production/multivariate"
)

declare -A PIPELINE_INDICES_COUNT=(
    ["temperature"]=35
    ["precipitation"]=13
    ["drought"]=12
    ["agricultural"]=5
    ["humidity"]=8
    ["human_comfort"]=3
    ["multivariate"]=4
)

# Recommended tile counts per pipeline (avoid threading deadlocks)
# Issue: 4 parallel threads calling compute() simultaneously causes deadlock
# Solution: Use 2 tiles for precipitation/drought to avoid parallel compute() deadlock
declare -A PIPELINE_DEFAULT_TILES=(
    ["temperature"]=4      # Works fine with 4 tiles
    ["precipitation"]=2    # Use 2 tiles - 4 causes threading deadlock
    ["drought"]=2          # Use 2 tiles - 4 causes threading deadlock
    ["agricultural"]=4     # Simple calculations, 4 tiles OK
    ["humidity"]=4         # Simple calculations, 4 tiles OK
    ["human_comfort"]=4    # Simple calculations, 4 tiles OK
    ["multivariate"]=4     # Works fine with 4 tiles
)

# Check if pipeline exists
# Usage: validate_pipeline PIPELINE_NAME
validate_pipeline() {
    local pipeline=$1

    if [ -z "${PIPELINE_SCRIPTS[$pipeline]}" ]; then
        log_error "Unknown pipeline: $pipeline"
        log_info "Available pipelines: ${!PIPELINE_SCRIPTS[@]}"
        return 1
    fi

    if [ ! -f "${PIPELINE_SCRIPTS[$pipeline]}" ]; then
        log_error "Pipeline script not found: ${PIPELINE_SCRIPTS[$pipeline]}"
        return 1
    fi

    return 0
}

# Check if output file exists for a given year
# Usage: output_exists PIPELINE YEAR
output_exists() {
    local pipeline=$1
    local year=$2
    local output_dir="${PIPELINE_OUTPUT_DIRS[$pipeline]}"
    local output_file="${output_dir}/${pipeline}_indices_${year}_${year}.nc"

    [ -f "$output_file" ]
}

# Get list of missing years for a pipeline
# Usage: get_missing_years PIPELINE START_YEAR END_YEAR
get_missing_years() {
    local pipeline=$1
    local start_year=$2
    local end_year=$3
    local missing_years=()

    for year in $(seq $start_year $end_year); do
        if ! output_exists "$pipeline" "$year"; then
            missing_years+=($year)
        fi
    done

    echo "${missing_years[@]}"
}

# Get count of completed years
# Usage: count_completed_years PIPELINE START_YEAR END_YEAR
count_completed_years() {
    local pipeline=$1
    local start_year=$2
    local end_year=$3
    local count=0

    for year in $(seq $start_year $end_year); do
        if output_exists "$pipeline" "$year"; then
            ((count++))
        fi
    done

    echo $count
}

# Run pipeline for a specific year
# Usage: run_pipeline_year PIPELINE YEAR [EXTRA_ARGS]
run_pipeline_year() {
    local pipeline=$1
    local year=$2
    shift 2
    local extra_args="$@"

    local script="${PIPELINE_SCRIPTS[$pipeline]}"
    local output_dir="${PIPELINE_OUTPUT_DIRS[$pipeline]}"

    # Ensure output directory exists
    mkdir -p "$output_dir"

    # Get recommended tile count for this pipeline (if not overridden in extra_args)
    local tiles=${PIPELINE_DEFAULT_TILES[$pipeline]}
    if [ -n "$tiles" ] && [[ ! "$extra_args" =~ "--n-tiles" ]]; then
        extra_args="--n-tiles $tiles $extra_args"
        log_debug "Using $tiles tiles for $pipeline (recommended default)"
    fi

    # Run pipeline
    log_info "Running $pipeline pipeline for year $year"

    python "$script" \
        --start-year "$year" \
        --end-year "$year" \
        --output-dir "$output_dir" \
        $extra_args \
        2>&1
}

# Validate output file
# Usage: validate_output PIPELINE YEAR
validate_output() {
    local pipeline=$1
    local year=$2
    local output_dir="${PIPELINE_OUTPUT_DIRS[$pipeline]}"
    local output_file="${output_dir}/${pipeline}_indices_${year}_${year}.nc"

    if [ ! -f "$output_file" ]; then
        log_error "Output file not found: $output_file"
        return 1
    fi

    # Check file size (should be > 1MB for reasonable data)
    local file_size=$(stat -c%s "$output_file" 2>/dev/null || stat -f%z "$output_file" 2>/dev/null)
    if [ "$file_size" -lt 1000000 ]; then
        log_warn "Output file suspiciously small: $(numfmt --to=iec $file_size)"
        return 1
    fi

    # Check with ncdump if available
    if command -v ncdump &> /dev/null; then
        if ! ncdump -h "$output_file" &> /dev/null; then
            log_error "Output file is not a valid NetCDF file"
            return 1
        fi

        # Check number of indices (match all numeric types: float, int, double)
        local expected_count=${PIPELINE_INDICES_COUNT[$pipeline]}
        local actual_count=$(ncdump -h "$output_file" | grep -E "(float|int|double|int64).*\(time, lat, lon\)" | wc -l || echo 0)

        if [ "$actual_count" -ne "$expected_count" ]; then
            log_warn "Expected $expected_count indices, found $actual_count"
            return 1
        fi
    fi

    log_success "Output validation passed for $year"
    return 0
}

# Get pipeline summary statistics
# Usage: pipeline_summary PIPELINE START_YEAR END_YEAR
pipeline_summary() {
    local pipeline=$1
    local start_year=$2
    local end_year=$3
    local output_dir="${PIPELINE_OUTPUT_DIRS[$pipeline]}"

    local total_years=$((end_year - start_year + 1))
    local completed=$(count_completed_years "$pipeline" "$start_year" "$end_year")
    local missing=$((total_years - completed))

    log_section "Pipeline Summary: $pipeline"
    log_info "Years requested: $start_year-$end_year ($total_years years)"
    log_info "Completed: $completed files"
    log_info "Missing: $missing files"

    if [ -d "$output_dir" ]; then
        local total_size=$(du -sh "$output_dir" 2>/dev/null | cut -f1)
        log_info "Total output size: $total_size"
    fi

    # Show completion percentage
    local percent=$((completed * 100 / total_years))
    if [ $percent -eq 100 ]; then
        log_success "Pipeline 100% complete! ✓"
    else
        log_warn "Pipeline ${percent}% complete ($missing years remaining)"
    fi
}

# Export functions
export -f validate_pipeline
export -f output_exists
export -f get_missing_years
export -f count_completed_years
export -f run_pipeline_year
export -f validate_output
export -f pipeline_summary

#!/bin/bash
# Master production orchestration script for xclim-timber
# Manages production runs across all climate indices pipelines
# Issue #69: Create unified production orchestration

set -euo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Source utilities
source "$SCRIPT_DIR/lib/logging_utils.sh"
source "$SCRIPT_DIR/lib/pipeline_utils.sh"

# Change to project root
cd "$PROJECT_ROOT"

# Default configuration
PIPELINE="all"
START_YEAR=1981
END_YEAR=2024
RESUME=false
VALIDATE=false
FAIL_FAST=false
DRY_RUN=false
PARALLEL=false
N_TILES=4
CHUNK_YEARS=1

# Log file
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/production_$(date +%Y%m%d_%H%M%S).log"

# Usage information
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Master orchestration script for climate indices production processing.

OPTIONS:
    -p, --pipeline PIPELINE     Pipeline to run (default: all)
                                Options: temperature, precipitation, drought,
                                        agricultural, humidity, human_comfort,
                                        multivariate, all

    -s, --start-year YEAR       Start year (default: 1981)
    -e, --end-year YEAR         End year (default: 2024)

    -r, --resume                Resume processing (skip existing files)
    -v, --validate              Run validation after processing
    -f, --fail-fast             Stop on first error (default: continue)

    --dry-run                   Show what would be processed without running
    --parallel                  Process multiple pipelines in parallel
    --n-tiles N                 Number of spatial tiles (2, 4, or 8; default: 4)
    --chunk-years N             Years to process per chunk (default: 1)

    -h, --help                  Show this help message

EXAMPLES:
    # Process all temperature years with resume
    $0 --pipeline temperature --resume

    # Process specific year range for precipitation
    $0 --pipeline precipitation --start-year 2020 --end-year 2024

    # Process all pipelines with validation
    $0 --pipeline all --validate --resume

    # Dry run to see what would be processed
    $0 --pipeline drought --resume --dry-run

    # Parallel processing (experimental)
    $0 --parallel --pipeline temperature,precipitation

EOF
    exit 0
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -p|--pipeline)
                PIPELINE="$2"
                shift 2
                ;;
            -s|--start-year)
                START_YEAR="$2"
                shift 2
                ;;
            -e|--end-year)
                END_YEAR="$2"
                shift 2
                ;;
            -r|--resume)
                RESUME=true
                shift
                ;;
            -v|--validate)
                VALIDATE=true
                shift
                ;;
            -f|--fail-fast)
                FAIL_FAST=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --parallel)
                PARALLEL=true
                shift
                ;;
            --n-tiles)
                N_TILES="$2"
                shift 2
                ;;
            --chunk-years)
                CHUNK_YEARS="$2"
                shift 2
                ;;
            -h|--help)
                usage
                ;;
            *)
                echo "Unknown option: $1"
                usage
                ;;
        esac
    done
}

# Process a single pipeline
process_pipeline() {
    local pipeline=$1
    local start_year=$2
    local end_year=$3

    log_section "Processing Pipeline: $pipeline"

    # Validate pipeline
    if ! validate_pipeline "$pipeline"; then
        return 1
    fi

    # Determine years to process
    local years_to_process
    if [ "$RESUME" = true ]; then
        years_to_process=($(get_missing_years "$pipeline" "$start_year" "$end_year"))
        local completed=$(count_completed_years "$pipeline" "$start_year" "$end_year")
        local total=$((end_year - start_year + 1))

        if [ ${#years_to_process[@]} -eq 0 ]; then
            log_success "Pipeline $pipeline: All $total years already complete!"
            return 0
        fi

        log_info "Resume mode: $completed/$total years complete, ${#years_to_process[@]} remaining"
    else
        years_to_process=($(seq $start_year $end_year))
    fi

    # Dry run mode
    if [ "$DRY_RUN" = true ]; then
        log_info "DRY RUN: Would process ${#years_to_process[@]} years:"
        for year in "${years_to_process[@]}"; do
            log_info "  - Year $year"
        done
        return 0
    fi

    # Process each year
    local failed_years=()
    local successful_years=()
    local total_to_process=${#years_to_process[@]}
    local current=0

    for year in "${years_to_process[@]}"; do
        ((current++))

        progress_bar $current $total_to_process "Year $year"

        # Build extra arguments
        local extra_args=""
        if [ "$N_TILES" != "4" ]; then
            extra_args="--n-tiles $N_TILES"
        fi

        # Run pipeline for this year
        local log_file_year="$LOG_DIR/${pipeline}_${year}.log"

        if run_pipeline_year "$pipeline" "$year" $extra_args > "$log_file_year" 2>&1; then
            # Validate output if requested
            if [ "$VALIDATE" = true ]; then
                if validate_output "$pipeline" "$year"; then
                    successful_years+=($year)
                else
                    log_error "Validation failed for year $year"
                    failed_years+=($year)

                    if [ "$FAIL_FAST" = true ]; then
                        log_error "Fail-fast mode enabled, stopping"
                        break
                    fi
                fi
            else
                successful_years+=($year)
            fi
        else
            log_error "Pipeline failed for year $year (see $log_file_year)"
            failed_years+=($year)

            if [ "$FAIL_FAST" = true ]; then
                log_error "Fail-fast mode enabled, stopping"
                break
            fi
        fi

        # Brief pause for memory cleanup
        sleep 1
    done

    # Summary
    log_section "Pipeline Complete: $pipeline"
    log_success "Successful: ${#successful_years[@]} years"

    if [ ${#failed_years[@]} -gt 0 ]; then
        log_error "Failed: ${#failed_years[@]} years"
        log_error "Failed years: ${failed_years[@]}"
        return 1
    fi

    # Show pipeline statistics
    pipeline_summary "$pipeline" "$start_year" "$end_year"

    return 0
}

# Main execution
main() {
    # Parse arguments
    parse_args "$@"

    # Initialize logging
    init_logging "$LOG_FILE"

    log_section "xclim-timber Production Orchestration"
    log_info "Pipeline: $PIPELINE"
    log_info "Year range: $START_YEAR-$END_YEAR"
    log_info "Resume mode: $RESUME"
    log_info "Validate: $VALIDATE"
    log_info "Fail-fast: $FAIL_FAST"
    log_info "Spatial tiles: $N_TILES"
    log_info "Log file: $LOG_FILE"

    if [ "$DRY_RUN" = true ]; then
        log_warn "DRY RUN MODE - No actual processing will occur"
    fi

    # Determine pipelines to process
    local pipelines_to_run=()

    if [ "$PIPELINE" = "all" ]; then
        pipelines_to_run=(temperature precipitation drought agricultural humidity human_comfort multivariate)
        log_info "Processing all 7 pipelines"
    elif [[ "$PIPELINE" =~ "," ]]; then
        IFS=',' read -ra pipelines_to_run <<< "$PIPELINE"
        log_info "Processing ${#pipelines_to_run[@]} pipelines: ${pipelines_to_run[@]}"
    else
        pipelines_to_run=("$PIPELINE")
    fi

    # Track overall success
    local failed_pipelines=()
    local successful_pipelines=()

    # Process pipelines
    local start_time=$(date +%s)

    for pipeline in "${pipelines_to_run[@]}"; do
        if process_pipeline "$pipeline" "$START_YEAR" "$END_YEAR"; then
            successful_pipelines+=("$pipeline")
        else
            failed_pipelines+=("$pipeline")

            if [ "$FAIL_FAST" = true ]; then
                break
            fi
        fi
    done

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local hours=$((duration / 3600))
    local minutes=$(((duration % 3600) / 60))
    local seconds=$((duration % 60))

    # Final summary
    log_section "Production Orchestration Complete"
    log_info "Total duration: ${hours}h ${minutes}m ${seconds}s"
    log_success "Successful pipelines: ${#successful_pipelines[@]}"

    if [ ${#successful_pipelines[@]} -gt 0 ]; then
        for pipeline in "${successful_pipelines[@]}"; do
            log_success "  ✓ $pipeline"
        done
    fi

    if [ ${#failed_pipelines[@]} -gt 0 ]; then
        log_error "Failed pipelines: ${#failed_pipelines[@]}"
        for pipeline in "${failed_pipelines[@]}"; do
            log_error "  ✗ $pipeline"
        done
        exit 1
    fi

    log_success "All pipelines completed successfully!"
    exit 0
}

# Run main
main "$@"

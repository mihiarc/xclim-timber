#!/bin/bash

"""
Integration script for running validation in production pipelines.

This script can be called after pipeline completion to validate outputs.
"""

set -e  # Exit on error

# Default values
PIPELINE=""
DIRECTORY=""
QUICK=false
REPORT=false
JSON_OUTPUT=""
FAIL_ON_WARNING=false
VERBOSE=false

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_color() {
    color=$1
    message=$2
    echo -e "${color}${message}${NC}"
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 -p PIPELINE -d DIRECTORY [OPTIONS]

Validate xclim-timber pipeline outputs with comprehensive quality checks.

Required arguments:
    -p, --pipeline PIPELINE     Pipeline type (temperature, precipitation, drought,
                                agricultural, multivariate, humidity, human_comfort, all)
    -d, --directory DIRECTORY   Directory containing pipeline outputs

Optional arguments:
    -q, --quick                 Run quick validation (sample files only)
    -r, --report               Generate HTML validation report
    -j, --json FILE            Save results to JSON file
    -w, --fail-on-warning      Exit with error code on warnings
    -v, --verbose              Enable verbose output
    -h, --help                 Show this help message

Examples:
    # Basic validation
    $0 -p temperature -d outputs/production/temperature/

    # Quick validation with report
    $0 -p precipitation -d outputs/production/precipitation/ -q -r

    # Full validation with JSON output
    $0 -p all -d outputs/production/ -j validation_results.json

    # Strict validation (fail on warnings)
    $0 -p drought -d outputs/production/drought/ -w

Integration with production scripts:
    # Add to end of production script
    ./run_temperature_pipeline.sh
    ./validation/run_validation.sh -p temperature -d outputs/production/temperature/ -w
EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--pipeline)
            PIPELINE="$2"
            shift 2
            ;;
        -d|--directory)
            DIRECTORY="$2"
            shift 2
            ;;
        -q|--quick)
            QUICK=true
            shift
            ;;
        -r|--report)
            REPORT=true
            shift
            ;;
        -j|--json)
            JSON_OUTPUT="$2"
            shift 2
            ;;
        -w|--fail-on-warning)
            FAIL_ON_WARNING=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_color "$RED" "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$PIPELINE" ] || [ -z "$DIRECTORY" ]; then
    print_color "$RED" "Error: Pipeline and directory are required"
    show_usage
    exit 1
fi

# Check if directory exists
if [ ! -d "$DIRECTORY" ]; then
    print_color "$RED" "Error: Directory does not exist: $DIRECTORY"
    exit 1
fi

# Get the script directory (where validation module is located)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Build Python command
PYTHON_CMD="python3 ${SCRIPT_DIR}/validate_dataset.py"
PYTHON_CMD="$PYTHON_CMD $DIRECTORY --pipeline $PIPELINE"

if [ "$QUICK" = true ]; then
    PYTHON_CMD="$PYTHON_CMD --quick"
fi

if [ "$REPORT" = true ]; then
    PYTHON_CMD="$PYTHON_CMD --report"
fi

if [ ! -z "$JSON_OUTPUT" ]; then
    PYTHON_CMD="$PYTHON_CMD --json $JSON_OUTPUT"
fi

if [ "$FAIL_ON_WARNING" = true ]; then
    PYTHON_CMD="$PYTHON_CMD --fail-on-warning"
fi

if [ "$VERBOSE" = true ]; then
    PYTHON_CMD="$PYTHON_CMD --verbose"
fi

# Print header
print_color "$GREEN" "========================================="
print_color "$GREEN" "xclim-timber Data Validation"
print_color "$GREEN" "========================================="
echo "Pipeline: $PIPELINE"
echo "Directory: $DIRECTORY"
echo "Mode: $([ "$QUICK" = true ] && echo "Quick" || echo "Full")"
echo "Report: $([ "$REPORT" = true ] && echo "Yes" || echo "No")"
echo ""

# Run validation
print_color "$YELLOW" "Running validation..."
$PYTHON_CMD

# Capture exit code
EXIT_CODE=$?

# Print result
echo ""
if [ $EXIT_CODE -eq 0 ]; then
    print_color "$GREEN" "✅ Validation completed successfully"
else
    print_color "$RED" "❌ Validation failed with exit code: $EXIT_CODE"
fi

exit $EXIT_CODE
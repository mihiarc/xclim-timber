#!/bin/bash
# Logging utilities for production orchestration
# Provides consistent logging across all pipeline scripts

# Color codes for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Log levels
LOG_DEBUG=0
LOG_INFO=1
LOG_WARN=2
LOG_ERROR=3

# Current log level (default: INFO)
CURRENT_LOG_LEVEL=${CURRENT_LOG_LEVEL:-$LOG_INFO}

# Log file location (can be overridden)
LOG_FILE="${LOG_FILE:-}"

# Initialize logging
# Usage: init_logging [log_file]
init_logging() {
    if [ -n "$1" ]; then
        LOG_FILE="$1"
        mkdir -p "$(dirname "$LOG_FILE")"
        echo "==================================================" > "$LOG_FILE"
        echo "Log started: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_FILE"
        echo "==================================================" >> "$LOG_FILE"
    fi
}

# Log a message with timestamp
# Usage: log_msg LEVEL COLOR MESSAGE
log_msg() {
    local level=$1
    local color=$2
    local message=$3
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    # Only log if level is >= current log level
    if [ $level -ge $CURRENT_LOG_LEVEL ]; then
        # Print to terminal with color
        echo -e "${color}[${timestamp}] ${message}${NC}"

        # Write to log file without color codes
        if [ -n "$LOG_FILE" ]; then
            echo "[${timestamp}] ${message}" >> "$LOG_FILE"
        fi
    fi
}

# Convenience functions
log_debug() {
    log_msg $LOG_DEBUG "$CYAN" "DEBUG: $1"
}

log_info() {
    log_msg $LOG_INFO "$BLUE" "INFO: $1"
}

log_success() {
    log_msg $LOG_INFO "$GREEN" "✓ $1"
}

log_warn() {
    log_msg $LOG_WARN "$YELLOW" "WARNING: $1"
}

log_error() {
    log_msg $LOG_ERROR "$RED" "ERROR: $1"
}

log_section() {
    local title=$1
    local width=60
    local padding=$(printf '%*s' $(((width - ${#title}) / 2)) '')

    echo -e "\n${CYAN}=========================================${NC}"
    echo -e "${CYAN}${padding}${title}${NC}"
    echo -e "${CYAN}=========================================${NC}\n"

    if [ -n "$LOG_FILE" ]; then
        echo "" >> "$LOG_FILE"
        echo "=========================================" >> "$LOG_FILE"
        echo "${padding}${title}" >> "$LOG_FILE"
        echo "=========================================" >> "$LOG_FILE"
        echo "" >> "$LOG_FILE"
    fi
}

# Progress bar
# Usage: progress_bar CURRENT TOTAL DESCRIPTION
progress_bar() {
    local current=$1
    local total=$2
    local description=$3
    local percent=$((current * 100 / total))
    local filled=$((percent / 2))
    local empty=$((50 - filled))

    printf "\r${CYAN}Progress: [${GREEN}"
    printf '%*s' "$filled" '' | tr ' ' '='
    printf "${CYAN}"
    printf '%*s' "$empty" '' | tr ' ' '-'
    printf "] %3d%% (%d/%d) %s${NC}" "$percent" "$current" "$total" "$description"

    if [ $current -eq $total ]; then
        echo ""
    fi
}

# Export functions
export -f init_logging
export -f log_msg
export -f log_debug
export -f log_info
export -f log_success
export -f log_warn
export -f log_error
export -f log_section
export -f progress_bar

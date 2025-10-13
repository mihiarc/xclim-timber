#!/bin/bash
# Full 44-year reprocessing with fixed pipeline (v5.2)
# Fixes: Issues #70 (count indices), #71 (validation), #72 (thread safety), #73 (resource leaks)

set -e  # Exit on error

OUTPUT_DIR="outputs/production_v2/temperature"
LOG_FILE="logs/reprocessing_$(date +%Y%m%d_%H%M%S).log"
PROGRESS_FILE="logs/reprocessing_progress.txt"

# Create directories
mkdir -p "$OUTPUT_DIR"
mkdir -p logs

# Initialize progress tracking
echo "Starting full 44-year reprocessing at $(date)" | tee "$PROGRESS_FILE"
echo "Output: $OUTPUT_DIR" | tee -a "$PROGRESS_FILE"
echo "Log: $LOG_FILE" | tee -a "$PROGRESS_FILE"
echo "======================================" | tee -a "$PROGRESS_FILE"

# Counters
TOTAL_YEARS=44
SUCCESS_COUNT=0
FAIL_COUNT=0
FAILED_YEARS=""

# Process each year
for year in {1981..2024}; do
    echo "" | tee -a "$PROGRESS_FILE"
    echo "[$((SUCCESS_COUNT + FAIL_COUNT + 1))/$TOTAL_YEARS] Processing year $year..." | tee -a "$PROGRESS_FILE"

    START_TIME=$(date +%s)

    if python temperature_pipeline.py \
        --start-year "$year" \
        --end-year "$year" \
        --output-dir "$OUTPUT_DIR" 2>&1 | tee -a "$LOG_FILE"; then

        END_TIME=$(date +%s)
        ELAPSED=$((END_TIME - START_TIME))

        echo "  ✓ Year $year completed successfully in ${ELAPSED}s" | tee -a "$PROGRESS_FILE"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))

        # Validate immediately after processing
        if python scripts/validate_production_data.py "$OUTPUT_DIR" --year "$year" --pipeline temperature >> "$LOG_FILE" 2>&1; then
            echo "  ✓ Validation passed" | tee -a "$PROGRESS_FILE"
        else
            echo "  ⚠ Validation failed (but processing succeeded)" | tee -a "$PROGRESS_FILE"
        fi
    else
        echo "  ✗ Year $year FAILED" | tee -a "$PROGRESS_FILE"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_YEARS="$FAILED_YEARS $year"
    fi

    # Progress summary
    echo "  Progress: $SUCCESS_COUNT/$TOTAL_YEARS complete, $FAIL_COUNT failed" | tee -a "$PROGRESS_FILE"
done

# Final summary
echo "" | tee -a "$PROGRESS_FILE"
echo "======================================" | tee -a "$PROGRESS_FILE"
echo "REPROCESSING COMPLETE" | tee -a "$PROGRESS_FILE"
echo "======================================" | tee -a "$PROGRESS_FILE"
echo "Successful: $SUCCESS_COUNT/$TOTAL_YEARS" | tee -a "$PROGRESS_FILE"
echo "Failed: $FAIL_COUNT/$TOTAL_YEARS" | tee -a "$PROGRESS_FILE"

if [ $FAIL_COUNT -gt 0 ]; then
    echo "Failed years:$FAILED_YEARS" | tee -a "$PROGRESS_FILE"
    exit 1
else
    echo "✅ All years processed successfully!" | tee -a "$PROGRESS_FILE"

    # Calculate total file size
    TOTAL_SIZE=$(du -sh "$OUTPUT_DIR" | cut -f1)
    FILE_COUNT=$(find "$OUTPUT_DIR" -name "*.nc" -type f | wc -l)
    echo "Output: $FILE_COUNT files, $TOTAL_SIZE total" | tee -a "$PROGRESS_FILE"

    echo "" | tee -a "$PROGRESS_FILE"
    echo "Completed at $(date)" | tee -a "$PROGRESS_FILE"
fi

#!/bin/bash
# Production processing for precipitation indices (1981-2024)
# Issue #61: Complete precipitation dataset generation

set -e

OUTPUT_DIR="outputs/production/precipitation"
mkdir -p "$OUTPUT_DIR"

echo "=========================================="
echo "Precipitation Production Processing"
echo "Years: 1981-2024 (44 years)"
echo "Indices: 13 precipitation indices"
echo "=========================================="
echo ""

# Process all years
for year in {1981..2024}; do
    echo "Processing year $year..."
    python precipitation_pipeline.py \
        --start-year $year \
        --end-year $year \
        --output-dir "$OUTPUT_DIR" \
        2>&1 | grep -E "(Successfully generated|Failed|ERROR|Pipeline complete)"
done

echo ""
echo "=========================================="
echo "Production Processing Complete"
echo "=========================================="
ls -lh "$OUTPUT_DIR" | tail -20

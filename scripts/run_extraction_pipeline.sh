#!/bin/bash
#
# Simple pipeline to run climate indices calculation and point extraction
# Usage: ./run_extraction_pipeline.sh [year_start] [year_end]

# Set defaults (matching pipeline defaults)
YEAR_START=${1:-2001}
YEAR_END=${2:-2024}
OUTPUT_DIR="./outputs"
PARCELS_CSV="./data/parcel_coordinates.csv"
PYTHON=".venv/bin/python"

echo "=========================================="
echo "Climate Data Extraction Pipeline"
echo "=========================================="
echo "Period: $YEAR_START - $YEAR_END"
echo "Output: $OUTPUT_DIR"
echo ""

# Step 1: Run temperature pipeline
echo "[1/3] Running temperature indices calculation..."
$PYTHON temperature_pipeline.py \
    --start-year $YEAR_START \
    --end-year $YEAR_END \
    --output-dir $OUTPUT_DIR

if [ $? -ne 0 ]; then
    echo "❌ Temperature pipeline failed"
    exit 1
fi

# Step 2: Extract points (handles multiple files if chunked)
echo ""
echo "[2/3] Extracting point data at parcel locations..."

# Simply run extraction with defaults (processes all NC files in outputs/)
$PYTHON extract_points.py

if [ $? -eq 0 ]; then
    echo "✓ Extraction complete: outputs/extracted_indices.csv"
    EXTRACT_OUTPUT="outputs/extracted_indices.csv"
else
    echo "❌ Point extraction failed"
    exit 1
fi

# Step 3: Summary
echo ""
echo "[3/3] Pipeline Summary"
echo "=========================================="
echo "✓ Temperature indices calculated"
echo "✓ Point data extracted for $(wc -l < $PARCELS_CSV) parcels"
echo "✓ Output saved to: $EXTRACT_OUTPUT"

# Show sample results
echo ""
echo "Sample results (first 3 parcels):"
head -4 "$EXTRACT_OUTPUT" | cut -d',' -f1-8

echo ""
echo "✅ Pipeline completed successfully!"
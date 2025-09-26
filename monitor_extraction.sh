#!/bin/bash

echo "=== Southeast Climate Extraction Progress ==="
echo "Process ID: df83c1"
echo "Target: 35,844 locations × 6 NetCDF files"
echo ""

# Check if output file is being created
if [ -f "outputs/extracted_indices_southeast.csv" ]; then
    SIZE=$(ls -lh outputs/extracted_indices_southeast.csv | awk '{print $5}')
    LINES=$(wc -l outputs/extracted_indices_southeast.csv | awk '{print $1}')
    echo "✓ Output file created"
    echo "  Size: $SIZE"
    echo "  Rows processed: $LINES"
else
    echo "⏳ Waiting for output file creation..."
fi

echo ""
echo "To check detailed progress, run:"
echo "  ./check_extraction_status.sh"
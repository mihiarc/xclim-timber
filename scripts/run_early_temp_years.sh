#!/bin/bash
# Process remaining temperature years 1981-1999 with parallel spatial tiling
# This completes the full temperature dataset (1981-2024)

echo "========================================="
echo "Starting temperature pipeline for 1981-1999"
echo "Using parallel spatial tiling (4 quadrants, default)"
echo "========================================="
echo ""

for year in {1981..1999}; do
    echo "========================================="
    echo "Processing year $year"
    echo "========================================="
    python temperature_pipeline.py \
        --start-year $year \
        --end-year $year \
        --output-dir outputs/production/temperature

    if [ $? -eq 0 ]; then
        echo "✓ Year $year completed successfully"
    else
        echo "✗ Year $year failed"
        exit 1
    fi

    # Brief pause between years to allow memory cleanup
    sleep 2
done

echo ""
echo "========================================="
echo "All early years (1981-1999) completed!"
echo "========================================="
echo ""
echo "Temperature dataset summary:"
ls outputs/production/temperature/temperature_indices_*.nc 2>/dev/null | grep -v "tile" | wc -l
echo "total files created (should be 44 for complete 1981-2024 dataset)"

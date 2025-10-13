#!/bin/bash
# Process remaining temperature years 2008-2024 with parallel spatial tiling

for year in {2008..2024}; do
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
done

echo "========================================="
echo "All years completed!"
echo "========================================="

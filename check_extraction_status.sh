#!/bin/bash

echo "=== Climate Extraction Status Dashboard ==="
echo "Process: Background extraction for Southeast US parcels"
echo "Started: $(date)"
echo ""

# Get latest status from background process
echo "📊 Latest Progress:"
echo "-------------------"

# Show last 10 lines of progress
if command -v claude &> /dev/null; then
    # If Claude Code is available, use BashOutput
    echo "Run: claude code bashoutput df83c1"
else
    # Otherwise check for log file
    if [ -f "logs/extract_southeast.log" ]; then
        tail -10 logs/extract_southeast.log | grep -E "INFO|ERROR|WARNING"
    fi
fi

echo ""
echo "📁 Output Status:"
echo "-----------------"
if [ -f "outputs/extracted_indices_southeast.csv" ]; then
    SIZE=$(ls -lh outputs/extracted_indices_southeast.csv | awk '{print $5}')
    LINES=$(wc -l outputs/extracted_indices_southeast.csv | awk '{print $1}')
    MODIFIED=$(ls -l outputs/extracted_indices_southeast.csv | awk '{print $6, $7, $8}')

    echo "✓ Output file: outputs/extracted_indices_southeast.csv"
    echo "  File size: $SIZE"
    echo "  Total rows: $LINES"
    echo "  Last modified: $MODIFIED"

    # Estimate progress
    # Expected: 35,844 locations × 24 years = 860,256 rows + 1 header
    EXPECTED=860257
    if [ "$LINES" -gt 1 ]; then
        PERCENT=$(echo "scale=1; ($LINES - 1) * 100 / 860256" | bc)
        echo "  Progress: ~${PERCENT}% complete"
    fi
else
    echo "⏳ Output file not yet created"
fi

echo ""
echo "💾 System Resources:"
echo "-------------------"
# Check memory usage
FREE_MEM=$(free -h | grep "^Mem:" | awk '{print $4}')
echo "  Free memory: $FREE_MEM"

# Check disk space
DISK_FREE=$(df -h outputs/ | tail -1 | awk '{print $4}')
echo "  Free disk space: $DISK_FREE"

echo ""
echo "📋 Expected Output:"
echo "------------------"
echo "  Locations: 35,844 Southeast US parcels"
echo "  Years: 2001-2024 (24 years)"
echo "  Expected rows: 860,256 (+ header)"
echo "  Indices: ~26 climate variables"
echo ""

echo "💡 Tips:"
echo "--------"
echo "- This process may take 2-4 hours depending on system resources"
echo "- The extraction processes 6 NetCDF files sequentially"
echo "- Each file contains different climate indices for different year ranges"
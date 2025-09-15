#!/bin/bash

echo "==========================================="
echo "Testing Annual Mean Temperature Calculation"
echo "==========================================="

# Step 1: Create sample data for testing
echo ""
echo "Step 1: Creating sample temperature data..."
python test_annual_temp.py --create-sample

# Step 2: Run the calculation
echo ""
echo "Step 2: Calculating annual mean temperature..."
python test_annual_temp.py --input sample_data/temperature_sample.nc --output outputs/annual_mean_test.nc

# Step 3: Check if output was created
echo ""
echo "Step 3: Checking output..."
if [ -f "outputs/annual_mean_test.nc" ]; then
    echo "✓ Success! Output file created."
    echo ""
    echo "Output file details:"
    ls -lh outputs/annual_mean_test.nc
else
    echo "✗ Error: Output file not created."
    exit 1
fi

echo ""
echo "==========================================="
echo "Test completed successfully!"
echo "==========================================="
echo ""
echo "To use with your real data:"
echo "1. Edit the INPUT_FILE path in test_annual_temp.py"
echo "2. Or run: python test_annual_temp.py --input /path/to/your/temperature/file.nc"
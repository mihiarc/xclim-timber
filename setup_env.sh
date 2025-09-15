#!/bin/bash

echo "Setting up Python environment for xclim-timber using uv..."
echo ""

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.cargo/env
fi

# Create virtual environment with uv
echo "Creating virtual environment with uv..."
uv venv

# Install minimal requirements for testing
echo "Installing core dependencies..."
uv pip install \
    xarray \
    netCDF4 \
    xclim \
    rioxarray \
    pandas \
    numpy \
    pyyaml

echo ""
echo "Environment setup complete!"
echo ""
echo "To activate the environment, run:"
echo "  source .venv/bin/activate"
echo ""
echo "Then run the test:"
echo "  python test_annual_temp.py --create-sample"
echo "  python test_annual_temp.py --input sample_data/temperature_sample.nc"
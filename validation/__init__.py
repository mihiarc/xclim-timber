"""
Automated data quality validation suite for xclim-timber.

This module provides comprehensive validation tools to ensure data quality
and integrity in climate indices pipeline outputs.
"""

from pathlib import Path

__version__ = "1.0.0"

# Define validation module exports
__all__ = [
    "FileValidator",
    "DimensionValidator",
    "DataValidator",
    "MetadataValidator",
    "ConsistencyValidator",
    "validate_pipeline_output"
]

# Import validators when available
try:
    from .validators import (
        FileValidator,
        DimensionValidator,
        DataValidator,
        MetadataValidator,
        ConsistencyValidator
    )
    from .validate_dataset import validate_pipeline_output
except ImportError:
    # Handle imports during development
    pass
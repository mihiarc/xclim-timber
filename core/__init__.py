"""
Core module for xclim-timber pipeline infrastructure.

This module provides base classes and utilities shared across all climate index pipelines.
"""

from core.base_pipeline import BasePipeline
from core.config import PipelineConfig
from core.baseline_loader import BaselineLoader
from core.cli_builder import PipelineCLI

__all__ = [
    'BasePipeline',
    'PipelineConfig',
    'BaselineLoader',
    'PipelineCLI',
]

__version__ = '1.0.0'

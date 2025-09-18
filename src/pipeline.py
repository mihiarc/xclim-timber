#!/usr/bin/env python
"""
Main pipeline orchestrator for xclim climate data processing.
Coordinates the entire workflow from data loading to index calculation.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import warnings
import traceback

import click
import yaml
from tqdm import tqdm
import xarray as xr

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from data_loader import ClimateDataLoader
from preprocessor import ClimateDataPreprocessor
from indices_calculator import ClimateIndicesCalculator


# Configure logging
def setup_logging(log_path: Path, verbose: bool = False):
    """Setup logging configuration."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(levelname)s: %(message)s'
    )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # File handler
    log_file = log_path / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    return log_file


class ClimateDataPipeline:
    """Main pipeline orchestrator for climate data processing."""
    
    def __init__(self, config_path: Optional[str] = None, verbose: bool = False):
        """
        Initialize the pipeline.

        Parameters:
        -----------
        config_path : str, optional
            Path to configuration file
        verbose : bool
            Enable verbose logging
        """
        self.config = Config(config_path)
        self.verbose = verbose
        self.datasets = {}
        self.processed_datasets = {}
        self.indices = {}

        # Setup logging
        log_file = setup_logging(self.config.log_path, verbose)
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Pipeline initialized. Log file: {log_file}")

        # Validate configuration
        if not self.config.validate():
            raise ValueError("Configuration validation failed")
    
    def load_data(self, variables: Optional[List[str]] = None) -> Dict[str, xr.Dataset]:
        """
        Load climate data from external drive.

        Parameters:
        -----------
        variables : list, optional
            List of variables to load (default: all configured)

        Returns:
        --------
        dict
            Dictionary of loaded datasets
        """
        self.logger.info("=== Data Loading Phase ===")

        loader = ClimateDataLoader(self.config)
        
        if variables is None:
            variables = list(self.config.get('data.file_patterns', {}).keys())
        
        for var in tqdm(variables, desc="Loading variables"):
            self.logger.info(f"Loading {var} data")
            try:
                ds = loader.load_variable_data(var)
                if ds is not None:
                    self.datasets[var] = ds
                    self.logger.info(f"Loaded {var}: shape={dict(ds.dims)}")
                else:
                    self.logger.warning(f"No data found for {var}")
            except Exception as e:
                self.logger.error(f"Error loading {var}: {e}")
                if self.verbose:
                    self.logger.debug(traceback.format_exc())
        
        # Print summary
        info = loader.get_info()
        for name, details in info.items():
            self.logger.info(f"{name}: {details['memory_size']:.2f} GB, "
                           f"dimensions: {details['dimensions']}")
        
        return self.datasets
    
    def preprocess_data(self) -> Dict[str, xr.Dataset]:
        """
        Preprocess loaded datasets.
        
        Returns:
        --------
        dict
            Dictionary of preprocessed datasets
        """
        self.logger.info("=== Data Preprocessing Phase ===")
        
        preprocessor = ClimateDataPreprocessor(self.config)
        
        for var_name, ds in tqdm(self.datasets.items(), desc="Preprocessing"):
            self.logger.info(f"Preprocessing {var_name}")
            try:
                processed = preprocessor.preprocess(ds, variable_type=var_name)
                
                
                self.processed_datasets[var_name] = processed
                self.logger.info(f"Preprocessed {var_name}: shape={dict(processed.dims)}")
                
            except Exception as e:
                self.logger.error(f"Error preprocessing {var_name}: {e}")
                if self.verbose:
                    self.logger.debug(traceback.format_exc())
                # Keep original if preprocessing fails
                self.processed_datasets[var_name] = ds
        
        return self.processed_datasets
    
    def calculate_indices(self) -> Dict[str, xr.DataArray]:
        """
        Calculate climate indices.
        
        Returns:
        --------
        dict
            Dictionary of calculated indices
        """
        self.logger.info("=== Climate Indices Calculation Phase ===")
        
        calculator = ClimateIndicesCalculator(self.config)
        
        try:
            self.indices = calculator.calculate_all_indices(self.processed_datasets)
            
            # Print summary
            summary = calculator.get_summary()
            for name, stats in summary.items():
                self.logger.info(f"{name}: mean={stats['mean']:.2f}, "
                               f"std={stats['std']:.2f}, "
                               f"range=[{stats['min']:.2f}, {stats['max']:.2f}]")
            
        except Exception as e:
            self.logger.error(f"Error calculating indices: {e}")
            if self.verbose:
                self.logger.debug(traceback.format_exc())
        
        return self.indices
    
    def save_results(self, output_dir: Optional[Path] = None):
        """
        Save processed data and indices.
        
        Parameters:
        -----------
        output_dir : Path, optional
            Output directory (default: from config)
        """
        self.logger.info("=== Saving Results ===")
        
        if output_dir is None:
            output_dir = self.config.output_path
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save indices
        if self.indices:
            indices_file = output_dir / f"climate_indices_{datetime.now().strftime('%Y%m%d')}.nc"
            
            calculator = ClimateIndicesCalculator(self.config)
            calculator.results = self.indices
            calculator.save_indices(
                str(indices_file),
                format=self.config.get('output.format', 'netcdf')
            )
            self.logger.info(f"Saved indices to {indices_file}")
        
        # Save preprocessed data if requested
        save_preprocessed = self.config.get('output.save_preprocessed', False)
        if save_preprocessed and self.processed_datasets:
            for var_name, ds in self.processed_datasets.items():
                output_file = output_dir / f"{var_name}_preprocessed.nc"
                
                compression = self.config.get('output.compression', {})
                ds.to_netcdf(
                    output_file,
                    engine=compression.get('engine', 'h5netcdf'),
                    encoding={
                        var: {'zlib': True, 'complevel': compression.get('complevel', 4)}
                        for var in ds.data_vars
                    }
                )
                self.logger.info(f"Saved preprocessed {var_name} to {output_file}")
    
    def run(self, variables: Optional[List[str]] = None):
        """
        Run the complete pipeline.
        
        Parameters:
        -----------
        variables : list, optional
            List of variables to process
        """
        self.logger.info("=" * 50)
        self.logger.info("Starting Climate Data Pipeline")
        self.logger.info("=" * 50)
        
        start_time = datetime.now()
        
        try:
            # Load data
            self.load_data(variables)
            
            if not self.datasets:
                self.logger.error("No data loaded. Exiting.")
                return
            
            # Preprocess data
            self.preprocess_data()
            
            # Calculate indices
            self.calculate_indices()
            
            # Save results
            self.save_results()
            
            # Report completion
            elapsed_time = datetime.now() - start_time
            self.logger.info("=" * 50)
            self.logger.info(f"Pipeline completed successfully in {elapsed_time}")
            self.logger.info(f"Processed {len(self.datasets)} variables")
            self.logger.info(f"Calculated {len(self.indices)} indices")
            self.logger.info("=" * 50)
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            if self.verbose:
                self.logger.debug(traceback.format_exc())
            raise
        
        finally:
            # Cleanup (no dask client to close)
            pass
    
    def get_status(self) -> Dict:
        """
        Get current pipeline status.
        
        Returns:
        --------
        dict
            Status information
        """
        return {
            'datasets_loaded': list(self.datasets.keys()),
            'datasets_processed': list(self.processed_datasets.keys()),
            'indices_calculated': list(self.indices.keys()),
            'config': {
                'input_path': str(self.config.input_path),
                'output_path': str(self.config.output_path),
            }
        }


# CLI Interface
@click.command()
@click.option('--config', '-c', 
              type=click.Path(exists=True),
              help='Path to configuration file')
@click.option('--variables', '-v',
              multiple=True,
              help='Variables to process (can specify multiple)')
@click.option('--output', '-o',
              type=click.Path(),
              help='Output directory')
@click.option('--verbose', 
              is_flag=True,
              help='Enable verbose logging')
@click.option('--create-config',
              is_flag=True,
              help='Create a sample configuration file')
def main(config, variables, output, verbose, create_config):
    """
    xclim-timber: Climate Data Processing Pipeline
    
    Process climate raster data and calculate indices using xclim.
    """
    
    if create_config:
        # Create sample configuration
        sample_config = Config()
        sample_config.save('config_sample.yaml')
        click.echo("Sample configuration saved to config_sample.yaml")
        click.echo("Edit this file to customize your pipeline settings.")
        return
    
    # Run pipeline
    try:
        pipeline = ClimateDataPipeline(config, verbose)
        
        if output:
            pipeline.config.set('data.output_path', output)
        
        pipeline.run(list(variables) if variables else None)
        
        click.echo("\n✓ Pipeline completed successfully!")
        
        # Print summary
        status = pipeline.get_status()
        click.echo(f"\nProcessed variables: {', '.join(status['datasets_processed'])}")
        click.echo(f"Calculated indices: {', '.join(status['indices_calculated'])}")
        
    except Exception as e:
        click.echo(f"\n✗ Pipeline failed: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
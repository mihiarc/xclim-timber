#!/usr/bin/env python
"""
Enhanced pipeline with explicit multiprocessing support.
Utilizes Dask distributed computing for parallel climate indices calculation.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import warnings
import traceback

import click
import yaml
from tqdm import tqdm
import xarray as xr
import numpy as np

# Dask imports for parallel processing
from dask.distributed import Client, as_completed, LocalCluster
from dask import delayed
import dask.array as da

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from data_loader import ClimateDataLoader
from preprocessor import ClimateDataPreprocessor
from indices_calculator import ClimateIndicesCalculator


logger = logging.getLogger(__name__)


class ParallelClimateDataPipeline:
    """Enhanced pipeline with explicit multiprocessing support."""

    def __init__(self, config_path: Optional[str] = None, verbose: bool = False):
        """
        Initialize the parallel pipeline.

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
        self.dask_client = None

        # Setup logging
        log_path = self.config.log_path
        log_path.mkdir(parents=True, exist_ok=True)
        log_file = log_path / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        logging.basicConfig(
            level=logging.DEBUG if verbose else logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Parallel pipeline initialized. Log file: {log_file}")

    def initialize_dask_cluster(self):
        """Initialize Dask distributed cluster for parallel processing."""

        self.logger.info("=== Initializing Dask Cluster ===")

        # Get Dask configuration
        dask_config = self.config.get('performance.dask', {})
        n_workers = dask_config.get('n_workers', 4)
        threads_per_worker = dask_config.get('threads_per_worker', 2)
        memory_limit = dask_config.get('memory_limit', '4GB')

        try:
            # Create local cluster
            cluster = LocalCluster(
                n_workers=n_workers,
                threads_per_worker=threads_per_worker,
                memory_limit=memory_limit,
                dashboard_address=':8787',
                processes=True,  # Use processes for true parallelism
                silence_logs=logging.WARNING
            )

            # Create client
            self.dask_client = Client(cluster)

            self.logger.info(f"Dask cluster initialized:")
            self.logger.info(f"  Workers: {n_workers}")
            self.logger.info(f"  Threads per worker: {threads_per_worker}")
            self.logger.info(f"  Total threads: {n_workers * threads_per_worker}")
            self.logger.info(f"  Memory per worker: {memory_limit}")
            self.logger.info(f"  Dashboard: http://localhost:8787")

            # Print client info
            if self.verbose:
                print(f"\n{self.dask_client}\n")

            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize Dask cluster: {e}")
            self.logger.warning("Falling back to single-threaded execution")
            return False

    def load_data_parallel(self, variables: Optional[List[str]] = None) -> Dict[str, xr.Dataset]:
        """
        Load climate data with parallel I/O operations.

        Parameters:
        -----------
        variables : list, optional
            List of variables to load (default: all configured)

        Returns:
        --------
        dict
            Dictionary of loaded datasets
        """
        self.logger.info("=== Parallel Data Loading Phase ===")

        loader = ClimateDataLoader(self.config)

        if variables is None:
            variables = list(self.config.get('data.zarr_stores', {}).keys())

        # Use Dask delayed for parallel loading
        if self.dask_client:
            self.logger.info(f"Loading {len(variables)} variables in parallel")

            @delayed
            def load_variable(var_name):
                """Delayed function for parallel loading."""
                try:
                    return var_name, loader.load_variable_data(var_name)
                except Exception as e:
                    self.logger.error(f"Error loading {var_name}: {e}")
                    return var_name, None

            # Create delayed tasks
            tasks = [load_variable(var) for var in variables]

            # Compute in parallel
            results = self.dask_client.compute(tasks, sync=True)

            # Process results
            for var_name, ds in results:
                if ds is not None:
                    self.datasets[var_name] = ds
                    self.logger.info(f"Loaded {var_name}: shape={dict(ds.sizes)}")

        else:
            # Fallback to sequential loading
            for var in tqdm(variables, desc="Loading variables"):
                self.logger.info(f"Loading {var} data")
                try:
                    ds = loader.load_variable_data(var)
                    if ds is not None:
                        self.datasets[var] = ds
                        self.logger.info(f"Loaded {var}: shape={dict(ds.sizes)}")
                except Exception as e:
                    self.logger.error(f"Error loading {var}: {e}")

        return self.datasets

    def calculate_indices_parallel(self) -> Dict[str, xr.DataArray]:
        """
        Calculate climate indices with parallel processing.

        Returns:
        --------
        dict
            Dictionary of calculated indices
        """
        self.logger.info("=== Parallel Climate Indices Calculation ===")

        calculator = ClimateIndicesCalculator(self.config)

        if not self.dask_client:
            # Fallback to sequential processing
            self.logger.warning("No Dask client, using sequential processing")
            return calculator.calculate_all_indices(self.processed_datasets)

        # Get indices configuration
        indices_config = self.config.get('indices', {})
        parallel_indices = self.config.get('performance.parallel_indices', True)

        if not parallel_indices:
            return calculator.calculate_all_indices(self.processed_datasets)

        # Prepare parallel tasks for each index category
        tasks = []

        # Temperature indices
        if 'temperature' in self.processed_datasets and 'temperature' in indices_config:
            temp_indices = indices_config['temperature']
            for index_name in temp_indices:
                task = delayed(self._calculate_single_index)(
                    calculator, 'temperature', index_name,
                    self.processed_datasets['temperature']
                )
                tasks.append(('temperature', index_name, task))

        # Precipitation indices
        if 'precipitation' in self.processed_datasets and 'precipitation' in indices_config:
            precip_indices = indices_config['precipitation']
            for index_name in precip_indices:
                task = delayed(self._calculate_single_index)(
                    calculator, 'precipitation', index_name,
                    self.processed_datasets['precipitation']
                )
                tasks.append(('precipitation', index_name, task))

        # Execute parallel computation
        self.logger.info(f"Calculating {len(tasks)} indices in parallel")

        # Use progress bar for parallel execution
        results = {}
        with tqdm(total=len(tasks), desc="Calculating indices") as pbar:
            futures = self.dask_client.compute([t[2] for t in tasks], sync=False)

            for future, (category, name, _) in zip(as_completed(futures), tasks):
                try:
                    result = future.result()
                    if result is not None:
                        results[f"{category}_{name}"] = result
                        self.logger.info(f"Calculated {category}_{name}")
                except Exception as e:
                    self.logger.error(f"Error calculating {category}_{name}: {e}")
                pbar.update(1)

        self.indices = results
        return results

    def _calculate_single_index(self, calculator, category, index_name, dataset):
        """Calculate a single climate index (used for parallel execution)."""
        try:
            # This would call specific index calculation methods
            # Simplified for demonstration
            if category == 'temperature':
                if index_name == 'frost_days':
                    from xclim import atmos
                    return atmos.frost_days(dataset['tasmin'], freq='YS')
                elif index_name == 'summer_days':
                    from xclim import atmos
                    return atmos.tx_days_above(dataset['tasmax'], thresh='25 degC', freq='YS')
            elif category == 'precipitation':
                if index_name == 'prcptot':
                    from xclim import atmos
                    return atmos.prcptot(dataset['pr'], freq='YS')
                elif index_name == 'rx1day':
                    from xclim import atmos
                    return atmos.rx1day(dataset['pr'], freq='YS')

            return None

        except Exception as e:
            self.logger.error(f"Error in _calculate_single_index: {e}")
            return None

    def optimize_chunk_sizes(self):
        """Automatically optimize chunk sizes based on available memory."""

        if not self.dask_client:
            return

        # Get worker info
        info = self.dask_client.scheduler_info()
        n_workers = len(info['workers'])

        # Get memory info from first worker
        if n_workers > 0:
            worker_key = list(info['workers'].keys())[0]
            worker_memory = info['workers'][worker_key]['memory_limit']

            # Convert to bytes if needed
            if isinstance(worker_memory, str):
                import dask.utils
                worker_memory = dask.utils.parse_bytes(worker_memory)

            total_memory = worker_memory * n_workers

            # Calculate optimal chunk size
            # Assume we want chunks that fit comfortably in memory
            target_chunk_memory = worker_memory / 10  # Use 10% of worker memory per chunk

            # For float32 data (4 bytes per element)
            elements_per_chunk = int(target_chunk_memory / 4)

            # Calculate spatial chunk dimensions (assuming square chunks)
            spatial_chunk = int(np.sqrt(elements_per_chunk / 365))  # Divide by time dimension

            self.logger.info(f"Optimized chunk sizes:")
            self.logger.info(f"  Total cluster memory: {total_memory / 1e9:.1f} GB")
            self.logger.info(f"  Target chunk memory: {target_chunk_memory / 1e6:.1f} MB")
            self.logger.info(f"  Recommended spatial chunk: {spatial_chunk} x {spatial_chunk}")

            # Update configuration
            self.config.set('processing.chunk_size.lat', spatial_chunk)
            self.config.set('processing.chunk_size.lon', spatial_chunk)

    def run(self, variables: Optional[List[str]] = None):
        """
        Run the complete pipeline with parallel processing.

        Parameters:
        -----------
        variables : list, optional
            List of variables to process
        """
        self.logger.info("=" * 50)
        self.logger.info("Starting Parallel Climate Data Pipeline")
        self.logger.info("=" * 50)

        start_time = datetime.now()

        try:
            # Initialize Dask cluster
            cluster_initialized = self.initialize_dask_cluster()

            if cluster_initialized:
                # Optimize chunk sizes
                self.optimize_chunk_sizes()

            # Load data in parallel
            self.load_data_parallel(variables)

            if not self.datasets:
                self.logger.error("No data loaded. Exiting.")
                return

            # Preprocess data
            self.logger.info("=== Preprocessing Phase ===")
            preprocessor = ClimateDataPreprocessor(self.config)

            for var_name, ds in self.datasets.items():
                self.logger.info(f"Preprocessing {var_name}")
                processed = preprocessor.preprocess(ds, variable_type=var_name)
                self.processed_datasets[var_name] = processed

            # Calculate indices in parallel
            self.calculate_indices_parallel()

            # Save results
            self.save_results()

            # Report completion
            elapsed_time = datetime.now() - start_time
            self.logger.info("=" * 50)
            self.logger.info(f"Pipeline completed successfully in {elapsed_time}")
            self.logger.info(f"Processed {len(self.datasets)} variables")
            self.logger.info(f"Calculated {len(self.indices)} indices")

            if cluster_initialized:
                # Show cluster statistics
                self.show_performance_stats()

            self.logger.info("=" * 50)

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            if self.verbose:
                self.logger.debug(traceback.format_exc())
            raise

        finally:
            # Cleanup Dask cluster
            if self.dask_client:
                self.logger.info("Closing Dask cluster")
                self.dask_client.close()

    def show_performance_stats(self):
        """Display performance statistics from Dask cluster."""

        if not self.dask_client:
            return

        # Get performance metrics
        try:
            # This would show detailed performance metrics
            # Simplified for demonstration
            info = self.dask_client.scheduler_info()

            self.logger.info("\n=== Performance Statistics ===")
            self.logger.info(f"Workers used: {len(info['workers'])}")

            # Get task stream info if available
            # This would require additional Dask diagnostics

        except Exception as e:
            self.logger.debug(f"Could not retrieve performance stats: {e}")

    def save_results(self):
        """Save results (same as original pipeline)."""
        self.logger.info("=== Saving Results ===")

        output_dir = self.config.output_path
        output_dir.mkdir(parents=True, exist_ok=True)

        if self.indices:
            indices_file = output_dir / f"climate_indices_{datetime.now().strftime('%Y%m%d')}.nc"

            # Combine indices into single dataset
            combined = xr.Dataset(self.indices)

            compression = self.config.get('output.compression', {})
            combined.to_netcdf(
                indices_file,
                engine=compression.get('engine', 'h5netcdf'),
                encoding={
                    var: {'zlib': True, 'complevel': compression.get('complevel', 4)}
                    for var in combined.data_vars
                }
            )
            self.logger.info(f"Saved indices to {indices_file}")


# CLI Interface
@click.command()
@click.option('--config', '-c',
              type=click.Path(exists=True),
              help='Path to configuration file')
@click.option('--variables', '-v',
              multiple=True,
              help='Variables to process')
@click.option('--verbose',
              is_flag=True,
              help='Enable verbose logging')
@click.option('--no-parallel',
              is_flag=True,
              help='Disable parallel processing')
def main(config, variables, verbose, no_parallel):
    """
    xclim-timber Parallel Pipeline

    Process climate data with multiprocessing support.
    """

    if no_parallel:
        # Use original pipeline
        from pipeline import ClimateDataPipeline
        pipeline = ClimateDataPipeline(config, verbose)
    else:
        # Use parallel pipeline
        pipeline = ParallelClimateDataPipeline(config, verbose)

    pipeline.run(list(variables) if variables else None)

    click.echo("\n✓ Pipeline completed successfully!")


if __name__ == "__main__":
    main()
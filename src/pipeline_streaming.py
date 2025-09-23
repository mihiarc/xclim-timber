#!/usr/bin/env python
"""
Streaming pipeline for climate indices calculation.
Processes data in temporal chunks to minimize memory usage.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
import xarray as xr
import numpy as np
from datetime import datetime
import dask
import dask.array as da
from dask.distributed import Client, as_completed
import zarr
from tqdm import tqdm

from config import Config
from indices_calculator import ClimateIndicesCalculator

logger = logging.getLogger(__name__)


class StreamingClimatePipeline:
    """
    Memory-efficient streaming pipeline for climate data processing.

    Key Features:
    - Processes data in temporal chunks (e.g., year by year)
    - Never loads full dataset into memory
    - Leverages Zarr's chunked storage for streaming
    - Parallel processing of independent time chunks
    """

    def __init__(self, config_path: str, chunk_years: int = 1):
        """
        Initialize streaming pipeline.

        Parameters:
        -----------
        config_path : str
            Path to configuration file
        chunk_years : int
            Number of years to process at once (default: 1)
        """
        self.config = Config(config_path)
        self.chunk_years = chunk_years
        self.client = None

        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging."""
        log_level = logging.DEBUG if self.config.get('verbose', False) else logging.INFO
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def _setup_dask_client(self):
        """Initialize Dask client for parallel processing."""
        if self.client is None:
            n_workers = self.config.get('processing.dask.n_workers', 4)
            memory_limit = self.config.get('processing.dask.memory_limit', '4GB')

            self.client = Client(
                n_workers=n_workers,
                threads_per_worker=1,
                memory_limit=memory_limit,
                dashboard_address=':8787'
            )
            logger.info(f"Dask dashboard: http://localhost:8787")

    def _get_zarr_stores(self) -> Dict[str, str]:
        """
        Get paths to Zarr stores for each variable.

        Returns:
        --------
        dict
            Mapping of variable names to Zarr store paths
        """
        stores = {}
        base_path = Path(self.config.get('data.input_path'))

        # Try multiple possible Zarr store configurations
        zarr_configs = [
            # Primary configuration - PRISM data structure
            {
                'temperature': base_path / 'prism.zarr/temperature',
                'precipitation': base_path / 'prism.zarr/precipitation',
                'humidity': base_path / 'prism.zarr/humidity'
            },
            # Alternative configuration - flat structure
            {
                'temperature': base_path / 'temperature.zarr',
                'precipitation': base_path / 'precipitation.zarr',
                'humidity': base_path / 'humidity.zarr'
            },
            # Alternative configuration - nested by variable
            {
                'temperature': base_path / 'zarr/temperature',
                'precipitation': base_path / 'zarr/precipitation',
                'humidity': base_path / 'zarr/humidity'
            }
        ]

        # Try each configuration
        for config_idx, zarr_config in enumerate(zarr_configs):
            config_stores = {}
            for var, path in zarr_config.items():
                if path.exists():
                    config_stores[var] = str(path)
                    logger.info(f"Found {var} store at {path}")

            # If we found at least one store with this config, use it
            if config_stores:
                stores.update(config_stores)
                if config_idx > 0:
                    logger.info(f"Using alternative Zarr configuration #{config_idx + 1}")
                break

        # Log detailed information about missing stores
        if not stores:
            logger.error("=" * 60)
            logger.error("NO ZARR STORES FOUND")
            logger.error("=" * 60)
            logger.error(f"Base path: {base_path}")
            logger.error("\nSearched for stores at:")
            for zarr_config in zarr_configs:
                for var, path in zarr_config.items():
                    exists = "✓ EXISTS" if path.exists() else "✗ NOT FOUND"
                    logger.error(f"  {var}: {path} [{exists}]")
            logger.error("\nPlease ensure Zarr stores are available at one of these locations.")

        return stores

    def _get_time_chunks(self, zarr_path: str,
                        start_year: int, end_year: int) -> List[Tuple[str, str]]:
        """
        Generate time chunks for processing.

        Parameters:
        -----------
        zarr_path : str
            Path to Zarr store
        start_year : int
            Start year for processing
        end_year : int
            End year for processing

        Returns:
        --------
        list
            List of (start_date, end_date) tuples for each chunk
        """
        chunks = []
        current_year = start_year

        while current_year <= end_year:
            chunk_end = min(current_year + self.chunk_years - 1, end_year)
            start_date = f"{current_year}-01-01"
            end_date = f"{chunk_end}-12-31"
            chunks.append((start_date, end_date))
            current_year = chunk_end + 1

        return chunks

    def process_single_chunk(self,
                           stores: Dict[str, str],
                           time_range: Tuple[str, str],
                           output_path: Path) -> Dict:
        """
        Process a single time chunk.

        Parameters:
        -----------
        stores : dict
            Zarr store paths
        time_range : tuple
            (start_date, end_date) for this chunk
        output_path : Path
            Where to save results

        Returns:
        --------
        dict
            Metadata about processed chunk
        """
        start_date, end_date = time_range
        logger.info(f"Processing chunk: {start_date} to {end_date}")

        # Load only the required time slice - data stays lazy!
        datasets = {}
        for var, store_path in stores.items():
            # Open with chunks - this is LAZY, no data loaded yet
            ds = xr.open_zarr(store_path, chunks='auto')

            # Select time range - still lazy!
            if 'time' in ds.dims:
                ds = ds.sel(time=slice(start_date, end_date))

            datasets[var] = ds
            logger.debug(f"Loaded {var} chunk: {ds.dims}")

        # Calculate indices - computation happens here
        calculator = ClimateIndicesCalculator(self.config)
        indices = calculator.calculate_all_indices(datasets)

        # Combine indices into single dataset
        result_ds = xr.Dataset(indices)
        result_ds.attrs['time_range'] = f"{start_date} to {end_date}"

        # Save chunk results immediately to free memory
        chunk_file = output_path / f"indices_{start_date[:4]}_{end_date[:4]}.nc"

        # Use compute() to trigger calculation and save
        with dask.config.set(scheduler='threads'):
            result_ds.to_netcdf(chunk_file,
                              engine='netcdf4',
                              encoding={var: {'zlib': True, 'complevel': 4}
                                      for var in result_ds.data_vars})

        logger.info(f"Saved chunk to {chunk_file}")

        # Return metadata only, not the actual data
        return {'chunk': time_range, 'file': str(chunk_file),
                'indices': list(result_ds.data_vars)}

    def run_streaming(self,
                     variables: List[str],
                     start_year: int = 2001,
                     end_year: int = 2024) -> Dict:
        """
        Run the pipeline in streaming mode.

        Parameters:
        -----------
        variables : list
            Variables to process
        start_year : int
            Start year
        end_year : int
            End year

        Returns:
        --------
        dict
            Processing summary
        """
        logger.info("=" * 80)
        logger.info("STREAMING CLIMATE PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Processing period: {start_year}-{end_year}")
        logger.info(f"Chunk size: {self.chunk_years} year(s)")
        logger.info(f"Variables: {variables}")

        # Setup
        self._setup_dask_client()
        output_path = Path(self.config.get('data.output_path'))
        output_path.mkdir(parents=True, exist_ok=True)

        # Get Zarr stores
        all_stores = self._get_zarr_stores()
        stores = {k: v for k, v in all_stores.items() if k in variables}

        if not stores:
            logger.error("No valid Zarr stores found for requested variables")

            # Provide detailed error information
            error_details = []
            if not all_stores:
                error_details.append("No Zarr stores found at any configured location")
            else:
                available = list(all_stores.keys())
                error_details.append(f"Available variables: {available}")
                error_details.append(f"Requested variables: {variables}")
                missing = [v for v in variables if v not in all_stores]
                if missing:
                    error_details.append(f"Missing variables: {missing}")

            error_msg = "No stores found. " + ". ".join(error_details)
            return {'status': 'failed', 'error': error_msg}

        # Generate time chunks
        sample_store = next(iter(stores.values()))
        time_chunks = self._get_time_chunks(sample_store, start_year, end_year)

        logger.info(f"Processing {len(time_chunks)} time chunks")

        # Process chunks sequentially with memory cleanup
        results = []

        for time_range in tqdm(time_chunks, desc="Processing chunks"):
            try:
                result = self.process_single_chunk(stores, time_range, output_path)
                results.append(result)

                # Force garbage collection between chunks
                import gc
                gc.collect()

            except Exception as e:
                logger.error(f"Chunk {time_range} failed: {e}")
                continue

        # Combine chunk outputs into final dataset
        if results:
            self._combine_chunks(results, output_path)

        logger.info("=" * 80)
        logger.info("STREAMING PROCESSING COMPLETE")
        logger.info("=" * 80)

        return {
            'status': 'success',
            'chunks_processed': len(results),
            'total_chunks': len(time_chunks),
            'output_path': str(output_path),
            'chunk_files': [r['file'] for r in results]
        }

    def _combine_chunks(self, chunks: List[Dict], output_path: Path):
        """
        Combine individual chunk files into single output.

        Parameters:
        -----------
        chunks : list
            List of chunk metadata
        output_path : Path
            Output directory
        """
        logger.info("Combining chunk outputs...")

        # Get all chunk files
        chunk_files = [c['file'] for c in chunks]

        # Open all chunks lazily
        datasets = []
        for file in sorted(chunk_files):
            ds = xr.open_dataset(file, chunks='auto')
            datasets.append(ds)

        # Concatenate along time dimension
        combined = xr.concat(datasets, dim='time')

        # Save combined output
        output_file = output_path / 'combined_indices.nc'

        # Save with compression
        encoding = {var: {'zlib': True, 'complevel': 4}
                   for var in combined.data_vars}

        combined.to_netcdf(output_file, engine='netcdf4', encoding=encoding)
        logger.info(f"Saved combined output to {output_file}")

    def close(self):
        """Clean up resources."""
        if self.client:
            self.client.close()


def main():
    """Example usage of streaming pipeline."""
    import sys

    # Configuration
    config_path = 'configs/config_comprehensive_2001_2024.yaml'

    # Initialize pipeline with 1-year chunks
    pipeline = StreamingClimatePipeline(config_path, chunk_years=1)

    try:
        # Run streaming processing
        results = pipeline.run_streaming(
            variables=['temperature', 'precipitation'],
            start_year=2001,
            end_year=2024
        )

        print("\nProcessing Results:")
        print(f"Status: {results['status']}")
        print(f"Chunks processed: {results['chunks_processed']}/{results['total_chunks']}")
        print(f"Output: {results['output_path']}")

    finally:
        pipeline.close()

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
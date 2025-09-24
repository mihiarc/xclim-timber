#!/usr/bin/env python
"""
Simplified climate data processing pipeline for PRISM Zarr data.
Streamlined for single-source, pre-processed data.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import xarray as xr
from datetime import datetime
import warnings

from config import Config
from data_loader import PrismDataLoader
from indices_calculator import ClimateIndicesCalculator

# Suppress common warnings
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*All-NaN slice.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*divide.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*invalid value.*')

logger = logging.getLogger(__name__)


class PrismPipeline:
    """
    Simplified climate data processing pipeline for PRISM data.

    Key simplifications:
    - Direct loading from known Zarr stores (no pattern matching)
    - No preprocessing (PRISM data is already clean)
    - Streamlined configuration
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize pipeline.

        Parameters:
        -----------
        config_path : str, optional
            Path to configuration file (uses defaults if not provided)
        """
        self.config = Config(config_path)
        self.loader = PrismDataLoader(self.config)
        self.calculator = ClimateIndicesCalculator(self.config)

        # Setup logging
        self._setup_logging()

        # Create output directory
        self.config.output_path.mkdir(parents=True, exist_ok=True)

    def _setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(self.config.log_path / 'pipeline.log')
            ]
        )

    def run(self,
            start_year: Optional[int] = None,
            end_year: Optional[int] = None,
            indices_categories: Optional[List[str]] = None) -> xr.Dataset:
        """
        Run the simplified pipeline.

        Parameters:
        -----------
        start_year : int, optional
            Start year for processing
        end_year : int, optional
            End year for processing
        indices_categories : list, optional
            Categories of indices to calculate (default: all)

        Returns:
        --------
        xr.Dataset
            Dataset with calculated climate indices
        """
        logger.info("="*60)
        logger.info("PRISM CLIMATE PIPELINE - SIMPLIFIED")
        logger.info("="*60)

        # Step 1: Load data (no preprocessing needed!)
        logger.info("Step 1: Loading PRISM data...")
        combined_ds = self.loader.get_combined_dataset()

        # Subset time if requested
        if start_year or end_year:
            combined_ds = self.loader.subset_time(combined_ds, start_year, end_year)
            logger.info(f"  Subsetted to {start_year or 'start'}-{end_year or 'end'}")

        logger.info(f"  Loaded variables: {list(combined_ds.data_vars)}")
        logger.info(f"  Time range: {combined_ds.time.min().values} to {combined_ds.time.max().values}")
        logger.info(f"  Spatial extent: {combined_ds.sizes}")

        # Step 2: Calculate indices
        logger.info("\nStep 2: Calculating climate indices...")

        # Determine which categories to calculate
        if indices_categories is None:
            indices_categories = ['temperature', 'precipitation', 'extremes',
                                 'humidity', 'comfort', 'agricultural']

        results = {}
        for category in indices_categories:
            if category in self.config.config['indices']:
                logger.info(f"  Processing {category} indices...")
                indices_list = self.config.config['indices'][category]

                for index_name in indices_list:
                    try:
                        # Calculate each index
                        result = self.calculator.calculate_index(
                            combined_ds,
                            index_name,
                            category
                        )
                        if result is not None:
                            results[index_name] = result
                            logger.info(f"    ✓ {index_name}")
                    except Exception as e:
                        logger.warning(f"    ✗ {index_name}: {str(e)}")

        # Combine all indices into single dataset
        indices_ds = xr.Dataset(results)

        # Add metadata
        indices_ds.attrs.update({
            'title': 'PRISM Climate Indices',
            'institution': 'Calculated using xclim-timber pipeline',
            'source': 'PRISM climate data',
            'history': f"{datetime.now().isoformat()}: Calculated climate indices",
            'time_range': f"{combined_ds.time.min().values} to {combined_ds.time.max().values}",
            'indices_count': len(results)
        })

        # Step 3: Save results
        logger.info(f"\nStep 3: Saving results...")
        output_file = self._generate_output_filename(start_year, end_year)
        self.save_results(indices_ds, output_file)

        logger.info("="*60)
        logger.info(f"✓ Pipeline complete! Results saved to {output_file}")
        logger.info("="*60)

        return indices_ds

    def save_results(self, ds: xr.Dataset, output_path: Path):
        """
        Save results to NetCDF file.

        Parameters:
        -----------
        ds : xr.Dataset
            Dataset to save
        output_path : Path
            Output file path
        """
        # Prepare encoding with compression
        encoding = {
            var: {
                'zlib': True,
                'complevel': self.config.config['output']['compression']['complevel']
            }
            for var in ds.data_vars
        }

        # Save to NetCDF
        ds.to_netcdf(
            output_path,
            engine='netcdf4',
            encoding=encoding
        )
        logger.info(f"  Saved to {output_path}")

        # Calculate and log file size
        file_size = output_path.stat().st_size / (1024 * 1024)  # MB
        logger.info(f"  File size: {file_size:.2f} MB")

    def _generate_output_filename(self,
                                 start_year: Optional[int] = None,
                                 end_year: Optional[int] = None) -> Path:
        """Generate output filename based on parameters."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if start_year and end_year:
            filename = f"prism_indices_{start_year}_{end_year}_{timestamp}.nc"
        else:
            filename = f"prism_indices_full_{timestamp}.nc"

        return self.config.output_path / filename

    def run_for_parcels(self,
                       parcels_file: str,
                       start_year: int,
                       end_year: int) -> Path:
        """
        Calculate indices for specific parcel locations.

        Parameters:
        -----------
        parcels_file : str
            CSV file with parcel locations (must have lat, lon columns)
        start_year : int
            Start year
        end_year : int
            End year

        Returns:
        --------
        Path
            Path to output CSV file
        """
        import pandas as pd

        logger.info("Running pipeline for parcel locations...")

        # Load parcel data
        parcels = pd.read_csv(parcels_file)
        if 'lat' not in parcels.columns or 'lon' not in parcels.columns:
            raise ValueError("Parcels file must have 'lat' and 'lon' columns")

        # Calculate indices for full spatial extent
        indices_ds = self.run(start_year, end_year)

        # Extract at parcel locations
        logger.info(f"Extracting indices at {len(parcels)} locations...")

        results = []
        for _, parcel in parcels.iterrows():
            # Find nearest grid point
            point_data = indices_ds.sel(
                lat=parcel['lat'],
                lon=parcel['lon'],
                method='nearest'
            )

            # Convert to dictionary and add parcel info
            point_dict = {
                'lat': parcel['lat'],
                'lon': parcel['lon']
            }

            # Add parcel metadata if available
            for col in parcels.columns:
                if col not in ['lat', 'lon']:
                    point_dict[col] = parcel[col]

            # Add index values (averaged over time if multiple years)
            for var in indices_ds.data_vars:
                values = point_data[var].values
                if values.size > 1:
                    point_dict[var] = values.mean()
                else:
                    point_dict[var] = float(values)

            results.append(point_dict)

        # Convert to DataFrame and save
        results_df = pd.DataFrame(results)
        output_file = self.config.output_path / f"parcel_indices_{start_year}_{end_year}.csv"
        results_df.to_csv(output_file, index=False)

        logger.info(f"✓ Saved parcel results to {output_file}")
        return output_file


# CLI interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Simplified PRISM Climate Data Pipeline"
    )
    parser.add_argument(
        "--config",
        help="Path to configuration file",
        default=None
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Start year for processing",
        default=None
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help="End year for processing",
        default=None
    )
    parser.add_argument(
        "--parcels",
        help="CSV file with parcel locations for point extraction",
        default=None
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        help="Categories of indices to calculate",
        default=None
    )

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = PrismPipeline(args.config)

    # Run appropriate mode
    if args.parcels:
        if not args.start_year or not args.end_year:
            print("Error: --start-year and --end-year required for parcel extraction")
            exit(1)
        pipeline.run_for_parcels(args.parcels, args.start_year, args.end_year)
    else:
        pipeline.run(
            start_year=args.start_year,
            end_year=args.end_year,
            indices_categories=args.categories
        )
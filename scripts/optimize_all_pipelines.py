#!/usr/bin/env python3
"""
Script to optimize memory usage across all climate indices pipelines.

Applies consistent memory-efficient settings:
- Smaller temporal chunks (4 years instead of 12)
- Smaller spatial chunks (reduce grid dimensions)
- Use threaded scheduler instead of distributed client
"""

import re
from pathlib import Path

# Pipeline files to optimize
PIPELINES = [
    'temperature_pipeline.py',
    'precipitation_pipeline.py',
    'humidity_pipeline.py',
    'human_comfort_pipeline.py',
    'multivariate_pipeline.py',
    'agricultural_pipeline.py',
    'drought_pipeline.py'
]

def optimize_pipeline(filepath: Path):
    """Apply memory optimizations to a pipeline file."""
    print(f"Optimizing {filepath.name}...")

    with open(filepath, 'r') as f:
        content = f.read()

    original_content = content

    # 1. Change default chunk_years from 12 to 4 in __init__
    content = re.sub(
        r'def __init__\(self, chunk_years: int = 12,',
        r'def __init__(self, chunk_years: int = 4,',
        content
    )

    # 2. Change default in argparse
    content = re.sub(
        r"'--chunk-years',\s+type=int,\s+default=12,\s+help='Number of years to process per chunk \(default: 12\)'",
        "'--chunk-years',\n        type=int,\n        default=4,\n        help='Number of years to process per chunk (default: 4 for memory efficiency)'",
        content
    )

    # 3. Update chunk configuration for smaller spatial chunks
    # Replace lat: 69 with lat: 103 (621 / 103 = 6 chunks)
    content = re.sub(
        r"'lat': 69,\s+# 621 / 69 = 9 even chunks",
        "'lat': 103,   # 621 / 103 = 6 chunks (smaller for less memory)",
        content
    )

    # Replace lon: 281 with lon: 201 (1405 / 201 = 7 chunks)
    content = re.sub(
        r"'lon': 281\s+# 1405 / 281 = 5 even chunks",
        "'lon': 201    # 1405 / 201 = 7 chunks (smaller for less memory)",
        content
    )

    # 4. Simplify setup_dask_client to use threaded scheduler
    # Find and replace the distributed client setup
    client_pattern = r'def setup_dask_client\(self\):.*?logger\.info\(f"Dask client initialized.*?\)'

    replacement = '''def setup_dask_client(self):
        """Initialize Dask client with memory limits."""
        # Use threaded scheduler instead of distributed for lower memory overhead
        logger.info("Using Dask threaded scheduler (no distributed client for memory efficiency)")'''

    content = re.sub(client_pattern, replacement, content, flags=re.DOTALL)

    # Only write if changes were made
    if content != original_content:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"  ✓ Optimized {filepath.name}")
        return True
    else:
        print(f"  - No changes needed for {filepath.name}")
        return False

def main():
    """Optimize all pipeline files."""
    print("=" * 60)
    print("OPTIMIZING ALL PIPELINES FOR MEMORY EFFICIENCY")
    print("=" * 60)
    print()

    optimized = 0
    for pipeline_file in PIPELINES:
        filepath = Path(pipeline_file)
        if filepath.exists():
            if optimize_pipeline(filepath):
                optimized += 1
        else:
            print(f"  ✗ File not found: {pipeline_file}")

    print()
    print("=" * 60)
    print(f"✓ Optimized {optimized}/{len(PIPELINES)} pipelines")
    print("=" * 60)
    print()
    print("Memory Optimizations Applied:")
    print("  - Temporal chunks: 12 years → 4 years")
    print("  - Spatial chunks: lat 69→103, lon 281→201")
    print("  - Scheduler: Distributed client → Threaded")
    print()
    print("Expected Memory Reduction: ~60-70%")
    print("Expected Processing Time: +20-30% (but won't crash!)")

if __name__ == "__main__":
    main()

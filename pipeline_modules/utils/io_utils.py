"""
Unified I/O utilities for the CRISP pipeline.
Handles Parquet/CSV format switching based on PIPELINE_OUTPUT_FORMAT env var.
"""

import os
import glob
import logging
import pandas as pd
from pathlib import Path

# Read format from environment (set by run_all_module.py)
OUTPUT_FORMAT = os.environ.get('PIPELINE_OUTPUT_FORMAT', 'parquet')


def get_output_format():
    """Return current output format (re-reads env var)."""
    return os.environ.get('PIPELINE_OUTPUT_FORMAT', 'parquet')


def save_df(df, path, index=False, **kwargs):
    """
    Save DataFrame in the configured format (parquet or csv).
    
    Args:
        df: DataFrame to save
        path: Output path (suffix will be adjusted automatically)
        index: Whether to include index
        **kwargs: Extra args passed to to_csv (ignored for parquet)
    
    Returns:
        Path: Actual path written
    """
    fmt = get_output_format()
    p = Path(path)
    
    if fmt == 'parquet':
        p = p.with_suffix('.parquet')
        df.to_parquet(p, index=index, engine='pyarrow')
    else:
        p = p.with_suffix('.csv')
        df.to_csv(p, index=index, **kwargs)
    
    return p


def load_df(path, **kwargs):
    """
    Load DataFrame, auto-detecting parquet or csv.
    Prefers .parquet if both exist.
    
    Args:
        path: Base path (with or without extension)
        **kwargs: Extra args passed to read_csv (ignored for parquet)
    
    Returns:
        DataFrame
    """
    p = Path(path)
    parquet_path = p.with_suffix('.parquet')
    csv_path = p.with_suffix('.csv')
    
    # Also check if original path exists as-is
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    elif csv_path.exists():
        return pd.read_csv(csv_path, low_memory=False, **kwargs)
    elif p.exists():
        # Original path exists (might have different extension or no extension)
        if p.suffix == '.parquet':
            return pd.read_parquet(p)
        else:
            return pd.read_csv(p, low_memory=False, **kwargs)
    else:
        raise FileNotFoundError(f"No parquet or csv file found for: {path}")


def resolve_input(path):
    """
    Find the actual file path, preferring .parquet over .csv.
    
    Args:
        path: Expected path (e.g., 'output/2_cleaning/MEASUREMENT_cleaned.csv')
    
    Returns:
        Path: Actual existing file path
    """
    p = Path(path)
    parquet_path = p.with_suffix('.parquet')
    
    if parquet_path.exists():
        return parquet_path
    if p.exists():
        return p
    # Try without suffix
    csv_path = p.with_suffix('.csv')
    if csv_path.exists():
        return csv_path
    
    return p  # Return original, let caller handle missing file


def cleanup_intermediate(output_dir, module_id):
    """
    Delete large intermediate data files from a completed module.
    Keeps reports, logs, JSONs, and removed_records.
    
    Args:
        output_dir: Base output directory
        module_id: Module ID (e.g., '2_cleaning')
    """
    module_dir = Path(output_dir) / module_id
    if not module_dir.exists():
        return
    
    # Patterns for large data files to delete
    patterns = [
        '*_cleaned.csv', '*_cleaned.parquet',
        '*_mapped.csv', '*_mapped.parquet',
        '*_standardized.csv', '*_standardized.parquet',
        '*_merged.csv', '*_merged.parquet',
        '.temp_*',  # Temp files
    ]
    
    deleted = 0
    freed_bytes = 0
    for pattern in patterns:
        for f in module_dir.glob(pattern):
            try:
                size = f.stat().st_size
                f.unlink()
                deleted += 1
                freed_bytes += size
            except OSError as e:
                logging.warning(f"Failed to delete {f}: {e}")
    
    if deleted > 0:
        freed_mb = freed_bytes / (1024 * 1024)
        logging.info(f"Cleaned up {deleted} intermediate files from {module_id} "
                     f"(freed {freed_mb:.1f} MB)")


def convert_csv_to_parquet(csv_path, delete_csv=True):
    """
    Convert a CSV file to Parquet and optionally delete the CSV.
    Used for modules that write CSV via streaming, then convert.
    
    Args:
        csv_path: Path to existing CSV file
        delete_csv: Whether to delete the CSV after conversion (default: True)
    
    Returns:
        Path: Path to new parquet file, or original csv_path if format is csv
    """
    if get_output_format() != 'parquet':
        return Path(csv_path)
    
    csv_p = Path(csv_path)
    if not csv_p.exists():
        return csv_p
    
    parquet_p = csv_p.with_suffix('.parquet')
    try:
        csv_size = csv_p.stat().st_size
        df = pd.read_csv(csv_p, low_memory=False)
        df.to_parquet(parquet_p, index=False, engine='pyarrow')
        parquet_size = parquet_p.stat().st_size
        ratio = csv_size / parquet_size if parquet_size > 0 else 0
        logging.info(f"Converted {csv_p.name} to parquet "
                     f"({csv_size / 1024 / 1024:.1f} MB → {parquet_size / 1024 / 1024:.1f} MB, {ratio:.1f}x compression)")
        if delete_csv:
            csv_p.unlink()
        return parquet_p
    except Exception as e:
        logging.warning(f"Failed to convert {csv_p} to parquet: {e}. Keeping CSV.")
        return csv_p

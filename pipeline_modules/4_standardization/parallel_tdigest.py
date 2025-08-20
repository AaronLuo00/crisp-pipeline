"""
Parallel T-Digest processing for efficient percentile calculation on large datasets.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import logging
from tdigest import TDigest
import multiprocessing as mp
import time


def process_measurement_chunk(args: Tuple[str, int, int, Optional[int]]) -> Tuple[Dict, float]:
    """
    Process a chunk of MEASUREMENT data to calculate T-Digest statistics.
    
    Args:
        args: Tuple containing (filepath, start_byte, end_byte, optional_chunk_size)
    
    Returns:
        Dictionary mapping concept_id to serialized T-Digest state
    """
    filepath, start_byte, end_byte, chunk_size = args
    start_time = time.time()
    
    # T-Digest storage for each concept
    concept_digests = {}
    
    # Column names from Module 3 output
    column_names = ['measurement_id', 'person_id', 'measurement_concept_id', 
                   'measurement_date', 'measurement_time', 'measurement_datetime',
                   'measurement_type_concept_id', 'operator_concept_id', 
                   'value_as_number', 'value_as_concept_id', 'unit_concept_id',
                   'range_low', 'range_high', 'provider_id', 'visit_occurrence_id',
                   'visit_detail_id', 'measurement_source_concept_id', 
                   'measurement_concept_id_mapped']
    
    # Specify dtypes to avoid mixed type warnings
    dtype_spec = {
        'measurement_id': 'str',
        'person_id': 'str',
        'measurement_concept_id': 'float64',
        'measurement_date': 'str',
        'measurement_time': 'str',  # Can be empty string
        'measurement_datetime': 'str',
        'measurement_type_concept_id': 'float64',
        'operator_concept_id': 'float64',
        'value_as_number': 'float64',
        'value_as_concept_id': 'float64',
        'unit_concept_id': 'float64',
        'range_low': 'float64',
        'range_high': 'float64',
        'provider_id': 'float64',
        'visit_occurrence_id': 'float64',
        'visit_detail_id': 'float64',
        'measurement_source_concept_id': 'float64',
        'measurement_concept_id_mapped': 'str'
    }
    
    # Process the file chunk
    with open(filepath, 'rb') as f:
        # Skip to start position if not at beginning
        if start_byte > 0:
            f.seek(start_byte)
            # Skip the partial line at the start
            f.readline()
            actual_start = f.tell()
        else:
            actual_start = 0
        
        # Check if we need to skip header
        is_first_chunk = (start_byte == 0)
        
        # Read the entire chunk at once for this worker
        if end_byte > actual_start:
            f.seek(actual_start)
            chunk_data = f.read(end_byte - actual_start)
            
            if chunk_data:
                # Find the last complete line
                last_newline = chunk_data.rfind(b'\n')
                if last_newline != -1:
                    # Process only complete lines
                    process_data = chunk_data[:last_newline]
                else:
                    # If no newline found, process entire chunk (edge case for last chunk)
                    process_data = chunk_data
                
                # Parse CSV chunk
                from io import BytesIO
                try:
                    df_chunk = pd.read_csv(
                        BytesIO(process_data),
                        header=0 if is_first_chunk else None,
                        names=None if is_first_chunk else column_names,
                        dtype=dtype_spec,
                        low_memory=False,
                        na_values=['', 'NA', 'null', 'NULL', 'None']
                    )
                except Exception as e:
                    # Only log if it's not an expected empty chunk
                    if len(process_data) > 0:
                        logging.debug(f"Skipping chunk at position {actual_start}: {e}")
                    return {}
                
                # Process measurements with numeric values
                valid_measurements = df_chunk[df_chunk['value_as_number'].notna()]
                
                # Group by concept and update T-Digests
                for concept_id, group in valid_measurements.groupby('measurement_concept_id'):
                    if concept_id not in concept_digests:
                        concept_digests[concept_id] = TDigest(delta=0.01, K=25)
                    
                    # Add all values to the digest
                    for value in group['value_as_number'].values:
                        if not np.isnan(value) and np.isfinite(value):
                            concept_digests[concept_id].update(value)
    
    # Serialize T-Digest states
    result = {}
    for concept_id, digest in concept_digests.items():
        result[concept_id] = digest.to_dict()
    
    # Return results with CPU time
    cpu_time = time.time() - start_time
    return (result, cpu_time)


def merge_tdigest_states(digest_states: List[Dict], delta: float = 0.01, K: int = 25) -> Dict:
    """
    Merge multiple T-Digest states into final statistics.
    
    Args:
        digest_states: List of dictionaries containing T-Digest states
        delta: T-Digest delta parameter
        K: T-Digest K parameter
    
    Returns:
        Dictionary mapping concept_id to statistics
    """
    merged_stats = {}
    
    # Collect all concept IDs
    all_concepts = set()
    for state in digest_states:
        all_concepts.update(state.keys())
    
    # Merge T-Digests for each concept
    for concept_id in all_concepts:
        merged_digest = TDigest(delta=delta, K=K)
        total_count = 0
        
        # Merge all digests for this concept
        for state in digest_states:
            if concept_id in state:
                concept_state = state[concept_id]
                # Merge using the centroids from the state
                for centroid in concept_state['centroids']:
                    # Each centroid has 'm' (mean) and 'c' (count)
                    merged_digest.update(centroid['m'], centroid['c'])
                total_count += concept_state['n']
        
        # Calculate percentiles
        if total_count > 0:
            percentiles = [1, 5, 25, 50, 75, 95, 99]
            percentile_values = {}
            for p in percentiles:
                try:
                    percentile_values[f'p{p}'] = merged_digest.percentile(p)
                except:
                    percentile_values[f'p{p}'] = None
            
            merged_stats[concept_id] = {
                'count': total_count,
                'percentiles': percentile_values,
                'min': merged_digest.percentile(0),
                'max': merged_digest.percentile(100)
            }
    
    return merged_stats


def split_file_for_parallel(filepath: str, num_workers: int = 6) -> List[Tuple]:
    """
    Split a file into chunks for parallel processing.
    
    Args:
        filepath: Path to the file to split
        num_workers: Number of parallel workers
    
    Returns:
        List of tuples (filepath, start_byte, end_byte, chunk_size)
    """
    file_path = Path(filepath)
    file_size = file_path.stat().st_size
    
    # Calculate chunk boundaries
    chunk_size = file_size // num_workers
    chunks = []
    
    for i in range(num_workers):
        start_byte = i * chunk_size
        end_byte = file_size if i == num_workers - 1 else (i + 1) * chunk_size
        
        # Use 50MB sub-chunks for memory efficiency
        sub_chunk_size = 50_000_000  # 50MB
        
        chunks.append((str(file_path), start_byte, end_byte, sub_chunk_size))
    
    return chunks
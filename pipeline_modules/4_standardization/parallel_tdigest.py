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
import sys

sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent.parent / 'utils'))

from unit_conversions import UNIT_ID_CONVERSIONS
from file_splitter import streaming_csv_reader


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
    
    # Calculate actual start position (skip partial line and header)
    with open(filepath, 'rb') as f:
        if start_byte > 0:
            f.seek(start_byte)
            f.readline()  # Skip partial line
            actual_start = f.tell()
        else:
            # First chunk: skip header line
            f.readline()
            actual_start = f.tell()
        
        # Read header from file start
        f.seek(0)
        header_line = f.readline().decode('utf-8').strip()
    
    import csv
    fieldnames = next(csv.reader([header_line]))
    
    # Helper function for unit conversion
    def convert_units(value, unit_concept_id, measurement_concept_id):
        """Convert units before adding to T-Digest."""
        if not value or not unit_concept_id:
            return value
        try:
            unit_id = int(float(unit_concept_id))
            if unit_id in UNIT_ID_CONVERSIONS:
                conversion = UNIT_ID_CONVERSIONS[unit_id]
                if measurement_concept_id in conversion['for_concepts']:
                    if 'factor' in conversion:
                        return value * conversion['factor']
                    elif 'formula' in conversion:
                        return conversion['formula'](value)
        except:
            pass
        return value
    
    # Stream rows line-by-line using shared utility (O(1) memory per row)
    try:
        for row in streaming_csv_reader(filepath, actual_start, end_byte, fieldnames):
            # Only process rows with numeric values
            value_str = row.get('value_as_number', '')
            if not value_str:
                continue
            
            try:
                value = float(value_str)
            except (ValueError, TypeError):
                continue
            
            if not np.isfinite(value):
                continue
            
            # Get concept ID (use int to ensure consistent key types across pipeline)
            try:
                concept_id = int(float(row.get('measurement_concept_id', '0')))
            except (ValueError, TypeError):
                continue
            
            # Initialize T-Digest for this concept if needed
            if concept_id not in concept_digests:
                concept_digests[concept_id] = TDigest(delta=0.01, K=25)
            
            # Convert units before adding to T-Digest
            unit_id_str = row.get('unit_concept_id', '')
            converted_value = convert_units(value, unit_id_str, concept_id)
            concept_digests[concept_id].update(converted_value)
    except Exception as e:
        logging.debug(f"Error streaming chunk at position {actual_start}: {e}")
    
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
#!/usr/bin/env python
"""Run concept mapping using processed SNOMED mappings for cleaned tables with deduplication."""

import csv
import sys
import json
import logging
import argparse
import numpy as np
import platform
import time
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Set CSV field size limit to maximum to avoid field size errors
csv.field_size_limit(sys.maxsize)

# Platform-specific settings for performance optimization
if platform.system() == 'Windows':
    PROGRESS_INTERVAL = 30.0  # Less frequent updates (reduce overhead)
    CHUNK_SIZE = 500000  # Larger chunks for Windows (better I/O performance)
    WRITE_BUFFER_SIZE = 50000  # Larger write buffer for Windows
    FILE_BUFFER_SIZE = 2 * 1024 * 1024  # 2MB file buffer
else:
    PROGRESS_INTERVAL = 10.0  # Default for macOS/Linux
    CHUNK_SIZE = 100000  # Default for macOS/Linux
    WRITE_BUFFER_SIZE = 20000  # Moderate buffer for Unix-like systems
    FILE_BUFFER_SIZE = 1024 * 1024  # 1MB file buffer

# Parallel processing configuration
MAX_WORKERS = min(multiprocessing.cpu_count(), 6)  # Use up to 6 cores
MEASUREMENT_SPLITS = 6  # Split MEASUREMENT table into 6 parts
LARGE_TABLE_THRESHOLD = 100000  # Tables larger than this will be considered for splitting

# Setup
base_dir = Path(__file__).parent
project_root = base_dir.parent.parent  # Go up to crisp_pipeline_code
input_dir = project_root / "output" / "2_cleaning"
output_dir = project_root / "output" / "3_mapping"
processed_mappings_dir = base_dir / "mapping_frequency" / "processed_mappings"

# Create output directories
output_dir.mkdir(parents=True, exist_ok=True)
removed_dir = output_dir / "removed_records"
removed_dir.mkdir(parents=True, exist_ok=True)

# Create subdirectories for different types of removed records
duplicates_dir = removed_dir / "duplicates"
duplicates_dir.mkdir(parents=True, exist_ok=True)
low_freq_dir = removed_dir / "low_frequency"
low_freq_dir.mkdir(parents=True, exist_ok=True)

# Setup logging
log_file = output_dir / f"mapping_process_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',  # No milliseconds
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Tables to map (only those with processed mappings)
TABLES_WITH_MAPPING = [
    'MEASUREMENT', 'OBSERVATION', 'PROCEDURE_OCCURRENCE', 'DEVICE_EXPOSURE'
]

# All tables to process (including those without SNOMED mapping but need filtering/standardization)
ALL_TABLES = [
    'MEASUREMENT', 'OBSERVATION', 'PROCEDURE_OCCURRENCE', 'DEVICE_EXPOSURE',
    'CONDITION_OCCURRENCE', 'CONDITION_ERA', 'DRUG_EXPOSURE', 'DRUG_ERA', 
    'VISIT_OCCURRENCE'
]

# Define concept columns for each table
CONCEPT_COLUMNS = {
    'MEASUREMENT': ['measurement_concept_id'],
    'OBSERVATION': ['observation_concept_id'],
    'PROCEDURE_OCCURRENCE': ['procedure_concept_id'],
    'DEVICE_EXPOSURE': ['device_concept_id'],
    'CONDITION_OCCURRENCE': ['condition_concept_id'],
    'CONDITION_ERA': ['condition_concept_id'],
    'DRUG_EXPOSURE': ['drug_concept_id'],
    'DRUG_ERA': ['drug_concept_id'],
    'VISIT_OCCURRENCE': ['visit_concept_id']
}

# Define datetime columns for deduplication (only for tables with SNOMED mapping)
DATETIME_COLUMNS = {
    'MEASUREMENT': 'measurement_datetime',
    'OBSERVATION': 'observation_datetime',
    'PROCEDURE_OCCURRENCE': 'procedure_datetime',
    'DEVICE_EXPOSURE': 'device_exposure_start_datetime'
}

# Define ID columns for tracking
ID_COLUMNS = {
    'MEASUREMENT': 'measurement_id',
    'OBSERVATION': 'observation_id',
    'PROCEDURE_OCCURRENCE': 'procedure_occurrence_id',
    'DEVICE_EXPOSURE': 'device_exposure_id',
    'CONDITION_OCCURRENCE': 'condition_occurrence_id',
    'CONDITION_ERA': 'condition_era_id',
    'DRUG_EXPOSURE': 'drug_exposure_id',
    'DRUG_ERA': 'drug_era_id',
    'VISIT_OCCURRENCE': 'visit_occurrence_id'
}

# Standalone function for parallel frequency counting
def count_single_table_frequency(table_name, input_dir, concept_columns, min_freq):
    """Count concept frequencies for a single table - used for parallel processing."""
    import csv
    from collections import Counter
    from pathlib import Path
    
    input_dir = Path(input_dir) if not isinstance(input_dir, Path) else input_dir
    input_file = input_dir / f"{table_name}_cleaned.csv"
    
    if not input_file.exists():
        return table_name, {}, {'low_frequency': 0, 'high_frequency': 0}
    
    concept_counter = Counter()
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            for concept_col in concept_columns.get(table_name, []):
                concept_value = row.get(concept_col, '').strip()
                if concept_value and concept_value != '':
                    concept_counter[concept_value] += 1
    
    # Calculate statistics
    freq_stats = Counter()
    for count in concept_counter.values():
        if count <= min_freq:
            freq_stats['low_frequency'] += 1
        else:
            freq_stats['high_frequency'] += 1
    
    return table_name, dict(concept_counter), dict(freq_stats)

def count_measurement_partial_frequency(input_file, concept_col, start_row, end_row):
    """Count concept frequencies for a partial MEASUREMENT table - used for parallel processing."""
    import csv
    from collections import Counter
    
    concept_counter = Counter()
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < start_row:
                continue
            if end_row is not None and i >= end_row:
                break
            
            concept_value = row.get(concept_col, '').strip()
            if concept_value and concept_value != '':
                concept_counter[concept_value] += 1
    
    return dict(concept_counter)

# Standalone functions for parallel table processing
def process_single_table(table_name, input_dir, output_dir, concept_frequencies, 
                        processed_mappings_dir, enable_dedup, min_concept_freq):
    """Process a complete table - used for parallel processing of non-MEASUREMENT tables."""
    import csv
    import time
    import logging
    from pathlib import Path
    from collections import defaultdict
    
    # Initialize paths
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    processed_mappings_dir = Path(processed_mappings_dir)
    
    input_file = input_dir / f"{table_name}_cleaned.csv"
    output_file = output_dir / f"{table_name}_mapped.csv"
    
    if not input_file.exists():
        return table_name, {'error': f'Input file not found: {input_file}'}, {}
    
    # Initialize statistics
    stats = {
        'total_rows': 0,
        'low_freq_removed': 0,
        'duplicates_removed': 0,
        'concept_columns': {},
        'total_mappings_applied': 0
    }
    
    time_stats = {
        'total': 0,
        'file_reading': 0,
        'low_freq_filtering': 0,
        'concept_mapping': 0,
        'deduplication': 0,
        'file_writing': 0
    }
    
    start_time = time.time()
    
    # Load mappings if needed
    mappings = {}
    if table_name in TABLES_WITH_MAPPING:
        mapping_file = processed_mappings_dir / f"{table_name}_mapping_reference.csv"
        if mapping_file.exists():
            mappings = {}
            with open(mapping_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    source_id = row['original_concept_id']
                    target_id = row['snomed_concept_id']
                    mappings[source_id] = target_id
    
    # Get concept columns
    concept_cols = CONCEPT_COLUMNS.get(table_name, [])
    datetime_col = DATETIME_COLUMNS.get(table_name)
    
    # Process the table
    read_start = time.time()
    all_rows = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            all_rows.append(row)
    time_stats['file_reading'] = time.time() - read_start
    
    stats['total_rows'] = len(all_rows)
    
    # Filter low frequency concepts
    filter_start = time.time()
    if table_name in concept_frequencies:
        freq_map = concept_frequencies[table_name]
        filtered_rows = []
        removed_rows = []
        
        for idx, row in enumerate(all_rows, start=2):  # Start from 2 (1 for header, 2 for first data row)
            keep_row = True
            removal_reason = None
            for concept_col in concept_cols:
                concept_value = row.get(concept_col, '').strip()
                if concept_value and freq_map.get(concept_value, 0) <= min_concept_freq:
                    keep_row = False
                    freq = freq_map.get(concept_value, 0)
                    removal_reason = f"low_frequency_concept ({concept_col}={concept_value}, freq={freq})"
                    break
            
            if keep_row:
                filtered_rows.append(row)
            else:
                row_copy = row.copy()
                row_copy['removal_reason'] = removal_reason
                row_copy['original_row_number'] = idx
                removed_rows.append(row_copy)
        
        stats['low_freq_removed'] = len(removed_rows)
        
        # Save removed records
        if removed_rows:
            removed_file = output_dir / "removed_records" / "low_frequency" / f"{table_name}_low_freq.csv"
            removed_file.parent.mkdir(parents=True, exist_ok=True)
            extended_fieldnames = list(fieldnames) + ['removal_reason', 'original_row_number']
            with open(removed_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=extended_fieldnames)
                writer.writeheader()
                writer.writerows(removed_rows)
        
        all_rows = filtered_rows
    time_stats['low_freq_filtering'] = time.time() - filter_start
    
    # Apply mappings
    mapping_start = time.time()
    if mappings:
        for row in all_rows:
            for concept_col in concept_cols:
                original_value = row.get(concept_col, '').strip()
                if original_value in mappings:
                    row[concept_col] = mappings[original_value]  # Replace with mapped SNOMED ID
                    row[f'{concept_col}_mapped'] = 'Y'
                    stats['total_mappings_applied'] += 1
                else:
                    row[f'{concept_col}_mapped'] = 'N'
                
                # Track statistics
                if concept_col not in stats['concept_columns']:
                    stats['concept_columns'][concept_col] = {
                        'total_non_null': 0,
                        'mapped': 0,
                        'unique_mapped': set()
                    }
                
                if original_value:
                    stats['concept_columns'][concept_col]['total_non_null'] += 1
                    if original_value in mappings:
                        stats['concept_columns'][concept_col]['mapped'] += 1
                        stats['concept_columns'][concept_col]['unique_mapped'].add(mappings[original_value])
    time_stats['concept_mapping'] = time.time() - mapping_start
    
    # Deduplication
    dedup_start = time.time()
    if enable_dedup and table_name in TABLES_WITH_MAPPING:
        seen_keys = {}
        unique_rows = []
        duplicate_records = []
        group_counter = 0
        
        for idx, row in enumerate(all_rows):
            person_id = row.get('person_id', '')
            # Use mapped concept for deduplication
            concept_col = concept_cols[0] if concept_cols else None
            if concept_col:
                original_value = row.get(concept_col, '')
                if original_value in mappings:
                    concept_id = mappings[original_value]
                else:
                    concept_id = original_value
            else:
                concept_id = ''
            datetime_val = row.get(datetime_col, '') if datetime_col else ''
            
            dedup_key = f"{person_id}|{concept_id}|{datetime_val}"
            
            if dedup_key not in seen_keys:
                seen_keys[dedup_key] = {'idx': idx, 'group_id': None}
                unique_rows.append(row)
            else:
                # First duplicate for this key - create group and add kept record
                if seen_keys[dedup_key]['group_id'] is None:
                    group_counter += 1
                    group_id = f"{table_name}_{group_counter}"
                    seen_keys[dedup_key]['group_id'] = group_id
                    
                    # Add kept record
                    kept_idx = seen_keys[dedup_key]['idx']
                    kept_row = all_rows[kept_idx].copy()
                    kept_row['duplicate_status'] = 'kept'
                    kept_row['duplicate_group_id'] = group_id
                    kept_row['original_row_number'] = kept_idx + 2
                    duplicate_records.append(kept_row)
                
                # Add removed record
                dup_row = row.copy()
                dup_row['duplicate_status'] = 'removed'
                dup_row['duplicate_group_id'] = seen_keys[dedup_key]['group_id']
                dup_row['original_row_number'] = idx + 2
                duplicate_records.append(dup_row)
        
        # Count only removed duplicates for stats
        stats['duplicates_removed'] = sum(1 for r in duplicate_records if r.get('duplicate_status') == 'removed')
        
        all_rows = unique_rows
    
    # Always create duplicate file for mapping tables
    if table_name in TABLES_WITH_MAPPING:
        dup_file = output_dir / "removed_records" / "duplicates" / f"{table_name}.csv"
        dup_file.parent.mkdir(parents=True, exist_ok=True)
        
        if duplicate_records:
            dup_fieldnames = list(duplicate_records[0].keys())
        else:
            # Create empty file with header
            dup_fieldnames = list(fieldnames) + [f'{concept_cols[0]}_mapped'] if concept_cols else fieldnames
            dup_fieldnames = dup_fieldnames + ['duplicate_status', 'duplicate_group_id', 'original_row_number']
        
        with open(dup_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=dup_fieldnames)
            writer.writeheader()
            if duplicate_records:
                writer.writerows(duplicate_records)
    time_stats['deduplication'] = time.time() - dedup_start
    
    # Write output
    write_start = time.time()
    extended_fieldnames = fieldnames.copy()
    if table_name in TABLES_WITH_MAPPING:
        for col in concept_cols:
            if f'{col}_mapped' not in extended_fieldnames:
                extended_fieldnames.append(f'{col}_mapped')
    
    with open(output_file, 'w', newline='', encoding='utf-8', buffering=FILE_BUFFER_SIZE) as f:
        writer = csv.DictWriter(f, fieldnames=extended_fieldnames)
        writer.writeheader()
        
        # Write in buffers
        buffer = []
        for row in all_rows:
            buffer.append(row)
            if len(buffer) >= WRITE_BUFFER_SIZE:
                writer.writerows(buffer)
                buffer = []
        
        if buffer:
            writer.writerows(buffer)
    
    time_stats['file_writing'] = time.time() - write_start
    time_stats['total'] = time.time() - start_time
    
    # Convert sets to counts for JSON serialization
    for col_stats in stats['concept_columns'].values():
        col_stats['unique_mapped'] = len(col_stats['unique_mapped'])
    
    return table_name, stats, time_stats

def process_measurement_chunk(chunk_id, total_chunks, input_file, output_dir,
                             concept_frequencies, mappings, enable_dedup, min_concept_freq):
    """Process a chunk of MEASUREMENT table - used for parallel processing."""
    import csv
    import time
    from pathlib import Path
    from collections import defaultdict
    
    input_file = Path(input_file)
    output_dir = Path(output_dir)
    
    # Calculate row range
    with open(input_file, 'r') as f:
        total_rows = sum(1 for _ in f) - 1  # Subtract header
    
    chunk_size = total_rows // total_chunks
    start_row = chunk_id * chunk_size
    end_row = (chunk_id + 1) * chunk_size if chunk_id < total_chunks - 1 else total_rows
    
    # Initialize statistics
    stats = {
        'rows_processed': 0,
        'low_freq_removed': 0,
        'duplicates_removed': 0,
        'mappings_applied': 0
    }
    
    # Read and process chunk
    processed_rows = []
    removed_low_freq = []
    duplicate_records = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for i, row in enumerate(reader):
            if i < start_row:
                continue
            if i >= end_row:
                break
            
            # Filter low frequency
            concept_value = row.get('measurement_concept_id', '').strip()
            if concept_value and concept_frequencies.get('MEASUREMENT', {}).get(concept_value, 0) <= min_concept_freq:
                freq = concept_frequencies.get('MEASUREMENT', {}).get(concept_value, 0)
                row_copy = row.copy()
                row_copy['removal_reason'] = f"low_frequency_concept (measurement_concept_id={concept_value}, freq={freq})"
                row_copy['original_row_number'] = i + 2  # +2 for header and 1-based indexing
                removed_low_freq.append(row_copy)
                stats['low_freq_removed'] += 1
                continue
            
            # Apply mapping
            if concept_value in mappings:
                row['measurement_concept_id'] = mappings[concept_value]  # Replace with mapped SNOMED ID
                row['measurement_concept_id_mapped'] = 'Y'
                stats['mappings_applied'] += 1
            else:
                row['measurement_concept_id_mapped'] = 'N'
            
            processed_rows.append(row)
            stats['rows_processed'] += 1
    
    # Simple deduplication within chunk
    if enable_dedup:
        seen_keys = {}
        unique_rows = []
        group_counter = 0
        
        for idx, row in enumerate(processed_rows):
            person_id = row.get('person_id', '')
            # Use mapped concept for deduplication
            original_value = row.get('measurement_concept_id', '')
            if original_value in mappings:
                concept_id = mappings[original_value]
            else:
                concept_id = original_value
            datetime_val = row.get('measurement_datetime', '')
            
            dedup_key = f"{person_id}|{concept_id}|{datetime_val}"
            
            if dedup_key not in seen_keys:
                seen_keys[dedup_key] = {'idx': idx, 'group_id': None, 'row': row.copy()}
                unique_rows.append(row)
            else:
                # First duplicate for this key - create group and add kept record
                if seen_keys[dedup_key]['group_id'] is None:
                    group_counter += 1
                    group_id = f"MEASUREMENT_CHUNK{chunk_id}_{group_counter}"
                    seen_keys[dedup_key]['group_id'] = group_id
                    
                    # Add kept record
                    kept_row = seen_keys[dedup_key]['row'].copy()
                    kept_row['duplicate_status'] = 'kept'
                    kept_row['duplicate_group_id'] = group_id
                    kept_row['original_row_number'] = seen_keys[dedup_key]['idx'] + start_row + 2
                    duplicate_records.append(kept_row)
                
                # Add removed record
                dup_row = row.copy()
                dup_row['duplicate_status'] = 'removed'
                dup_row['duplicate_group_id'] = seen_keys[dedup_key]['group_id']
                dup_row['original_row_number'] = idx + start_row + 2
                duplicate_records.append(dup_row)
                stats['duplicates_removed'] += 1
        
        processed_rows = unique_rows
    
    # Save chunk results temporarily
    chunk_file = output_dir / f".temp_measurement_chunk_{chunk_id}.csv"
    extended_fieldnames = list(fieldnames) + ['measurement_concept_id_mapped']
    dup_fieldnames = extended_fieldnames + ['duplicate_status', 'duplicate_group_id', 'original_row_number']
    
    with open(chunk_file, 'w', newline='', encoding='utf-8', buffering=FILE_BUFFER_SIZE) as f:
        writer = csv.DictWriter(f, fieldnames=extended_fieldnames)
        writer.writeheader()  # Always write header for each chunk
        writer.writerows(processed_rows)
    
    # Save removed records for this chunk
    if removed_low_freq:
        removed_file = output_dir / "removed_records" / "low_frequency" / f".temp_MEASUREMENT_low_freq_chunk_{chunk_id}.csv"
        removed_file.parent.mkdir(parents=True, exist_ok=True)
        low_freq_fieldnames = list(fieldnames) + ['removal_reason', 'original_row_number']
        with open(removed_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=low_freq_fieldnames)
            writer.writeheader()  # Always write header
            writer.writerows(removed_low_freq)
    
    if duplicate_records:
        dup_file = output_dir / "removed_records" / "duplicates" / f".temp_MEASUREMENT_duplicates_chunk_{chunk_id}.csv"
        dup_file.parent.mkdir(parents=True, exist_ok=True)
        with open(dup_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=dup_fieldnames)
            writer.writeheader()  # Always write header
            writer.writerows(duplicate_records)
    
    return chunk_id, stats

def merge_measurement_chunks(output_dir, total_chunks):
    """Merge MEASUREMENT chunk results into final files."""
    import csv
    from pathlib import Path
    
    output_dir = Path(output_dir)
    
    # Merge main output file
    output_file = output_dir / "MEASUREMENT_mapped.csv"
    with open(output_file, 'w', newline='', encoding='utf-8', buffering=FILE_BUFFER_SIZE) as outf:
        writer = None
        
        for chunk_id in range(total_chunks):
            chunk_file = output_dir / f".temp_measurement_chunk_{chunk_id}.csv"
            if chunk_file.exists():
                with open(chunk_file, 'r', encoding='utf-8') as inf:
                    reader = csv.DictReader(inf)
                    # Get fieldnames from the first file
                    if writer is None and reader.fieldnames:
                        writer = csv.DictWriter(outf, fieldnames=reader.fieldnames)
                        writer.writeheader()
                    # Write rows
                    if writer is not None:
                        for row in reader:
                            writer.writerow(row)
                chunk_file.unlink()  # Delete temp file
    
    # Merge low frequency removed records
    low_freq_file = output_dir / "removed_records" / "low_frequency" / "MEASUREMENT_low_freq.csv"
    low_freq_file.parent.mkdir(parents=True, exist_ok=True)
    with open(low_freq_file, 'w', newline='', encoding='utf-8') as outf:
        writer = None
        
        for chunk_id in range(total_chunks):
            chunk_file = output_dir / "removed_records" / "low_frequency" / f".temp_MEASUREMENT_low_freq_chunk_{chunk_id}.csv"
            if chunk_file.exists():
                with open(chunk_file, 'r', encoding='utf-8') as inf:
                    reader = csv.DictReader(inf)
                    if writer is None and reader.fieldnames:
                        writer = csv.DictWriter(outf, fieldnames=reader.fieldnames)
                        writer.writeheader()
                    if writer is not None:
                        for row in reader:
                            writer.writerow(row)
                chunk_file.unlink()
    
    # Merge duplicate records
    dup_file = output_dir / "removed_records" / "duplicates" / "MEASUREMENT.csv"
    dup_file.parent.mkdir(parents=True, exist_ok=True)
    has_content = False
    
    # First check if any duplicate files exist
    for chunk_id in range(total_chunks):
        chunk_file = output_dir / "removed_records" / "duplicates" / f".temp_MEASUREMENT_duplicates_chunk_{chunk_id}.csv"
        if chunk_file.exists():
            has_content = True
            break
    
    if has_content:
        with open(dup_file, 'w', newline='', encoding='utf-8') as outf:
            writer = None
            
            for chunk_id in range(total_chunks):
                chunk_file = output_dir / "removed_records" / "duplicates" / f".temp_MEASUREMENT_duplicates_chunk_{chunk_id}.csv"
                if chunk_file.exists():
                    with open(chunk_file, 'r', encoding='utf-8') as inf:
                        reader = csv.DictReader(inf)
                        if writer is None and reader.fieldnames:
                            writer = csv.DictWriter(outf, fieldnames=reader.fieldnames)
                            writer.writeheader()
                        if writer is not None:
                            for row in reader:
                                writer.writerow(row)
                    chunk_file.unlink()

class ConceptMapper:
    def __init__(self, enable_dedup=True, min_concept_freq=10):
        self.stats = defaultdict(lambda: defaultdict(int))
        self.enable_dedup = enable_dedup
        self.min_concept_freq = min_concept_freq
        self.concept_frequencies = {}  # Store concept frequencies across all tables
        self.all_table_data = {}  # Store all table data for processing
        
    def count_concept_frequencies(self):
        """Count concept frequencies across all tables using parallel processing."""
        logging.info("="*60)
        logging.info("Counting concept frequencies across all tables (parallel)...")
        
        # Use ProcessPoolExecutor for parallel counting
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            futures = []
            future_info = {}  # Track what each future is for
            
            for table_name in ALL_TABLES:
                if table_name == 'MEASUREMENT':
                    # Special handling for MEASUREMENT - split into chunks
                    measurement_file = input_dir / "MEASUREMENT_cleaned.csv"
                    if measurement_file.exists():
                        # First get row count
                        row_count = 0
                        with open(measurement_file, 'r', encoding='utf-8') as f:
                            for _ in f:
                                row_count += 1
                        row_count -= 1  # Subtract header
                        
                        # Calculate chunk size
                        chunk_size = row_count // MEASUREMENT_SPLITS + 1
                        
                        # Submit partial tasks
                        for i in range(MEASUREMENT_SPLITS):
                            start_row = i * chunk_size
                            end_row = (i + 1) * chunk_size if i < MEASUREMENT_SPLITS - 1 else None
                            
                            future = executor.submit(
                                count_measurement_partial_frequency,
                                measurement_file, 
                                'measurement_concept_id',
                                start_row, 
                                end_row
                            )
                            futures.append(future)
                            future_info[future] = ('MEASUREMENT_PARTIAL', i)
                else:
                    # Regular table processing
                    future = executor.submit(
                        count_single_table_frequency,
                        table_name, input_dir, CONCEPT_COLUMNS, self.min_concept_freq
                    )
                    futures.append(future)
                    future_info[future] = ('TABLE', table_name)
            
            # Collect results
            measurement_partials = []
            
            for future in as_completed(futures):
                info_type, info_data = future_info[future]
                
                if info_type == 'MEASUREMENT_PARTIAL':
                    # Collect partial MEASUREMENT results
                    partial_freq = future.result()
                    measurement_partials.append(partial_freq)
                    logging.info(f"  MEASUREMENT chunk {info_data + 1}/{MEASUREMENT_SPLITS} processed")
                else:
                    # Regular table result
                    table_name, frequencies, stats = future.result()
                    if frequencies:
                        self.concept_frequencies[table_name] = frequencies
                        logging.info(f"  {table_name}: Low freq (<={self.min_concept_freq}): {stats.get('low_frequency', 0)}, "
                                   f"High freq (>{self.min_concept_freq}): {stats.get('high_frequency', 0)}")
            
            # Merge MEASUREMENT partials if available
            if measurement_partials:
                from collections import Counter
                merged_counter = Counter()
                for partial in measurement_partials:
                    for concept, count in partial.items():
                        merged_counter[concept] += count
                
                self.concept_frequencies['MEASUREMENT'] = dict(merged_counter)
                
                # Calculate statistics for MEASUREMENT
                freq_stats = {'low_frequency': 0, 'high_frequency': 0}
                for count in merged_counter.values():
                    if count <= self.min_concept_freq:
                        freq_stats['low_frequency'] += 1
                    else:
                        freq_stats['high_frequency'] += 1
                
                logging.info(f"  MEASUREMENT (merged): Low freq (<={self.min_concept_freq}): {freq_stats['low_frequency']}, "
                           f"High freq (>{self.min_concept_freq}): {freq_stats['high_frequency']}")
    
    def filter_low_frequency_concepts(self, rows, table_name):
        """Filter out records with low frequency concepts."""
        if table_name not in self.concept_frequencies:
            return rows, []
        
        concept_freq = self.concept_frequencies[table_name]
        concept_cols = CONCEPT_COLUMNS.get(table_name, [])
        
        filtered_rows = []
        removed_rows = []
        
        for idx, row in enumerate(rows):
            keep_row = True
            removal_reason = None
            
            # Check each concept column
            for concept_col in concept_cols:
                concept_value = row.get(concept_col, '').strip()
                if concept_value and concept_freq.get(concept_value, 0) <= self.min_concept_freq:
                    keep_row = False
                    removal_reason = f'low_frequency_concept ({concept_col}={concept_value}, freq={concept_freq.get(concept_value, 0)})'
                    break
            
            if keep_row:
                filtered_rows.append(row)
            else:
                removed_row = row.copy()
                removed_row['removal_reason'] = removal_reason
                removed_row['original_row_number'] = idx + 2  # +2 for header and 1-based indexing
                removed_rows.append(removed_row)
        
        return filtered_rows, removed_rows
    
    def load_processed_mappings(self, table_name):
        """Load processed mappings from reference file."""
        mapping_file = processed_mappings_dir / f"{table_name}_mapping_reference.csv"
        
        if not mapping_file.exists():
            logging.warning(f"No mapping reference file found for {table_name}")
            return {}
        
        mappings = {}
        with open(mapping_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                original_id = row['original_concept_id']
                mappings[original_id] = {
                    'snomed_concept_id': row['snomed_concept_id'],
                    'mapping_relationship': row['mapping_relationship']
                }
        
        logging.info(f"Loaded {len(mappings)} mappings for {table_name}")
        return mappings
    
    def deduplicate_mapped_data(self, rows, table_name, fieldnames):
        """Optimized deduplication using set for seen keys, with full duplicate tracking."""
        if not self.enable_dedup:
            return rows, []
        
        concept_col = CONCEPT_COLUMNS[table_name][0]
        datetime_col = DATETIME_COLUMNS[table_name]
        
        unique_rows = []
        duplicate_records = []
        group_counter = 0
        
        # Special handling for MEASUREMENT table to prioritize records with value_as_number
        if table_name == 'MEASUREMENT':
            # Track all duplicate groups
            key_records = {}  # key -> list of records with metadata
            
            for idx, row in enumerate(rows):
                person_id = row.get('person_id', '')
                concept_id = row.get(concept_col, '')
                datetime_val = row.get(datetime_col, '')
                
                dedup_key = f"{person_id}|{concept_id}|{datetime_val}"
                
                record_info = {
                    'row': row,
                    'idx': idx,
                    'has_value': bool(row.get('value_as_number') and 
                                    str(row.get('value_as_number')).strip())
                }
                
                if dedup_key not in key_records:
                    key_records[dedup_key] = []
                key_records[dedup_key].append(record_info)
            
            # Process each key group
            for dedup_key, records in key_records.items():
                if len(records) == 1:
                    # No duplicates - just keep the single row
                    unique_rows.append(records[0]['row'])
                else:
                    # Multiple records - create duplicate group
                    group_counter += 1
                    group_id = f"{table_name}_{group_counter}"
                    
                    # Sort to prioritize records with value_as_number
                    sorted_records = sorted(records, key=lambda x: (
                        not x['has_value'],  # True (has value) sorts before False
                        x['idx']  # Then by original order
                    ))
                    
                    # First record is kept
                    for i, record_info in enumerate(sorted_records):
                        dup_row = record_info['row'].copy()
                        dup_row['duplicate_group_id'] = group_id
                        dup_row['original_row_number'] = record_info['idx'] + 2
                        
                        if i == 0:
                            dup_row['duplicate_status'] = 'kept'
                            unique_rows.append(record_info['row'])
                        else:
                            dup_row['duplicate_status'] = 'removed'
                        
                        duplicate_records.append(dup_row)
                    
        else:
            # For other tables, use simple first-occurrence logic with group tracking
            seen_keys = {}  # key -> first occurrence info
            
            for idx, row in enumerate(rows):
                person_id = row.get('person_id', '')
                concept_id = row.get(concept_col, '')
                datetime_val = row.get(datetime_col, '')
                
                dedup_key = f"{person_id}|{concept_id}|{datetime_val}"
                
                if dedup_key not in seen_keys:
                    # First occurrence - keep it
                    seen_keys[dedup_key] = {
                        'idx': idx,
                        'group_id': None
                    }
                    unique_rows.append(row)
                else:
                    # Duplicate found
                    if seen_keys[dedup_key]['group_id'] is None:
                        # First duplicate for this key - create group
                        group_counter += 1
                        group_id = f"{table_name}_{group_counter}"
                        seen_keys[dedup_key]['group_id'] = group_id
                        
                        # Add the kept record to duplicate_records
                        first_idx = seen_keys[dedup_key]['idx']
                        kept_row = rows[first_idx].copy()
                        kept_row['duplicate_status'] = 'kept'
                        kept_row['duplicate_group_id'] = group_id
                        kept_row['original_row_number'] = first_idx + 2
                        duplicate_records.append(kept_row)
                    
                    # Add the duplicate record
                    dup_row = row.copy()
                    dup_row['duplicate_status'] = 'removed'
                    dup_row['duplicate_group_id'] = seen_keys[dedup_key]['group_id']
                    dup_row['original_row_number'] = idx + 2
                    duplicate_records.append(dup_row)
        
        return unique_rows, duplicate_records
    
    
    def process_table(self, table_name):
        """Process a table with chunk-based reading for memory efficiency."""
        # Initialize timing
        table_time_stats = {
            'total': 0,
            'file_reading': 0,
            'low_freq_filtering': 0,
            'concept_mapping': 0,
            'deduplication': 0,
            'file_writing': 0
        }
        table_start_time = time.time()
        
        logging.info(f"\n{'='*60}")
        logging.info(f"Processing {table_name}")
        logging.info('='*60)
        
        # Determine input/output files
        input_file = input_dir / f"{table_name}_cleaned.csv"
        if not input_file.exists():
            logging.warning(f"Input file not found for {table_name}: {input_file}")
            return
        
        output_file = output_dir / f"{table_name}_mapped.csv"
        
        # Initialize statistics
        table_stats = {
            'total_rows': 0,
            'low_freq_removed': 0,
            'duplicates_removed': 0,
            'concept_columns': {},
            'total_mappings_applied': 0
        }
        
        # Get total row count for progress bar
        with open(input_file, 'r') as f:
            total_rows = sum(1 for _ in f) - 1  # Subtract header
        
        
        table_stats['total_rows'] = total_rows
        logging.info(f"Total rows to process: {total_rows:,}")
        
        # Load mappings if needed
        mappings = {}
        if table_name in TABLES_WITH_MAPPING:
            mappings = self.load_processed_mappings(table_name)
        
        # Get concept columns and fieldnames
        concept_cols = CONCEPT_COLUMNS.get(table_name, [])
        datetime_col = DATETIME_COLUMNS.get(table_name)
        
        # Read header to get fieldnames
        with open(input_file, 'r') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
        
        # Determine extended fieldnames
        if table_name in TABLES_WITH_MAPPING:
            extended_fieldnames = fieldnames + [f'{col}_mapped' for col in concept_cols]
        else:
            extended_fieldnames = fieldnames
        
        # Initialize tracking for deduplication and statistics
        seen_keys = {}  # For deduplication
        duplicate_records = []
        group_counter = 0
        # Only initialize col_stats for tables with mappings
        if table_name in TABLES_WITH_MAPPING:
            col_stats = {col: {
                'total_values': 0,
                'null_values': 0,
                'mapped_count': 0,
                'unique_concepts_mapped': set()
            } for col in concept_cols}
        else:
            col_stats = {}
        
        # Open output files with optimized buffering
        output_file_handle = open(output_file, 'w', newline='', buffering=FILE_BUFFER_SIZE)
        output_writer = csv.DictWriter(output_file_handle, fieldnames=extended_fieldnames)
        output_writer.writeheader()
        
        # Open removed records files
        low_freq_file = low_freq_dir / f"{table_name}.csv"
        low_freq_handle = open(low_freq_file, 'w', newline='', buffering=FILE_BUFFER_SIZE)
        low_freq_writer = csv.DictWriter(low_freq_handle, 
                                        fieldnames=fieldnames + ['removal_reason', 'original_row_number'])
        low_freq_writer.writeheader()
        
        # Only create duplicates file for tables with mapping (that have deduplication)
        if table_name in TABLES_WITH_MAPPING:
            duplicates_file = duplicates_dir / f"{table_name}.csv"
            dup_fieldnames = extended_fieldnames + ['duplicate_status', 'duplicate_group_id', 'original_row_number']
            dup_handle = open(duplicates_file, 'w', newline='', buffering=FILE_BUFFER_SIZE)
            dup_writer = csv.DictWriter(dup_handle, fieldnames=dup_fieldnames)
            dup_writer.writeheader()
        else:
            dup_handle = None
            dup_writer = None
        
        # Process file in chunks
        total_time_stats = {
            'file_reading': 0,
            'low_freq_filtering': 0,
            'concept_mapping': 0,
            'deduplication': 0,
            'file_writing': 0
        }
        
        write_buffer = []
        low_freq_buffer = []
        dup_buffer = []
        rows_written = 0
        
        try:
            with open(input_file, 'r') as infile:
                reader = csv.DictReader(infile)
                
                # Process with progress bar
                with tqdm(total=total_rows, desc=f"Processing {table_name}",
                         unit="rows", miniters=max(100, total_rows//100) if total_rows > 0 else 1,
                         mininterval=PROGRESS_INTERVAL, leave=False, ncols=100) as pbar:
                    
                    chunk = []
                    row_idx = 0
                    
                    for row in reader:
                        row_idx += 1
                        chunk.append((row_idx, row))
                        
                        # Process chunk when it reaches desired size or at end
                        if len(chunk) >= CHUNK_SIZE or row_idx == total_rows:
                            chunk_start_time = time.time()
                            
                            # Process each row in chunk
                            for original_row_num, row in chunk:
                                # Step 1: Low frequency filtering
                                t0 = time.time()
                                keep_row = True
                                removal_reason = None
                                
                                if self.min_concept_freq > 0:
                                    concept_freq = self.concept_frequencies.get(table_name, {})
                                    for concept_col in concept_cols:
                                        concept_value = row.get(concept_col, '').strip()
                                        if concept_value and concept_freq.get(concept_value, 0) <= self.min_concept_freq:
                                            keep_row = False
                                            removal_reason = f'low_frequency_concept ({concept_col}={concept_value}, freq={concept_freq.get(concept_value, 0)})'
                                            break
                                
                                if not keep_row:
                                    removed_row = row.copy()
                                    removed_row['removal_reason'] = removal_reason
                                    removed_row['original_row_number'] = original_row_num + 1  # +1 for header
                                    low_freq_buffer.append(removed_row)
                                    table_stats['low_freq_removed'] += 1
                                    
                                    if len(low_freq_buffer) >= WRITE_BUFFER_SIZE:
                                        low_freq_writer.writerows(low_freq_buffer)
                                        low_freq_buffer = []
                                    continue
                                
                                total_time_stats['low_freq_filtering'] += time.time() - t0
                                
                                # Step 2: Apply mappings
                                t0 = time.time()
                                if table_name in TABLES_WITH_MAPPING:
                                    for col in concept_cols:
                                        row[f'{col}_mapped'] = 'N'
                                        concept_value = row.get(col, '')
                                        
                                        if not concept_value or concept_value == '':
                                            col_stats[col]['null_values'] += 1
                                            continue
                                        
                                        col_stats[col]['total_values'] += 1
                                        
                                        if concept_value in mappings:
                                            original_value = concept_value
                                            row[col] = mappings[concept_value]['snomed_concept_id']
                                            row[f'{col}_mapped'] = 'Y'
                                            col_stats[col]['mapped_count'] += 1
                                            col_stats[col]['unique_concepts_mapped'].add(original_value)
                                            table_stats['total_mappings_applied'] += 1
                                
                                total_time_stats['concept_mapping'] += time.time() - t0
                                
                                # Step 3: Deduplication
                                t0 = time.time()
                                if self.enable_dedup and table_name in TABLES_WITH_MAPPING:
                                    person_id = row.get('person_id', '')
                                    concept_id = row.get(concept_cols[0], '')
                                    datetime_val = row.get(datetime_col, '')
                                    dedup_key = f"{person_id}|{concept_id}|{datetime_val}"
                                    
                                    if dedup_key not in seen_keys:
                                        # First occurrence - keep it
                                        seen_keys[dedup_key] = {
                                            'idx': original_row_num,
                                            'group_id': None,
                                            'row': row.copy(),  # Save the kept row for later
                                            'has_value': bool(row.get('value_as_number') and 
                                                            str(row.get('value_as_number')).strip()) if table_name == 'MEASUREMENT' else False
                                        }
                                        # Write to output
                                        write_buffer.append(row)
                                        rows_written += 1
                                    else:
                                        # Duplicate found
                                        if seen_keys[dedup_key]['group_id'] is None:
                                            # First duplicate for this key - create group
                                            group_counter += 1
                                            group_id = f"{table_name}_{group_counter}"
                                            seen_keys[dedup_key]['group_id'] = group_id
                                            
                                            # Add the kept record to duplicate_records
                                            kept_row = seen_keys[dedup_key]['row'].copy()
                                            kept_row['duplicate_status'] = 'kept'
                                            kept_row['duplicate_group_id'] = group_id
                                            kept_row['original_row_number'] = seen_keys[dedup_key]['idx'] + 1
                                            dup_buffer.append(kept_row)
                                        
                                        # Add the duplicate record
                                        dup_row = row.copy()
                                        dup_row['duplicate_status'] = 'removed'
                                        dup_row['duplicate_group_id'] = seen_keys[dedup_key]['group_id']
                                        dup_row['original_row_number'] = original_row_num + 1
                                        dup_buffer.append(dup_row)
                                        table_stats['duplicates_removed'] += 1
                                        
                                        if len(dup_buffer) >= WRITE_BUFFER_SIZE and dup_writer:
                                            dup_writer.writerows(dup_buffer)
                                            dup_buffer = []
                                else:
                                    # No deduplication - just write
                                    write_buffer.append(row)
                                    rows_written += 1
                                
                                total_time_stats['deduplication'] += time.time() - t0
                                
                                # Write buffer if full
                                t0 = time.time()
                                if len(write_buffer) >= WRITE_BUFFER_SIZE:
                                    output_writer.writerows(write_buffer)
                                    write_buffer = []
                                total_time_stats['file_writing'] += time.time() - t0
                            
                            # Update progress bar
                            pbar.update(len(chunk))
                            chunk = []
            
            # Write remaining buffers
            t0 = time.time()
            if write_buffer:
                output_writer.writerows(write_buffer)
            if low_freq_buffer:
                low_freq_writer.writerows(low_freq_buffer)
            if dup_buffer and dup_writer:
                dup_writer.writerows(dup_buffer)
            total_time_stats['file_writing'] += time.time() - t0
            
        finally:
            # Close all files
            output_file_handle.close()
            low_freq_handle.close()
            if dup_handle:
                dup_handle.close()
        
        # Update statistics
        table_stats['output_rows'] = rows_written
        # Only update concept column stats for tables with mappings
        if table_name in TABLES_WITH_MAPPING and col_stats:
            for col in concept_cols:
                if col in col_stats:
                    if col_stats[col]['total_values'] > 0:
                        col_stats[col]['mapping_rate'] = (col_stats[col]['mapped_count'] / col_stats[col]['total_values']) * 100
                    else:
                        col_stats[col]['mapping_rate'] = 0
                    col_stats[col]['unique_concepts_mapped'] = len(col_stats[col]['unique_concepts_mapped'])
            table_stats['concept_columns'] = col_stats
        else:
            table_stats['concept_columns'] = {}
        
        # Copy timing stats
        table_time_stats.update(total_time_stats)
        
        logging.info(f"\nSaved processed data to: {output_file}")
        logging.info(f"Final row count: {table_stats['output_rows']:,}")
        if table_stats['low_freq_removed'] > 0:
            logging.info(f"Low frequency records removed: {table_stats['low_freq_removed']:,}")
        if table_stats['duplicates_removed'] > 0:
            logging.info(f"Duplicates removed: {table_stats['duplicates_removed']:,}")
        
        # Log mapping statistics
        if table_name in TABLES_WITH_MAPPING and table_stats['concept_columns']:
            for col, stats in table_stats['concept_columns'].items():
                if stats['total_values'] > 0:
                    logging.info(f"\n{col} mapping:")
                    logging.info(f"  Total non-null values: {stats['total_values']:,}")
                    logging.info(f"  Mapped values: {stats['mapped_count']:,} ({stats['mapping_rate']:.1f}%)")
                    logging.info(f"  Unique concepts mapped: {stats['unique_concepts_mapped']:,}")
        
        # Calculate total time and store statistics
        table_time_stats['total'] = time.time() - table_start_time
        table_stats['time_stats'] = table_time_stats
        self.stats[table_name] = table_stats
        
        return table_stats
    
    def apply_mappings(self, rows, table_name, fieldnames, table_stats):
        """Apply SNOMED mappings to concept columns."""
        # Load mappings
        mappings = self.load_processed_mappings(table_name)
        
        # Get concept columns
        concept_cols = CONCEPT_COLUMNS.get(table_name, [])
        
        # Add mapping tracking columns
        extended_fieldnames = fieldnames + [f'{col}_mapped' for col in concept_cols]
        
        # Process each concept column
        for col in concept_cols:
            if col not in fieldnames:
                logging.warning(f"Column {col} not found in {table_name}")
                continue
            
            logging.info(f"\nMapping {col}...")
            
            col_stats = {
                'total_values': 0,
                'null_values': 0,
                'mapped_count': 0,
                'unique_concepts_mapped': 0
            }
            
            unique_mappings = set()
            
            # Process each row (without progress bar for cleaner output)
            for row in rows:
                # Add mapping indicator column
                row[f'{col}_mapped'] = 'N'
                
                # Get concept value
                concept_value = row.get(col, '')
                
                if not concept_value or concept_value == '':
                    col_stats['null_values'] += 1
                    continue
                
                col_stats['total_values'] += 1
                
                # Check if mapping exists
                if concept_value in mappings:
                    # Apply mapping
                    original_value = concept_value
                    row[col] = mappings[concept_value]['snomed_concept_id']
                    row[f'{col}_mapped'] = 'Y'
                    col_stats['mapped_count'] += 1
                    unique_mappings.add(original_value)
                    table_stats['total_mappings_applied'] += 1
            
            col_stats['unique_concepts_mapped'] = len(unique_mappings)
            
            # Calculate percentages
            if col_stats['total_values'] > 0:
                col_stats['mapping_rate'] = (col_stats['mapped_count'] / col_stats['total_values']) * 100
            else:
                col_stats['mapping_rate'] = 0
            
            table_stats['concept_columns'][col] = col_stats
            
            logging.info(f"  Total non-null values: {col_stats['total_values']:,}")
            logging.info(f"  Mapped values: {col_stats['mapped_count']:,} ({col_stats['mapping_rate']:.1f}%)")
            logging.info(f"  Unique concepts mapped: {col_stats['unique_concepts_mapped']:,}")
        
        return extended_fieldnames
    

def generate_mapping_report(stats, mapper):
    """Generate comprehensive mapping report."""
    report_path = output_dir / "mapping_report.md"
    stats_path = output_dir / "mapping_statistics.json"
    
    # Save detailed statistics
    with open(stats_path, 'w') as f:
        json.dump(stats, f, indent=2)
    
    # Generate markdown report
    with open(report_path, 'w') as f:
        f.write("# Comprehensive Data Processing Report\n\n")
        f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Pipeline Module**: 3_mapping (comprehensive processing)\n\n")
        
        f.write("## Processing Configuration\n\n")
        f.write(f"- **Low frequency threshold**: <={mapper.min_concept_freq}\n")
        f.write(f"- **Deduplication**: {'Enabled' if mapper.enable_dedup else 'Disabled'}\n\n")
        
        f.write("## Summary Statistics\n\n")
        
        total_low_freq = sum(t.get('low_freq_removed', 0) for t in stats.values())
        total_mappings = sum(t.get('total_mappings_applied', 0) for t in stats.values())
        total_duplicates = sum(t.get('duplicates_removed', 0) for t in stats.values())
        
        f.write(f"**Total low frequency records removed**: {total_low_freq:,}\n")
        f.write(f"**Total mappings applied**: {total_mappings:,}\n")
        f.write(f"**Total duplicates removed**: {total_duplicates:,}\n\n")
        
        f.write("| Table | Input Rows | Low Freq Removed | Mappings Applied | Duplicates Removed | Output Rows |\n")
        f.write("|-------|------------|------------------|------------------|-------------------|-------------|\n")
        
        for table, table_stats in stats.items():
            input_rows = table_stats['total_rows']
            output_rows = table_stats.get('output_rows', input_rows)
            low_freq = table_stats.get('low_freq_removed', 0)
            mappings = table_stats.get('total_mappings_applied', 0)
            duplicates = table_stats.get('duplicates_removed', 0)
            
            f.write(f"| {table} | {input_rows:,} | {low_freq:,} | {mappings:,} | {duplicates:,} | {output_rows:,} |\n")
        
        f.write("\n## Detailed Column Statistics\n\n")
        
        for table, table_stats in stats.items():
            f.write(f"### {table}\n\n")
            
            # Check if this table has SNOMED mappings
            if table not in TABLES_WITH_MAPPING:
                f.write("No SNOMED mapping applied (filtering only).\n\n")
                continue
                
            if not table_stats['concept_columns']:
                f.write("No concept columns processed.\n\n")
                continue
            
            f.write("| Column | Total Values | Mapped | Mapping Rate | Unique Concepts Mapped |\n")
            f.write("|--------|--------------|---------|--------------|------------------------|\n")
            
            for col, col_stats in table_stats['concept_columns'].items():
                # Handle different stats structures
                if isinstance(col_stats, dict):
                    total_vals = col_stats.get('total_values', col_stats.get('total_non_null', 0))
                    mapped = col_stats.get('mapped_count', col_stats.get('mapped', 0))
                    rate = col_stats.get('mapping_rate', 0)
                    if rate == 0 and total_vals > 0:
                        rate = (mapped / total_vals * 100)
                    unique = col_stats.get('unique_concepts_mapped', col_stats.get('unique_mapped', 0))
                    
                    f.write(f"| {col} | {total_vals:,} | ")
                    f.write(f"{mapped:,} | ")
                    f.write(f"{rate:.1f}% | ")
                    f.write(f"{unique:,} |\n")
            
            if table_stats.get('duplicates_removed', 0) > 0:
                dup_rate = (table_stats['duplicates_removed'] / table_stats['total_rows']) * 100
                f.write(f"\n**Duplicates removed**: {table_stats['duplicates_removed']:,} ({dup_rate:.2f}%)\n")
            
            f.write("\n")
        
        f.write("## Notes\n\n")
        f.write("- **Low frequency filtering**: Removes records with concept frequency <= threshold\n")
        f.write("- **SNOMED mapping**: Applied to MEASUREMENT, OBSERVATION, PROCEDURE_OCCURRENCE, DEVICE_EXPOSURE\n")
        f.write("- **Deduplication**: Based on person_id + mapped_concept_id + datetime\n")
        f.write("- All removed records are saved for traceability in the removed_records subdirectories\n")
    
    logging.info(f"Report saved to: {report_path}")

def main():
    """Main execution function with parallel processing."""
    # Start timing
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Comprehensive data processing with concept mapping')
    parser.add_argument('--no-dedup', action='store_true', 
                        help='Disable deduplication after mapping')
    parser.add_argument('--min-concept-freq', type=int, default=10,
                        help='Minimum concept frequency threshold (default: 10)')
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("COMPREHENSIVE DATA PROCESSING MODULE (PARALLEL)")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nConfiguration:")
    print(f"  Input directory: {input_dir}")
    print(f"  Output directory: {output_dir}")
    print(f"  Processed mappings: {processed_mappings_dir}")
    print(f"  Parallel workers: {MAX_WORKERS}")
    print(f"\nFeatures enabled:")
    print(f"  Low frequency filtering: <={args.min_concept_freq}")
    print(f"  Deduplication: {'DISABLED' if args.no_dedup else 'ENABLED'}")
    print(f"\nTables to process: {', '.join(ALL_TABLES)}")
    print("="*80 + "\n")
    
    # Initialize mapper for frequency counting
    mapper = ConceptMapper(
        enable_dedup=not args.no_dedup,
        min_concept_freq=args.min_concept_freq
    )
    
    # Step 1: Count concept frequencies across all tables (already parallel)
    freq_count_start = time.time()
    mapper.count_concept_frequencies()
    freq_count_time = time.time() - freq_count_start
    
    # Step 2: Parallel table processing
    parallel_start = time.time()
    
    # Load MEASUREMENT mappings for chunks
    measurement_mappings = {}
    if 'MEASUREMENT' in TABLES_WITH_MAPPING:
        mapping_file = processed_mappings_dir / "MEASUREMENT_mapping_reference.csv"
        if mapping_file.exists():
            with open(mapping_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    measurement_mappings[row['original_concept_id']] = row['snomed_concept_id']
            logging.info(f"Loaded {len(measurement_mappings)} mappings for MEASUREMENT")
    
    # Process tables in parallel
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        
        # Submit MEASUREMENT chunks
        measurement_file = input_dir / "MEASUREMENT_cleaned.csv"
        if measurement_file.exists():
            logging.info("Submitting MEASUREMENT chunks for parallel processing...")
            for i in range(MEASUREMENT_SPLITS):
                future = executor.submit(
                    process_measurement_chunk,
                    chunk_id=i,
                    total_chunks=MEASUREMENT_SPLITS,
                    input_file=measurement_file,
                    output_dir=output_dir,
                    concept_frequencies=mapper.concept_frequencies,
                    mappings=measurement_mappings,
                    enable_dedup=not args.no_dedup,
                    min_concept_freq=args.min_concept_freq
                )
                futures.append(('MEASUREMENT_CHUNK', i, future))
        
        # Submit other tables
        other_tables = [t for t in ALL_TABLES if t != 'MEASUREMENT']
        logging.info(f"Submitting {len(other_tables)} other tables for parallel processing...")
        for table in other_tables:
            future = executor.submit(
                process_single_table,
                table_name=table,
                input_dir=input_dir,
                output_dir=output_dir,
                concept_frequencies=mapper.concept_frequencies,
                processed_mappings_dir=processed_mappings_dir,
                enable_dedup=not args.no_dedup,
                min_concept_freq=args.min_concept_freq
            )
            futures.append(('TABLE', table, future))
        
        # Collect results
        measurement_stats = {
            'total_rows': 0,
            'low_freq_removed': 0,
            'duplicates_removed': 0,
            'total_mappings_applied': 0,
            'concept_columns': {},
            'time_stats': {'total': 0}
        }
        
        # Get total MEASUREMENT rows from file
        if measurement_file.exists():
            with open(measurement_file, 'r') as f:
                measurement_total_rows = sum(1 for _ in f) - 1
            measurement_stats['total_rows'] = measurement_total_rows
        
        for task_type, info, future in tqdm(futures, desc="Processing tables"):
            try:
                if task_type == 'MEASUREMENT_CHUNK':
                    chunk_id, stats = future.result()
                    # Aggregate MEASUREMENT stats (don't add rows_processed to total_rows)
                    measurement_stats['low_freq_removed'] += stats['low_freq_removed']
                    measurement_stats['duplicates_removed'] += stats['duplicates_removed']
                    measurement_stats['total_mappings_applied'] += stats['mappings_applied']
                    logging.info(f"MEASUREMENT chunk {chunk_id + 1}/{MEASUREMENT_SPLITS} completed")
                else:
                    table_name, stats, time_stats = future.result()
                    if 'error' not in stats:
                        mapper.stats[table_name] = stats
                        mapper.stats[table_name]['time_stats'] = time_stats
                        mapper.stats[table_name]['output_rows'] = stats['total_rows'] - stats.get('low_freq_removed', 0) - stats.get('duplicates_removed', 0)
                        logging.info(f"{table_name} completed: {stats['total_rows']} rows processed")
                    else:
                        logging.error(f"{table_name}: {stats['error']}")
            except Exception as e:
                logging.error(f"Error processing {task_type} {info}: {str(e)}")
                import traceback
                traceback.print_exc()
    
    # Merge MEASUREMENT chunks
    if measurement_file.exists():
        logging.info("Merging MEASUREMENT chunks...")
        merge_measurement_chunks(output_dir, MEASUREMENT_SPLITS)
        
        # Add MEASUREMENT stats
        measurement_stats['output_rows'] = measurement_stats['total_rows'] - measurement_stats['low_freq_removed'] - measurement_stats['duplicates_removed']
        
        # Calculate mapping statistics for MEASUREMENT
        if measurement_mappings:
            total_mapped = measurement_stats['total_mappings_applied']
            # Calculate actual non-empty values (total - low_freq)
            total_values = measurement_stats['total_rows'] - measurement_stats['low_freq_removed']
            measurement_stats['concept_columns'] = {
                'measurement_concept_id': {
                    'total_values': total_values,
                    'mapped_count': total_mapped,
                    'mapping_rate': (total_mapped / total_values * 100) if total_values > 0 else 0,
                    'unique_concepts_mapped': len(set(measurement_mappings.values()))
                }
            }
        
        mapper.stats['MEASUREMENT'] = measurement_stats
    
    parallel_time = time.time() - parallel_start
    
    # Generate summary report
    generate_mapping_report(mapper.stats, mapper)
    
    # Calculate total execution time
    total_time = time.time() - start_time
    
    print("\n" + "="*80)
    print("PROCESSING COMPLETED SUCCESSFULLY")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"\nOutputs saved to: {output_dir}")
    print(f"Removed records saved to: {removed_dir}")
    print("="*80)
    
    # Performance breakdown
    print("\n" + "="*50)
    print("PERFORMANCE BREAKDOWN - Parallel Concept Mapping")
    print("="*50)
    
    print(f"Frequency counting:    {freq_count_time:.2f}s ({freq_count_time/total_time*100:.1f}%)")
    print(f"Parallel processing:   {parallel_time:.2f}s ({parallel_time/total_time*100:.1f}%)")
    
    # Calculate other operations
    other_time = total_time - freq_count_time - parallel_time
    if other_time > 0.1:
        print(f"Other operations:      {other_time:.2f}s ({other_time/total_time*100:.1f}%)")
    
    print(f"\nTables processed in parallel: {len(ALL_TABLES)}")
    print(f"MEASUREMENT chunks: {MEASUREMENT_SPLITS}")

if __name__ == "__main__":
    main()
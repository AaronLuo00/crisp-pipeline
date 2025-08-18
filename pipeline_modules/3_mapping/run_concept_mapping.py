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
episodes_dir = removed_dir / "visit_episodes"
episodes_dir.mkdir(parents=True, exist_ok=True)

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

# Define datetime columns for deduplication
DATETIME_COLUMNS = {
    'MEASUREMENT': 'measurement_datetime',
    'OBSERVATION': 'observation_datetime',
    'PROCEDURE_OCCURRENCE': 'procedure_datetime',
    'DEVICE_EXPOSURE': 'device_exposure_start_datetime',
    'CONDITION_OCCURRENCE': 'condition_start_datetime',
    'DRUG_EXPOSURE': 'drug_exposure_start_datetime',
    'VISIT_OCCURRENCE': 'visit_start_datetime'
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

class ConceptMapper:
    def __init__(self, enable_dedup=True, min_concept_freq=10, 
                 episode_window_hours=2):
        self.stats = defaultdict(lambda: defaultdict(int))
        self.enable_dedup = enable_dedup
        self.min_concept_freq = min_concept_freq
        self.episode_window_hours = episode_window_hours
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
        f.write(f"- **Deduplication**: {'Enabled' if mapper.enable_dedup else 'Disabled'}\n")
        f.write(f"- **Visit episode window**: {mapper.episode_window_hours} hours\n\n")
        
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
                f.write(f"| {col} | {col_stats['total_values']:,} | ")
                f.write(f"{col_stats['mapped_count']:,} | ")
                f.write(f"{col_stats['mapping_rate']:.1f}% | ")
                f.write(f"{col_stats['unique_concepts_mapped']:,} |\n")
            
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
    """Main execution function."""
    # Start timing
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Comprehensive data processing with concept mapping')
    parser.add_argument('--no-dedup', action='store_true', 
                        help='Disable deduplication after mapping')
    parser.add_argument('--min-concept-freq', type=int, default=10,
                        help='Minimum concept frequency threshold (default: 10)')
    parser.add_argument('--episode-window-hours', type=float, default=2,
                        help='Time window for visit episode consolidation (default: 2 hours)')
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("COMPREHENSIVE DATA PROCESSING MODULE")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nConfiguration:")
    print(f"  Input directory: {input_dir}")
    print(f"  Output directory: {output_dir}")
    print(f"  Processed mappings: {processed_mappings_dir}")
    print(f"\nFeatures enabled:")
    print(f"  Low frequency filtering: <={args.min_concept_freq}")
    print(f"  Deduplication: {'DISABLED' if args.no_dedup else 'ENABLED'}")
    print(f"  Visit episode window: {args.episode_window_hours} hours")
    print(f"\nTables to process: {', '.join(ALL_TABLES)}")
    print("="*80 + "\n")
    
    # Initialize mapper with all parameters
    mapper = ConceptMapper(
        enable_dedup=not args.no_dedup,
        min_concept_freq=args.min_concept_freq,
        episode_window_hours=args.episode_window_hours
    )
    
    # Step 1: Count concept frequencies across all tables
    freq_count_start = time.time()
    mapper.count_concept_frequencies()
    freq_count_time = time.time() - freq_count_start
    
    # Step 2: Process each table
    for table in ALL_TABLES:
        try:
            mapper.process_table(table)
        except Exception as e:
            logging.error(f"Error mapping {table}: {str(e)}")
            import traceback
            traceback.print_exc()
    
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
    print("PERFORMANCE BREAKDOWN - Concept Mapping")
    print("="*50)
    
    # Aggregate timing from all tables
    total_file_reading = 0
    total_low_freq = 0
    total_mapping = 0
    total_dedup = 0
    total_file_writing = 0
    total_chunk_processing = 0
    
    for table_stats in mapper.stats.values():
        if 'time_stats' in table_stats:
            ts = table_stats['time_stats']
            total_file_reading += ts.get('file_reading', 0)
            total_low_freq += ts.get('low_freq_filtering', 0)
            total_mapping += ts.get('concept_mapping', 0)
            total_dedup += ts.get('deduplication', 0)
            total_file_writing += ts.get('file_writing', 0)
            # Calculate chunk processing overhead (total - sum of components)
            table_total = ts.get('total', 0)
            table_components = (ts.get('file_reading', 0) + ts.get('low_freq_filtering', 0) + 
                              ts.get('concept_mapping', 0) + ts.get('deduplication', 0) + 
                              ts.get('file_writing', 0))
            total_chunk_processing += max(0, table_total - table_components)
    
    print(f"Frequency counting:    {freq_count_time:.2f}s ({freq_count_time/total_time*100:.1f}%)")
    print(f"Chunk processing:      {total_chunk_processing:.2f}s ({total_chunk_processing/total_time*100:.1f}%)")
    print(f"Low freq filtering:    {total_low_freq:.2f}s ({total_low_freq/total_time*100:.1f}%)")
    print(f"Concept mapping:       {total_mapping:.2f}s ({total_mapping/total_time*100:.1f}%)")
    print(f"Deduplication:         {total_dedup:.2f}s ({total_dedup/total_time*100:.1f}%)")
    print(f"File writing:          {total_file_writing:.2f}s ({total_file_writing/total_time*100:.1f}%)")
    
    # Calculate accounted vs unaccounted time
    accounted_time = freq_count_time + total_chunk_processing + total_low_freq + total_mapping + total_dedup + total_file_writing
    unaccounted_time = total_time - accounted_time
    if unaccounted_time > 0.1:  # Only show if significant
        print(f"Other operations:      {unaccounted_time:.2f}s ({unaccounted_time/total_time*100:.1f}%)")
    
    # Find slowest tables
    table_times = [(name, stats.get('time_stats', {}).get('total', 0)) 
                   for name, stats in mapper.stats.items()]
    table_times.sort(key=lambda x: x[1], reverse=True)
    
    print("\nSlowest tables:")
    for name, time_taken in table_times[:3]:
        if time_taken > 0:
            print(f"  {name}: {time_taken:.2f}s")

if __name__ == "__main__":
    main()
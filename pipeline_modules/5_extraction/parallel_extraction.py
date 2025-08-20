#!/usr/bin/env python
"""Parallel processing functions for ICU data extraction."""

import os
import csv
import time
import pandas as pd
import platform
import shutil
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Any
import logging

# Platform-specific settings
if platform.system() == 'Windows':
    CHUNK_SIZE = 500000
    FILE_BUFFER_SIZE = 2 * 1024 * 1024
else:
    CHUNK_SIZE = 100000
    FILE_BUFFER_SIZE = 1 * 1024 * 1024


def get_patient_path(person_id: str, patient_data_dir: Path) -> Path:
    """Generate patient data storage path with hierarchical structure."""
    prefix = person_id[:9]
    
    if prefix == "600000071":
        # Special handling for 600000071 prefix
        suffix_int = int(person_id[-6:])
        group_start = (suffix_int // 1000) * 1000
        group_end = group_start + 999
        group_folder = f"{group_start:06d}-{group_end:06d}"
        return patient_data_dir / prefix / group_folder / person_id
    else:
        # Standard two-level structure
        return patient_data_dir / prefix / person_id


def process_table_batch(tables: List[str], standardized_dir: Path, 
                       patient_data_dir: Path) -> Dict[str, Dict]:
    """Process a batch of small tables in a single worker."""
    results = {}
    
    for table_name in tables:
        try:
            stats = process_single_table_worker(
                table_name, standardized_dir, patient_data_dir
            )
            results[table_name] = stats
        except Exception as e:
            logging.error(f"Error processing {table_name}: {e}")
            results[table_name] = {'error': str(e)}
    
    return results


def process_single_table_worker(table_name: str, standardized_dir: Path,
                               patient_data_dir: Path) -> Dict[str, Any]:
    """Process a single table in a worker process."""
    t0 = time.time()
    
    input_file = standardized_dir / f"{table_name}_standardized.csv"
    if not input_file.exists():
        return {'error': f"File not found: {input_file}"}
    
    total_records = 0
    patients_processed = set()
    
    try:
        # Count total rows
        with open(input_file, 'r') as f:
            total_rows = sum(1 for _ in f) - 1
        
        # Process in chunks
        reader = pd.read_csv(input_file, dtype={"person_id": str}, chunksize=CHUNK_SIZE)
        
        # Track file handles for optimized appending
        file_handles = {}
        
        for chunk in reader:
            # Remove null person_ids
            chunk = chunk[chunk["person_id"].notnull()]
            total_records += len(chunk)
            
            # Group by person_id
            grouped = chunk.groupby("person_id")
            
            for person_id, group_df in grouped:
                # Get patient folder
                patient_path = get_patient_path(person_id, patient_data_dir)
                patient_path.mkdir(parents=True, exist_ok=True)
                
                # Get or create file handle with buffering
                file_path = patient_path / f"{table_name}.csv"
                if person_id not in file_handles:
                    # Open file with optimized buffering
                    file_handles[person_id] = open(
                        file_path, 'w', newline='', 
                        encoding='utf-8', buffering=FILE_BUFFER_SIZE
                    )
                    # Write header for new file
                    group_df.to_csv(file_handles[person_id], index=False)
                else:
                    # Append without header
                    group_df.to_csv(file_handles[person_id], header=False, index=False)
                
                patients_processed.add(person_id)
        
        # Close all file handles
        for handle in file_handles.values():
            handle.close()
        
        processing_time = time.time() - t0
        
        return {
            'total_records': total_records,
            'unique_patients': len(patients_processed),
            'processing_time': processing_time,
            'total_rows': total_rows
        }
        
    except Exception as e:
        return {'error': str(e)}


def process_measurement_chunk(args: Tuple) -> Tuple[int, Dict, str]:
    """Process a chunk of the MEASUREMENT table."""
    chunk_id, total_chunks, input_file, output_base_dir = args
    
    input_file = Path(input_file)
    output_base_dir = Path(output_base_dir)
    
    # Calculate chunk's row range
    with open(input_file, 'r') as f:
        total_rows = sum(1 for _ in f) - 1  # Subtract header
    
    rows_per_chunk = total_rows // total_chunks
    start_row = chunk_id * rows_per_chunk
    
    if chunk_id == total_chunks - 1:
        # Last chunk processes remaining rows
        nrows = None
    else:
        nrows = rows_per_chunk
    
    # Create temporary output directory
    temp_dir = output_base_dir / "temp" / f"chunk_{chunk_id}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # Track statistics
    file_handles = {}
    patient_counts = defaultdict(int)
    total_processed = 0
    
    try:
        # Read and process chunk
        if start_row > 0:
            reader = pd.read_csv(
                input_file,
                skiprows=range(1, start_row + 1),
                nrows=nrows,
                chunksize=min(CHUNK_SIZE, rows_per_chunk) if nrows else CHUNK_SIZE,
                dtype={"person_id": str},
                low_memory=False
            )
        else:
            reader = pd.read_csv(
                input_file,
                nrows=nrows,
                chunksize=min(CHUNK_SIZE, rows_per_chunk) if nrows else CHUNK_SIZE,
                dtype={"person_id": str},
                low_memory=False
            )
        
        for sub_chunk in reader:
            sub_chunk = sub_chunk[sub_chunk["person_id"].notnull()]
            total_processed += len(sub_chunk)
            
            grouped = sub_chunk.groupby("person_id")
            
            for person_id, group_df in grouped:
                # Write to temporary file
                temp_file = temp_dir / f"{person_id}.csv"
                
                if person_id not in file_handles:
                    file_handles[person_id] = open(
                        temp_file, 'w', newline='',
                        encoding='utf-8', buffering=FILE_BUFFER_SIZE
                    )
                    group_df.to_csv(file_handles[person_id], index=False)
                else:
                    group_df.to_csv(file_handles[person_id], header=False, index=False)
                
                patient_counts[person_id] += len(group_df)
        
        # Close all file handles
        for handle in file_handles.values():
            handle.close()
        
        return chunk_id, dict(patient_counts), str(temp_dir)
        
    except Exception as e:
        # Clean up on error
        for handle in file_handles.values():
            try:
                handle.close()
            except:
                pass
        raise e


def merge_measurement_chunks(output_base_dir: Path, patient_data_dir: Path, 
                            num_chunks: int) -> Dict[str, int]:
    """Merge MEASUREMENT chunk files into final patient files."""
    temp_base = output_base_dir / "temp"
    merge_stats = {'total_records': 0, 'unique_patients': 0}
    
    # Collect all patient IDs from all chunks
    all_patients = set()
    for chunk_id in range(num_chunks):
        chunk_dir = temp_base / f"chunk_{chunk_id}"
        if chunk_dir.exists():
            for file in chunk_dir.glob("*.csv"):
                patient_id = file.stem
                all_patients.add(patient_id)
    
    merge_stats['unique_patients'] = len(all_patients)
    
    # Merge files for each patient
    for patient_id in all_patients:
        # Get patient directory
        patient_path = get_patient_path(patient_id, patient_data_dir)
        patient_path.mkdir(parents=True, exist_ok=True)
        
        final_file = patient_path / "MEASUREMENT.csv"
        
        # Merge all chunks for this patient
        first_file = True
        record_count = 0
        
        with open(final_file, 'w', newline='', encoding='utf-8', 
                 buffering=FILE_BUFFER_SIZE) as outf:
            for chunk_id in range(num_chunks):
                chunk_file = temp_base / f"chunk_{chunk_id}" / f"{patient_id}.csv"
                if chunk_file.exists():
                    with open(chunk_file, 'r', encoding='utf-8') as inf:
                        if first_file:
                            # Include header
                            content = inf.read()
                            outf.write(content)
                            # Count records (lines minus header)
                            record_count += content.count('\n') - 1
                            first_file = False
                        else:
                            # Skip header
                            inf.readline()
                            content = inf.read()
                            outf.write(content)
                            record_count += content.count('\n')
        
        merge_stats['total_records'] += record_count
    
    # Clean up temporary files
    try:
        shutil.rmtree(temp_base, ignore_errors=True)
    except Exception as e:
        logging.warning(f"Could not remove temp directory: {e}")
    
    return merge_stats


def estimate_table_size(file_path: Path) -> str:
    """Estimate table size category based on file size."""
    if not file_path.exists():
        return 'missing'
    
    file_size = file_path.stat().st_size
    
    # Rough estimation: ~200 bytes per row average
    estimated_rows = file_size // 200
    
    if estimated_rows < 50000:
        return 'small'
    elif estimated_rows < 500000:
        return 'medium'
    else:
        return 'large'


def group_tables_by_size(tables: List[str], standardized_dir: Path) -> Tuple[List[str], List[str], List[str]]:
    """Group tables by their estimated size."""
    small_tables = []
    medium_tables = []
    large_tables = []
    
    for table in tables:
        if table == 'MEASUREMENT':
            # Always treat MEASUREMENT as large
            large_tables.append(table)
        else:
            file_path = standardized_dir / f"{table}_standardized.csv"
            size_category = estimate_table_size(file_path)
            
            if size_category == 'small':
                small_tables.append(table)
            elif size_category == 'medium':
                medium_tables.append(table)
            elif size_category == 'large':
                large_tables.append(table)
            # Skip missing files
    
    return small_tables, medium_tables, large_tables
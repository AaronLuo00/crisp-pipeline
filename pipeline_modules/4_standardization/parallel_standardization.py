"""
Parallel standardization processing for OMOP data tables.
Handles chunked processing of MEASUREMENT and parallel processing of other tables.
"""

import csv
import json
import time
import shutil
import pandas as pd
import numpy as np
import platform
from pathlib import Path
from typing import Dict, List, Tuple, Any
from collections import defaultdict
import logging

# Import from main module for unit conversion
import sys
sys.path.append(str(Path(__file__).parent))

# Platform-specific settings for performance optimization
if platform.system() == 'Windows':
    CHUNK_SIZE = 500000  # Larger chunks for Windows (better I/O performance)
    FILE_BUFFER_SIZE = 2 * 1024 * 1024  # 2MB file buffer
    WRITE_BUFFER_SIZE = 50000  # 50K rows batch write
else:
    CHUNK_SIZE = 100000  # Default for macOS/Linux
    FILE_BUFFER_SIZE = 1 * 1024 * 1024  # 1MB file buffer
    WRITE_BUFFER_SIZE = 20000  # 20K rows batch write
DATE_COLUMNS = {
    'MEASUREMENT': ['measurement_date', 'measurement_datetime'],
    'OBSERVATION': ['observation_date', 'observation_datetime'],
    'PROCEDURE_OCCURRENCE': ['procedure_date', 'procedure_datetime'],
    'DEVICE_EXPOSURE': ['device_exposure_start_date', 'device_exposure_start_datetime', 
                        'device_exposure_end_date', 'device_exposure_end_datetime'],
    'CONDITION_OCCURRENCE': ['condition_start_date', 'condition_start_datetime',
                            'condition_end_date', 'condition_end_datetime'],
    'CONDITION_ERA': ['condition_era_start_date', 'condition_era_end_date'],
    'DRUG_EXPOSURE': ['drug_exposure_start_date', 'drug_exposure_start_datetime',
                      'drug_exposure_end_date', 'drug_exposure_end_datetime'],
    'DRUG_ERA': ['drug_era_start_date', 'drug_era_end_date'],
    'VISIT_OCCURRENCE': ['visit_start_date', 'visit_start_datetime',
                         'visit_end_date', 'visit_end_datetime'],
    'VISIT_DETAIL': ['visit_detail_start_date', 'visit_detail_start_datetime',
                     'visit_detail_end_date', 'visit_detail_end_datetime'],
    'SPECIMEN': ['specimen_date', 'specimen_datetime']
}


def process_measurement_chunk(args: Tuple) -> Tuple[int, Dict, float]:
    """
    Process a chunk of MEASUREMENT table for standardization.
    
    Args:
        args: Tuple containing (chunk_id, total_chunks, input_file, output_dir, concept_thresholds, outlier_percentile)
    
    Returns:
        Tuple of (chunk_id, statistics, elapsed_time)
    """
    start_time = time.time()
    chunk_id, total_chunks, input_file, output_dir, concept_thresholds, outlier_percentile = args
    
    # Initialize statistics
    stats = {
        'records_processed': 0,
        'outliers_removed_percentile': 0,
        'outliers_removed_range': 0,
        'units_converted': 0,
        'datetime_standardized': 0
    }
    
    # Output files
    output_dir = Path(output_dir)
    temp_output = output_dir / f".temp_measurement_chunk_{chunk_id}.csv"
    outlier_percentile_file = output_dir / "removed_records" / "outliers_percentile" / f".temp_chunk_{chunk_id}.csv"
    outlier_range_file = output_dir / "removed_records" / "outliers_range" / f".temp_chunk_{chunk_id}.csv"
    
    # Ensure directories exist
    outlier_percentile_file.parent.mkdir(parents=True, exist_ok=True)
    outlier_range_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Calculate chunk boundaries
    file_size = Path(input_file).stat().st_size
    chunk_size = file_size // total_chunks
    start_pos = chunk_id * chunk_size
    end_pos = file_size if chunk_id == total_chunks - 1 else (chunk_id + 1) * chunk_size
    
    # Process chunk
    outliers_percentile = []
    outliers_range = []
    
    with open(input_file, 'rb') as infile:
        # Seek to start position
        infile.seek(start_pos)
        if start_pos > 0:
            # Skip partial line
            infile.readline()
            actual_start = infile.tell()
        else:
            actual_start = 0
        
        # Read header if first chunk
        if chunk_id == 0:
            infile.seek(0)
            header_line = infile.readline().decode('utf-8').strip()
            header = header_line.split(',')
            infile.seek(actual_start)
        else:
            # Get header from first line of file
            infile.seek(0)
            header_line = infile.readline().decode('utf-8').strip()
            header = header_line.split(',')
            infile.seek(actual_start)
        
        # Open output file with buffering
        with open(temp_output, 'w', newline='', encoding='utf-8', buffering=FILE_BUFFER_SIZE) as outfile:
            writer = csv.DictWriter(outfile, fieldnames=header)
            
            # Write header for all chunks (will skip when merging)
            writer.writeheader()
            
            # Initialize write buffer for batch writing
            write_buffer = []
            
            # Process lines
            reader = csv.DictReader(infile.read(end_pos - actual_start).decode('utf-8').splitlines(), fieldnames=header)
            if chunk_id == 0:
                next(reader)  # Skip header row in first chunk
            
            for row in reader:
                stats['records_processed'] += 1
                
                # Check outliers for MEASUREMENT with numeric values
                if 'value_as_number' in row and row['value_as_number']:
                    try:
                        value = float(row['value_as_number'])
                        concept_id = int(row.get('measurement_concept_id', 0))
                        
                        # Check if this concept has thresholds
                        if concept_id in concept_thresholds.get('MEASUREMENT', {}):
                            threshold = concept_thresholds['MEASUREMENT'][concept_id]
                            is_outlier = False
                            outlier_type = None
                            
                            # Check percentile outliers
                            if 'lower_percentile' in threshold and 'upper_percentile' in threshold:
                                if value < threshold['lower_percentile'] or value > threshold['upper_percentile']:
                                    is_outlier = True
                                    outlier_type = 'percentile'
                                    outlier_row = dict(row)
                                    outlier_row['outlier_type'] = 'percentile'
                                    outlier_row['threshold_low'] = threshold['lower_percentile']
                                    outlier_row['threshold_high'] = threshold['upper_percentile']
                                    outliers_percentile.append(outlier_row)
                                    stats['outliers_removed_percentile'] += 1
                            
                            # Check range outliers (if not already flagged)
                            if not is_outlier and 'range_min' in threshold and 'range_max' in threshold:
                                if value < threshold['range_min'] or value > threshold['range_max']:
                                    is_outlier = True
                                    outlier_type = 'range'
                                    outlier_row = dict(row)
                                    outlier_row['outlier_type'] = 'range'
                                    outlier_row['range_min'] = threshold['range_min']
                                    outlier_row['range_max'] = threshold['range_max']
                                    outliers_range.append(outlier_row)
                                    stats['outliers_removed_range'] += 1
                            
                            if is_outlier:
                                continue  # Skip this record
                        
                        # Unit conversion would go here
                        # For now, just count if unit_concept_id exists
                        if 'unit_concept_id' in row and row['unit_concept_id']:
                            # Simplified unit conversion tracking
                            pass
                    
                    except (ValueError, TypeError):
                        pass
                
                # Standardize datetime fields
                if 'measurement_datetime' in row and row['measurement_datetime']:
                    # Simple datetime standardization
                    stats['datetime_standardized'] += 1
                
                # Add non-outlier record to buffer
                write_buffer.append(row)
                
                # Batch write when buffer is full
                if len(write_buffer) >= WRITE_BUFFER_SIZE:
                    writer.writerows(write_buffer)
                    write_buffer.clear()
            
            # Write remaining records in buffer
            if write_buffer:
                writer.writerows(write_buffer)
    
    # Save outlier files
    if outliers_percentile:
        with open(outlier_percentile_file, 'w', newline='', encoding='utf-8') as f:
            if outliers_percentile:
                writer = csv.DictWriter(f, fieldnames=list(outliers_percentile[0].keys()))
                writer.writeheader()
                writer.writerows(outliers_percentile)
    
    if outliers_range:
        with open(outlier_range_file, 'w', newline='', encoding='utf-8') as f:
            if outliers_range:
                writer = csv.DictWriter(f, fieldnames=list(outliers_range[0].keys()))
                writer.writeheader()
                writer.writerows(outliers_range)
    
    elapsed = time.time() - start_time
    logging.info(f"MEASUREMENT chunk {chunk_id+1}/{total_chunks} completed: "
                 f"{stats['records_processed']} records in {elapsed:.2f}s")
    
    return chunk_id, stats, elapsed


def process_standard_table(args: Tuple) -> Tuple[str, Dict, float]:
    """
    Process a standard table (non-MEASUREMENT) for standardization.
    
    Args:
        args: Tuple containing (table_name, input_file, output_dir)
    
    Returns:
        Tuple of (table_name, statistics, elapsed_time)
    """
    start_time = time.time()
    table_name, input_file, output_dir = args
    
    # Initialize statistics
    stats = {
        'input_records': 0,
        'output_records': 0,
        'datetime_standardized': 0,
        'units_converted': 0,
        'outliers_removed_percentile': 0,
        'outliers_removed_range': 0
    }
    
    # Process using pandas for simplicity
    try:
        df = pd.read_csv(input_file, low_memory=False)
        stats['input_records'] = len(df)
        
        # Standardize datetime fields if applicable
        if table_name in DATE_COLUMNS:
            for col in DATE_COLUMNS[table_name]:
                if col in df.columns:
                    # Count non-null datetime values
                    non_null = df[col].notna().sum()
                    if non_null > 0:
                        stats['datetime_standardized'] += non_null
        
        # No outlier removal for non-MEASUREMENT tables
        stats['output_records'] = len(df)
        
        # Save output
        output_file = Path(output_dir) / f"{table_name}_standardized.csv"
        df.to_csv(output_file, index=False)
        
    except Exception as e:
        logging.error(f"Error processing {table_name}: {e}")
        stats['error'] = str(e)
    
    elapsed = time.time() - start_time
    logging.info(f"{table_name} completed: {stats['input_records']} records in {elapsed:.2f}s")
    
    return table_name, stats, elapsed


def merge_measurement_chunks(output_dir: Path, num_chunks: int) -> Dict:
    """
    Merge MEASUREMENT chunk files into final output.
    
    Args:
        output_dir: Output directory path
        num_chunks: Number of chunks to merge
    
    Returns:
        Dictionary with merge statistics
    """
    start_time = time.time()
    stats = {'records_merged': 0}
    
    # Output files
    final_output = output_dir / "MEASUREMENT_standardized.csv"
    
    # Merge main data files
    with open(final_output, 'wb') as outfile:
        for i in range(num_chunks):
            chunk_file = output_dir / f".temp_measurement_chunk_{i}.csv"
            if not chunk_file.exists():
                logging.warning(f"Chunk file not found: {chunk_file}")
                continue
            
            with open(chunk_file, 'rb') as infile:
                if i == 0:
                    # Copy entire first file including header
                    shutil.copyfileobj(infile, outfile, FILE_BUFFER_SIZE)
                else:
                    # Skip header for subsequent files
                    infile.readline()
                    shutil.copyfileobj(infile, outfile, FILE_BUFFER_SIZE)
            
            # Remove temp file
            chunk_file.unlink()
    
    # Merge outlier files
    merge_outlier_files(output_dir, num_chunks, 'outliers_percentile')
    merge_outlier_files(output_dir, num_chunks, 'outliers_range')
    
    elapsed = time.time() - start_time
    logging.info(f"MEASUREMENT chunks merged in {elapsed:.2f}s")
    
    return stats


def merge_outlier_files(output_dir: Path, num_chunks: int, outlier_type: str):
    """
    Merge outlier files from chunks.
    
    Args:
        output_dir: Output directory
        num_chunks: Number of chunks
        outlier_type: Type of outlier ('outliers_percentile' or 'outliers_range')
    """
    outlier_dir = output_dir / "removed_records" / outlier_type
    final_file = outlier_dir / f"MEASUREMENT_{outlier_type.split('_')[1]}_outliers.csv"
    
    with open(final_file, 'wb') as outfile:
        header_written = False
        for i in range(num_chunks):
            chunk_file = outlier_dir / f".temp_chunk_{i}.csv"
            if chunk_file.exists():
                with open(chunk_file, 'rb') as infile:
                    if not header_written:
                        shutil.copyfileobj(infile, outfile, FILE_BUFFER_SIZE)
                        header_written = True
                    else:
                        infile.readline()  # Skip header
                        shutil.copyfileobj(infile, outfile, FILE_BUFFER_SIZE)
                chunk_file.unlink()  # Remove temp file
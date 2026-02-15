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
import re

# Import from main module for unit conversion
import sys
sys.path.append(str(Path(__file__).parent))
# Add utils to path for shared file splitter
sys.path.append(str(Path(__file__).parent.parent / 'utils'))

# Import unit conversion mappings
from unit_conversions import UNIT_ID_CONVERSIONS
from file_splitter import streaming_csv_reader

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

# ID columns that should be formatted as integers (remove .0)
ID_COLUMNS_TO_FORMAT = {
    # Common ID columns across all tables
    'ALL': ['person_id'],
    
    # Table-specific ID columns
    'CONDITION_OCCURRENCE': [
        'provider_id', 'visit_occurrence_id', 'visit_detail_id',
        'condition_source_concept_id', 'condition_status_concept_id'
    ],
    'DEVICE_EXPOSURE': [
        'unique_device_id', 'provider_id', 'visit_occurrence_id', 
        'visit_detail_id', 'device_source_concept_id'
    ],
    'DRUG_EXPOSURE': [
        'route_concept_id', 'provider_id', 'visit_occurrence_id',
        'visit_detail_id', 'drug_source_concept_id'
    ],
    'MEASUREMENT': [
        'operator_concept_id', 'value_as_concept_id', 'unit_concept_id',
        'provider_id', 'visit_occurrence_id', 'visit_detail_id',
        'measurement_source_concept_id'
    ],
    'OBSERVATION': [
        'value_as_concept_id', 'qualifier_concept_id', 'provider_id',
        'visit_occurrence_id', 'visit_detail_id', 
        'observation_source_concept_id'
    ],
    'PROCEDURE_OCCURRENCE': [
        'modifier_concept_id', 'provider_id', 'visit_occurrence_id',
        'visit_detail_id', 'procedure_source_concept_id'
    ],
    'SPECIMEN': [
        'unit_concept_id', 'anatomic_site_concept_id'
    ],
    'VISIT_DETAIL': [
        'provider_id', 'care_site_id', 'visit_detail_source_concept_id',
        'admitting_source_concept_id', 'preceding_visit_detail_id'
    ],
    'VISIT_OCCURRENCE': [
        'provider_id', 'care_site_id', 'visit_source_concept_id',
        'admitting_source_concept_id', 'discharge_to_concept_id',
        'preceding_visit_occurrence_id'
    ]
}

def format_id_columns_df(df, table_name):
    """
    Format ID columns in DataFrame by removing .0 suffix.
    Handles both float64 columns and object columns with float string values.
    
    Args:
        df: DataFrame to format
        table_name: Name of the table for column lookup
    
    Returns:
        Modified DataFrame with formatted ID columns
    """
    # Get ID columns for this table
    id_columns = ID_COLUMNS_TO_FORMAT.get('ALL', []) + \
                 ID_COLUMNS_TO_FORMAT.get(table_name, [])
    
    for col in id_columns:
        if col in df.columns:
            # Handle float64 columns
            if df[col].dtype == 'float64':
                # For columns with NaN, format non-null values
                mask = df[col].notna()
                if mask.any():
                    # Convert to string, removing .0 for integers
                    df.loc[mask, col] = df.loc[mask, col].apply(
                        lambda x: str(int(x)) if x == int(x) else str(x)
                    )
            # Handle object (string) columns that may contain float representations
            elif df[col].dtype == 'object':
                # Process non-null string values
                mask = df[col].notna() & (df[col] != '')
                if mask.any():
                    def format_value(val):
                        if isinstance(val, str) and '.' in val:
                            try:
                                float_val = float(val)
                                if float_val == int(float_val):
                                    return str(int(float_val))
                            except (ValueError, TypeError):
                                pass
                        return val
                    
                    df.loc[mask, col] = df.loc[mask, col].apply(format_value)
    
    return df

def format_id_columns_row(row, table_name):
    """
    Format ID columns in a dictionary row by removing .0 suffix.
    Similar to format_id_columns_df but works on a single row dictionary.
    
    Args:
        row: Dictionary row from CSV reader
        table_name: Name of the table for column lookup
    
    Returns:
        Modified row with formatted ID columns
    """
    # Get ID columns for this table
    id_columns = ID_COLUMNS_TO_FORMAT.get('ALL', []) + \
                 ID_COLUMNS_TO_FORMAT.get(table_name, [])
    
    for col in id_columns:
        if col in row and row[col]:  # Column exists and is not empty
            value = row[col]
            try:
                # Handle string representation of float
                if isinstance(value, str) and '.' in value:
                    float_val = float(value)
                    if float_val == int(float_val):  # No decimal part
                        row[col] = str(int(float_val))
                # Handle float type directly (shouldn't happen with CSV reader, but just in case)
                elif isinstance(value, float):
                    if value == int(value):  # No decimal part
                        row[col] = str(int(value))
            except (ValueError, TypeError, OverflowError):
                # Keep original value if conversion fails
                pass
    
    return row


def standardize_units_parallel(value, unit_concept_id, measurement_concept_id):
    """Standardize measurement units (parallel version)."""
    if not value or not unit_concept_id:
        return value, unit_concept_id, False
        
    try:
        value_float = float(value)
        unit_id = int(float(unit_concept_id))
    except:
        return value, unit_concept_id, False
    
    # Check if this unit needs conversion
    if unit_id in UNIT_ID_CONVERSIONS:
        conversion = UNIT_ID_CONVERSIONS[unit_id]
        
        # Only convert if this measurement type needs it
        if measurement_concept_id in conversion['for_concepts']:
            # Apply conversion
            if 'factor' in conversion:
                new_value = value_float * conversion['factor']
            elif 'formula' in conversion:
                new_value = conversion['formula'](value_float)
            
            # Update to target unit
            new_unit_id = conversion['target_id']
            return str(new_value), str(new_unit_id), True
    
    return value, unit_concept_id, False


def process_measurement_chunk(args: Tuple) -> Tuple[int, Dict, float, List]:
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
    
    # List to collect unit conversion changes (limited to avoid memory issues)
    changes = []
    max_changes = 100000  # Limit changes per chunk to avoid memory issues
    
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
    
    # Calculate actual byte range boundaries
    with open(input_file, 'rb') as infile:
        if start_pos > 0:
            infile.seek(start_pos)
            infile.readline()  # Skip partial line
            actual_start = infile.tell()
        else:
            # First chunk: skip header line
            infile.readline()
            actual_start = infile.tell()
        
        # Read header from first line of file
        infile.seek(0)
        header_line = infile.readline().decode('utf-8').strip()
        header = header_line.split(',')
    
    # Open output file with buffering
    with open(temp_output, 'w', newline='', encoding='utf-8', buffering=FILE_BUFFER_SIZE) as outfile:
        writer = csv.DictWriter(outfile, fieldnames=header)
        
        # Write header for all chunks (will skip when merging)
        writer.writeheader()
        
        # Initialize write buffer for batch writing
        write_buffer = []
        
        # Stream rows line-by-line using shared utility (O(1) memory per row)
        reader = streaming_csv_reader(input_file, actual_start, end_pos, header)
        
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
                    
                    # Unit conversion
                    if 'unit_concept_id' in row and row['unit_concept_id'] and value:
                        new_value, new_unit_id, converted = standardize_units_parallel(
                            value, row['unit_concept_id'], concept_id
                        )
                        if converted:
                            # Record change if under limit
                            if len(changes) < max_changes:
                                changes.append({
                                    'row_number': stats['records_processed'],
                                    'measurement_id': row.get('measurement_id', ''),
                                    'concept_id': concept_id,
                                    'original_value': value,
                                    'original_unit_id': row['unit_concept_id'],
                                    'new_value': new_value,
                                    'new_unit_id': new_unit_id,
                                    'change_type': 'unit_conversion'
                                })
                            row['value_as_number'] = new_value
                            row['unit_concept_id'] = new_unit_id
                            stats['units_converted'] += 1
                
                except (ValueError, TypeError):
                    pass
            
            # Standardize datetime fields
            for col in ['measurement_date', 'measurement_datetime']:
                if col in row and row[col]:
                    # Remove fractional seconds (milliseconds/microseconds)
                    original = row[col]
                    standardized = re.sub(r'(\d{2}:\d{2}:\d{2})\.\d+', r'\1', str(original))
                    if original != standardized:
                        row[col] = standardized
                        stats['datetime_standardized'] += 1
            
            # Format ID columns before adding to buffer
            formatted_row = format_id_columns_row(row, 'MEASUREMENT')
            
            # Add non-outlier record to buffer
            write_buffer.append(formatted_row)
            
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
    
    return chunk_id, stats, elapsed, changes


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
                    # Remove milliseconds/microseconds from datetime columns
                    # Convert to string, remove fractional seconds, and keep as string
                    mask = df[col].notna()
                    non_null = mask.sum()
                    if non_null > 0:
                        # Convert to string and remove fractional seconds
                        df.loc[mask, col] = df.loc[mask, col].astype(str).str.replace(r'(\d{2}:\d{2}:\d{2})\.\d+', r'\1', regex=True)
                        stats['datetime_standardized'] += non_null
        
        # No outlier removal for non-MEASUREMENT tables
        stats['output_records'] = len(df)
        
        # Format ID columns before saving
        df = format_id_columns_df(df, table_name)
        
        # Save output
        output_file = Path(output_dir) / f"{table_name}_standardized.csv"
        # Use date_format to preserve time component even when it's 00:00:00
        df.to_csv(output_file, index=False, date_format='%Y-%m-%d %H:%M:%S')
        
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


def process_visit_patient_chunk(args: Tuple) -> Tuple[int, Dict, float]:
    """
    Process a chunk of patients for VISIT table merging.
    
    Args:
        args: Tuple containing (chunk_id, total_chunks, input_file, output_dir, patient_chunk, table_name, threshold_hours)
    
    Returns:
        Tuple of (chunk_id, statistics, elapsed_time)
    """
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent))
    from visit_concept_merger import VisitConceptMerger
    
    start_time = time.time()
    chunk_id, total_chunks, input_file, output_dir, patient_chunk, table_name, threshold_hours = args
    
    # Initialize statistics
    stats = {
        'patients_processed': len(patient_chunk),
        'merged_episodes': 0,
        'unchanged_records': 0,
        'records_merged': 0,
        'total_output_records': 0
    }
    
    # Read input file and add original row numbers
    df = pd.read_csv(input_file, low_memory=False)
    
    # Add original row numbers (index + 2 because index starts at 0 and we have header)
    df['original_row_number'] = df.index + 2
    
    # Filter to only patients in this chunk
    chunk_df = df[df['person_id'].isin(patient_chunk)]
    
    if chunk_df.empty:
        logging.warning(f"No data found for chunk {chunk_id}")
        return chunk_id, stats, time.time() - start_time
    
    # Initialize merger
    merger = VisitConceptMerger(threshold_minutes=int(threshold_hours * 60))
    
    # Process each patient in chunk
    all_results = []
    all_mappings = []
    
    for patient_id in patient_chunk:
        patient_data = chunk_df[chunk_df['person_id'] == patient_id]
        if not patient_data.empty:
            patient_results, patient_mappings = merger.merge_visits_for_patient(patient_data, patient_id)
            
            if not patient_results.empty:
                all_results.append(patient_results)
            
            if not patient_mappings.empty:
                all_mappings.append(patient_mappings)
    
    # Combine chunk results
    if all_results:
        merged_df = pd.concat(all_results, ignore_index=True)
        stats['total_output_records'] = len(merged_df)
    else:
        merged_df = pd.DataFrame()
    
    if all_mappings:
        mapping_df = pd.concat(all_mappings, ignore_index=True)
    else:
        mapping_df = pd.DataFrame()
    
    # Update statistics from merger
    stats['merged_episodes'] = merger.statistics.get('merged_episodes', 0)
    stats['unchanged_records'] = merger.statistics.get('unchanged_records', 0)
    stats['records_merged'] = merger.statistics.get('records_merged', 0)
    
    # Save chunk results to temporary files
    output_dir = Path(output_dir)
    temp_output = output_dir / f".temp_{table_name}_chunk_{chunk_id}.csv"
    temp_mapping = output_dir / "merged_visit" / f".temp_{table_name}_mapping_chunk_{chunk_id}.csv"
    
    # Ensure directories exist
    temp_mapping.parent.mkdir(parents=True, exist_ok=True)
    
    # Save results
    if not merged_df.empty:
        # Use date_format to preserve time component even when it's 00:00:00
        merged_df.to_csv(temp_output, index=False, date_format='%Y-%m-%d %H:%M:%S')
    
    if not mapping_df.empty:
        mapping_df.to_csv(temp_mapping, index=False)
    
    elapsed = time.time() - start_time
    logging.info(f"{table_name} chunk {chunk_id+1}/{total_chunks} completed: "
                 f"{stats['patients_processed']} patients, "
                 f"{stats['total_output_records']} output records in {elapsed:.2f}s")
    
    return chunk_id, stats, elapsed


def merge_visit_chunks(output_dir: Path, table_name: str, num_chunks: int) -> Dict:
    """
    Merge VISIT chunk files into final output.
    
    Args:
        output_dir: Output directory path
        table_name: Table name (VISIT_DETAIL or VISIT_OCCURRENCE)
        num_chunks: Number of chunks to merge
    
    Returns:
        Dictionary with merge statistics
    """
    start_time = time.time()
    stats = {'chunks_merged': 0, 'records_merged': 0}
    
    # Output files
    final_output = output_dir / f"{table_name}_standardized.csv"
    final_mapping = output_dir / "merged_visit" / f"{table_name}_merge_mapping.csv"
    
    # Ensure directories exist
    final_mapping.parent.mkdir(parents=True, exist_ok=True)
    
    # Merge main data files
    chunk_files = []
    mapping_files = []
    
    for i in range(num_chunks):
        chunk_file = output_dir / f".temp_{table_name}_chunk_{i}.csv"
        mapping_file = output_dir / "merged_visit" / f".temp_{table_name}_mapping_chunk_{i}.csv"
        
        if chunk_file.exists():
            chunk_files.append(chunk_file)
        if mapping_file.exists():
            mapping_files.append(mapping_file)
    
    # Merge main output files using fast binary copy
    if chunk_files:
        with open(final_output, 'wb') as outfile:
            for i, chunk_file in enumerate(chunk_files):
                with open(chunk_file, 'rb') as infile:
                    if i == 0:
                        # Copy entire first file including header
                        shutil.copyfileobj(infile, outfile, FILE_BUFFER_SIZE)
                    else:
                        # Skip header for subsequent files
                        infile.readline()
                        shutil.copyfileobj(infile, outfile, FILE_BUFFER_SIZE)
                
                # Clean up temp file
                chunk_file.unlink()
                stats['chunks_merged'] += 1
    
    # Merge mapping files
    if mapping_files:
        with open(final_mapping, 'wb') as outfile:
            for i, mapping_file in enumerate(mapping_files):
                with open(mapping_file, 'rb') as infile:
                    if i == 0:
                        # Copy entire first file including header
                        shutil.copyfileobj(infile, outfile, FILE_BUFFER_SIZE)
                    else:
                        # Skip header for subsequent files
                        infile.readline()
                        shutil.copyfileobj(infile, outfile, FILE_BUFFER_SIZE)
                
                # Clean up temp file
                mapping_file.unlink()
    
    elapsed = time.time() - start_time
    logging.info(f"{table_name} chunks merged in {elapsed:.2f}s")
    
    return stats
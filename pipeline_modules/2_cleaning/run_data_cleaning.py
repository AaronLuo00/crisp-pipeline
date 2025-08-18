#!/usr/bin/env python
"""Run data cleaning on key tables of subdataset_1000 with chunked processing."""

import csv
import json
import os
import platform
import time
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
from tqdm import tqdm
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Performance optimization settings
if platform.system() == 'Windows':
    CHUNK_SIZE = 500000
else:
    CHUNK_SIZE = 100000
PROGRESS_INTERVAL = 10.0  # Progress update interval in seconds

# Setup
base_dir = Path(__file__).parent
project_root = base_dir.parent.parent  # Go up to crisp_pipeline_code
data_dir = project_root / "data"

# Check if data directory exists, if not try subdataset_1000
if not data_dir.exists():
    subdataset_dir = project_root.parent / "subdataset_1000"
    if subdataset_dir.exists():
        data_dir = subdataset_dir
        print(f"Using data directory: {data_dir}")

output_dir = project_root / "output" / "2_cleaning"
output_dir.mkdir(parents=True, exist_ok=True)

# Create directories for removed records
removed_dir = output_dir / "removed_records"
duplicates_dir = removed_dir / "duplicates"
invalid_concept_dir = removed_dir / "invalid_concept_id"
temporal_issues_dir = removed_dir / "temporal_issues"
duplicates_dir.mkdir(parents=True, exist_ok=True)
invalid_concept_dir.mkdir(parents=True, exist_ok=True)
temporal_issues_dir.mkdir(parents=True, exist_ok=True)

# Expanded list of tables to clean - includes ALL tables in data directory
KEY_TABLES = [
    'MEASUREMENT', 'OBSERVATION', 'DRUG_EXPOSURE',      # Original high-volume tables
    'CONDITION_OCCURRENCE', 'VISIT_OCCURRENCE',         # High priority additions
    'PROCEDURE_OCCURRENCE', 'VISIT_DETAIL',              # Medium priority
    'CONDITION_ERA', 'SPECIMEN', 'DEATH',               # Other tables with concept_id issues
    'DEVICE_EXPOSURE', 'DRUG_ERA', 'OBSERVATION_PERIOD', 'PERSON'  # Additional OMOP tables
]

# Define primary concept_id field for each table
PRIMARY_CONCEPT_FIELDS = {
    'MEASUREMENT': 'measurement_concept_id',
    'OBSERVATION': 'observation_concept_id',
    'DRUG_EXPOSURE': 'drug_concept_id',
    'CONDITION_ERA': 'condition_concept_id',
    'CONDITION_OCCURRENCE': 'condition_concept_id',
    'DEATH': 'death_datetime',  # Check death_datetime instead of concept_id
    'DEVICE_EXPOSURE': 'device_concept_id',
    'DRUG_ERA': 'drug_concept_id',
    'PROCEDURE_OCCURRENCE': 'procedure_concept_id',
    'SPECIMEN': 'specimen_concept_id',
    'VISIT_DETAIL': 'visit_detail_concept_id',
    'VISIT_OCCURRENCE': 'visit_concept_id',
    'OBSERVATION_PERIOD': 'period_type_concept_id'
}

# Define temporal fields for validation
TEMPORAL_FIELDS = {
    'DRUG_EXPOSURE': ('drug_exposure_start_datetime', 'drug_exposure_end_datetime'),
    'VISIT_OCCURRENCE': ('visit_start_datetime', 'visit_end_datetime'),
    'VISIT_DETAIL': ('visit_detail_start_datetime', 'visit_detail_end_datetime'),
    'DEVICE_EXPOSURE': ('device_exposure_start_datetime', 'device_exposure_end_datetime'),
    'DRUG_ERA': ('drug_era_start_date', 'drug_era_end_date'),
    'OBSERVATION_PERIOD': ('observation_period_start_date', 'observation_period_end_date'),
    'CONDITION_ERA': ('condition_era_start_date', 'condition_era_end_date'),
    'CONDITION_OCCURRENCE': ('condition_start_datetime', 'condition_end_datetime'),
    'PROCEDURE_OCCURRENCE': ('procedure_datetime', 'procedure_end_datetime')
}

# Define duplicate key columns for each table
DUPLICATE_KEY_COLUMNS = {
    'MEASUREMENT': ['person_id', 'measurement_concept_id', 'measurement_datetime'],
    'OBSERVATION': ['person_id', 'observation_concept_id', 'observation_datetime'],
    'DRUG_EXPOSURE': ['person_id', 'drug_concept_id', 'drug_exposure_start_datetime'],
    'CONDITION_OCCURRENCE': ['person_id', 'condition_concept_id', 'condition_start_datetime'],
    'PROCEDURE_OCCURRENCE': ['person_id', 'procedure_concept_id', 'procedure_datetime'],
    'VISIT_OCCURRENCE': ['person_id', 'visit_concept_id', 'visit_start_datetime'],
    'VISIT_DETAIL': ['person_id', 'visit_detail_concept_id', 'visit_detail_start_datetime'],
    'DEVICE_EXPOSURE': ['person_id', 'device_concept_id', 'device_exposure_start_datetime'],
    'DRUG_ERA': ['person_id', 'drug_concept_id', 'drug_era_start_date'],
    'OBSERVATION_PERIOD': ['person_id', 'observation_period_start_date'],
    'PERSON': ['person_id'],
    'CONDITION_ERA': ['person_id', 'condition_concept_id', 'condition_era_start_date'],
    'SPECIMEN': ['specimen_id'],
    'DEATH': ['person_id']
}

# Start timing
start_time = time.time()

print("Starting data cleaning on subdataset_1000...")
print(f"Output directory: {output_dir}")
print(f"Chunk size: {CHUNK_SIZE:,} rows")
print(f"Tables to clean: {', '.join(KEY_TABLES)}")

# Check if column analysis from EDA is available
column_analysis_path = project_root / "output" / "1_eda" / "column_analysis.json"
if column_analysis_path.exists():
    print(f"\nFound column_analysis.json from EDA module, will use cached analysis")
else:
    print(f"\nNo column_analysis.json found, will analyze columns for each table")

# Print input information
print("\n" + "="*70)
print("DATA CLEANING PROCESS - INPUT INFORMATION")
print("="*70)
print(f"Data Directory: {data_dir}")
print(f"Output Directory: {output_dir}")
print("\nTables to be cleaned:")
for table in KEY_TABLES:
    input_file = data_dir / f"{table}.csv"
    print(f"  - {table}: {input_file}")

print("\nCleaning Strategy:")
print("  1. Remove columns with >95% missing data")
print("  2. Remove duplicate records based on table-specific keys")
print("  3. Remove records with invalid concept IDs (0 or null)")
print("  4. Validate temporal consistency for date ranges")
print("="*70 + "\n")

# Store cleaning results
cleaning_results = {
    "cleaning_date": datetime.now().isoformat(),
    "dataset": "subdataset_1000",
    "chunk_size": CHUNK_SIZE,
    "tables": {}
}

def get_file_row_count(file_path):
    """Get the number of rows in a CSV file with caching."""
    # Cache file path - use output directory
    cache_file = output_dir / ".row_counts_cache.json"
    
    # Try to load cache
    cache = {}
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                cache = json.load(f)
        except:
            cache = {}
    
    # Check if cache is valid
    file_name = file_path.name
    file_mtime = os.path.getmtime(file_path)
    
    if file_name in cache and cache[file_name].get('mtime') == file_mtime:
        # Use cached row count
        print(f"  Using cached row count for {file_name}: {cache[file_name]['row_count']:,} rows")
        return cache[file_name]['row_count']
    
    # Calculate row count (using fast method with 1MB buffer)
    def blocks(file, size=1024*1024):
        while True:
            b = file.read(size)
            if not b: break
            yield b
    
    with open(file_path, "r", encoding="utf-8", errors='ignore') as f:
        row_count = sum(bl.count("\n") for bl in blocks(f)) - 1
    
    # Update cache
    cache[file_name] = {
        'row_count': row_count,
        'mtime': file_mtime
    }
    
    # Save cache
    with open(cache_file, 'w') as f:
        json.dump(cache, f, indent=2)
    
    return row_count

def analyze_columns_chunked(file_path, chunk_size=CHUNK_SIZE):
    """Analyze columns to identify those with >95% missing data using pandas vectorized operations."""
    total_rows = 0
    missing_counts = defaultdict(int)
    non_missing_counts = defaultdict(int)
    unique_values = defaultdict(set)
    sample_values = defaultdict(list)
    data_types = {}
    headers = None
    
    # Process file in chunks using pandas (much faster than csv.DictReader)
    for chunk_idx, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size, low_memory=False)):
        if chunk_idx == 0:
            headers = list(chunk.columns)
            # Infer data types from first chunk
            for col in headers:
                dtype_str = str(chunk[col].dtype)
                if 'int' in dtype_str or 'float' in dtype_str:
                    data_types[col] = 'numeric'
                else:
                    data_types[col] = 'string'
        
        total_rows += len(chunk)
        
        # Vectorized operations for counting (much faster than loops)
        chunk_missing = chunk.isnull().sum()
        chunk_non_missing = chunk.notna().sum()
        
        # Update counts
        for col in headers:
            missing_counts[col] += int(chunk_missing[col])
            non_missing_counts[col] += int(chunk_non_missing[col])
            
            # Collect unique values and samples from non-null values
            non_null_values = chunk[col].dropna()
            if len(non_null_values) > 0:
                # Collect unique values (limit to prevent memory issues)
                if len(unique_values[col]) < 1000:
                    unique_vals = non_null_values.unique()[:min(100, len(non_null_values))]
                    for val in unique_vals:
                        unique_values[col].add(str(val))
                
                # Collect sample values
                if len(sample_values[col]) < 20:
                    samples = non_null_values.head(20 - len(sample_values[col])).tolist()
                    sample_values[col].extend([str(s) for s in samples])
                            
    # Identify columns to remove and prepare detailed info
    columns_to_remove = []
    columns_info = []
    
    for col in headers:
        missing_pct = (missing_counts[col] / total_rows * 100) if total_rows > 0 else 0
        
        column_detail = {
            'column_name': col,
            'total_rows': total_rows,
            'non_missing_count': non_missing_counts[col],
            'missing_count': missing_counts[col],
            'missing_percentage': missing_pct,
            'unique_values': len(unique_values[col]),
            'sample_values': sample_values[col][:10],  # Limit to 10 samples
            'data_type': data_types.get(col, 'unknown')
        }
        
        if missing_pct > 95:
            columns_to_remove.append(col)
            column_detail['removal_reason'] = '>95% missing'
            columns_info.append(column_detail)
            
    return headers, columns_to_remove, missing_counts, total_rows, columns_info

def clean_table_partial(table_name, start_row=0, end_row=-1, position=0, disable_progress=False, part_suffix=""):
    """Clean a table or part of a table with chunked processing.
    
    Args:
        table_name: Name of the table to clean
        start_row: Starting row (0-based), 0 for beginning
        end_row: Ending row (exclusive), -1 for end of file
        position: Position for progress bar (for parallel processing)
        disable_progress: Whether to disable progress bar
        part_suffix: Suffix for output files when processing partial table
    """
    
    # Initialize time tracking
    table_time_stats = {
        'total': 0,
        'column_analysis': 0,
        'duplicate_detection': 0,
        'invalid_concept': 0,
        'temporal_validation': 0,
        'file_io': 0,
        'other_processing': 0
    }
    table_start_time = time.time()
    
    input_file = data_dir / f"{table_name}.csv"
    
    # Add part suffix to output files if processing partial table
    if part_suffix:
        output_file = output_dir / f"{table_name}_cleaned{part_suffix}.csv"
        temp_file = output_dir / f"{table_name}_temp{part_suffix}.csv"
        duplicates_file = duplicates_dir / f"{table_name}{part_suffix}.csv"
        invalid_concept_file = invalid_concept_dir / f"{table_name}{part_suffix}.csv"
        temporal_issues_file = temporal_issues_dir / f"{table_name}{part_suffix}.csv"
    else:
        output_file = output_dir / f"{table_name}_cleaned.csv"
        temp_file = output_dir / f"{table_name}_temp.csv"
        duplicates_file = duplicates_dir / f"{table_name}.csv"
        invalid_concept_file = invalid_concept_dir / f"{table_name}.csv"
        temporal_issues_file = temporal_issues_dir / f"{table_name}.csv"
    
    # Check if file exists
    if not input_file.exists():
        print(f"Warning: {input_file} not found, skipping...")
        return 0
    
    # Get total row count for progress bar
    file_total_rows = get_file_row_count(input_file)
    
    # Determine actual rows to process
    if end_row == -1:
        end_row = file_total_rows
    total_rows = end_row - start_row
    
    # For partial processing, show which part is being processed
    if part_suffix:
        print(f"  Processing {table_name} rows {start_row:,} to {end_row:,} ({total_rows:,} rows)")
    
    # Try to load column analysis from EDA module
    t0 = time.time()
    column_analysis_path = project_root / "output" / "1_eda" / "column_analysis.json"
    columns_to_remove = []
    missing_counts = {}
    columns_info = []
    
    if column_analysis_path.exists():
        # Use pre-computed column analysis from EDA
        print(f"  Using cached column analysis from EDA module")
        with open(column_analysis_path, 'r') as f:
            column_analysis = json.load(f)
        
        if table_name in column_analysis:
            table_analysis = column_analysis[table_name]
            columns_to_remove = table_analysis.get("columns_to_remove", [])
            headers = table_analysis.get("columns", [])
            
            # Reconstruct missing_counts from column_stats
            for col, stats in table_analysis.get("column_stats", {}).items():
                missing_counts[col] = stats.get("missing_count", 0)
            
            # Build columns_info for removed columns
            for col in columns_to_remove:
                col_stats = table_analysis["column_stats"].get(col, {})
                total_records = col_stats.get("total_records", total_rows)
                missing_count = col_stats.get("missing_count", 0)
                columns_info.append({
                    'column_name': col,
                    'total_rows': total_records,
                    'non_missing_count': total_records - missing_count,  # Calculate from available data
                    'missing_count': missing_count,
                    'missing_percentage': col_stats.get("missing_percentage", 0),
                    'unique_values': col_stats.get("unique_count", 0),  # Get from cache, default to 0
                    'sample_values': [],  # EDA doesn't store samples, use empty list
                    'data_type': col_stats.get("data_type", "unknown"),  # Get from cache
                    'removal_reason': '>95% missing'
                })
        else:
            # Table not in cached analysis, fall back to original method
            print(f"  Table {table_name} not found in cached analysis, analyzing columns...")
            headers, columns_to_remove, missing_counts, _, columns_info = analyze_columns_chunked(input_file)
    else:
        # No cached analysis available, use original method
        print(f"  No cached column analysis found, analyzing columns...")
        headers, columns_to_remove, missing_counts, _, columns_info = analyze_columns_chunked(input_file)
    
    table_time_stats['column_analysis'] = time.time() - t0
    
    # Add table_name to each column info
    for col_info in columns_info:
        col_info['table_name'] = table_name
    
    # Silent processing - no need to print column removal details
    
    # Get duplicate key columns for this table
    duplicate_key_cols = DUPLICATE_KEY_COLUMNS.get(table_name, [])
    
    # Process data in chunks
    
    # Initialize counters
    duplicate_count = 0
    invalid_concept_count = 0
    temporal_issues = 0
    rows_written = 0
    seen_keys = {}  # Changed from set to dict to store first occurrence info
    duplicate_groups = defaultdict(list)  # Store all records in each duplicate group
    
    # Initialize write buffers for batch writing
    WRITE_BUFFER_SIZE = 5000
    write_buffer = []
    invalid_buffer = []
    temporal_buffer = []
    
    # Clean headers
    clean_headers = [h for h in headers if h not in columns_to_remove]
    
    # Open output file and removed records files
    with open(temp_file, 'w', encoding='utf-8', newline='') as outfile, \
         open(invalid_concept_file, 'w', encoding='utf-8', newline='') as invalid_file, \
         open(temporal_issues_file, 'w', encoding='utf-8', newline='') as temporal_file:
        
        writer = csv.DictWriter(outfile, fieldnames=clean_headers)
        writer.writeheader()
        
        invalid_writer = csv.DictWriter(invalid_file, fieldnames=headers)
        invalid_writer.writeheader()
        
        temporal_headers = headers + ['temporal_issue_reason', 'start_datetime', 'end_datetime', 'original_row_number']
        temporal_writer = csv.DictWriter(temporal_file, fieldnames=temporal_headers)
        temporal_writer.writeheader()
        
        # Process file in chunks with progress bar
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            
            # Create progress bar with more frequent updates
            desc_text = f"Cleaning {table_name}{part_suffix}" if part_suffix else f"Cleaning {table_name}"
            with tqdm(total=total_rows, desc=desc_text, unit="rows",
                     miniters=max(100, total_rows//100) if total_rows > 0 else 1,  # Update every 1% or at least 100 rows
                     mininterval=PROGRESS_INTERVAL,
                     position=position,  # Use provided position for parallel processing
                     leave=False, ncols=100,
                     disable=disable_progress) as pbar:
                chunk = []
                
                rows_processed = 0
                for row_num, row in enumerate(reader):
                    # Skip rows before start_row
                    if row_num < start_row:
                        continue
                    
                    # Stop if we've reached end_row
                    if row_num >= end_row:
                        break
                    
                    chunk.append((row_num + 1, row))  # Store with 1-based row number
                    rows_processed += 1
                    
                    # Process chunk when it reaches the desired size
                    if len(chunk) >= CHUNK_SIZE or rows_processed == total_rows:
                        # Process each row in the chunk
                        for original_row_num, row in chunk:
                            # Check for duplicates
                            skip_row = False
                            removal_reason = None
                            
                            t1 = time.time()
                            if duplicate_key_cols:
                                key = tuple(row.get(col, '') for col in duplicate_key_cols)
                                if key in seen_keys:
                                    duplicate_count += 1
                                    skip_row = True
                                    removal_reason = 'duplicate'
                                    # Add to duplicate group
                                    group_id = seen_keys[key]
                                    duplicate_groups[group_id].append((row, 'removed', original_row_num))
                                else:
                                    # First occurrence - create new group
                                    group_id = f"{table_name}_{len(seen_keys)+1}"
                                    seen_keys[key] = group_id
                                    duplicate_groups[group_id].append((row, 'kept', original_row_num))
                            table_time_stats['duplicate_detection'] += time.time() - t1
                            
                            if skip_row:
                                continue
                            
                            # Check for invalid concept_id
                            t1 = time.time()
                            if table_name in PRIMARY_CONCEPT_FIELDS:
                                concept_field = PRIMARY_CONCEPT_FIELDS[table_name]
                                if concept_field in headers:
                                    concept_id = row.get(concept_field, '')
                                    if not concept_id or str(concept_id).strip() == '' or str(concept_id).strip() == '0':
                                        invalid_concept_count += 1
                                        skip_row = True
                                        removal_reason = 'invalid_concept'
                                        # Save invalid concept record to buffer
                                        invalid_buffer.append(row)
                                        
                                        # Batch write when buffer is full
                                        if len(invalid_buffer) >= WRITE_BUFFER_SIZE:
                                            invalid_writer.writerows(invalid_buffer)
                                            invalid_buffer = []
                            table_time_stats['invalid_concept'] += time.time() - t1
                            
                            if skip_row:
                                continue
                            
                            # Temporal validation
                            t1 = time.time()
                            temporal_invalid = False
                            start = None
                            end = None
                            
                            if table_name in TEMPORAL_FIELDS:
                                start_col, end_col = TEMPORAL_FIELDS[table_name]
                                start = row.get(start_col)
                                end = row.get(end_col)
                            
                            if start and end and end < start:
                                temporal_invalid = True
                                temporal_issues += 1
                                skip_row = True
                                removal_reason = 'temporal_issue'
                                
                                # Save temporal issue record to buffer
                                temporal_row = row.copy()
                                temporal_row['temporal_issue_reason'] = f"End datetime ({end}) is before start datetime ({start})"
                                temporal_row['start_datetime'] = start
                                temporal_row['end_datetime'] = end
                                temporal_row['original_row_number'] = original_row_num
                                temporal_buffer.append(temporal_row)
                                
                                # Batch write when buffer is full
                                if len(temporal_buffer) >= WRITE_BUFFER_SIZE:
                                    temporal_writer.writerows(temporal_buffer)
                                    temporal_buffer = []
                            
                            table_time_stats['temporal_validation'] += time.time() - t1
                            
                            if skip_row:
                                continue
                            
                            # Write cleaned row to buffer
                            t_write = time.time()
                            clean_row = {k: v for k, v in row.items() if k not in columns_to_remove}
                            write_buffer.append(clean_row)
                            rows_written += 1
                            
                            # Batch write when buffer is full
                            if len(write_buffer) >= WRITE_BUFFER_SIZE:
                                writer.writerows(write_buffer)
                                write_buffer = []
                            
                            table_time_stats['file_io'] += time.time() - t_write
                        
                        # Update progress
                        pbar.update(len(chunk))
                        
                        # Clear chunk
                        chunk = []
        
        # Write any remaining buffered data before closing files
        if write_buffer:
            writer.writerows(write_buffer)
        if invalid_buffer:
            invalid_writer.writerows(invalid_buffer)
        if temporal_buffer:
            temporal_writer.writerows(temporal_buffer)
    
    # Rename temp file to final output (Windows-compatible)
    # Use shutil.move for cross-platform compatibility
    import shutil
    shutil.move(str(temp_file), str(output_file))
    
    # Write duplicate groups to file (only groups with multiple records)
    duplicate_groups_written = 0
    with open(duplicates_file, 'w', encoding='utf-8', newline='') as dup_file:
        dup_headers = headers + ['duplicate_status', 'duplicate_group_id', 'original_row_number']
        dup_writer = csv.DictWriter(dup_file, fieldnames=dup_headers)
        dup_writer.writeheader()
        
        for group_id, group_records in duplicate_groups.items():
            # Only write groups that have duplicates (more than 1 record)
            if len(group_records) > 1:
                for row, status, row_num in group_records:
                    row_with_meta = row.copy()
                    row_with_meta['duplicate_status'] = status
                    row_with_meta['duplicate_group_id'] = group_id
                    row_with_meta['original_row_number'] = row_num
                    dup_writer.writerow(row_with_meta)
                    duplicate_groups_written += 1
    
    # Silent summary - details saved to results file
    rows_removed = total_rows - rows_written
    
    # Calculate total time
    table_time_stats['total'] = time.time() - table_start_time
    
    # Calculate other_processing as the time not accounted for by specific operations
    accounted_time = (
        table_time_stats['column_analysis'] + 
        table_time_stats['duplicate_detection'] +
        table_time_stats['invalid_concept'] +
        table_time_stats['temporal_validation'] +
        table_time_stats['file_io']
    )
    table_time_stats['other_processing'] = max(0, table_time_stats['total'] - accounted_time)
    
    # Store results - use table_name with part_suffix for proper aggregation
    result_key = f"{table_name}{part_suffix}" if part_suffix else table_name
    cleaning_results["tables"][result_key] = {
        "original_records": total_rows,
        "cleaned_records": rows_written,
        "records_removed": rows_removed,
        "removal_percentage": rows_removed/total_rows*100 if total_rows > 0 else 0,
        "original_columns": len(headers),
        "cleaned_columns": len(clean_headers),
        "columns_removed": columns_to_remove,
        "duplicates_removed": duplicate_count,
        "invalid_concept_removed": invalid_concept_count,
        "temporal_issues": temporal_issues,
        "output_file": str(output_file),
        "removed_records_files": {
            "duplicates": str(duplicates_file),
            "invalid_concept_id": str(invalid_concept_file),
            "temporal_issues": str(temporal_issues_file)
        },
        "removed_columns_details": columns_info,
        "time_stats": table_time_stats
    }
    
    # Silent completion
    
    # Return statistics for parallel processing aggregation
    stats_for_aggregation = {
        "duplicates_removed": duplicate_count,
        "invalid_concept_removed": invalid_concept_count,
        "temporal_issues": temporal_issues,
        "columns_removed": columns_to_remove
    }
    
    return rows_written, table_time_stats, stats_for_aggregation

def clean_table(table_name, position=0, disable_progress=False):
    """Wrapper function for backward compatibility."""
    return clean_table_partial(table_name, 0, -1, position, disable_progress, "")

def merge_table_parts(table_name, num_parts):
    """Merge multiple parts of a cleaned table into one file."""
    output_dir = project_root / "output" / "2_cleaning"
    final_file = output_dir / f"{table_name}_cleaned.csv"
    
    # Merge main cleaned files
    first_part = True
    with open(final_file, 'w') as outfile:
        for i in range(num_parts):
            part_file = output_dir / f"{table_name}_cleaned_part{i+1}.csv"
            if part_file.exists():
                with open(part_file, 'r') as infile:
                    if first_part:
                        # Include header from first part
                        outfile.write(infile.read())
                        first_part = False
                    else:
                        # Skip header for subsequent parts
                        lines = infile.readlines()
                        if len(lines) > 1:
                            outfile.writelines(lines[1:])
                # Remove part file after merging
                part_file.unlink()
    
    # Merge removed records files
    for subdir, file_pattern in [
        (duplicates_dir, "duplicates"),
        (invalid_concept_dir, "invalid_concept_id"),
        (temporal_issues_dir, "temporal_issues")
    ]:
        final_removed = subdir / f"{table_name}.csv"
        first_part = True
        with open(final_removed, 'w') as outfile:
            for i in range(num_parts):
                part_file = subdir / f"{table_name}_part{i+1}.csv"
                if part_file.exists():
                    with open(part_file, 'r') as infile:
                        if first_part:
                            outfile.write(infile.read())
                            first_part = False
                        else:
                            lines = infile.readlines()
                            if len(lines) > 1:
                                outfile.writelines(lines[1:])
                    part_file.unlink()

if __name__ == '__main__':
    # Process each key table
    print("\nStarting table cleaning process...")
    total_original = 0
    total_cleaned = 0

    # Determine whether to use parallel processing
    USE_PARALLEL = os.environ.get('PARALLEL_CLEANING', 'false').lower() == 'true'
    MAX_WORKERS = min(multiprocessing.cpu_count() - 1, 6)  # Leave one CPU free, max 6 workers
    MEASUREMENT_SPLITS = int(os.environ.get('MEASUREMENT_SPLITS', '6'))  # Control MEASUREMENT table splits (optimized default)

    # Process tables with progress tracking
    print("\nCleaning tables...")
    all_time_stats = []

    if USE_PARALLEL:
        print(f"Using parallel processing with {MAX_WORKERS} workers")
        print(f"MEASUREMENT table will be split into {MEASUREMENT_SPLITS} parts")
        print("Set PARALLEL_CLEANING=false to disable parallel processing")
        print(f"Set MEASUREMENT_SPLITS=N to change splits (current: {MEASUREMENT_SPLITS})")
        
        # Determine which tables need splitting
        # LARGE_TABLE_THRESHOLD = 1000000  # 1M rows (commented out - only split MEASUREMENT)
        table_splits = {}
        
        # Only split MEASUREMENT table for now
        for table in KEY_TABLES:
            if table == 'MEASUREMENT':
                input_file = data_dir / f"{table}.csv"
                if input_file.exists():
                    row_count = get_file_row_count(input_file)
                    num_splits = MEASUREMENT_SPLITS
                    table_splits[table] = num_splits
                    print(f"  {table} has {row_count:,} rows - will split into {num_splits} parts")
        
        # Generic large table splitting (commented out)
        # for table in KEY_TABLES:
        #     input_file = data_dir / f"{table}.csv"
        #     if input_file.exists():
        #         row_count = get_file_row_count(input_file)
        #         if row_count > LARGE_TABLE_THRESHOLD:
        #             # Use configurable splits for MEASUREMENT, default 4 for others
        #             if table == 'MEASUREMENT':
        #                 num_splits = MEASUREMENT_SPLITS
        #             else:
        #                 num_splits = 4
        #             table_splits[table] = num_splits
        #             print(f"  {table} has {row_count:,} rows - will split into {num_splits} parts")
        
        # Parallel processing
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_info = {}
            position_counter = 1
            
            for table in KEY_TABLES:
                if table in table_splits:
                    # Submit multiple tasks for large table
                    num_splits = table_splits[table]
                    input_file = data_dir / f"{table}.csv"
                    total_rows = get_file_row_count(input_file)
                    rows_per_split = total_rows // num_splits
                    
                    for i in range(num_splits):
                        start_row = i * rows_per_split
                        end_row = (i + 1) * rows_per_split if i < num_splits - 1 else total_rows
                        part_suffix = f"_part{i+1}"
                        
                        future = executor.submit(
                            clean_table_partial, table, start_row, end_row,
                            position_counter, False, part_suffix
                        )
                        future_to_info[future] = (table, i+1, num_splits)
                        position_counter += 1
                else:
                    # Submit single task for normal table
                    future = executor.submit(clean_table, table, position_counter, False)
                    future_to_info[future] = (table, 0, 0)
                    position_counter += 1
            
            # Track completed tasks
            total_tasks = len(future_to_info)
            tables_completed = set()
            table_parts_completed = defaultdict(int)
            table_parts_results = defaultdict(list)  # Store results for each part
            
            with tqdm(total=total_tasks, desc="Overall progress", position=0) as overall_pbar:
                for future in as_completed(future_to_info):
                    table, part_num, num_parts = future_to_info[future]
                    try:
                        result = future.result()
                        
                        # clean_table_partial now returns (cleaned_count, time_stats, stats_for_aggregation)
                        if isinstance(result, tuple) and len(result) == 3:
                            cleaned_count, time_stats, detail_stats = result
                        elif isinstance(result, tuple) and len(result) == 2:
                            # Backward compatibility
                            cleaned_count, time_stats = result
                            detail_stats = {}
                        else:
                            cleaned_count = result
                            time_stats = {}
                            detail_stats = {}
                        
                        total_cleaned += cleaned_count
                        all_time_stats.append(time_stats)
                        
                        if num_parts > 0:
                            # Part of a split table
                            table_parts_completed[table] += 1
                            table_parts_results[table].append((cleaned_count, time_stats, detail_stats))
                            overall_pbar.set_postfix_str(f"Completed: {table} part {part_num}/{num_parts}")
                            
                            # Check if all parts are done
                            if table_parts_completed[table] == num_parts:
                                # Merge the parts
                                print(f"\n  Merging {num_parts} parts of {table}...")
                                merge_table_parts(table, num_parts)
                                
                                # Initialize columns_removed (will be set from first part's detail_stats)
                                columns_removed = []
                                
                                # Aggregate statistics for the complete table
                                total_rows_cleaned = sum(c for c, _, _ in table_parts_results[table])
                                input_file = data_dir / f"{table}.csv"
                                total_original_rows = get_file_row_count(input_file)
                                
                                # Aggregate detailed statistics from all parts using returned values
                                total_duplicates = 0
                                total_invalid = 0
                                total_temporal = 0
                                
                                # Use the returned statistics from each part
                                for cleaned_count, time_stats, detail_stats in table_parts_results[table]:
                                    if detail_stats:
                                        total_duplicates += detail_stats.get("duplicates_removed", 0)
                                        total_invalid += detail_stats.get("invalid_concept_removed", 0)
                                        total_temporal += detail_stats.get("temporal_issues", 0)
                                        # Use columns_removed from first part if not already set
                                        if not columns_removed and "columns_removed" in detail_stats:
                                            columns_removed = detail_stats["columns_removed"]
                                
                                # Aggregate time stats
                                aggregated_time_stats = {
                                    'total': sum(s['total'] for _, s, _ in table_parts_results[table]),
                                    'column_analysis': sum(s.get('column_analysis', 0) for _, s, _ in table_parts_results[table]),
                                    'duplicate_detection': sum(s.get('duplicate_detection', 0) for _, s, _ in table_parts_results[table]),
                                    'invalid_concept': sum(s.get('invalid_concept', 0) for _, s, _ in table_parts_results[table]),
                                    'temporal_validation': sum(s.get('temporal_validation', 0) for _, s, _ in table_parts_results[table]),
                                    'file_io': sum(s.get('file_io', 0) for _, s, _ in table_parts_results[table]),
                                    'other_processing': sum(s.get('other_processing', 0) for _, s, _ in table_parts_results[table])
                                }
                                
                                # Store aggregated results
                                cleaning_results["tables"][table] = {
                                    "original_records": total_original_rows,
                                    "cleaned_records": total_rows_cleaned,
                                    "records_removed": total_original_rows - total_rows_cleaned,
                                    "removal_percentage": (total_original_rows - total_rows_cleaned) / total_original_rows * 100 if total_original_rows > 0 else 0,
                                    "output_file": str(output_dir / f"{table}_cleaned.csv"),
                                    "duplicates_removed": total_duplicates,
                                    "invalid_concept_removed": total_invalid,
                                    "temporal_issues": total_temporal,
                                    "columns_removed": columns_removed,
                                    "removed_records_files": {
                                        "duplicates": str(duplicates_dir / f"{table}.csv"),
                                        "invalid_concept_id": str(invalid_concept_dir / f"{table}.csv"),
                                        "temporal_issues": str(temporal_issues_dir / f"{table}.csv")
                                    },
                                    "time_stats": aggregated_time_stats
                                }
                                tables_completed.add(table)
                        else:
                            # Single table - use returned statistics
                            input_file = data_dir / f"{table}.csv"
                            if input_file.exists():
                                total_original_rows = get_file_row_count(input_file)
                                cleaning_results["tables"][table] = {
                                    "original_records": total_original_rows,
                                    "cleaned_records": cleaned_count,
                                    "records_removed": total_original_rows - cleaned_count,
                                    "removal_percentage": (total_original_rows - cleaned_count) / total_original_rows * 100 if total_original_rows > 0 else 0,
                                    "output_file": str(output_dir / f"{table}_cleaned.csv"),
                                    "duplicates_removed": detail_stats.get("duplicates_removed", 0) if detail_stats else 0,
                                    "invalid_concept_removed": detail_stats.get("invalid_concept_removed", 0) if detail_stats else 0,
                                    "temporal_issues": detail_stats.get("temporal_issues", 0) if detail_stats else 0,
                                    "columns_removed": detail_stats.get("columns_removed", []) if detail_stats else [],
                                    "removed_records_files": {
                                        "duplicates": str(duplicates_dir / f"{table}.csv"),
                                        "invalid_concept_id": str(invalid_concept_dir / f"{table}.csv"),
                                        "temporal_issues": str(temporal_issues_dir / f"{table}.csv")
                                    },
                                    "time_stats": time_stats
                                }
                            overall_pbar.set_postfix_str(f"Completed: {table}")
                            tables_completed.add(table)
                        
                        overall_pbar.update(1)
                    except Exception as e:
                        print(f"\nError cleaning {table}: {str(e)}")
                        cleaning_results["tables"][table] = {"error": str(e)}
                        overall_pbar.update(1)
            
            # Clear any lingering progress bars
            print("\r" + " " * 100 + "\r", end="", flush=True)
    else:
        print("Using sequential processing")
        print("Set PARALLEL_CLEANING=true to enable parallel processing")
        
        # Sequential processing (original code)
        for i, table in enumerate(KEY_TABLES):
            try:
                print(f"  [{i+1}/{len(KEY_TABLES)}] Processing {table}...", end="", flush=True)
                cleaned_count, time_stats = clean_table(table, position=1, disable_progress=False)
                total_cleaned += cleaned_count
                all_time_stats.append(time_stats)
                print(f" Done ({time_stats['total']:.2f}s)")
            except Exception as e:
                print(f"\nError cleaning {table}: {str(e)}")
                cleaning_results["tables"][table] = {"error": str(e)}

    # Save cleaning results
    results_path = output_dir / "cleaning_results.json"
    with open(results_path, 'w') as f:
        json.dump(cleaning_results, f, indent=2)

    print(f"\n{'='*60}")
    print("CLEANING COMPLETE")
    print('='*60)
    print(f"Detailed results saved to: {results_path}")
    
    # Generate removed columns analysis
    removed_columns_path = removed_dir / "removed_columns_analysis.csv"
    with open(removed_columns_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['table_name', 'column_name', 'total_rows', 'non_missing_count', 
                      'missing_count', 'missing_percentage', 'unique_values', 
                      'sample_values', 'data_type', 'removal_reason']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for table_name, table_stats in cleaning_results["tables"].items():
            if "removed_columns_details" in table_stats and "error" not in table_stats:
                for col_info in table_stats["removed_columns_details"]:
                    writer.writerow({
                        'table_name': col_info['table_name'],
                        'column_name': col_info['column_name'],
                        'total_rows': col_info['total_rows'],
                        'non_missing_count': col_info['non_missing_count'],
                        'missing_count': col_info['missing_count'],
                        'missing_percentage': f"{col_info['missing_percentage']:.1f}",
                        'unique_values': col_info['unique_values'],
                        'sample_values': json.dumps(col_info['sample_values'][:5]) if col_info['sample_values'] else "[]",
                        'data_type': col_info['data_type'],
                        'removal_reason': col_info['removal_reason']
                    })
    
    print(f"Removed columns analysis saved to: {removed_columns_path}")
    
    # Generate cleaning report
    report_path = output_dir / "cleaning_report.md"
    with open(report_path, 'w') as f:
        f.write("# Data Cleaning Report - subdataset_1000\n\n")
        f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Chunk Size**: {CHUNK_SIZE:,} rows\n")
        f.write(f"**Tables Cleaned**: {', '.join(KEY_TABLES)}\n")
        f.write(f"**Removed Records Directory**: {removed_dir}\n\n")
        
        f.write("## Cleaning Summary\n\n")
        f.write("| Table | Original Records | Cleaned Records | Records Removed | Duplicates | Invalid Concept ID | Columns Removed |\n")
        f.write("|-------|-----------------|-----------------|-----------------|------------|-------------------|----------------|\n")
        
        for table in KEY_TABLES:
            if table in cleaning_results["tables"] and "error" not in cleaning_results["tables"][table]:
                stats = cleaning_results["tables"][table]
                f.write(f"| {table} | {stats['original_records']:,} | {stats['cleaned_records']:,} | ")
                f.write(f"{stats['records_removed']:,} ({stats['removal_percentage']:.1f}%) | ")
                f.write(f"{stats['duplicates_removed']:,} | ")
                f.write(f"{stats.get('invalid_concept_removed', 0):,} | ")
                f.write(f"{len(stats['columns_removed'])} |\n")
        
        # Calculate totals
        total_original = sum(s['original_records'] for s in cleaning_results['tables'].values() if 'error' not in s)
        total_cleaned = sum(s['cleaned_records'] for s in cleaning_results['tables'].values() if 'error' not in s)
        total_removed = total_original - total_cleaned
        total_duplicates = sum(s['duplicates_removed'] for s in cleaning_results['tables'].values() if 'error' not in s)
        total_invalid = sum(s.get('invalid_concept_removed', 0) for s in cleaning_results['tables'].values() if 'error' not in s)
        
        f.write(f"| **TOTAL** | **{total_original:,}** | **{total_cleaned:,}** | ")
        if total_original > 0:
            f.write(f"**{total_removed:,} ({total_removed/total_original*100:.1f}%)** | ")
        else:
            f.write(f"**{total_removed:,} (0.0%)** | ")
        f.write(f"**{total_duplicates:,}** | **{total_invalid:,}** | - |\n")
        
        f.write("\n## Detailed Results\n\n")
        
        for table in KEY_TABLES:
            if table in cleaning_results["tables"] and "error" not in cleaning_results["tables"][table]:
                stats = cleaning_results["tables"][table]
                f.write(f"### {table}\n\n")
                
                if stats['columns_removed']:
                    f.write("**Columns Removed** (>95% missing):\n")
                    for col in stats['columns_removed']:
                        f.write(f"- {col}\n")
                    f.write("\n")
                
                if stats.get('invalid_concept_removed', 0) > 0:
                    f.write(f"**Invalid Concept ID**: {stats['invalid_concept_removed']:,} records removed ")
                    f.write(f"(concept_id = 0 or null)\n\n")
                
                if stats['temporal_issues'] > 0:
                    f.write(f"**Temporal Issues**: {stats['temporal_issues']:,} records with invalid date ranges\n\n")
                
                f.write("**Removed Records Files**:\n")
                f.write(f"- Duplicates: `{stats['removed_records_files']['duplicates']}`\n")
                f.write(f"- Invalid Concept ID: `{stats['removed_records_files']['invalid_concept_id']}`\n")
                if stats['temporal_issues'] > 0:
                    f.write(f"- Temporal Issues: `{stats['removed_records_files']['temporal_issues']}`\n")
                f.write("\n")
    
    print(f"Report saved to: {report_path}")
    
    # Print comprehensive summary
    print("\n" + "="*70)
    print("DATA CLEANING PROCESS - SUMMARY")
    print("="*70)
    
    # Calculate summary statistics
    tables_processed = 0
    tables_with_errors = 0
    total_input_records = 0
    total_output_records = 0
    total_duplicates = 0
    total_invalid_concepts = 0
    total_columns_removed = 0
    total_temporal_issues = 0
    output_files_created = []
    
    for table, stats in cleaning_results["tables"].items():
        if "error" in stats:
            tables_with_errors += 1
        else:
            tables_processed += 1
            total_input_records += stats['original_records']
            total_output_records += stats['cleaned_records']
            total_duplicates += stats['duplicates_removed']
            total_invalid_concepts += stats.get('invalid_concept_removed', 0)
            total_columns_removed += len(stats['columns_removed'])
            total_temporal_issues += stats.get('temporal_issues', 0)
            
            # Track output files
            if Path(stats['output_file']).exists():
                output_files_created.append(stats['output_file'])
    
    print(f"\nTables Processed:")
    print(f"  - Successfully processed: {tables_processed}")
    print(f"  - Failed with errors: {tables_with_errors}")
    
    print(f"\nTotal Records:")
    print(f"  - Input: {total_input_records:,}")
    print(f"  - Output: {total_output_records:,}")
    print(f"  - Removed: {total_input_records - total_output_records:,} ({(total_input_records - total_output_records)/total_input_records*100:.1f}%)" if total_input_records > 0 else "  - Removed: 0 (0.0%)")
    
    print(f"\nBreakdown of Removals:")
    print(f"  - Duplicate records: {total_duplicates:,}")
    print(f"  - Invalid concept IDs: {total_invalid_concepts:,}")
    print(f"  - Temporal issues: {total_temporal_issues:,}")
    print(f"  - Columns removed: {total_columns_removed}")
    
    print("\nOutput Files Created:")
    print("  Cleaned Data:")
    for table in KEY_TABLES:
        cleaned_file = output_dir / f"{table}_cleaned.csv"
        if cleaned_file.exists():
            print(f"    - {cleaned_file}")
    
    print("\n  Removed Records:")
    # Check for duplicate files
    has_duplicates = False
    for table in KEY_TABLES:
        dup_file = duplicates_dir / f"{table}.csv"
        if dup_file.exists() and dup_file.stat().st_size > 0:
            has_duplicates = True
            break
    
    if has_duplicates:
        print("    Duplicates:")
        for table in KEY_TABLES:
            dup_file = duplicates_dir / f"{table}.csv"
            if dup_file.exists() and dup_file.stat().st_size > 0:
                print(f"      - {dup_file}")
    
    # Check for invalid concept files
    has_invalid = False
    for table in KEY_TABLES:
        invalid_file = invalid_concept_dir / f"{table}.csv"
        if invalid_file.exists() and invalid_file.stat().st_size > 0:
            has_invalid = True
            break
    
    if has_invalid:
        print("    Invalid Concept IDs:")
        for table in KEY_TABLES:
            invalid_file = invalid_concept_dir / f"{table}.csv"
            if invalid_file.exists() and invalid_file.stat().st_size > 0:
                print(f"      - {invalid_file}")
    
    # Check for temporal issues files
    has_temporal = False
    for table in KEY_TABLES:
        temporal_file = temporal_issues_dir / f"{table}.csv"
        if temporal_file.exists() and temporal_file.stat().st_size > 0:
            has_temporal = True
            break
    
    if has_temporal:
        print("    Temporal Issues:")
        for table in KEY_TABLES:
            temporal_file = temporal_issues_dir / f"{table}.csv"
            if temporal_file.exists() and temporal_file.stat().st_size > 0:
                print(f"      - {temporal_file}")
    
    print("\n  Reports:")
    print(f"    - {results_path}")
    print(f"    - {report_path}")
    print(f"    - {removed_columns_path}")
    
    # Calculate total execution time
    total_time = time.time() - start_time
    print(f"\nTotal execution time: {total_time:.2f} seconds")
    
    # Performance breakdown
    if all_time_stats:
        print("\n" + "="*50)
        print("PERFORMANCE BREAKDOWN - Data Cleaning")
        print("="*50)
        
        # Sum up times from all tables
        total_column_analysis = sum(s.get('column_analysis', 0) for s in all_time_stats)
        total_duplicate_detection = sum(s.get('duplicate_detection', 0) for s in all_time_stats)
        total_invalid_concept = sum(s.get('invalid_concept', 0) for s in all_time_stats)
        total_temporal_validation = sum(s.get('temporal_validation', 0) for s in all_time_stats)
        total_file_io = sum(s.get('file_io', 0) for s in all_time_stats)
        total_other = sum(s.get('other_processing', 0) for s in all_time_stats)
        
        # For parallel processing, percentages should be based on CPU time (sum of all tasks)
        # not wall clock time, since tasks run in parallel
        if USE_PARALLEL:
            # Sum of all task times represents total CPU time used
            total_cpu_time = sum(s.get('total', 0) for s in all_time_stats)
            if total_cpu_time > 0:
                print(f"Total CPU time:        {total_cpu_time:.2f}s (across {len(all_time_stats)} tasks)")
                print(f"Wall clock time:       {total_time:.2f}s")
                print(f"Parallelization efficiency: {total_cpu_time/total_time/MAX_WORKERS*100:.1f}%\n")
                
                # Show breakdown as percentage of total CPU time
                print("CPU Time Breakdown:")
                print(f"  Column analysis:       {total_column_analysis:.2f}s ({total_column_analysis/total_cpu_time*100:.1f}%)")
                print(f"  Duplicate detection:   {total_duplicate_detection:.2f}s ({total_duplicate_detection/total_cpu_time*100:.1f}%)")
                print(f"  Invalid concept ID:    {total_invalid_concept:.2f}s ({total_invalid_concept/total_cpu_time*100:.1f}%)")
                print(f"  Temporal validation:   {total_temporal_validation:.2f}s ({total_temporal_validation/total_cpu_time*100:.1f}%)")
                print(f"  File I/O:              {total_file_io:.2f}s ({total_file_io/total_cpu_time*100:.1f}%)")
                print(f"  Other processing:      {total_other:.2f}s ({total_other/total_cpu_time*100:.1f}%)")
            else:
                print("No timing statistics available")
        else:
            # Sequential processing - use wall clock time
            print(f"Column analysis:       {total_column_analysis:.2f}s ({total_column_analysis/total_time*100:.1f}%)")
            print(f"Duplicate detection:   {total_duplicate_detection:.2f}s ({total_duplicate_detection/total_time*100:.1f}%)")
            print(f"Invalid concept ID:    {total_invalid_concept:.2f}s ({total_invalid_concept/total_time*100:.1f}%)")
            print(f"Temporal validation:   {total_temporal_validation:.2f}s ({total_temporal_validation/total_time*100:.1f}%)")
            print(f"File I/O:              {total_file_io:.2f}s ({total_file_io/total_time*100:.1f}%)")
            print(f"Other processing:      {total_other:.2f}s ({total_other/total_time*100:.1f}%)")
        
        # Find slowest operations (skip if using parallel processing with splits)
        if not USE_PARALLEL or len(all_time_stats) == len(KEY_TABLES):
            print("\nSlowest tables:")
            table_times = [(KEY_TABLES[i], s['total']) for i, s in enumerate(all_time_stats)]
            table_times.sort(key=lambda x: x[1], reverse=True)
            for name, time_taken in table_times[:3]:
                print(f"  {name}: {time_taken:.2f}s")
    
    print("="*70)
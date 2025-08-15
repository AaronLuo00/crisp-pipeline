#!/usr/bin/env python
"""Run data cleaning on key tables of subdataset_1000 with chunked processing."""

import csv
import json
import platform
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
from tqdm import tqdm

# Platform-specific settings for performance optimization
if platform.system() == 'Windows':
    CHUNK_SIZE = 500000  # Larger chunks for Windows (better I/O performance)
    PROGRESS_INTERVAL = 30.0  # Less frequent updates (reduce overhead)
else:
    CHUNK_SIZE = 100000  # Default for macOS/Linux
    PROGRESS_INTERVAL = 10.0

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
    'DEATH': 'death_type_concept_id',  # Primary check on death_type, not cause
    'DEVICE_EXPOSURE': 'device_concept_id',
    'DRUG_ERA': 'drug_concept_id',
    'PROCEDURE_OCCURRENCE': 'procedure_concept_id',
    'SPECIMEN': 'specimen_concept_id',
    'VISIT_DETAIL': 'visit_detail_concept_id',
    'VISIT_OCCURRENCE': 'visit_concept_id',
    'OBSERVATION_PERIOD': 'period_type_concept_id'
}

print("Starting data cleaning on subdataset_1000...")
print(f"Output directory: {output_dir}")
print(f"Chunk size: {CHUNK_SIZE:,} rows")
print(f"Tables to clean: {', '.join(KEY_TABLES)}")

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
    """Get the number of rows in a CSV file (excluding header)."""
    with open(file_path, 'r') as f:
        return sum(1 for _ in f) - 1

def analyze_columns_chunked(file_path, chunk_size=CHUNK_SIZE):
    """Analyze columns to identify those with >95% missing data using chunked reading."""
    total_rows = 0
    missing_counts = defaultdict(int)
    non_missing_counts = defaultdict(int)
    unique_values = defaultdict(set)
    sample_values = defaultdict(list)
    data_types = {}
    headers = None
    
    # First pass: count missing values and collect samples
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        for row in reader:
            total_rows += 1
            for col, value in row.items():
                if not value or value.strip() == '':
                    missing_counts[col] += 1
                else:
                    non_missing_counts[col] += 1
                    # Collect unique values (limit to prevent memory issues)
                    if len(unique_values[col]) < 1000:
                        unique_values[col].add(value)
                    # Collect sample values
                    if len(sample_values[col]) < 20:
                        sample_values[col].append(value)
                    # Infer data type from first non-empty value
                    if col not in data_types and value:
                        try:
                            float(value)
                            data_types[col] = 'numeric'
                        except:
                            data_types[col] = 'string'
                            
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

def clean_table(table_name):
    """Clean a single table with chunked processing."""
    
    input_file = data_dir / f"{table_name}.csv"
    output_file = output_dir / f"{table_name}_cleaned.csv"
    temp_file = output_dir / f"{table_name}_temp.csv"
    
    # Files for removed records
    duplicates_file = duplicates_dir / f"{table_name}.csv"
    invalid_concept_file = invalid_concept_dir / f"{table_name}.csv"
    temporal_issues_file = temporal_issues_dir / f"{table_name}.csv"
    
    # Check if file exists
    if not input_file.exists():
        print(f"Warning: {input_file} not found, skipping...")
        return 0
    
    # Get total row count for progress bar
    total_rows = get_file_row_count(input_file)
    
    # Analyze columns
    headers, columns_to_remove, missing_counts, _, columns_info = analyze_columns_chunked(input_file)
    
    # Add table_name to each column info
    for col_info in columns_info:
        col_info['table_name'] = table_name
    
    # Silent processing - no need to print column removal details
    
    # Define duplicate key columns based on table
    duplicate_key_cols = []
    if table_name == 'MEASUREMENT':
        duplicate_key_cols = ['person_id', 'measurement_concept_id', 'measurement_datetime']
    elif table_name == 'OBSERVATION':
        duplicate_key_cols = ['person_id', 'observation_concept_id', 'observation_datetime']
    elif table_name == 'DRUG_EXPOSURE':
        duplicate_key_cols = ['person_id', 'drug_concept_id', 'drug_exposure_start_datetime']
    elif table_name == 'CONDITION_OCCURRENCE':
        duplicate_key_cols = ['person_id', 'condition_concept_id', 'condition_start_datetime']
    elif table_name == 'PROCEDURE_OCCURRENCE':
        duplicate_key_cols = ['person_id', 'procedure_concept_id', 'procedure_datetime']
    elif table_name == 'VISIT_OCCURRENCE':
        duplicate_key_cols = ['person_id', 'visit_concept_id', 'visit_start_datetime']
    elif table_name == 'VISIT_DETAIL':
        duplicate_key_cols = ['person_id', 'visit_detail_concept_id', 'visit_detail_start_datetime']
    elif table_name == 'DEVICE_EXPOSURE':
        duplicate_key_cols = ['person_id', 'device_concept_id', 'device_exposure_start_datetime']
    elif table_name == 'DRUG_ERA':
        duplicate_key_cols = ['person_id', 'drug_concept_id', 'drug_era_start_date']
    elif table_name == 'OBSERVATION_PERIOD':
        duplicate_key_cols = ['person_id', 'observation_period_start_date']
    elif table_name == 'PERSON':
        duplicate_key_cols = ['person_id']  # Primary key
    
    # Process data in chunks
    
    # Initialize counters
    duplicate_count = 0
    invalid_concept_count = 0
    temporal_issues = 0
    rows_written = 0
    seen_keys = {}  # Changed from set to dict to store first occurrence info
    duplicate_groups = defaultdict(list)  # Store all records in each duplicate group
    
    # For outlier detection (MEASUREMENT table only)
    numeric_values = []
    
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
            with tqdm(total=total_rows, desc=f"Cleaning {table_name}", unit="rows",
                     miniters=max(100, total_rows//100) if total_rows > 0 else 1,  # Update every 1% or at least 100 rows
                     mininterval=PROGRESS_INTERVAL,
                     position=1,  # Nested position to avoid overlap
                     leave=False, ncols=100) as pbar:
                chunk = []
                
                for row_num, row in enumerate(reader):
                    chunk.append((row_num + 1, row))  # Store with 1-based row number
                    
                    # Process chunk when it reaches the desired size
                    if len(chunk) >= CHUNK_SIZE or row_num == total_rows - 1:
                        # Process each row in the chunk
                        for original_row_num, row in chunk:
                            # Check for duplicates
                            skip_row = False
                            removal_reason = None
                            
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
                            
                            if skip_row:
                                continue
                            
                            # Check for invalid concept_id
                            if table_name in PRIMARY_CONCEPT_FIELDS:
                                concept_field = PRIMARY_CONCEPT_FIELDS[table_name]
                                if concept_field in headers:
                                    concept_id = row.get(concept_field, '')
                                    if not concept_id or str(concept_id).strip() == '' or str(concept_id).strip() == '0':
                                        invalid_concept_count += 1
                                        skip_row = True
                                        removal_reason = 'invalid_concept'
                                        # Save invalid concept record
                                        invalid_writer.writerow(row)
                            
                            if skip_row:
                                continue
                            
                            # Temporal validation
                            temporal_invalid = False
                            start = None
                            end = None
                            
                            if table_name == 'DRUG_EXPOSURE':
                                start = row.get('drug_exposure_start_datetime')
                                end = row.get('drug_exposure_end_datetime')
                            elif table_name == 'VISIT_OCCURRENCE':
                                start = row.get('visit_start_datetime')
                                end = row.get('visit_end_datetime')
                            elif table_name == 'VISIT_DETAIL':
                                start = row.get('visit_detail_start_datetime')
                                end = row.get('visit_detail_end_datetime')
                            elif table_name == 'DEVICE_EXPOSURE':
                                start = row.get('device_exposure_start_datetime')
                                end = row.get('device_exposure_end_datetime')
                            elif table_name == 'DRUG_ERA':
                                start = row.get('drug_era_start_date')
                                end = row.get('drug_era_end_date')
                            elif table_name == 'OBSERVATION_PERIOD':
                                start = row.get('observation_period_start_date')
                                end = row.get('observation_period_end_date')
                            elif table_name == 'CONDITION_ERA':
                                start = row.get('condition_era_start_date')
                                end = row.get('condition_era_end_date')
                            elif table_name == 'CONDITION_OCCURRENCE':
                                start = row.get('condition_start_datetime')
                                end = row.get('condition_end_datetime')
                            elif table_name == 'PROCEDURE_OCCURRENCE':
                                start = row.get('procedure_datetime')
                                end = row.get('procedure_end_datetime')
                            
                            if start and end and end < start:
                                temporal_invalid = True
                                temporal_issues += 1
                                skip_row = True
                                removal_reason = 'temporal_issue'
                                
                                # Save temporal issue record
                                temporal_row = row.copy()
                                temporal_row['temporal_issue_reason'] = f"End datetime ({end}) is before start datetime ({start})"
                                temporal_row['start_datetime'] = start
                                temporal_row['end_datetime'] = end
                                temporal_row['original_row_number'] = original_row_num
                                temporal_writer.writerow(temporal_row)
                            
                            if skip_row:
                                continue
                            
                            # Collect numeric values for outlier detection (MEASUREMENT only)
                            if table_name == 'MEASUREMENT' and row.get('value_as_number'):
                                try:
                                    val = float(row['value_as_number'])
                                    numeric_values.append(val)
                                except:
                                    pass
                            
                            # Write cleaned row
                            clean_row = {k: v for k, v in row.items() if k not in columns_to_remove}
                            writer.writerow(clean_row)
                            rows_written += 1
                        
                        # Update progress
                        pbar.update(len(chunk))
                        
                        # Clear chunk
                        chunk = []
    
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
    
    # Silent processing
    
    # Calculate outlier statistics for MEASUREMENT table
    outlier_stats = {}
    if table_name == 'MEASUREMENT' and numeric_values:
        pass  # Calculate silently
        numeric_values.sort()
        q1 = numeric_values[len(numeric_values)//4]
        q3 = numeric_values[3*len(numeric_values)//4]
        iqr = q3 - q1
        lower_bound = q1 - 3 * iqr
        upper_bound = q3 + 3 * iqr
        
        outlier_count = sum(1 for v in numeric_values if v < lower_bound or v > upper_bound)
        outlier_stats['value_as_number'] = {
            'total_values': len(numeric_values),
            'outliers': outlier_count,
            'outlier_pct': outlier_count / len(numeric_values) * 100 if numeric_values else 0,
            'q1': q1,
            'q3': q3,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound
        }
        
        # Silent processing
    
    # Silent summary - details saved to results file
    rows_removed = total_rows - rows_written
    
    # Store results
    cleaning_results["tables"][table_name] = {
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
        "outlier_stats": outlier_stats,
        "output_file": str(output_file),
        "removed_records_files": {
            "duplicates": str(duplicates_file),
            "invalid_concept_id": str(invalid_concept_file),
            "temporal_issues": str(temporal_issues_file)
        },
        "removed_columns_details": columns_info
    }
    
    # Silent completion
    
    return rows_written

# Process each key table
print("\nStarting table cleaning process...")
total_original = 0
total_cleaned = 0

# Process tables with progress tracking
print("\nCleaning tables...")
for i, table in enumerate(KEY_TABLES):
    try:
        print(f"  [{i+1}/{len(KEY_TABLES)}] Processing {table}...")
        cleaned_count = clean_table(table)
        total_cleaned += cleaned_count
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
            
            if stats['outlier_stats']:
                f.write("**Outlier Detection**:\n")
                for col, outlier_info in stats['outlier_stats'].items():
                    f.write(f"- {col}: {outlier_info['outliers']:,} outliers ")
                    f.write(f"({outlier_info['outlier_pct']:.1f}% of {outlier_info['total_values']:,} values)\n")
                f.write("\n")
            
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

print("="*70)
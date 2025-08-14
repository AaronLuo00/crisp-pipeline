#!/usr/bin/env python
"""Run concept mapping using processed SNOMED mappings for cleaned tables with deduplication."""

import csv
import json
import logging
import argparse
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from tqdm import tqdm
import re

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
outliers_dir = removed_dir / "outliers"
outliers_dir.mkdir(parents=True, exist_ok=True)
episodes_dir = removed_dir / "visit_episodes"
episodes_dir.mkdir(parents=True, exist_ok=True)

# Setup logging
log_file = output_dir / f"mapping_process_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
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

# Define date/datetime columns for standardization
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
                         'visit_end_date', 'visit_end_datetime']
}

class ConceptMapper:
    def __init__(self, enable_dedup=True, min_concept_freq=10, 
                 outlier_lower_pct=0.5, outlier_upper_pct=0.5,
                 episode_window_hours=2):
        self.stats = defaultdict(lambda: defaultdict(int))
        self.enable_dedup = enable_dedup
        self.min_concept_freq = min_concept_freq
        self.outlier_lower_pct = outlier_lower_pct
        self.outlier_upper_pct = outlier_upper_pct
        self.episode_window_hours = episode_window_hours
        self.concept_frequencies = {}  # Store concept frequencies across all tables
        self.all_table_data = {}  # Store all table data for processing
        
    def count_concept_frequencies(self):
        """Count concept frequencies across all tables."""
        logging.info("="*60)
        logging.info("Counting concept frequencies across all tables...")
        
        for table_name in ALL_TABLES:
            logging.info(f"\nProcessing {table_name}...")
            input_file = input_dir / f"{table_name}_cleaned.csv"
            
            if not input_file.exists():
                logging.warning(f"Input file not found: {input_file}")
                continue
            
            # Count concepts for this table
            concept_counter = Counter()
            
            with open(input_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    for concept_col in CONCEPT_COLUMNS.get(table_name, []):
                        concept_value = row.get(concept_col, '').strip()
                        if concept_value and concept_value != '':
                            concept_counter[concept_value] += 1
            
            # Store frequencies
            self.concept_frequencies[table_name] = concept_counter
            logging.info(f"  Found {len(concept_counter)} unique concepts")
            
            # Log statistics
            freq_stats = Counter()
            for count in concept_counter.values():
                if count <= self.min_concept_freq:
                    freq_stats['low_frequency'] += 1
                else:
                    freq_stats['high_frequency'] += 1
            
            logging.info(f"  Low frequency (<={self.min_concept_freq}): {freq_stats['low_frequency']}")
            logging.info(f"  High frequency (>{self.min_concept_freq}): {freq_stats['high_frequency']}")
    
    def standardize_date(self, date_str):
        """Standardize date/datetime string to consistent format."""
        if not date_str or date_str.strip() == '':
            return date_str
        
        date_str = date_str.strip()
        
        # Common datetime patterns
        datetime_patterns = [
            ('%Y-%m-%d %H:%M:%S', True),
            ('%Y-%m-%d %H:%M:%S.%f', True),
            ('%Y-%m-%d %H:%M', True),
            ('%Y/%m/%d %H:%M:%S', True),
            ('%Y-%m-%d', False),
            ('%Y/%m/%d', False),
            ('%m/%d/%Y', False),
            ('%d/%m/%Y', False)
        ]
        
        for pattern, has_time in datetime_patterns:
            try:
                dt = datetime.strptime(date_str, pattern)
                if has_time:
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # If no pattern matches, return original
        logging.debug(f"Could not parse date: {date_str}")
        return date_str
    
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
        """Remove duplicates based on person_id + mapped_concept_id + datetime.
        For MEASUREMENT table, prioritize records with value_as_number."""
        if not self.enable_dedup:
            return rows, []
        
        concept_col = CONCEPT_COLUMNS[table_name][0]
        datetime_col = DATETIME_COLUMNS[table_name]
        id_col = ID_COLUMNS[table_name]
        
        # Collect all records by dedup key
        dedup_groups = {}
        
        for idx, row in enumerate(rows):
            # Create deduplication key
            person_id = row.get('person_id', '')
            concept_id = row.get(concept_col, '')
            datetime_val = row.get(datetime_col, '')
            
            dedup_key = f"{person_id}|{concept_id}|{datetime_val}"
            original_row_num = idx + 2  # +2 for header and 1-based indexing
            
            # Add row info to the group
            row_info = {
                'row': row,
                'original_row_number': original_row_num,
                'index': idx
            }
            
            if dedup_key not in dedup_groups:
                dedup_groups[dedup_key] = []
            dedup_groups[dedup_key].append(row_info)
        
        # Process groups and select which record to keep
        unique_rows = []
        all_duplicate_records = []
        group_counter = 0
        
        for dedup_key, group_rows in dedup_groups.items():
            if len(group_rows) == 1:
                # No duplicates - just keep the single row
                unique_rows.append(group_rows[0]['row'])
            else:
                # Multiple records - need to decide which to keep
                group_counter += 1
                group_id = f"{table_name}_{group_counter}"
                
                # For MEASUREMENT table, prioritize records with value_as_number
                if table_name == 'MEASUREMENT':
                    # Sort to prioritize records with value_as_number
                    # First, records with non-empty value_as_number
                    # Then, by original order
                    sorted_rows = sorted(group_rows, key=lambda x: (
                        # Priority 1: Has value_as_number (True sorts before False)
                        not bool(x['row'].get('value_as_number') and 
                                str(x['row'].get('value_as_number')).strip()),
                        # Priority 2: Original order
                        x['index']
                    ))
                else:
                    # For other tables, keep original order (first occurrence)
                    sorted_rows = group_rows
                
                # First record in sorted list is kept
                kept_idx = 0
                
                # Process all records in the group
                for i, row_info in enumerate(sorted_rows):
                    dup_row = row_info['row'].copy()
                    dup_row['duplicate_group_id'] = group_id
                    dup_row['original_row_number'] = row_info['original_row_number']
                    
                    if i == kept_idx:
                        # This is the kept record
                        dup_row['duplicate_status'] = 'kept'
                        unique_rows.append(row_info['row'])
                    else:
                        # This is a removed record
                        dup_row['duplicate_status'] = 'removed'
                    
                    all_duplicate_records.append(dup_row)
        
        # Sort duplicate records by group_id for better readability
        all_duplicate_records.sort(key=lambda x: (x['duplicate_group_id'], x['original_row_number']))
        
        return unique_rows, all_duplicate_records
    
    def remove_outliers_measurement(self, rows):
        """Remove outliers from MEASUREMENT table based on value_as_number percentiles."""
        if not rows:
            return rows, []
        
        # Group measurements by concept_id
        concept_measurements = defaultdict(list)
        row_indices = defaultdict(list)
        
        for idx, row in enumerate(rows):
            concept_id = row.get('measurement_concept_id', '')
            value_str = row.get('value_as_number', '')
            
            if concept_id and value_str and value_str.strip():
                try:
                    value = float(value_str)
                    concept_measurements[concept_id].append(value)
                    row_indices[concept_id].append(idx)
                except ValueError:
                    continue
        
        # Calculate percentiles and identify outliers
        outlier_indices = set()
        outlier_stats = {}
        
        for concept_id, values in concept_measurements.items():
            if len(values) < 10:  # Skip concepts with too few measurements
                continue
            
            values_array = np.array(values)
            lower_percentile = np.percentile(values_array, self.outlier_lower_pct)
            upper_percentile = np.percentile(values_array, 100 - self.outlier_upper_pct)
            
            # Track statistics
            outlier_stats[concept_id] = {
                'total': len(values),
                'lower_threshold': lower_percentile,
                'upper_threshold': upper_percentile,
                'removed_lower': 0,
                'removed_upper': 0
            }
            
            # Identify outliers
            indices = row_indices[concept_id]
            for i, (value, row_idx) in enumerate(zip(values, indices)):
                if value < lower_percentile:
                    outlier_indices.add(row_idx)
                    outlier_stats[concept_id]['removed_lower'] += 1
                elif value > upper_percentile:
                    outlier_indices.add(row_idx)
                    outlier_stats[concept_id]['removed_upper'] += 1
        
        # Separate outliers from clean data
        clean_rows = []
        outlier_rows = []
        
        for idx, row in enumerate(rows):
            if idx in outlier_indices:
                outlier_row = row.copy()
                concept_id = row.get('measurement_concept_id', '')
                value = row.get('value_as_number', '')
                
                if concept_id in outlier_stats:
                    stats = outlier_stats[concept_id]
                    if float(value) < stats['lower_threshold']:
                        outlier_row['removal_reason'] = f'outlier_low (value={value}, threshold={stats["lower_threshold"]:.2f})'
                    else:
                        outlier_row['removal_reason'] = f'outlier_high (value={value}, threshold={stats["upper_threshold"]:.2f})'
                else:
                    outlier_row['removal_reason'] = 'outlier'
                
                outlier_row['original_row_number'] = idx + 2
                outlier_rows.append(outlier_row)
            else:
                clean_rows.append(row)
        
        # Log statistics
        total_outliers = len(outlier_rows)
        if total_outliers > 0:
            logging.info(f"\nOutlier removal statistics:")
            logging.info(f"  Total outliers removed: {total_outliers}")
            logging.info(f"  Concepts analyzed: {len(outlier_stats)}")
            
            for concept_id, stats in list(outlier_stats.items())[:5]:  # Show top 5
                logging.info(f"  Concept {concept_id}: {stats['removed_lower']} low, {stats['removed_upper']} high outliers")
        
        return clean_rows, outlier_rows
    
    def standardize_dates_in_rows(self, rows, table_name):
        """Standardize all date/datetime columns in rows."""
        date_cols = DATE_COLUMNS.get(table_name, [])
        
        for row in rows:
            for date_col in date_cols:
                if date_col in row:
                    row[date_col] = self.standardize_date(row[date_col])
        
        return rows
    
    def process_table(self, table_name):
        """Process a table with all features: filtering, mapping, standardization, etc."""
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
            'outliers_removed': 0,
            'duplicates_removed': 0,
            'concept_columns': {},
            'total_mappings_applied': 0
        }
        
        # Read input file
        with open(input_file, 'r') as infile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            rows = list(reader)
        
        table_stats['total_rows'] = len(rows)
        logging.info(f"Loaded {len(rows):,} rows")
        
        # Step 1: Filter low frequency concepts
        if self.min_concept_freq > 0:
            logging.info(f"\nFiltering low frequency concepts (<={self.min_concept_freq})...")
            rows, removed_low_freq = self.filter_low_frequency_concepts(rows, table_name)
            table_stats['low_freq_removed'] = len(removed_low_freq)
            
            if removed_low_freq:
                # Save removed records
                low_freq_file = low_freq_dir / f"{table_name}.csv"
                with open(low_freq_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames + ['removal_reason', 'original_row_number'])
                    writer.writeheader()
                    writer.writerows(removed_low_freq)
                logging.info(f"  Removed {len(removed_low_freq)} low frequency records")
        
        # Step 2: Standardize dates
        logging.info("\nStandardizing dates...")
        rows = self.standardize_dates_in_rows(rows, table_name)
        
        # Step 3: Apply SNOMED mappings (only for tables with mappings)
        if table_name in TABLES_WITH_MAPPING:
            extended_fieldnames = self.apply_mappings(rows, table_name, fieldnames, table_stats)
        else:
            extended_fieldnames = fieldnames
        
        # Step 4: Remove outliers (MEASUREMENT only)
        if table_name == 'MEASUREMENT' and self.outlier_lower_pct > 0:
            logging.info(f"\nRemoving outliers ({self.outlier_lower_pct}% lower, {self.outlier_upper_pct}% upper)...")
            rows, outlier_rows = self.remove_outliers_measurement(rows)
            table_stats['outliers_removed'] = len(outlier_rows)
            
            if outlier_rows:
                # Save outlier records
                outlier_file = outliers_dir / f"{table_name}.csv"
                with open(outlier_file, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=extended_fieldnames + ['removal_reason', 'original_row_number'])
                    writer.writeheader()
                    writer.writerows(outlier_rows)
                logging.info(f"  Removed {len(outlier_rows)} outliers")
        
        # Step 5: Deduplication (only for mapped tables)
        if self.enable_dedup and table_name in TABLES_WITH_MAPPING:
            logging.info("\nPerforming deduplication...")
            unique_rows, duplicate_rows = self.deduplicate_mapped_data(rows, table_name, extended_fieldnames)
            
            # Count only removed duplicates
            removed_count = sum(1 for row in duplicate_rows if row['duplicate_status'] == 'removed')
            table_stats['duplicates_removed'] = removed_count
            table_stats['output_rows'] = len(unique_rows)
            
            logging.info(f"  Duplicates found and removed: {removed_count:,}")
            
            if duplicate_rows:
                duplicates_file = duplicates_dir / f"{table_name}.csv"
                dup_fieldnames = extended_fieldnames + ['duplicate_status', 'duplicate_group_id', 'original_row_number']
                with open(duplicates_file, 'w', newline='') as dupfile:
                    writer = csv.DictWriter(dupfile, fieldnames=dup_fieldnames)
                    writer.writeheader()
                    writer.writerows(duplicate_rows)
                logging.info(f"  Saved duplicate records to: {duplicates_file}")
            
            rows_to_save = unique_rows
        else:
            rows_to_save = rows
            table_stats['output_rows'] = len(rows)
        
        # Save processed data
        with open(output_file, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=extended_fieldnames)
            writer.writeheader()
            writer.writerows(rows_to_save)
        
        logging.info(f"\nSaved processed data to: {output_file}")
        logging.info(f"Final row count: {table_stats['output_rows']:,}")
        
        # Store statistics
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
            
            # Process each row
            for row in tqdm(rows, desc=f"Mapping {col}"):
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
        f.write(f"- **Outlier removal**: {mapper.outlier_lower_pct}% lower, {mapper.outlier_upper_pct}% upper\n")
        f.write(f"- **Deduplication**: {'Enabled' if mapper.enable_dedup else 'Disabled'}\n")
        f.write(f"- **Visit episode window**: {mapper.episode_window_hours} hours\n\n")
        
        f.write("## Summary Statistics\n\n")
        
        total_low_freq = sum(t.get('low_freq_removed', 0) for t in stats.values())
        total_outliers = sum(t.get('outliers_removed', 0) for t in stats.values())
        total_mappings = sum(t.get('total_mappings_applied', 0) for t in stats.values())
        total_duplicates = sum(t.get('duplicates_removed', 0) for t in stats.values())
        
        f.write(f"**Total low frequency records removed**: {total_low_freq:,}\n")
        f.write(f"**Total outliers removed**: {total_outliers:,}\n")
        f.write(f"**Total mappings applied**: {total_mappings:,}\n")
        f.write(f"**Total duplicates removed**: {total_duplicates:,}\n\n")
        
        f.write("| Table | Input Rows | Low Freq Removed | Outliers Removed | Mappings Applied | Duplicates Removed | Output Rows |\n")
        f.write("|-------|------------|------------------|------------------|------------------|-------------------|-------------|\n")
        
        for table, table_stats in stats.items():
            input_rows = table_stats['total_rows']
            output_rows = table_stats.get('output_rows', input_rows)
            low_freq = table_stats.get('low_freq_removed', 0)
            outliers = table_stats.get('outliers_removed', 0)
            mappings = table_stats.get('total_mappings_applied', 0)
            duplicates = table_stats.get('duplicates_removed', 0)
            
            f.write(f"| {table} | {input_rows:,} | {low_freq:,} | {outliers:,} | {mappings:,} | {duplicates:,} | {output_rows:,} |\n")
        
        f.write("\n## Detailed Column Statistics\n\n")
        
        for table, table_stats in stats.items():
            f.write(f"### {table}\n\n")
            
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
        f.write("- **Date standardization**: Formats all dates consistently (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)\n")
        f.write("- **SNOMED mapping**: Applied to MEASUREMENT, OBSERVATION, PROCEDURE_OCCURRENCE, DEVICE_EXPOSURE\n")
        f.write("- **Outlier removal**: Applied to MEASUREMENT table based on value_as_number percentiles\n")
        f.write("- **Deduplication**: Based on person_id + mapped_concept_id + datetime\n")
        f.write("- All removed records are saved for traceability in the removed_records subdirectories\n")
    
    logging.info(f"Report saved to: {report_path}")

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Comprehensive data processing with concept mapping')
    parser.add_argument('--no-dedup', action='store_true', 
                        help='Disable deduplication after mapping')
    parser.add_argument('--min-concept-freq', type=int, default=10,
                        help='Minimum concept frequency threshold (default: 10)')
    parser.add_argument('--outlier-lower-pct', type=float, default=0.5,
                        help='Lower percentile for outlier removal (default: 0.5%%)')
    parser.add_argument('--outlier-upper-pct', type=float, default=0.5,
                        help='Upper percentile for outlier removal (default: 0.5%%)')
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
    print(f"  Date standardization: ENABLED")
    print(f"  Outlier removal (MEASUREMENT): {args.outlier_lower_pct}% lower, {args.outlier_upper_pct}% upper")
    print(f"  Deduplication: {'DISABLED' if args.no_dedup else 'ENABLED'}")
    print(f"  Visit episode window: {args.episode_window_hours} hours")
    print(f"\nTables to process: {', '.join(ALL_TABLES)}")
    print("="*80 + "\n")
    
    # Initialize mapper with all parameters
    mapper = ConceptMapper(
        enable_dedup=not args.no_dedup,
        min_concept_freq=args.min_concept_freq,
        outlier_lower_pct=args.outlier_lower_pct,
        outlier_upper_pct=args.outlier_upper_pct,
        episode_window_hours=args.episode_window_hours
    )
    
    # Step 1: Count concept frequencies across all tables
    mapper.count_concept_frequencies()
    
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
    
    print("\n" + "="*80)
    print("PROCESSING COMPLETED SUCCESSFULLY")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nOutputs saved to: {output_dir}")
    print(f"Removed records saved to: {removed_dir}")
    print("="*80)

if __name__ == "__main__":
    main()
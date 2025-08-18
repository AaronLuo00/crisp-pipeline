#!/usr/bin/env python
"""Run data standardization with outlier removal, low frequency filtering, and visit merging."""

import csv
import json
import argparse
import numpy as np
import pandas as pd
import platform
import time
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from tqdm import tqdm
import logging
import warnings
from visit_concept_merger import VisitConceptMerger

# Platform-specific settings for performance optimization
if platform.system() == 'Windows':
    CHUNK_SIZE = 500000  # Larger chunks for Windows (better I/O performance)
    PROGRESS_INTERVAL = 30.0  # Less frequent updates (reduce overhead)
else:
    CHUNK_SIZE = 100000  # Default for macOS/Linux
    PROGRESS_INTERVAL = 10.0

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')  # No milliseconds

# Suppress warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

# Setup
base_dir = Path(__file__).parent
project_root = base_dir.parent.parent  # Go up to crisp_pipeline_code
data_dir = project_root / "data"
cleaning_dir = project_root / "output" / "2_cleaning"
mapping_dir = project_root / "output" / "3_mapping"
output_dir = project_root / "output" / "4_standardization"

# Create output directories
removed_dir = output_dir / "removed_records"
outliers_percentile_dir = removed_dir / "outliers_percentile"
outliers_range_dir = removed_dir / "outliers_range"
low_frequency_dir = removed_dir / "low_frequency"
merged_visits_dir = output_dir / "merged_visits"
changes_dir = output_dir / "standardization_changes"

# Create all directories
for dir_path in [output_dir, outliers_percentile_dir, outliers_range_dir, 
                 low_frequency_dir, merged_visits_dir, changes_dir]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Tables that have been mapped to SNOMED
MAPPED_TABLES = ['MEASUREMENT', 'OBSERVATION', 'PROCEDURE_OCCURRENCE', 'DEVICE_EXPOSURE']

# All tables to standardize (including visit tables)
TABLES_TO_STANDARDIZE = ['MEASUREMENT', 'OBSERVATION', 'PROCEDURE_OCCURRENCE', 
                        'DEVICE_EXPOSURE', 'DRUG_EXPOSURE', 
                        'VISIT_DETAIL', 'VISIT_OCCURRENCE',
                        'CONDITION_ERA', 'CONDITION_OCCURRENCE', 
                        'DRUG_ERA', 'SPECIMEN']

# Visit tables that need merging (subset of TABLES_TO_STANDARDIZE)
VISIT_TABLES = ['VISIT_DETAIL', 'VISIT_OCCURRENCE']

# Concept-specific reasonable ranges
# Using SNOMED IDs for concepts that have been mapped, keeping original IDs for unmapped concepts
CONCEPT_RANGES = {
    # Glucose measurements
    4151414: {'name': 'glucose_urine', 'min': 0, 'max': 500, 'unit': 'mg/dL'},  # SNOMED for urine glucose
    4018317: {'name': 'glucose_serum', 'min': 0, 'max': 500, 'unit': 'mg/dL'},  # SNOMED for serum/plasma glucose
    3020891: {'name': 'temperature', 'min': 35, 'max': 42, 'unit': 'C'},  # Original LOINC (not mapped)
    37174455: {'name': 'weight', 'min': 0.5, 'max': 300, 'unit': 'kg'},  # SNOMED
    4212065: {'name': 'ESR', 'min': 0, 'max': 150, 'unit': 'mm/hr'},  # SNOMED for ESR (corrected from height)
    4324383: {'name': 'creatinine', 'min': 0, 'max': 20, 'unit': 'mg/dL'},  # SNOMED
    4097430: {'name': 'sodium', 'min': 110, 'max': 170, 'unit': 'mEq/L'},  # SNOMED
    4245152: {'name': 'potassium', 'min': 2, 'max': 8, 'unit': 'mEq/L'},  # SNOMED
    # Adding common height concept
    3036277: {'name': 'height', 'min': 30, 'max': 250, 'unit': 'cm'}  # Body height LOINC concept
}

# Unit conversion mappings
UNIT_CONVERSIONS = {
    # Glucose conversions (using SNOMED IDs)
    'mmol/L': {'target': 'mg/dL', 'factor': 18.0182, 'concepts': [4151414, 4018317]},
    
    # Temperature conversions (using original LOINC ID as not mapped to SNOMED)
    'Fahrenheit': {'target': 'Celsius', 'formula': lambda f: (f - 32) * 5/9, 'concepts': [3020891]},
    
    # Weight conversions (using SNOMED ID)
    'lb': {'target': 'kg', 'factor': 0.453592, 'concepts': [37174455]},
    'lbs': {'target': 'kg', 'factor': 0.453592, 'concepts': [37174455]},
    
    # Height conversions (using LOINC ID as common height concept)
    'in': {'target': 'cm', 'factor': 2.54, 'concepts': [3036277]},
    'inch': {'target': 'cm', 'factor': 2.54, 'concepts': [3036277]},
}

class DataStandardizer:
    def __init__(self, outlier_percentile=99.0, min_concept_frequency=10, 
                 merge_visits=True, merge_threshold_hours=2.0):
        self.outlier_percentile = outlier_percentile
        self.min_concept_frequency = min_concept_frequency
        self.merge_visits = merge_visits
        self.merge_threshold_hours = merge_threshold_hours
        self.concept_thresholds = {}
        self.concept_frequencies = {}
        self.standardization_results = {
            "standardization_date": datetime.now().isoformat(),
            "dataset": "OMOP CDM",
            "parameters": {
                "outlier_percentile": outlier_percentile,
                "min_concept_frequency": min_concept_frequency,
                "merge_visits": merge_visits,
                "merge_threshold_hours": merge_threshold_hours
            },
            "tables": {}
        }
        
    def get_input_path(self, table_name):
        """Get the correct input path based on whether table has been mapped."""
        if table_name in MAPPED_TABLES:
            input_file = mapping_dir / f"{table_name}_mapped.csv"
        else:
            input_file = cleaning_dir / f"{table_name}_cleaned.csv"
        
        return input_file
    
    def standardize_datetime(self, dt_string):
        """Standardize datetime format preserving original precision."""
        if not dt_string:
            return dt_string
        
        # Define formats with their types (format, has_time)
        datetime_formats = [
            ('%Y-%m-%d %H:%M:%S', True),   # Has time with seconds
            ('%Y-%m-%d %H:%M', True),       # Has time without seconds
            ('%m/%d/%Y %H:%M:%S', True),   # US format with time and seconds
            ('%m/%d/%Y %H:%M', True),       # US format with time
            ('%d/%m/%Y %H:%M:%S', True),   # EU format with time and seconds
            ('%d/%m/%Y %H:%M', True),       # EU format with time
            ('%Y-%m-%d', False),            # ISO date only
            ('%m/%d/%Y', False),            # US date only
            ('%d/%m/%Y', False),            # EU date only
            ('%Y/%m/%d', False),            # Alternative date format
        ]
        
        for fmt, has_time in datetime_formats:
            try:
                dt = datetime.strptime(dt_string, fmt)
                if has_time:
                    # Return with time if original had time
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    # Return date only if original was date only
                    return dt.strftime('%Y-%m-%d')
            except:
                continue
        
        return dt_string  # Return original if can't parse
    
    def standardize_units(self, value, unit_concept_id, measurement_concept_id):
        """Standardize measurement units based on concept IDs."""
        if not value:
            return value, unit_concept_id, None
            
        try:
            value = float(value)
        except:
            return value, unit_concept_id, None
        
        # Track conversion
        conversion_info = None
        
        # Check if this measurement type needs unit conversion
        if measurement_concept_id in CONCEPT_RANGES:
            test_type = CONCEPT_RANGES[measurement_concept_id]['name']
            
            # Apply conversions based on unit concept ID
            for unit, conversion in UNIT_CONVERSIONS.items():
                if measurement_concept_id in conversion.get('concepts', []):
                    original_value = value
                    if 'factor' in conversion:
                        value = value * conversion['factor']
                    elif 'formula' in conversion:
                        value = conversion['formula'](value)
                    
                    if value != original_value:
                        conversion_info = {
                            'type': 'unit_conversion',
                            'original_value': original_value,
                            'new_value': value,
                            'conversion': f"{unit} to {conversion['target']}"
                        }
        
        return value, unit_concept_id, conversion_info
    
    def calculate_concept_statistics(self, table_name):
        """Calculate concept frequencies and outlier thresholds."""
        logging.info(f"Calculating concept statistics for {table_name}")
        
        input_file = self.get_input_path(table_name)
        if not input_file.exists():
            logging.warning(f"Input file not found: {input_file}")
            return
        
        # Determine the concept column name
        # Note: The _mapped column is a flag, the actual concept ID is still in the original column
        concept_col = None
        if table_name == 'MEASUREMENT':
            concept_col = 'measurement_concept_id'
        elif table_name == 'OBSERVATION':
            concept_col = 'observation_concept_id'
        elif table_name == 'PROCEDURE_OCCURRENCE':
            concept_col = 'procedure_concept_id'
        elif table_name == 'DEVICE_EXPOSURE':
            concept_col = 'device_concept_id'
        elif table_name == 'DRUG_EXPOSURE':
            concept_col = 'drug_concept_id'
        elif table_name == 'CONDITION_ERA':
            concept_col = 'condition_concept_id'
        elif table_name == 'CONDITION_OCCURRENCE':
            concept_col = 'condition_concept_id'
        elif table_name == 'DRUG_ERA':
            concept_col = 'drug_concept_id'
        elif table_name == 'SPECIMEN':
            concept_col = 'specimen_concept_id'
        elif table_name == 'VISIT_DETAIL':
            concept_col = 'visit_detail_concept_id'
        elif table_name == 'VISIT_OCCURRENCE':
            concept_col = 'visit_concept_id'
        
        if not concept_col:
            return
        
        # Collect values by concept
        concept_values = defaultdict(list)
        concept_counts = Counter()
        
        # Read data to collect values
        df = pd.read_csv(input_file, chunksize=CHUNK_SIZE, low_memory=False)
        chunk_num = 0
        # Process chunks with more frequent progress updates
        for chunk in tqdm(df, desc=f"Standardizing {table_name} (statistics)", 
                         unit='chunks', leave=False, ncols=100,
                         mininterval=PROGRESS_INTERVAL,  # Update at most once per 10 seconds
                         disable=False):  # Enable progress tracking
            chunk_num += 1
            # Count concept frequencies
            if concept_col in chunk.columns:
                # Debug logging for first chunk
                if chunk_num == 1:
                    logging.info(f"Column '{concept_col}' found in {table_name}")
                    logging.info(f"Sample values: {chunk[concept_col].dropna().head(5).tolist()}")
                
                for concept_id in chunk[concept_col].dropna():
                    try:
                        concept_id = int(concept_id)
                        concept_counts[concept_id] += 1
                    except Exception as e:
                        if chunk_num == 1:  # Log error only for first chunk
                            logging.warning(f"Error processing concept_id '{concept_id}': {e}")
                        pass
            else:
                if chunk_num == 1:
                    logging.warning(f"Column '{concept_col}' not found in {table_name}. Available columns: {list(chunk.columns)[:10]}")
            
            # Collect values for outlier detection (only for MEASUREMENT)
            if table_name == 'MEASUREMENT' and 'value_as_number' in chunk.columns:
                for _, row in chunk.iterrows():
                    if pd.notna(row['value_as_number']) and pd.notna(row[concept_col]):
                        try:
                            value = float(row['value_as_number'])
                            concept_id = int(row[concept_col])
                            concept_values[concept_id].append(value)
                        except:
                            pass
        
        # Store concept frequencies
        self.concept_frequencies[table_name] = dict(concept_counts)
        
        # Calculate outlier thresholds for MEASUREMENT
        if table_name == 'MEASUREMENT':
            self.concept_thresholds[table_name] = {}
            for concept_id, values in concept_values.items():
                if len(values) >= self.min_concept_frequency:  # Need sufficient data
                    values_array = np.array(values)
                    lower = np.percentile(values_array, 100 - self.outlier_percentile)
                    upper = np.percentile(values_array, self.outlier_percentile)
                    
                    self.concept_thresholds[table_name][concept_id] = {
                        'lower_percentile': lower,
                        'upper_percentile': upper,
                        'count': len(values),
                        'mean': np.mean(values_array),
                        'std': np.std(values_array)
                    }
                    
                    # Add range limits if defined
                    if concept_id in CONCEPT_RANGES:
                        self.concept_thresholds[table_name][concept_id].update({
                            'range_min': CONCEPT_RANGES[concept_id]['min'],
                            'range_max': CONCEPT_RANGES[concept_id]['max'],
                            'name': CONCEPT_RANGES[concept_id]['name']
                        })
            
            logging.info(f"Calculated thresholds for {len(self.concept_thresholds[table_name])} concepts")
        
        logging.info(f"Found {len(concept_counts)} unique concepts in {table_name}")
        low_freq_concepts = sum(1 for count in concept_counts.values() if count < self.min_concept_frequency)
        logging.info(f"  - {low_freq_concepts} concepts have frequency < {self.min_concept_frequency}")
    
    def check_outlier(self, value, concept_id, table_name):
        """Check if a value is an outlier based on percentile and range."""
        if table_name not in self.concept_thresholds:
            return False, None
        
        if concept_id not in self.concept_thresholds[table_name]:
            return False, None
        
        thresholds = self.concept_thresholds[table_name][concept_id]
        
        # Check percentile-based outliers
        if value < thresholds['lower_percentile'] or value > thresholds['upper_percentile']:
            return True, 'percentile'
        
        # Check range-based outliers
        if 'range_min' in thresholds:
            if value < thresholds['range_min'] or value > thresholds['range_max']:
                return True, 'range'
        
        return False, None
    
    def check_low_frequency(self, concept_id, table_name):
        """Check if a concept has low frequency."""
        if table_name not in self.concept_frequencies:
            return False
        
        frequency = self.concept_frequencies[table_name].get(concept_id, 0)
        return frequency < self.min_concept_frequency
    
    def _process_rows_with_outliers(self, reader, writer, perc_writer, range_writer, 
                                   freq_writer, change_writer, stats, table_name, concept_col, input_file):
        """Process rows for MEASUREMENT table with outlier checking."""
        headers = writer.fieldnames
        
        # Count total rows for progress bar
        with open(input_file, 'r') as f:
            total_rows = sum(1 for _ in f) - 1  # Subtract header
        
        for row_num, row in enumerate(tqdm(reader, desc=f"Standardizing {table_name}",
                                          total=total_rows, unit='rows',
                                          miniters=max(100, total_rows//100),  # Update every 1% or at least 100 rows
                                          mininterval=PROGRESS_INTERVAL,  # Update at most once per 10 seconds
                                          leave=False, ncols=100,
                                          disable=False), 1):  # Enable for all tables
            stats['input_records'] += 1
            new_row = row.copy()
            is_removed = False
            removal_reason = None
            removal_info = {}
            changes = []
            
            # Check for low frequency concepts first
            if concept_col and concept_col in row and row[concept_col]:
                try:
                    concept_id = int(row[concept_col])
                    if self.check_low_frequency(concept_id, table_name):
                        is_removed = True
                        removal_reason = 'low_frequency'
                        removal_info = {
                            'concept_id': concept_id,
                            'frequency': self.concept_frequencies[table_name].get(concept_id, 0),
                            'threshold': self.min_concept_frequency
                        }
                except:
                    pass
            
            # If not removed for low frequency, continue with other checks
            if not is_removed:
                # Standardize datetime fields
                for col in headers:
                    if 'datetime' in col.lower() or 'date' in col.lower():
                        original = row[col]
                        standardized = self.standardize_datetime(original)
                        if original != standardized:
                            stats['datetime_standardized'] += 1
                            new_row[col] = standardized
                            changes.append({
                                'row_number': row_num,
                                'field': col,
                                'original_value': original,
                                'new_value': standardized,
                                'change_type': 'datetime_standardization'
                            })
                
                # MEASUREMENT-specific processing for outliers
                if 'value_as_number' in row and row['value_as_number']:
                    try:
                        value = float(row['value_as_number'])
                        concept_id = int(row.get(concept_col, 0))
                        
                        # Unit conversion
                        unit_id = row.get('unit_concept_id', '')
                        new_value, new_unit_id, conversion_info = self.standardize_units(
                            value, unit_id, concept_id
                        )
                        
                        if conversion_info:
                            stats['units_converted'] += 1
                            new_row['value_as_number'] = str(new_value)
                            changes.append({
                                'row_number': row_num,
                                'field': 'value_as_number',
                                'original_value': value,
                                'new_value': new_value,
                                'change_type': conversion_info['type']
                            })
                            value = new_value
                        
                        # Check for outliers
                        is_outlier, outlier_type = self.check_outlier(value, concept_id, table_name)
                        if is_outlier:
                            is_removed = True
                            removal_reason = f'{outlier_type}_outlier'
                            if outlier_type == 'percentile':
                                thresholds = self.concept_thresholds[table_name][concept_id]
                                removal_info = {
                                    'value': value,
                                    'lower_threshold': thresholds['lower_percentile'],
                                    'upper_threshold': thresholds['upper_percentile'],
                                    'percentile': self.outlier_percentile
                                }
                            elif outlier_type == 'range':
                                thresholds = self.concept_thresholds[table_name][concept_id]
                                removal_info = {
                                    'value': value,
                                    'range_min': thresholds['range_min'],
                                    'range_max': thresholds['range_max'],
                                    'concept_name': thresholds.get('name', 'Unknown')
                                }
                    except:
                        pass
            
            # Write to appropriate file
            if is_removed:
                removed_row = row.copy()
                removed_row['removal_reason'] = removal_reason
                removed_row['original_row_number'] = row_num
                removed_row['additional_info'] = json.dumps(removal_info)
                
                if removal_reason == 'low_frequency':
                    stats['low_frequency_removed'] += 1
                    freq_writer.writerow(removed_row)
                elif removal_reason == 'percentile_outlier':
                    stats['outliers_removed_percentile'] += 1
                    perc_writer.writerow(removed_row)
                elif removal_reason == 'range_outlier':
                    stats['outliers_removed_range'] += 1
                    range_writer.writerow(removed_row)
            else:
                writer.writerow(new_row)
                stats['output_records'] += 1
                
                # Write changes
                for change in changes:
                    change_writer.writerow(change)
    
    def _process_rows_without_outliers(self, reader, writer, freq_writer, 
                                      change_writer, stats, table_name, concept_col, input_file):
        """Process rows for non-MEASUREMENT tables without outlier checking."""
        headers = writer.fieldnames
        
        # Count total rows for progress bar
        with open(input_file, 'r') as f:
            total_rows = sum(1 for _ in f) - 1  # Subtract header
        
        for row_num, row in enumerate(tqdm(reader, desc=f"Standardizing {table_name}",
                                          total=total_rows, unit='rows',
                                          miniters=max(100, total_rows//100),  # Update every 1% or at least 100 rows
                                          mininterval=PROGRESS_INTERVAL,  # Update at most once per 10 seconds
                                          leave=False, ncols=100,
                                          disable=False), 1):  # Enable for all tables
            stats['input_records'] += 1
            new_row = row.copy()
            is_removed = False
            removal_reason = None
            removal_info = {}
            changes = []
            
            # Check for low frequency concepts
            if concept_col and concept_col in row and row[concept_col]:
                try:
                    concept_id = int(row[concept_col])
                    if self.check_low_frequency(concept_id, table_name):
                        is_removed = True
                        removal_reason = 'low_frequency'
                        removal_info = {
                            'concept_id': concept_id,
                            'frequency': self.concept_frequencies[table_name].get(concept_id, 0),
                            'threshold': self.min_concept_frequency
                        }
                except:
                    pass
            
            # If not removed, standardize datetime fields
            if not is_removed:
                for col in headers:
                    if 'datetime' in col.lower() or 'date' in col.lower():
                        original = row[col]
                        standardized = self.standardize_datetime(original)
                        if original != standardized:
                            stats['datetime_standardized'] += 1
                            new_row[col] = standardized
                            changes.append({
                                'row_number': row_num,
                                'field': col,
                                'original_value': original,
                                'new_value': standardized,
                                'change_type': 'datetime_standardization'
                            })
            
            # Write to appropriate file
            if is_removed:
                removed_row = row.copy()
                removed_row['removal_reason'] = removal_reason
                removed_row['original_row_number'] = row_num
                removed_row['additional_info'] = json.dumps(removal_info)
                
                stats['low_frequency_removed'] += 1
                freq_writer.writerow(removed_row)
            else:
                writer.writerow(new_row)
                stats['output_records'] += 1
                
                # Write changes
                for change in changes:
                    change_writer.writerow(change)
    
    def standardize_table(self, table_name):
        """Standardize a single table with outlier removal and low frequency filtering."""
        # Initialize timing
        table_time_stats = {
            'total': 0,
            'concept_statistics': 0,
            'data_processing': 0,
            'file_io': 0
        }
        table_start_time = time.time()
        
        logging.info(f"\n{'='*60}")
        logging.info(f"Standardizing {table_name}")
        logging.info('='*60)
        
        input_file = self.get_input_path(table_name)
        output_file = output_dir / f"{table_name}_standardized.csv"
        
        if not input_file.exists():
            logging.warning(f"Input file not found: {input_file}")
            return 0
        
        # First, calculate statistics
        t0 = time.time()
        self.calculate_concept_statistics(table_name)
        table_time_stats['concept_statistics'] = time.time() - t0
        
        # Determine the concept column name
        # Note: The _mapped column is a flag, the actual concept ID is still in the original column
        concept_col = None
        if table_name == 'MEASUREMENT':
            concept_col = 'measurement_concept_id'
        elif table_name == 'OBSERVATION':
            concept_col = 'observation_concept_id'
        elif table_name == 'PROCEDURE_OCCURRENCE':
            concept_col = 'procedure_concept_id'
        elif table_name == 'DEVICE_EXPOSURE':
            concept_col = 'device_concept_id'
        elif table_name == 'DRUG_EXPOSURE':
            concept_col = 'drug_concept_id'
        elif table_name == 'CONDITION_ERA':
            concept_col = 'condition_concept_id'
        elif table_name == 'CONDITION_OCCURRENCE':
            concept_col = 'condition_concept_id'
        elif table_name == 'DRUG_ERA':
            concept_col = 'drug_concept_id'
        elif table_name == 'SPECIMEN':
            concept_col = 'specimen_concept_id'
        elif table_name == 'VISIT_DETAIL':
            concept_col = 'visit_detail_concept_id'
        elif table_name == 'VISIT_OCCURRENCE':
            concept_col = 'visit_concept_id'
        
        # Open files for removed records
        low_frequency_file = low_frequency_dir / f"{table_name}_low_frequency.csv"
        changes_file = changes_dir / f"{table_name}_changes.csv"
        
        # Only create outlier files for MEASUREMENT table
        if table_name == 'MEASUREMENT':
            outliers_percentile_file = outliers_percentile_dir / f"{table_name}_percentile_outliers.csv"
            outliers_range_file = outliers_range_dir / f"{table_name}_range_outliers.csv"
        
        # Track statistics
        stats = {
            'input_records': 0,
            'output_records': 0,
            'datetime_standardized': 0,
            'units_converted': 0,
            'outliers_removed_percentile': 0,
            'outliers_removed_range': 0,
            'low_frequency_removed': 0,
            'values_normalized': 0
        }
        
        # Process file
        t0 = time.time()
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            headers = reader.fieldnames
            
            # Prepare output files
            # For MEASUREMENT table, open all files including outlier files
            if table_name == 'MEASUREMENT':
                with open(output_file, 'w', encoding='utf-8', newline='') as outfile, \
                     open(outliers_percentile_file, 'w', encoding='utf-8', newline='') as perc_file, \
                     open(outliers_range_file, 'w', encoding='utf-8', newline='') as range_file, \
                     open(low_frequency_file, 'w', encoding='utf-8', newline='') as freq_file, \
                     open(changes_file, 'w', encoding='utf-8', newline='') as change_file:
                    
                    # Writers
                    writer = csv.DictWriter(outfile, fieldnames=headers)
                    writer.writeheader()
                    
                    removed_headers = headers + ['removal_reason', 'original_row_number', 'additional_info']
                    perc_writer = csv.DictWriter(perc_file, fieldnames=removed_headers)
                    perc_writer.writeheader()
                    
                    range_writer = csv.DictWriter(range_file, fieldnames=removed_headers)
                    range_writer.writeheader()
                    
                    freq_writer = csv.DictWriter(freq_file, fieldnames=removed_headers)
                    freq_writer.writeheader()
                    
                    change_headers = ['row_number', 'field', 'original_value', 'new_value', 'change_type']
                    change_writer = csv.DictWriter(change_file, fieldnames=change_headers)
                    change_writer.writeheader()
                    
                    # Process rows with outlier checking
                    self._process_rows_with_outliers(reader, writer, perc_writer, range_writer, 
                                                   freq_writer, change_writer, stats, 
                                                   table_name, concept_col, input_file)
            else:
                # For other tables, only open necessary files (no outlier files)
                with open(output_file, 'w', encoding='utf-8', newline='') as outfile, \
                     open(low_frequency_file, 'w', encoding='utf-8', newline='') as freq_file, \
                     open(changes_file, 'w', encoding='utf-8', newline='') as change_file:
                    
                    # Writers
                    writer = csv.DictWriter(outfile, fieldnames=headers)
                    writer.writeheader()
                    
                    removed_headers = headers + ['removal_reason', 'original_row_number', 'additional_info']
                    freq_writer = csv.DictWriter(freq_file, fieldnames=removed_headers)
                    freq_writer.writeheader()
                    
                    change_headers = ['row_number', 'field', 'original_value', 'new_value', 'change_type']
                    change_writer = csv.DictWriter(change_file, fieldnames=change_headers)
                    change_writer.writeheader()
                    
                    # Process rows without outlier checking
                    self._process_rows_without_outliers(reader, writer, freq_writer, 
                                                      change_writer, stats, 
                                                      table_name, concept_col, input_file)
        
        # Calculate times
        table_time_stats['data_processing'] = time.time() - t0
        table_time_stats['total'] = time.time() - table_start_time
        table_time_stats['file_io'] = table_time_stats['total'] - table_time_stats['concept_statistics'] - table_time_stats['data_processing']
        
        # Save statistics
        stats['time_stats'] = table_time_stats
        self.standardization_results["tables"][table_name] = stats
        
        logging.info(f"\nStandardization Summary for {table_name}:")
        logging.info(f"  - Input records: {stats['input_records']:,}")
        logging.info(f"  - Output records: {stats['output_records']:,}")
        logging.info(f"  - Records removed: {stats['input_records'] - stats['output_records']:,}")
        logging.info(f"    - Low frequency concepts: {stats['low_frequency_removed']:,}")
        logging.info(f"    - Percentile outliers: {stats['outliers_removed_percentile']:,}")
        logging.info(f"    - Range outliers: {stats['outliers_removed_range']:,}")
        logging.info(f"  - Datetime fields standardized: {stats['datetime_standardized']:,}")
        logging.info(f"  - Units converted: {stats['units_converted']:,}")
        logging.info(f"  - Processing time: {table_time_stats['total']:.2f}s")
        
        return stats['output_records']
    
    def merge_close_visits(self, table_name):
        """Merge temporally close visits using concept-based merging."""
        logging.info(f"\nMerging close visits in {table_name} using concept-based merging")
        
        # Only process VISIT_DETAIL and VISIT_OCCURRENCE tables
        if table_name not in ['VISIT_DETAIL', 'VISIT_OCCURRENCE']:
            logging.info(f"Skipping merge for {table_name} - not a visit table")
            return
        
        # Get the standardized input file
        input_file = output_dir / f"{table_name}_standardized.csv"
        if not input_file.exists():
            # If standardized file doesn't exist, use the cleaned file
            input_file = self.get_input_path(table_name)
            if not input_file.exists():
                logging.warning(f"Input file not found: {input_file}")
                return
        
        logging.info(f"Processing {table_name} from: {input_file}")
        
        # Create merged subdirectory for mapping files
        merged_dir = output_dir / "merged_visit"
        merged_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize the VisitConceptMerger with 60-minute threshold
        merger = VisitConceptMerger(
            threshold_minutes=60,  # Use 60 minutes as per user requirement
            output_dir=merged_dir
        )
        
        # Process the table
        merged_df, mapping_df = merger.process_table(input_file, table_name)
        
        if merged_df.empty:
            logging.warning(f"No data returned from merging {table_name}")
            return
        
        # Save the merged result as the standardized output
        # This overwrites the standardized file with the merged version
        output_file = output_dir / f"{table_name}_standardized.csv"
        merged_df.to_csv(output_file, index=False)
        logging.info(f"Saved merged {table_name} to: {output_file}")
        
        # Save mapping file to merged subdirectory
        if not mapping_df.empty:
            mapping_file = merged_dir / f"{table_name}_merge_mapping.csv"
            mapping_df.to_csv(mapping_file, index=False)
            logging.info(f"Saved merge mappings to: {mapping_file}")
        
        # Save statistics to merged subdirectory
        stats_file = merged_dir / f"{table_name}_merge_statistics.json"
        with open(stats_file, 'w') as f:
            json.dump(merger.statistics, f, indent=2)
        logging.info(f"Saved merge statistics to: {stats_file}")
        
        # Update standardization results
        self.standardization_results["tables"][table_name + "_merging"] = {
            'total_input_records': merger.statistics.get('total_records', len(merged_df)),
            'merged_episodes': merger.statistics['merged_episodes'],
            'unchanged_records': merger.statistics['unchanged_records'],
            'records_merged': merger.statistics['records_merged'],
            'output_records': len(merged_df)
        }
        
        logging.info(f"Merge Summary for {table_name}:")
        logging.info(f"  - Merged episodes: {merger.statistics['merged_episodes']:,}")
        logging.info(f"  - Records merged: {merger.statistics['records_merged']:,}")
        logging.info(f"  - Unchanged records: {merger.statistics['unchanged_records']:,}")
    
    
    def generate_removal_summary(self):
        """Generate summary of all removed records."""
        removal_summary = []
        
        # Collect removal statistics for each table
        for table_name, stats in self.standardization_results["tables"].items():
            if isinstance(stats, dict) and 'input_records' in stats:
                if stats['low_frequency_removed'] > 0:
                    removal_summary.append({
                        'table': table_name,
                        'removal_reason': 'low_frequency',
                        'count': stats['low_frequency_removed'],
                        'percentage': (stats['low_frequency_removed'] / stats['input_records'] * 100) if stats['input_records'] > 0 else 0
                    })
                if stats['outliers_removed_percentile'] > 0:
                    removal_summary.append({
                        'table': table_name,
                        'removal_reason': 'percentile_outlier',
                        'count': stats['outliers_removed_percentile'],
                        'percentage': (stats['outliers_removed_percentile'] / stats['input_records'] * 100) if stats['input_records'] > 0 else 0
                    })
                if stats['outliers_removed_range'] > 0:
                    removal_summary.append({
                        'table': table_name,
                        'removal_reason': 'range_outlier',
                        'count': stats['outliers_removed_range'],
                        'percentage': (stats['outliers_removed_range'] / stats['input_records'] * 100) if stats['input_records'] > 0 else 0
                    })
        
        if removal_summary:
            summary_df = pd.DataFrame(removal_summary)
            summary_df.to_csv(removed_dir / 'removal_summary.csv', index=False)
            logging.info("Generated removal summary")
    
    def run(self):
        """Run the complete standardization process."""
        logging.info("Starting data standardization process...")
        logging.info(f"Parameters: outlier_percentile={self.outlier_percentile}, "
                    f"min_concept_frequency={self.min_concept_frequency}, "
                    f"merge_visits={self.merge_visits}, merge_threshold={self.merge_threshold_hours}h")
        
        # Print input information
        print("\n" + "="*70)
        print("DATA STANDARDIZATION PROCESS - INPUT INFORMATION")
        print("="*70)
        print(f"Cleaning Directory: {cleaning_dir}")
        print(f"Mapping Directory: {mapping_dir}")
        print(f"Output Directory: {output_dir}")
        print("\nTables to standardize:")
        for table in TABLES_TO_STANDARDIZE:
            input_file = self.get_input_path(table)
            source = "mapped" if table in MAPPED_TABLES else "cleaned"
            print(f"  - {table}: {input_file} (from {source})")
        
        if self.merge_visits:
            print("\nVisit tables for merging:")
            for table in VISIT_TABLES:
                input_file = self.get_input_path(table)
                print(f"  - {table}: {input_file}")
        print("="*70 + "\n")
        
        # Process main tables
        for table in TABLES_TO_STANDARDIZE:
            try:
                logging.info(f"Processing table: {table}")
                self.standardize_table(table)
            except Exception as e:
                logging.error(f"Error standardizing {table}: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())
                self.standardization_results["tables"][table] = {"error": str(e)}
        
        # Process visit merging
        if self.merge_visits:
            for table in VISIT_TABLES:
                try:
                    self.merge_close_visits(table)
                except Exception as e:
                    logging.error(f"Error merging visits in {table}: {str(e)}")
        
        # Generate reports
        self.generate_removal_summary()
        
        # Save results
        results_path = output_dir / "standardization_results.json"
        with open(results_path, 'w') as f:
            json.dump(self.standardization_results, f, indent=2)
        
        # Generate markdown report
        self.generate_markdown_report()
        
        # Print comprehensive summary
        print("\n" + "="*70)
        print("DATA STANDARDIZATION PROCESS - SUMMARY")
        print("="*70)
        
        # Calculate totals
        total_input = 0
        total_output = 0
        total_removed = 0
        total_low_freq = 0
        total_outliers = 0
        total_visits_merged = 0
        tables_modified = []
        
        for table, stats in self.standardization_results["tables"].items():
            if isinstance(stats, dict) and 'error' not in stats:
                if 'input_records' in stats:  # Main standardization tables
                    total_input += stats['input_records']
                    total_output += stats['output_records']
                    total_low_freq += stats.get('low_frequency_removed', 0)
                    total_outliers += stats.get('outliers_removed_percentile', 0) + stats.get('outliers_removed_range', 0)
                    if stats['input_records'] != stats['output_records']:
                        tables_modified.append(table)
                elif 'visits_merged' in stats:  # Visit merging tables
                    total_visits_merged += stats['visits_merged']
                    if stats['visits_merged'] > 0:
                        tables_modified.append(table.replace('_merging', '') + ' (visits merged)')
        
        total_removed = total_input - total_output
        
        print("\nTables Modified:")
        for table in tables_modified:
            print(f"  - {table}")
        
        print(f"\nTotal Records:")
        print(f"  - Input: {total_input:,}")
        print(f"  - Output: {total_output:,}")
        if total_input > 0:
            print(f"  - Removed: {total_removed:,} ({total_removed/total_input*100:.1f}%)")
        else:
            print("  - Removed: 0 (0.0%)")
        
        print(f"\nRecords Removed by Type:")
        print(f"  - Low frequency concepts: {total_low_freq:,}")
        print(f"  - Outliers: {total_outliers:,}")
        
        if self.merge_visits:
            print(f"\nVisit Merging:")
            print(f"  - Visits merged: {total_visits_merged:,}")
        
        print("\nOutput Files Created:")
        # List key output files
        print("  Standardized data:")
        for table in TABLES_TO_STANDARDIZE:
            output_file = output_dir / f"{table}_standardized.csv"
            if output_file.exists():
                print(f"    - {output_file.name}")
        
        print("\n  Removed records:")
        for subdir in ['outliers_percentile', 'outliers_range', 'low_frequency']:
            dir_path = removed_dir / subdir
            if dir_path.exists():
                files = list(dir_path.glob('*.csv'))
                if files:
                    print(f"    {subdir}:")
                    for f in files:
                        print(f"      - {f.name}")
        
        print("\n  Reports:")
        print(f"    - {output_dir / 'standardization_results.json'}")
        print(f"    - {output_dir / 'standardization_report.md'}")
        
        print("="*70)
        
        logging.info("Standardization process completed!")
    
    def generate_markdown_report(self):
        """Generate comprehensive markdown report."""
        report_path = output_dir / "standardization_report.md"
        
        with open(report_path, 'w') as f:
            f.write("# Data Standardization Report\n\n")
            f.write(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Dataset**: OMOP CDM\n")
            f.write(f"**Outlier Percentile**: {self.outlier_percentile}%\n")
            f.write(f"**Minimum Concept Frequency**: {self.min_concept_frequency}\n")
            f.write(f"**Visit Merge Threshold**: {self.merge_threshold_hours} hours\n\n")
            
            f.write("## Standardization Summary\n\n")
            f.write("| Table | Input Records | Output Records | Low Freq Removed | Outliers Removed | Total Removed |\n")
            f.write("|-------|---------------|----------------|------------------|------------------|---------------|\n")
            
            total_input = 0
            total_output = 0
            total_removed = 0
            
            for table, stats in self.standardization_results["tables"].items():
                if isinstance(stats, dict) and 'error' not in stats and 'input_records' in stats:
                    total_input += stats['input_records']
                    total_output += stats['output_records']
                    outliers = stats['outliers_removed_percentile'] + stats['outliers_removed_range']
                    removed = stats['input_records'] - stats['output_records']
                    total_removed += removed
                    
                    f.write(f"| {table} | {stats['input_records']:,} | {stats['output_records']:,} | ")
                    f.write(f"{stats['low_frequency_removed']:,} | {outliers:,} | {removed:,} |\n")
            
            f.write(f"| **Total** | **{total_input:,}** | **{total_output:,}** | - | - | **{total_removed:,}** |\n")
            
            f.write("\n## Key Features\n\n")
            f.write("### 1. Low Frequency Filtering\n")
            f.write(f"- Removed concepts with frequency < {self.min_concept_frequency}\n")
            f.write("- Applied to all tables with concept IDs\n")
            f.write("- Preserves data quality by removing rare/erroneous concepts\n\n")
            
            f.write("### 2. Outlier Removal\n")
            f.write(f"- Percentile-based: Values beyond {self.outlier_percentile}th percentile\n")
            f.write("- Range-based: Physiologically implausible values\n")
            f.write("- Applied per concept ID to preserve concept-specific distributions\n\n")
            
            f.write("### 3. Visit Merging\n")
            if self.merge_visits:
                f.write(f"- Merged visits within {self.merge_threshold_hours} hours\n")
                f.write("- Created episode identifiers for continuous care\n")
                f.write("- Generated merged data files with traceable mappings\n")
            else:
                f.write("- Visit merging was disabled\n")
            
            f.write("\n### 4. Standardizations Applied\n")
            f.write("- Datetime format: ISO 8601 (YYYY-MM-DD HH:MM:SS)\n")
            f.write("- Unit conversions: glucose, temperature, weight, height\n")
            f.write("- All changes are traceable through change logs\n\n")
            
            f.write("## Data Sources\n\n")
            f.write("- **Mapped tables** (from 3_mapping): ")
            f.write(", ".join(MAPPED_TABLES) + "\n")
            f.write("- **Cleaned tables** (from 2_cleaning): ")
            cleaned_tables = [t for t in TABLES_TO_STANDARDIZE if t not in MAPPED_TABLES]
            f.write(", ".join(cleaned_tables) + "\n\n")
            
            f.write("## Output Structure\n\n")
            f.write("```\n")
            f.write("output/4_standardization/\n")
            f.write("|-- [table]_standardized.csv      # Standardized data\n")
            f.write("|-- removed_records/\n")
            f.write("|   |-- outliers_percentile/     # Percentile-based outliers\n")
            f.write("|   |-- outliers_range/          # Range-based outliers\n")
            f.write("|   |-- low_frequency/           # Low frequency concepts\n")
            f.write("|   +-- removal_summary.csv      # Summary of all removals\n")
            f.write("|-- merged_visits/               # Visit merge information\n")
            f.write("|   |-- [table]_merged.csv       # Merged visit data\n")
            f.write("|   +-- [table]_merge_mapping.csv # Merge mappings\n")
            f.write("+-- standardization_changes/     # All standardization changes\n")
            f.write("```\n")
            
            f.write("\n## Traceability\n\n")
            f.write("All removed records include:\n")
            f.write("- `removal_reason`: Specific reason for removal\n")
            f.write("- `original_row_number`: Row number in input file\n")
            f.write("- `additional_info`: JSON with detailed removal context\n")
        
        logging.info(f"Report saved to: {report_path}")


def main():
    """Main execution function."""
    # Start timing
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Data standardization with outlier removal, low frequency filtering, and visit merging')
    parser.add_argument('--outlier-percentile', type=float, default=99.0,
                        help='Percentile threshold for outlier removal (default: 99)')
    parser.add_argument('--min-concept-frequency', type=int, default=10,
                        help='Minimum concept frequency threshold (default: 10)')
    parser.add_argument('--no-merge-visits', action='store_true',
                        help='Disable visit merging')
    parser.add_argument('--merge-threshold', type=float, default=2.0,
                        help='Hours threshold for visit merging (default: 2)')
    
    args = parser.parse_args()
    
    # Initialize and run standardizer
    standardizer = DataStandardizer(
        outlier_percentile=args.outlier_percentile,
        min_concept_frequency=args.min_concept_frequency,
        merge_visits=not args.no_merge_visits,
        merge_threshold_hours=args.merge_threshold
    )
    
    standardizer.run()
    
    # Calculate total time
    total_time = time.time() - start_time
    print(f"\nTotal execution time: {total_time:.2f} seconds")
    
    # Performance breakdown
    print("\n" + "="*50)
    print("PERFORMANCE BREAKDOWN - Data Standardization")
    print("="*50)
    
    # Aggregate timing from all tables
    total_concept_stats = 0
    total_data_processing = 0
    total_file_io = 0
    
    for table_stats in standardizer.standardization_results.get('tables', {}).values():
        if 'time_stats' in table_stats:
            ts = table_stats['time_stats']
            total_concept_stats += ts.get('concept_statistics', 0)
            total_data_processing += ts.get('data_processing', 0)
            total_file_io += ts.get('file_io', 0)
    
    # Also add visit merging time if available
    visit_merge_time = 0
    if 'visit_merging' in standardizer.standardization_results:
        for merge_stats in standardizer.standardization_results['visit_merging'].values():
            if 'processing_time_seconds' in merge_stats:
                visit_merge_time += merge_stats['processing_time_seconds']
    
    print(f"Concept statistics:    {total_concept_stats:.2f}s ({total_concept_stats/total_time*100:.1f}%)")
    print(f"Data processing:       {total_data_processing:.2f}s ({total_data_processing/total_time*100:.1f}%)")
    print(f"File I/O:              {total_file_io:.2f}s ({total_file_io/total_time*100:.1f}%)")
    if visit_merge_time > 0:
        print(f"Visit merging:         {visit_merge_time:.2f}s ({visit_merge_time/total_time*100:.1f}%)")
    
    # Find slowest tables
    table_times = [(name, stats.get('time_stats', {}).get('total', 0)) 
                   for name, stats in standardizer.standardization_results.get('tables', {}).items()]
    table_times.sort(key=lambda x: x[1], reverse=True)
    
    print("\nSlowest tables:")
    for name, time_taken in table_times[:3]:
        if time_taken > 0:
            print(f"  {name}: {time_taken:.2f}s")
    
    print("="*50)


if __name__ == "__main__":
    main()
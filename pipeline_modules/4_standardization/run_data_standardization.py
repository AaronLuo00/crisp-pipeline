#!/usr/bin/env python
"""Run data standardization with outlier removal, low frequency filtering, and visit merging."""

import csv
import json
import re
import argparse
import numpy as np
import pandas as pd
import platform
import time
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import List, Dict, Tuple, Any
from tqdm import tqdm
import logging
import warnings
try:
    # Try relative imports first (when run as module)
    from .visit_concept_merger import VisitConceptMerger
    from .parallel_tdigest import process_measurement_chunk, merge_tdigest_states, split_file_for_parallel
    from .parallel_standardization import (
        process_measurement_chunk as process_measurement_chunk_std,
        process_standard_table,
        merge_measurement_chunks,
        process_visit_patient_chunk,
        merge_visit_chunks
    )
except ImportError:
    # Fall back to absolute imports (when run as script)
    from visit_concept_merger import VisitConceptMerger
    from parallel_tdigest import process_measurement_chunk, merge_tdigest_states, split_file_for_parallel
    from parallel_standardization import (
        process_measurement_chunk as process_measurement_chunk_std,
        process_standard_table,
        merge_measurement_chunks,
        process_visit_patient_chunk,
        merge_visit_chunks
    )
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

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
# All inputs now come from Module 3 (mapping) output
mapping_dir = project_root / "output" / "3_mapping"
# Ensure we're using absolute paths
mapping_dir = mapping_dir.resolve()
output_dir = project_root / "output" / "4_standardization"
output_dir = output_dir.resolve()

# Create output directories
removed_dir = output_dir / "removed_records"
outliers_percentile_dir = removed_dir / "outliers_percentile"
outliers_range_dir = removed_dir / "outliers_range"
merged_visit_dir = output_dir / "merged_visit"
changes_dir = output_dir / "standardization_changes"

# Create all directories
for dir_path in [output_dir, outliers_percentile_dir, outliers_range_dir, 
                 merged_visit_dir, changes_dir]:
    dir_path.mkdir(parents=True, exist_ok=True)

# All tables to standardize (all have been processed by Module 3)
TABLES_TO_STANDARDIZE = ['MEASUREMENT', 'OBSERVATION', 'PROCEDURE_OCCURRENCE', 
                        'DEVICE_EXPOSURE', 'DRUG_EXPOSURE', 
                        'VISIT_DETAIL', 'VISIT_OCCURRENCE',
                        'CONDITION_ERA', 'CONDITION_OCCURRENCE', 
                        'DRUG_ERA', 'SPECIMEN']

# Visit tables that need merging (subset of TABLES_TO_STANDARDIZE)
VISIT_TABLES = ['VISIT_DETAIL', 'VISIT_OCCURRENCE']

# Date/datetime columns for optimized date standardization
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
    def __init__(self, outlier_percentile=99.0, 
                 merge_visits=True, merge_threshold_hours=2.0):
        self.outlier_percentile = outlier_percentile
        self.merge_visits = merge_visits
        self.merge_threshold_hours = merge_threshold_hours
        self.concept_thresholds = {}
        # No longer need to track concept frequencies for filtering
        # self.concept_frequencies = {}
        # Enhanced timing statistics
        self.parallel_stats = {
            'cpu_time': 0,
            'wall_time': 0,
            'workers_used': 0
        }
        self.visit_merge_stats = {}
        # Phase timing statistics
        self.phase_stats = {
            'phase1': {'wall_time': 0, 'cpu_time': 0},
            'phase2': {'wall_time': 0, 'cpu_time': 0, 'tasks': 0},
            'phase3': {'wall_time': 0, 'cpu_time': 0, 'tasks': 0}
        }
        
        # Date processing optimization - caching for performance
        self.date_cache = {}  # Cache for parsed dates
        self.format_cache = {}  # Cache for detected date formats
        
        # Initialize results dictionary
        self.standardization_results = {
            "standardization_date": datetime.now().isoformat(),
            "dataset": "OMOP CDM",
            "parameters": {
                "outlier_percentile": self.outlier_percentile,
                "merge_visits": self.merge_visits,
                "merge_threshold_hours": self.merge_threshold_hours
            },
            "tables": {}
        }
        
    def get_input_path(self, table_name):
        """Get the correct input path - all tables should come from mapping output."""
        # All tables have been processed by Module 3 (mapping and low frequency filtering)
        input_file = mapping_dir / f"{table_name}_mapped.csv"
        return input_file
    
    def standardize_datetime(self, dt_string, column_name=""):
        """Optimized datetime standardization with caching and format detection."""
        if not dt_string or not dt_string.strip():
            return dt_string
        
        dt_string = dt_string.strip()
        
        # Check cache first for performance
        if dt_string in self.date_cache:
            return self.date_cache[dt_string]
        
        # Normalize: Remove fractional seconds first (major optimization)
        normalized = re.sub(r'(\d{2}:\d{2}:\d{2})\.\d+', r'\1', dt_string)
        
        # Check cache again after normalization
        if normalized in self.date_cache:
            self.date_cache[dt_string] = self.date_cache[normalized]
            return self.date_cache[normalized]
        
        # Use column name for direct format detection (ultra-fast optimization)
        expected_format = None
        has_time = False
        
        if column_name.endswith('_datetime'):
            expected_format = '%Y-%m-%d %H:%M:%S'
            has_time = True
        elif column_name.endswith('_date'):
            expected_format = '%Y-%m-%d'
            has_time = False
        
        # Try expected format first
        if expected_format:
            try:
                dt = datetime.strptime(normalized, expected_format)
                if has_time:
                    result = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    result = dt.strftime('%Y-%m-%d')
                
                # Cache both original and normalized
                self.date_cache[dt_string] = result
                self.date_cache[normalized] = result
                return result
            except ValueError:
                pass
        
        # Fallback: Fast format detection with regex (if column name detection failed)
        format_patterns = [
            (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', '%Y-%m-%d %H:%M:%S', True),
            (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d', False),
            (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$', '%Y-%m-%d %H:%M', True),
            (r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$', '%Y/%m/%d %H:%M:%S', True),
            (r'^\d{4}/\d{2}/\d{2}$', '%Y/%m/%d', False),
            (r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$', '%m/%d/%Y %H:%M:%S', True),
            (r'^\d{2}/\d{2}/\d{4}$', '%m/%d/%Y', False),
        ]
        
        for pattern, format_str, has_time in format_patterns:
            if re.match(pattern, normalized):
                try:
                    dt = datetime.strptime(normalized, format_str)
                    if has_time:
                        result = dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        result = dt.strftime('%Y-%m-%d')
                    
                    # Cache both original and normalized
                    self.date_cache[dt_string] = result
                    self.date_cache[normalized] = result
                    return result
                except ValueError:
                    continue
        
        # If all else fails, cache and return original
        self.date_cache[dt_string] = dt_string
        return dt_string
    
    def batch_standardize_dates_in_rows(self, rows, table_name):
        """Batch-process date standardization for optimal performance."""
        date_cols = DATE_COLUMNS.get(table_name, [])
        
        if not date_cols:
            # Fallback: detect date columns by name
            if rows:
                first_row = rows[0]
                date_cols = [col for col in first_row.keys() 
                            if 'datetime' in col.lower() or 'date' in col.lower()]
        
        if not date_cols:
            return rows
        
        # Step 1: Collect all unique date values from all date columns
        unique_dates_by_column = {}
        for col in date_cols:
            unique_dates_by_column[col] = set()
        
        for row in rows:
            for col in date_cols:
                if col in row and row[col] and row[col].strip():
                    unique_dates_by_column[col].add(row[col].strip())
        
        # Step 2: Batch process unique dates for each column
        for col, unique_dates in unique_dates_by_column.items():
            # Only process dates that aren't already cached
            uncached_dates = unique_dates - set(self.date_cache.keys())
            
            if uncached_dates:
                for date_str in uncached_dates:
                    # Process with column name for optimized format detection
                    self.standardize_datetime(date_str, col)
        
        # Step 3: Apply cached results to all rows
        for row in rows:
            for col in date_cols:
                if col in row and row[col] and row[col].strip():
                    original_date = row[col].strip()
                    # Use cached result
                    standardized_date = self.date_cache.get(original_date, original_date)
                    row[col] = standardized_date
        
        return rows
    
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
        """Calculate outlier thresholds for MEASUREMENT table."""
        
        # Early return: Only MEASUREMENT table needs statistics for outlier detection
        if table_name != 'MEASUREMENT':
            return
        
        logging.info(f"Calculating concept statistics for {table_name}")
        
        input_file = self.get_input_path(table_name)
        if not input_file.exists():
            logging.warning(f"Input file not found: {input_file}")
            return
        
        # Check file size to decide between parallel and single-threaded processing
        file_size = input_file.stat().st_size
        use_parallel = file_size > 100_000_000  # Use parallel for files > 100MB
        
        if use_parallel:
            self._calculate_statistics_parallel(input_file, table_name)
        else:
            self._calculate_statistics_single(input_file, table_name)
    
    def _calculate_statistics_parallel(self, input_file, table_name):
        """Calculate statistics using parallel T-Digest processing."""
        logging.info(f"Using parallel T-Digest processing for {table_name}")
        
        # Track wall time
        wall_start = time.time()
        
        # Determine number of workers
        num_workers = min(6, mp.cpu_count())
        
        # Split file for parallel processing
        chunks = split_file_for_parallel(str(input_file), num_workers)
        
        # Process chunks in parallel
        with mp.Pool(processes=num_workers) as pool:
            # Process chunks and show progress
            results = list(tqdm(
                pool.imap(process_measurement_chunk, chunks),
                total=len(chunks),
                desc=f"Processing {table_name} chunks",
                unit='chunks',
                leave=False
            ))
        
        # Extract statistics and CPU times
        digest_results = []
        total_cpu_time = 0
        for result, cpu_time in results:
            digest_results.append(result)
            total_cpu_time += cpu_time
        
        # Track parallel stats
        wall_time = time.time() - wall_start
        self.parallel_stats = {
            'cpu_time': total_cpu_time,
            'wall_time': wall_time,
            'workers_used': num_workers,
            'speedup': total_cpu_time / wall_time if wall_time > 0 else 1.0,
            'efficiency': (total_cpu_time / wall_time / num_workers * 100) if wall_time > 0 else 0
        }
        
        # Merge T-Digest states
        merged_stats = merge_tdigest_states(digest_results, delta=0.01, K=25)
        
        # Convert to format expected by rest of pipeline
        self.concept_thresholds[table_name] = {}
        
        for concept_id, stats in merged_stats.items():
            if stats['count'] >= 10:  # Keep threshold for minimum data
                percentiles = stats['percentiles']
                
                # Calculate outlier thresholds using configured percentile
                lower_percentile = percentiles.get('p1')  # 1st percentile for lower bound
                upper_percentile = percentiles.get('p99')  # 99th percentile for upper bound
                
                # Use configured percentile if different from default
                if self.outlier_percentile != 99:
                    # For custom percentiles, use closest available
                    if self.outlier_percentile >= 99:
                        upper_percentile = percentiles.get('p99')
                        lower_percentile = percentiles.get('p1')
                    elif self.outlier_percentile >= 95:
                        upper_percentile = percentiles.get('p95')
                        lower_percentile = percentiles.get('p5')
                    else:
                        upper_percentile = percentiles.get('p75')
                        lower_percentile = percentiles.get('p25')
                
                self.concept_thresholds[table_name][concept_id] = {
                    'lower_percentile': lower_percentile,
                    'upper_percentile': upper_percentile,
                    'count': stats['count'],
                    'min': stats['min'],
                    'max': stats['max'],
                    'p50': percentiles.get('p50')  # median
                }
                
                # Add range limits if defined
                if concept_id in CONCEPT_RANGES:
                    self.concept_thresholds[table_name][concept_id].update({
                        'range_min': CONCEPT_RANGES[concept_id]['min'],
                        'range_max': CONCEPT_RANGES[concept_id]['max'],
                        'name': CONCEPT_RANGES[concept_id]['name']
                    })
        
        logging.info(f"Calculated thresholds for {len(self.concept_thresholds[table_name])} concepts using parallel processing")
    
    def _calculate_statistics_single(self, input_file, table_name):
        """Calculate statistics using single-threaded processing (fallback for small files)."""
        logging.info(f"Using single-threaded processing for {table_name}")
        
        concept_col = 'measurement_concept_id'
        
        # Collect values by concept
        concept_values = defaultdict(list)
        concept_counts = Counter()
        
        # Read data to collect values
        df = pd.read_csv(input_file, chunksize=CHUNK_SIZE, low_memory=False)
        chunk_num = 0
        
        for chunk in tqdm(df, desc=f"Standardizing {table_name} (statistics)", 
                         unit='chunks', leave=False, ncols=100,
                         mininterval=PROGRESS_INTERVAL,
                         disable=False):
            chunk_num += 1
            
            # Count concept frequencies
            if concept_col in chunk.columns:
                if chunk_num == 1:
                    logging.info(f"Column '{concept_col}' found in {table_name}")
                
                for concept_id in chunk[concept_col].dropna():
                    try:
                        concept_id = int(concept_id)
                        concept_counts[concept_id] += 1
                    except Exception as e:
                        if chunk_num == 1:
                            logging.warning(f"Error processing concept_id '{concept_id}': {e}")
                        pass
            
            # Collect values using vectorized operations
            if 'value_as_number' in chunk.columns:
                valid_rows = chunk[chunk['value_as_number'].notna() & chunk[concept_col].notna()]
                
                for concept_id, group in valid_rows.groupby(concept_col):
                    try:
                        concept_id = int(concept_id)
                        values = group['value_as_number'].astype(float).tolist()
                        concept_values[concept_id].extend(values)
                    except:
                        pass
        
        # Calculate outlier thresholds
        self.concept_thresholds[table_name] = {}
        for concept_id, values in concept_values.items():
            if len(values) >= 10:
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
    
    def _process_rows_with_outliers(self, reader, writer, perc_writer, range_writer, 
                                   change_writer, stats, table_name, concept_col, input_file):
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
            
            # If not removed for low frequency, continue with other checks
            if not is_removed:
                # Standardize datetime fields - use DATE_COLUMNS config for better performance
                date_cols = DATE_COLUMNS.get(table_name, [])
                if not date_cols:
                    # Fallback: detect date columns by name
                    date_cols = [col for col in headers if 'datetime' in col.lower() or 'date' in col.lower()]
                
                for col in date_cols:
                    if col in row:
                        original = row[col]
                        standardized = self.standardize_datetime(original, col)
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
                
                if removal_reason == 'percentile_outlier':
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
    
    def _process_rows_without_outliers(self, reader, writer, 
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
            
            # Standardize datetime fields - use DATE_COLUMNS config for better performance
            if not is_removed:
                date_cols = DATE_COLUMNS.get(table_name, [])
                if not date_cols:
                    # Fallback: detect date columns by name
                    date_cols = [col for col in headers if 'datetime' in col.lower() or 'date' in col.lower()]
                
                for col in date_cols:
                    if col in row:
                        original = row[col]
                        standardized = self.standardize_datetime(original, col)
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
            else:
                writer.writerow(new_row)
                stats['output_records'] += 1
                
                # Write changes
                for change in changes:
                    change_writer.writerow(change)
    
    def standardize_table(self, table_name):
        """Standardize a single table with outlier removal and low frequency filtering."""
        # Initialize enhanced timing
        table_time_stats = {
            'total': 0,
            'concept_statistics': 0,
            'concept_statistics_cpu': 0,
            'data_processing': 0,
            'file_io': 0,
            'outlier_detection': 0,
            'unit_conversion': 0,
            'datetime_standardization': 0,
            'file_reading': 0,
            'file_writing': 0
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
        # Copy parallel stats if available (for MEASUREMENT)
        if table_name == 'MEASUREMENT' and self.parallel_stats['wall_time'] > 0:
            table_time_stats['concept_statistics_cpu'] = self.parallel_stats['cpu_time']
        
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
                     open(changes_file, 'w', encoding='utf-8', newline='') as change_file:
                    
                    # Writers
                    writer = csv.DictWriter(outfile, fieldnames=headers)
                    writer.writeheader()
                    
                    removed_headers = headers + ['removal_reason', 'original_row_number', 'additional_info']
                    perc_writer = csv.DictWriter(perc_file, fieldnames=removed_headers)
                    perc_writer.writeheader()
                    
                    range_writer = csv.DictWriter(range_file, fieldnames=removed_headers)
                    range_writer.writeheader()
                    
                    change_headers = ['row_number', 'field', 'original_value', 'new_value', 'change_type']
                    change_writer = csv.DictWriter(change_file, fieldnames=change_headers)
                    change_writer.writeheader()
                    
                    # Process rows with outlier checking
                    self._process_rows_with_outliers(reader, writer, perc_writer, range_writer, 
                                                   change_writer, stats, 
                                                   table_name, concept_col, input_file)
            else:
                # For other tables, only open necessary files (no outlier files)
                with open(output_file, 'w', encoding='utf-8', newline='') as outfile, \
                     open(changes_file, 'w', encoding='utf-8', newline='') as change_file:
                    
                    # Writers
                    writer = csv.DictWriter(outfile, fieldnames=headers)
                    writer.writeheader()
                    
                    removed_headers = headers + ['removal_reason', 'original_row_number', 'additional_info']
                    change_headers = ['row_number', 'field', 'original_value', 'new_value', 'change_type']
                    change_writer = csv.DictWriter(change_file, fieldnames=change_headers)
                    change_writer.writeheader()
                    
                    # Process rows without outlier checking
                    self._process_rows_without_outliers(reader, writer, 
                                                      change_writer, stats, 
                                                      table_name, concept_col, input_file)
        
        # Calculate times
        table_time_stats['data_processing'] = time.time() - t0
        table_time_stats['total'] = time.time() - table_start_time
        table_time_stats['file_io'] = table_time_stats['total'] - table_time_stats['concept_statistics'] - table_time_stats['data_processing']
        
        # Save statistics
        stats['time_stats'] = table_time_stats
        self.standardization_results["tables"][table_name] = stats
        
        # Only show detailed summary if changes were made
        if stats['input_records'] != stats['output_records'] or \
           stats.get('units_converted', 0) > 0 or \
           stats.get('datetime_standardized', 0) > 0:
            # Detailed output for tables with changes
            logging.info(f"\nStandardization Summary for {table_name}:")
            logging.info(f"  - Input records: {stats['input_records']:,}")
            logging.info(f"  - Output records: {stats['output_records']:,}")
            if stats['input_records'] != stats['output_records']:
                logging.info(f"  - Records removed: {stats['input_records'] - stats['output_records']:,}")
                if stats.get('outliers_removed_percentile', 0) > 0:
                    logging.info(f"    - Percentile outliers: {stats['outliers_removed_percentile']:,}")
                if stats.get('outliers_removed_range', 0) > 0:
                    logging.info(f"    - Range outliers: {stats['outliers_removed_range']:,}")
            if stats.get('datetime_standardized', 0) > 0:
                logging.info(f"  - Datetime fields standardized: {stats['datetime_standardized']:,}")
            if stats.get('units_converted', 0) > 0:
                logging.info(f"  - Units converted: {stats['units_converted']:,}")
            logging.info(f"  - Processing time: {table_time_stats['total']:.2f}s")
        else:
            # Single line output for tables with no changes
            logging.info(f"{table_name}: No changes needed ({stats['input_records']:,} records, {table_time_stats['total']:.2f}s)")
        
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
        
        # Process the table and track time
        merge_start = time.time()
        merged_df, mapping_df = merger.process_table(input_file, table_name)
        merge_time = time.time() - merge_start
        
        # Store merge stats
        if table_name not in self.visit_merge_stats:
            self.visit_merge_stats[table_name] = {}
        self.visit_merge_stats[table_name]['wall_time'] = merge_time
        
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
            'visits_merged': merger.statistics['merged_episodes'],  # Add for summary compatibility
            'unchanged_records': merger.statistics['unchanged_records'],
            'records_merged': merger.statistics['records_merged'],
            'output_records': len(merged_df)
        }
        
        # Update the original table's output records after merging
        if table_name in self.standardization_results['tables']:
            original_input = self.standardization_results['tables'][table_name]['input_records']
            records_removed_by_merge = merger.statistics['records_merged']
            self.standardization_results['tables'][table_name]['output_records'] = original_input - records_removed_by_merge
            self.standardization_results['tables'][table_name]['records_merged'] = records_removed_by_merge
        
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
                if stats.get('outliers_removed_percentile', 0) > 0:
                    removal_summary.append({
                        'table': table_name,
                        'removal_reason': 'percentile_outlier',
                        'count': stats['outliers_removed_percentile'],
                        'percentage': (stats['outliers_removed_percentile'] / stats['input_records'] * 100) if stats['input_records'] > 0 else 0
                    })
                if stats.get('outliers_removed_range', 0) > 0:
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
    
    def standardize_all_tables_parallel(self):
        """Phase 2: Parallel processing of all tables' standardization."""
        logging.info("\n" + "="*60)
        logging.info("Phase 2: Parallel Data Standardization")
        logging.info("="*60)
        
        phase2_start = time.time()
        total_cpu_time = 0
        
        # Prepare tasks
        tasks = []
        
        # Add MEASUREMENT chunks (6 tasks)
        measurement_file = self.get_input_path('MEASUREMENT')
        if measurement_file.exists():
            for i in range(6):
                tasks.append(('measurement_chunk', i, 6, measurement_file))
        
        # Add other tables
        other_tables = ['OBSERVATION', 'DRUG_EXPOSURE', 'CONDITION_OCCURRENCE',
                       'PROCEDURE_OCCURRENCE', 'DEVICE_EXPOSURE', 'DRUG_ERA',
                       'CONDITION_ERA', 'SPECIMEN', 'VISIT_DETAIL', 'VISIT_OCCURRENCE']
        for table in other_tables:
            input_file = self.get_input_path(table)
            if input_file.exists():
                tasks.append(('standard_table', table, input_file))
        
        self.phase_stats['phase2']['tasks'] = len(tasks)
        logging.info(f"Submitting {len(tasks)} parallel tasks...")
        
        # Execute parallel processing
        with ProcessPoolExecutor(max_workers=6) as executor:
            futures = []
            
            # Submit MEASUREMENT chunks
            measurement_stats = {}
            for task in tasks:
                if task[0] == 'measurement_chunk':
                    future = executor.submit(
                        process_measurement_chunk_std,
                        (task[1], task[2], task[3], output_dir, 
                         self.concept_thresholds, self.outlier_percentile)
                    )
                    futures.append(('measurement', future))
                elif task[0] == 'standard_table':
                    future = executor.submit(
                        process_standard_table,
                        (task[1], task[2], output_dir)
                    )
                    futures.append(('table', future))
            
            # Collect results
            for task_type, future in futures:
                try:
                    result = future.result(timeout=300)
                    if task_type == 'measurement':
                        chunk_id, stats, elapsed = result
                        measurement_stats[chunk_id] = stats
                        total_cpu_time += elapsed
                        logging.info(f"MEASUREMENT chunk {chunk_id+1}/6: {stats['records_processed']} records")
                    else:
                        table_name, stats, elapsed = result
                        self.standardization_results['tables'][table_name] = stats
                        total_cpu_time += elapsed
                        logging.info(f"{table_name}: {stats['input_records']} -> {stats['output_records']} records")
                except Exception as e:
                    logging.error(f"Task failed: {e}")
        
        # Aggregate MEASUREMENT statistics
        if measurement_stats:
            aggregated = {
                'input_records': sum(s['records_processed'] for s in measurement_stats.values()),
                'output_records': sum(s['records_processed'] - s['outliers_removed_percentile'] - s['outliers_removed_range'] 
                                    for s in measurement_stats.values()),
                'outliers_removed_percentile': sum(s['outliers_removed_percentile'] for s in measurement_stats.values()),
                'outliers_removed_range': sum(s['outliers_removed_range'] for s in measurement_stats.values()),
                'units_converted': sum(s['units_converted'] for s in measurement_stats.values()),
                'datetime_standardized': sum(s['datetime_standardized'] for s in measurement_stats.values())
            }
            self.standardization_results['tables']['MEASUREMENT'] = aggregated
        
        self.phase_stats['phase2']['wall_time'] = time.time() - phase2_start
        self.phase_stats['phase2']['cpu_time'] = total_cpu_time
        
        logging.info(f"Phase 2 completed in {self.phase_stats['phase2']['wall_time']:.2f}s "
                    f"(CPU time: {total_cpu_time:.2f}s)")
    
    def execute_post_processing_parallel(self):
        """Phase 3: Parallel post-processing with chunked VISIT processing."""
        logging.info("\n" + "="*60)
        logging.info("Phase 3: Parallel Post-Processing")
        logging.info("="*60)
        
        phase3_start = time.time()
        
        # Prepare parallel tasks
        tasks = []
        
        # Task 1: Merge MEASUREMENT chunks
        if 'MEASUREMENT' in self.standardization_results['tables']:
            tasks.append(('merge_measurement', None))
        
        # Task 2-3: VISIT tables - process with patient chunks
        visit_tables = []
        if self.merge_visits:
            for table_name in ['VISIT_DETAIL', 'VISIT_OCCURRENCE']:
                if table_name in self.standardization_results['tables']:
                    visit_tables.append(table_name)
                    tasks.append(('visit_chunks', table_name))
        
        self.phase_stats['phase3']['tasks'] = len(tasks)
        self.phase_stats['phase3']['task_times'] = {}
        logging.info(f"Executing {len(tasks)} post-processing tasks...")
        
        # Execute with ProcessPoolExecutor for CPU-bound VISIT processing
        with ProcessPoolExecutor(max_workers=6) as executor:
            futures = []
            task_start_times = {}
            
            # Submit MEASUREMENT merge task
            if any(task[0] == 'merge_measurement' for task in tasks):
                task_start_times[('merge_measurement', None)] = time.time()
                future = executor.submit(merge_measurement_chunks, output_dir, 6)
                futures.append(('merge_measurement', None, future))
            
            # Submit VISIT chunk processing tasks
            for task_type, table_name in tasks:
                if task_type == 'visit_chunks':
                    task_start_times[('visit_chunks', table_name)] = time.time()
                    
                    # Get patient list for this table
                    input_file = self.get_input_path(table_name)
                    if input_file.exists():
                        # Read just to get patient list
                        import pandas as pd
                        df_sample = pd.read_csv(input_file, usecols=['person_id'], low_memory=False)
                        patients = df_sample['person_id'].unique()
                        
                        # Split patients into 6 chunks
                        import numpy as np
                        patient_chunks = np.array_split(patients, 6)
                        
                        # Submit chunk processing tasks
                        chunk_futures = []
                        for i, patient_chunk in enumerate(patient_chunks):
                            chunk_future = executor.submit(
                                process_visit_patient_chunk,
                                (i, 6, input_file, output_dir, patient_chunk.tolist(), 
                                 table_name, self.merge_threshold_hours)
                            )
                            chunk_futures.append(('visit_chunk', table_name, i, chunk_future))
                        
                        futures.extend(chunk_futures)
            
            # Wait for chunk processing completion
            visit_chunk_results = {}
            for future_data in futures:
                try:
                    if future_data[0] == 'merge_measurement':
                        result = future_data[2].result(timeout=60)
                        task_time = time.time() - task_start_times[('merge_measurement', None)]
                        self.phase_stats['phase3']['task_times']['MEASUREMENT_merge'] = task_time
                        logging.info(f"MEASUREMENT chunks merged successfully in {task_time:.2f}s")
                        
                    elif future_data[0] == 'visit_chunk':
                        table_name = future_data[1]
                        chunk_id = future_data[2]
                        chunk_result = future_data[3].result(timeout=120)
                        
                        if table_name not in visit_chunk_results:
                            visit_chunk_results[table_name] = []
                        visit_chunk_results[table_name].append(chunk_result)
                        
                except Exception as e:
                    logging.error(f"Post-processing task failed: {e}")
        
        # Now merge VISIT chunks sequentially (fast file operations)
        for table_name in visit_tables:
            if table_name in visit_chunk_results and visit_chunk_results[table_name]:
                merge_start = time.time()
                
                # Merge the chunk files
                merge_stats = merge_visit_chunks(output_dir, table_name, 6)
                
                # Generate statistics file
                self._generate_visit_statistics(table_name, visit_chunk_results[table_name])
                
                merge_time = time.time() - merge_start
                
                # Calculate total time including chunk processing
                total_time = time.time() - task_start_times[('visit_chunks', table_name)]
                self.phase_stats['phase3']['task_times'][f'{table_name}_merge'] = total_time
                
                logging.info(f"{table_name} visits merged successfully in {total_time:.2f}s")
        
        self.phase_stats['phase3']['wall_time'] = time.time() - phase3_start
        
        logging.info(f"Phase 3 completed in {self.phase_stats['phase3']['wall_time']:.2f}s")
    
    def _generate_visit_statistics(self, table_name: str, chunk_results: List):
        """Generate aggregated statistics from chunk results."""
        # Aggregate statistics from all chunks
        total_stats = {
            'merged_episodes': 0,
            'unchanged_records': 0,
            'records_merged': 0,
            'total_records': 0,
            'output_records': 0
        }
        
        for chunk_id, stats, elapsed in chunk_results:
            total_stats['merged_episodes'] += stats.get('merged_episodes', 0)
            total_stats['unchanged_records'] += stats.get('unchanged_records', 0)
            total_stats['records_merged'] += stats.get('records_merged', 0)
            total_stats['output_records'] += stats.get('total_output_records', 0)
        
        # Update results
        self.standardization_results["tables"][table_name + "_merging"] = {
            'total_input_records': 0,
            'merged_episodes': total_stats['merged_episodes'],
            'visits_merged': total_stats['merged_episodes'],
            'unchanged_records': total_stats['unchanged_records'],
            'records_merged': total_stats['records_merged'],
            'output_records': total_stats['output_records']
        }
        
        # Save statistics to JSON file
        import json
        stats_file = output_dir / "merged_visit" / f"{table_name}_merge_statistics.json"
        stats_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(stats_file, 'w') as f:
            json.dump({
                'total_records': total_stats['total_records'],
                'merged_episodes': total_stats['merged_episodes'],
                'unchanged_records': total_stats['unchanged_records'],
                'records_merged': total_stats['records_merged']
            }, f, indent=2)
        
        logging.info(f"Saved {table_name} merge statistics to: {stats_file}")
    
    def run_parallel(self):
        """Run the standardization process with three-phase parallel architecture."""
        total_start = time.time()
        
        logging.info("Starting parallel data standardization process...")
        logging.info(f"Parameters: outlier_percentile={self.outlier_percentile}, "
                    f"merge_visits={self.merge_visits}, merge_threshold={self.merge_threshold_hours}h")
        
        # Phase 1: Concept Statistics (T-Digest)
        logging.info("\n" + "="*60)
        logging.info("Phase 1: Concept Statistics Calculation")
        logging.info("="*60)
        phase1_start = time.time()
        self.calculate_concept_statistics('MEASUREMENT')
        self.phase_stats['phase1']['wall_time'] = time.time() - phase1_start
        self.phase_stats['phase1']['cpu_time'] = self.parallel_stats.get('cpu_time', 0)
        
        # Phase 2: Parallel Standardization
        self.standardize_all_tables_parallel()
        
        # Phase 3: Parallel Post-Processing
        self.execute_post_processing_parallel()
        
        # Save results to JSON
        self.standardization_results["standardization_date"] = datetime.now().isoformat()
        self.standardization_results["dataset"] = "OMOP CDM"
        self.standardization_results["parameters"] = {
            "outlier_percentile": self.outlier_percentile,
            "merge_visits": self.merge_visits,
            "merge_threshold_hours": self.merge_threshold_hours
        }
        
        # Save JSON results (convert numpy types for JSON serialization)
        results_path = output_dir / "standardization_results.json"
        
        # Convert numpy types to native Python types
        def convert_numpy(obj):
            if isinstance(obj, dict):
                return {k: convert_numpy(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy(v) for v in obj]
            elif hasattr(obj, 'item'):  # numpy scalars
                return obj.item()
            else:
                return obj
        
        json_safe_results = convert_numpy(self.standardization_results)
        
        with open(results_path, 'w') as f:
            json.dump(json_safe_results, f, indent=2)
        
        # Generate markdown report
        self.generate_markdown_report()
        
        total_time = time.time() - total_start
        
        # Print enhanced performance summary
        print("\n" + "="*60)
        print("PARALLEL PROCESSING PERFORMANCE SUMMARY")
        print("="*60)
        print(f"\nTotal execution time: {total_time:.2f}s")
        print(f"\nPhase Breakdown:")
        print(f"  Phase 1 (Statistics): {self.phase_stats['phase1']['wall_time']:.2f}s")
        if self.phase_stats['phase1']['cpu_time'] > 0:
            print(f"    - CPU time: {self.phase_stats['phase1']['cpu_time']:.2f}s")
            print(f"    - Speedup: {self.phase_stats['phase1']['cpu_time']/self.phase_stats['phase1']['wall_time']:.2f}x")
        print(f"  Phase 2 (Standardization): {self.phase_stats['phase2']['wall_time']:.2f}s")
        print(f"    - {self.phase_stats['phase2']['tasks']} parallel tasks")
        if self.phase_stats['phase2']['cpu_time'] > 0:
            print(f"    - CPU time: {self.phase_stats['phase2']['cpu_time']:.2f}s")
            print(f"    - Speedup: {self.phase_stats['phase2']['cpu_time']/self.phase_stats['phase2']['wall_time']:.2f}x")
        print(f"  Phase 3 (Post-Processing): {self.phase_stats['phase3']['wall_time']:.2f}s")
        print(f"    - {self.phase_stats['phase3']['tasks']} parallel tasks:")
        if 'task_times' in self.phase_stats['phase3']:
            for task_name, task_time in self.phase_stats['phase3']['task_times'].items():
                display_name = task_name.replace('_merge', ' merge')
                print(f"      * {display_name}: {task_time:.2f}s")
        
        logging.info("Parallel standardization process completed!")
        
        return self.standardization_results
    
    def run(self):
        """Run the complete standardization process."""
        logging.info("Starting data standardization process...")
        logging.info(f"Parameters: outlier_percentile={self.outlier_percentile}, "
                    f"merge_visits={self.merge_visits}, merge_threshold={self.merge_threshold_hours}h")
        
        # Print input information
        print("\n" + "="*70)
        print("DATA STANDARDIZATION PROCESS - INPUT INFORMATION")
        print("="*70)
        print(f"Input Directory: {mapping_dir}")
        print(f"Output Directory: {output_dir}")
        print("\nTables to standardize:")
        for table in TABLES_TO_STANDARDIZE:
            input_file = self.get_input_path(table)
            # All tables now come from Module 3 output (mapped/filtered)
            print(f"  - {table}: {input_file}")
        
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
        total_outliers = 0
        total_visits_merged = 0
        tables_modified = []
        
        for table, stats in self.standardization_results["tables"].items():
            if isinstance(stats, dict) and 'error' not in stats:
                if 'input_records' in stats:  # Main standardization tables
                    total_input += stats['input_records']
                    total_output += stats['output_records']
                    total_outliers += stats.get('outliers_removed_percentile', 0) + stats.get('outliers_removed_range', 0)
                    # Check if table was modified
                    if stats['input_records'] != stats['output_records']:
                        tables_modified.append(table)
                    elif stats.get('records_merged', 0) > 0:
                        # VISIT tables that had merging
                        if table not in tables_modified:
                            tables_modified.append(table)
                elif 'visits_merged' in stats:  # Visit merging tables
                    total_visits_merged += stats['visits_merged']
        
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
        for subdir in ['outliers_percentile', 'outliers_range']:
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
            f.write(f"**Visit Merge Threshold**: {self.merge_threshold_hours} hours\n\n")
            
            f.write("## Standardization Summary\n\n")
            f.write("| Table | Input Records | Output Records | Outliers Removed | Total Removed |\n")
            f.write("|-------|---------------|----------------|------------------|---------------|\n")
            
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
                    f.write(f"{outliers:,} | {removed:,} |\n")
            
            f.write(f"| **Total** | **{total_input:,}** | **{total_output:,}** | - | - | **{total_removed:,}** |\n")
            
            f.write("\n## Key Features\n\n")
            f.write("### 1. Outlier Removal\n")
            f.write(f"- Percentile-based: Values beyond {self.outlier_percentile}th percentile\n")
            f.write("- Range-based: Physiologically implausible values\n")
            f.write("- Applied per concept ID to preserve concept-specific distributions\n\n")
            
            f.write("### 2. Visit Merging\n")
            if self.merge_visits:
                f.write(f"- Merged visits within {self.merge_threshold_hours} hours\n")
                f.write("- Created episode identifiers for continuous care\n")
                f.write("- Generated merged data files with traceable mappings\n")
            else:
                f.write("- Visit merging was disabled\n")
            
            f.write("\n### 3. Standardizations Applied\n")
            f.write("- Datetime format: ISO 8601 (YYYY-MM-DD HH:MM:SS)\n")
            f.write("- Unit conversions: glucose, temperature, weight, height\n")
            f.write("- All changes are traceable through change logs\n\n")
            
            f.write("## Data Sources\n\n")
            f.write("- **All tables** (from Module 3 - mapping): ")
            f.write(", ".join(TABLES_TO_STANDARDIZE) + "\n")
            f.write("- All tables have been processed through Module 3 for concept mapping and low frequency filtering\n\n")
            
            f.write("## Output Structure\n\n")
            f.write("```\n")
            f.write("output/4_standardization/\n")
            f.write("|-- [table]_standardized.csv      # Standardized data\n")
            f.write("|-- removed_records/\n")
            f.write("|   |-- outliers_percentile/     # Percentile-based outliers\n")
            f.write("|   |-- outliers_range/          # Range-based outliers\n")
            f.write("|   +-- removal_summary.csv      # Summary of all removals\n")
            f.write("|-- merged_visit/                # Visit merge information\n")
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
    parser.add_argument('--no-merge-visits', action='store_true',
                        help='Disable visit merging')
    parser.add_argument('--merge-threshold', type=float, default=2.0,
                        help='Hours threshold for visit merging (default: 2)')
    parser.add_argument('--parallel', action='store_true', default=True,
                        help='Use parallel processing mode (default: True)')
    parser.add_argument('--sequential', action='store_true',
                        help='Use sequential processing mode instead of parallel')
    
    args = parser.parse_args()
    
    # Initialize and run standardizer
    standardizer = DataStandardizer(
        outlier_percentile=args.outlier_percentile,
        merge_visits=not args.no_merge_visits,
        merge_threshold_hours=args.merge_threshold
    )
    
    # Run in parallel or sequential mode
    # Sequential flag overrides parallel (which is now default)
    if args.sequential:
        standardizer.run()
        is_parallel = False
    else:
        standardizer.run_parallel()
        is_parallel = True
    
    # Calculate total time
    total_time = time.time() - start_time
    print(f"\nTotal execution time: {total_time:.2f} seconds")
    
    # Only show the detailed performance breakdown for sequential mode
    # Parallel mode already has its own detailed report
    if not is_parallel:
        # Performance breakdown
        print("\n" + "="*50)
        print("PERFORMANCE BREAKDOWN - Data Standardization")
        print("="*50)
        
        # Aggregate timing from all tables
        total_concept_stats_wall = 0
        total_concept_stats_cpu = 0
        total_data_processing = 0
        total_file_io = 0
        total_outlier_detection = 0
        total_unit_conversion = 0
        total_datetime_std = 0
        
        for table_stats in standardizer.standardization_results.get('tables', {}).values():
            if 'time_stats' in table_stats:
                ts = table_stats['time_stats']
                total_concept_stats_wall += ts.get('concept_statistics', 0)
                total_concept_stats_cpu += ts.get('concept_statistics_cpu', 0)
                total_data_processing += ts.get('data_processing', 0)
                total_file_io += ts.get('file_io', 0)
                total_outlier_detection += ts.get('outlier_detection', 0)
                total_unit_conversion += ts.get('unit_conversion', 0)
                total_datetime_std += ts.get('datetime_standardization', 0)
        
        # Also add visit merging time
        visit_merge_time = sum(stats.get('wall_time', 0) for stats in standardizer.visit_merge_stats.values())
        
        # Calculate total CPU time
        total_cpu_time = total_concept_stats_cpu if total_concept_stats_cpu > 0 else total_concept_stats_wall
        total_cpu_time += total_data_processing + visit_merge_time
        
        # Display enhanced metrics
        print(f"\nTotal CPU time:        {total_cpu_time:.2f}s")
        print(f"Wall clock time:       {total_time:.2f}s")
        if total_cpu_time > total_time:
            print(f"Speedup:               {total_cpu_time/total_time:.2f}x")
        
        print(f"\nPhase-specific Performance:")
        
        # Phase 1: T-Digest Statistics (parallel for MEASUREMENT)
        if standardizer.parallel_stats['wall_time'] > 0:
            print(f"  T-Digest Statistics ({standardizer.parallel_stats['workers_used']} workers):")
            print(f"    CPU time: {standardizer.parallel_stats['cpu_time']:.2f}s, Wall time: {standardizer.parallel_stats['wall_time']:.2f}s")
            print(f"    Speedup: {standardizer.parallel_stats['speedup']:.2f}x, Efficiency: {standardizer.parallel_stats['efficiency']:.1f}%")
        
        # Phase 2: Data Processing (sequential)
        print(f"  Data Processing (sequential):")
        print(f"    Total time: {total_data_processing:.2f}s")
        if total_outlier_detection > 0:
            print(f"    - Outlier detection: {total_outlier_detection:.2f}s")
        if total_unit_conversion > 0:
            print(f"    - Unit conversion: {total_unit_conversion:.2f}s")
        if total_datetime_std > 0:
            print(f"    - Datetime standardization: {total_datetime_std:.2f}s")
        
        # Phase 3: Visit Merging
        if visit_merge_time > 0:
            print(f"  Visit Merging (sequential):")
            for table_name, stats in standardizer.visit_merge_stats.items():
                print(f"    - {table_name}: {stats['wall_time']:.2f}s")
        
        print(f"\nDetailed Time Breakdown:")
        print(f"  Concept statistics:    {total_concept_stats_wall:.2f}s ({total_concept_stats_wall/total_time*100:.1f}%)")
        print(f"  Data processing:       {total_data_processing:.2f}s ({total_data_processing/total_time*100:.1f}%)")
        print(f"  File I/O:              {total_file_io:.2f}s ({total_file_io/total_time*100:.1f}%)")
        if visit_merge_time > 0:
            print(f"  Visit merging:         {visit_merge_time:.2f}s ({visit_merge_time/total_time*100:.1f}%)")
        
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
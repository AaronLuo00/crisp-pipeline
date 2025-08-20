#!/usr/bin/env python
"""Extract patient data and organize by patient ID with ICU timeline information."""

import os
import csv
import json
import pandas as pd
import platform
import time
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm
import logging
import warnings
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

# Import parallel processing functions
try:
    from .parallel_extraction import (
        process_table_batch,
        process_single_table_worker,
        process_measurement_chunk,
        merge_measurement_chunks,
        group_tables_by_size
    )
except ImportError:
    from parallel_extraction import (
        process_table_batch,
        process_single_table_worker,
        process_measurement_chunk,
        merge_measurement_chunks,
        group_tables_by_size
    )

# Platform-specific settings for performance optimization
if platform.system() == 'Windows':
    PROGRESS_INTERVAL = 30.0  # Less frequent updates (reduce overhead)
    CHUNK_SIZE = 500000  # Larger chunks for Windows (better I/O performance)
    WRITE_BUFFER_SIZE = 50000  # 50K rows batch write
    FILE_BUFFER_SIZE = 2 * 1024 * 1024  # 2MB file buffer
else:
    PROGRESS_INTERVAL = 10.0  # Default for macOS/Linux
    CHUNK_SIZE = 100000  # Default for macOS/Linux
    WRITE_BUFFER_SIZE = 20000  # 20K rows batch write
    FILE_BUFFER_SIZE = 1 * 1024 * 1024  # 1MB file buffer

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')  # No milliseconds

# Suppress warnings
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
warnings.filterwarnings('ignore', category=UserWarning)

# Setup paths
base_dir = Path(__file__).parent
project_root = base_dir.parent.parent  # Go up to crisp_pipeline_public
data_dir = project_root / "data"

# Input directories - Module 5 should primarily use Module 4's standardized output
standardized_dir = project_root / "output" / "4_standardization"
# Ensure we're using absolute paths
standardized_dir = standardized_dir.resolve()

# Also need Module 2's cleaned PERSON table for patient demographics
cleaning_dir = project_root / "output" / "2_cleaning"
cleaning_dir = cleaning_dir.resolve()

# Output directories
output_dir = project_root / "output" / "5_extraction"
output_dir = output_dir.resolve()
# Subdirectories for organized output
patient_data_dir = output_dir / "patient_data"
statistics_dir = output_dir / "statistics"

# Create output directories
for dir_path in [output_dir, patient_data_dir, statistics_dir]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Configuration
TABLES_TO_PROCESS = [
    'MEASUREMENT', 'OBSERVATION', 'DRUG_EXPOSURE', 'CONDITION_OCCURRENCE', 
    'VISIT_OCCURRENCE', 'VISIT_DETAIL', 'PROCEDURE_OCCURRENCE', 
    'DEVICE_EXPOSURE', 'SPECIMEN', 'CONDITION_ERA', 'DRUG_ERA'
]

# Basic tables from cleaning directory
BASIC_TABLES = ['PERSON', 'DEATH']

# Tables and time columns for pre-ICU statistics
PRE_ICU_STAT_TABLES = {
    "CONDITION_ERA": "condition_era_start_date",
    "DEVICE_EXPOSURE": "device_exposure_start_datetime",
    "DRUG_ERA": "drug_era_start_date"
}

# ICU concept IDs
ICU_CONCEPT_IDS = [581379, 32037]
ICU_SUMMARY_COL = "visit_detail_start_datetime_earliest"

# Parallel processing configuration
MAX_WORKERS = min(6, mp.cpu_count())
MEASUREMENT_CHUNKS = 6


class PatientDataExtractor:
    def __init__(self):
        self.extraction_results = {
            "extraction_date": datetime.now().isoformat(),
            "dataset": "subdataset_1000",
            "icu_concept_ids": ICU_CONCEPT_IDS,
            "statistics": {},
            "errors": [],
            "phase_stats": {}
        }
        self.skipped_patients = []
        self.phase_times = {}
        
    def get_patient_path(self, person_id: str) -> Path:
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
    
    def extract_icu_summaries(self):
        """Extract ICU visit summaries for each patient."""
        t0 = time.time()
        logging.info("Extracting ICU visit summaries...")
        
        visit_detail_file = standardized_dir / "VISIT_DETAIL_standardized.csv"
        if not visit_detail_file.exists():
            logging.error(f"VISIT_DETAIL file not found: {visit_detail_file}")
            return {}
        
        # Read VISIT_DETAIL and filter ICU visits
        icu_summaries = {}
        total_icu_patients = 0
        
        try:
            # Estimate number of chunks for progress bar
            file_size = visit_detail_file.stat().st_size
            estimated_chunks = max(1, file_size // (CHUNK_SIZE * 500))  # Rough estimate
            
            # Read in chunks for memory efficiency with progress bar
            chunks = pd.read_csv(visit_detail_file, chunksize=CHUNK_SIZE, dtype={"person_id": str}, low_memory=False)
            for chunk in tqdm(chunks, total=estimated_chunks, desc="Extracting ICU visits", 
                             leave=False, mininterval=PROGRESS_INTERVAL,  # 10-second update interval
                             disable=False):  # Enable progress tracking
                # Filter ICU visits (use .copy() to avoid SettingWithCopyWarning)
                icu_visits = chunk[chunk['visit_detail_concept_id'].isin(ICU_CONCEPT_IDS)].copy()
                
                if not icu_visits.empty:
                    # Parse datetime columns
                    icu_visits['visit_detail_start_datetime'] = pd.to_datetime(icu_visits['visit_detail_start_datetime'])
                    icu_visits['visit_detail_end_datetime'] = pd.to_datetime(icu_visits['visit_detail_end_datetime'])
                    
                    # Group by person_id and get earliest/latest times
                    grouped = icu_visits.groupby('person_id').agg({
                        'visit_detail_start_datetime': 'min',
                        'visit_detail_end_datetime': 'max'
                    }).rename(columns={
                        'visit_detail_start_datetime': 'visit_detail_start_datetime_earliest',
                        'visit_detail_end_datetime': 'visit_detail_end_datetime_latest'
                    })
                    
                    # Save to dictionary
                    for person_id, row in grouped.iterrows():
                        icu_summaries[person_id] = row.to_dict()
                        
                        # Create patient folder and save summary
                        patient_path = self.get_patient_path(person_id)
                        patient_path.mkdir(parents=True, exist_ok=True)
                        
                        summary_file = patient_path / f"icu_visit_summary_{person_id}.csv"
                        summary_df = pd.DataFrame([{
                            'person_id': person_id,
                            'visit_detail_start_datetime_earliest': row['visit_detail_start_datetime_earliest'],
                            'visit_detail_end_datetime_latest': row['visit_detail_end_datetime_latest']
                        }])
                        summary_df.to_csv(summary_file, index=False)
                        total_icu_patients += 1
                        
        except Exception as e:
            logging.error(f"Error processing VISIT_DETAIL: {str(e)}")
            self.extraction_results['errors'].append(f"VISIT_DETAIL processing: {str(e)}")
        
        extraction_time = time.time() - t0
        logging.info(f"Extracted ICU summaries for {total_icu_patients} patients in {extraction_time:.2f}s")
        self.extraction_results['statistics']['total_icu_patients'] = total_icu_patients
        self.extraction_results['statistics']['icu_extraction_time'] = extraction_time
        
        return icu_summaries
    
    def split_table_by_patient(self, table_name: str):
        """Split a standardized table by patient ID and save to patient folders."""
        t0 = time.time()
        logging.info(f"Processing {table_name}...")
        
        input_file = standardized_dir / f"{table_name}_standardized.csv"
        if not input_file.exists():
            logging.warning(f"File not found: {input_file}")
            return
        
        # Track statistics
        total_records = 0
        patients_processed = set()
        
        try:
            # Count total rows for progress bar (consistent with Module 4)
            with open(input_file, 'r') as f:
                total_rows = sum(1 for _ in f) - 1  # Subtract header
            
            # Calculate number of chunks based on actual rows
            num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
            
            # Process in chunks
            reader = pd.read_csv(input_file, dtype={"person_id": str}, chunksize=CHUNK_SIZE)
            
            # Enable progress bar for tables with significant data (consistent with Module 4)
            show_progress = total_rows > 10000  # Show progress for tables > 10k rows
            
            # Create progress bar with row-based tracking (consistent with Module 4)
            pbar = tqdm(total=total_rows, desc=f"Processing {table_name}",
                       unit='rows',
                       miniters=max(100, total_rows//100),  # Update every 1% or at least 100 rows
                       mininterval=PROGRESS_INTERVAL,  # Update at most once per 10 seconds
                       leave=False, ncols=100,
                       disable=not show_progress)
            
            # Track file handles for optimized appending
            file_handles = {}
            file_writers = {}
            
            for chunk_idx, chunk in enumerate(reader):
                # Remove null person_ids
                chunk = chunk[chunk["person_id"].notnull()]
                chunk_size = len(chunk)
                total_records += chunk_size
                
                # Update progress bar with number of rows processed
                pbar.update(chunk_size)
                
                # Group by person_id
                grouped = chunk.groupby("person_id")
                
                for person_id, group_df in grouped:
                    # Get patient folder
                    patient_path = self.get_patient_path(person_id)
                    patient_path.mkdir(parents=True, exist_ok=True)
                    
                    # Get or create file handle with buffering
                    file_path = patient_path / f"{table_name}.csv"
                    if person_id not in file_handles:
                        # Open file with optimized buffering
                        file_handles[person_id] = open(file_path, 'w', newline='', 
                                                      encoding='utf-8', buffering=FILE_BUFFER_SIZE)
                        # Write header for new file
                        group_df.to_csv(file_handles[person_id], index=False)
                    else:
                        # Append without header
                        group_df.to_csv(file_handles[person_id], header=False, index=False)
                    
                    patients_processed.add(person_id)
            
            # Close all file handles
            for handle in file_handles.values():
                handle.close()
            
            # Close progress bar
            pbar.close()
            
            logging.info(f"Processed {total_records:,} records for {len(patients_processed):,} patients in {table_name}")
            
            # Update statistics
            processing_time = time.time() - t0
            if table_name not in self.extraction_results['statistics']:
                self.extraction_results['statistics'][table_name] = {}
            self.extraction_results['statistics'][table_name]['total_records'] = total_records
            self.extraction_results['statistics'][table_name]['unique_patients'] = len(patients_processed)
            self.extraction_results['statistics'][table_name]['processing_time'] = processing_time
            
        except Exception as e:
            logging.error(f"Error processing {table_name}: {str(e)}")
            self.extraction_results['errors'].append(f"{table_name}: {str(e)}")
    
    def split_basic_table_by_patient(self, table_name: str):
        """Split a basic table (PERSON/DEATH) by patient ID and save to patient folders."""
        t0 = time.time()
        logging.info(f"Processing {table_name} from cleaning directory...")
        
        input_file = cleaning_dir / f"{table_name}_cleaned.csv"
        if not input_file.exists():
            logging.warning(f"File not found: {input_file}")
            return
        
        # Track statistics
        total_records = 0
        patients_processed = set()
        
        try:
            # Read the entire table (basic tables are usually smaller)
            df = pd.read_csv(input_file, dtype={"person_id": str})
            df = df[df["person_id"].notnull()]
            total_records = len(df)
            
            # Group by person_id
            grouped = df.groupby("person_id")
            
            for person_id, group_df in tqdm(grouped, desc=f"Processing {table_name}", 
                                           unit="patients", leave=False, 
                                           disable=True):  # Keep disabled for basic tables
                # Get patient folder
                patient_path = self.get_patient_path(person_id)
                patient_path.mkdir(parents=True, exist_ok=True)
                
                # Write to patient's table file with optimized buffering
                file_path = patient_path / f"{table_name}.csv"
                with open(file_path, 'w', newline='', encoding='utf-8', buffering=FILE_BUFFER_SIZE) as f:
                    group_df.to_csv(f, index=False)
                
                patients_processed.add(person_id)
            
            logging.info(f"Processed {total_records:,} records for {len(patients_processed):,} patients in {table_name}")
            
            # Update statistics
            processing_time = time.time() - t0
            if table_name not in self.extraction_results['statistics']:
                self.extraction_results['statistics'][table_name] = {}
            self.extraction_results['statistics'][table_name]['total_records'] = total_records
            self.extraction_results['statistics'][table_name]['unique_patients'] = len(patients_processed)
            self.extraction_results['statistics'][table_name]['processing_time'] = processing_time
            
        except Exception as e:
            logging.error(f"Error processing {table_name}: {str(e)}")
            self.extraction_results['errors'].append(f"{table_name}: {str(e)}")
    
    def calculate_pre_icu_statistics(self, icu_summaries: dict):
        """Calculate statistics for records before ICU admission."""
        t0 = time.time()
        logging.info("Calculating pre-ICU statistics...")
        
        results = []
        
        # Process each patient with ICU records
        for person_id, icu_info in tqdm(icu_summaries.items(), 
                                       desc="Calculating pre-ICU stats", 
                                       unit="patients", leave=False, 
                                       mininterval=PROGRESS_INTERVAL,  # 10-second update interval
                                       disable=len(icu_summaries) < 100):  # Enable for many patients
            patient_path = self.get_patient_path(person_id)
            
            if not patient_path.exists():
                self.skipped_patients.append({
                    "patient_id": person_id,
                    "skip_reason": "Patient folder not found"
                })
                continue
            
            try:
                icu_time = pd.to_datetime(icu_info['visit_detail_start_datetime_earliest'])
                total_count = 0
                
                # Count records in specified tables
                for table_name, time_col in PRE_ICU_STAT_TABLES.items():
                    table_path = patient_path / f"{table_name}.csv"
                    if not table_path.exists():
                        continue
                    
                    try:
                        df = pd.read_csv(table_path, parse_dates=[time_col], low_memory=False)
                        # Count records before ICU
                        count = df[df[time_col] <= icu_time].shape[0]
                        total_count += count
                    except Exception as e:
                        logging.warning(f"Error reading {table_path}: {e}")
                
                results.append({
                    "patient_id": person_id,
                    "count_before_icu": total_count
                })
                
            except Exception as e:
                logging.error(f"Error processing patient {person_id}: {e}")
                self.skipped_patients.append({
                    "patient_id": person_id,
                    "skip_reason": f"Processing error: {e}"
                })
        
        # Save results
        if results:
            results_df = pd.DataFrame(results)
            results_file = statistics_dir / "patient_before_icu_statistics.csv"
            results_df.to_csv(results_file, index=False)
            
            # Calculate summary statistics
            self.extraction_results['statistics']['pre_icu_stats'] = {
                'total_patients': len(results),
                'avg_records_before_icu': float(results_df['count_before_icu'].mean()),
                'max_records_before_icu': int(results_df['count_before_icu'].max()),
                'min_records_before_icu': int(results_df['count_before_icu'].min())
            }
            
            logging.info(f"Pre-ICU statistics saved to: {results_file}")
        
        # Save skipped patients
        if self.skipped_patients:
            skipped_df = pd.DataFrame(self.skipped_patients)
            skipped_file = statistics_dir / "skipped_patients.csv"
            skipped_df.to_csv(skipped_file, index=False)
            logging.info(f"Skipped patients saved to: {skipped_file}")
        
        # Record processing time
        processing_time = time.time() - t0
        self.extraction_results['statistics']['pre_icu_calculation_time'] = processing_time
    
    def generate_report(self):
        """Generate extraction report."""
        report_path = output_dir / "extraction_report.md"
        
        with open(report_path, 'w') as f:
            f.write("# Patient Data Extraction Report\n\n")
            f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Dataset**: subdataset_1000\n")
            f.write(f"**ICU Concept IDs**: {', '.join(map(str, ICU_CONCEPT_IDS))}\n\n")
            
            f.write("## Summary\n\n")
            stats = self.extraction_results['statistics']
            
            if 'total_icu_patients' in stats:
                f.write(f"- **Total ICU patients**: {stats['total_icu_patients']:,}\n")
            
            if 'pre_icu_stats' in stats:
                pre_stats = stats['pre_icu_stats']
                f.write(f"- **Patients with pre-ICU analysis**: {pre_stats['total_patients']:,}\n")
                f.write(f"- **Average records before ICU**: {pre_stats['avg_records_before_icu']:.2f}\n")
                f.write(f"- **Max records before ICU**: {pre_stats['max_records_before_icu']}\n")
                f.write(f"- **Min records before ICU**: {pre_stats['min_records_before_icu']}\n")
            
            f.write(f"- **Skipped patients**: {len(self.skipped_patients):,}\n\n")
            
            f.write("## Table Processing Summary\n\n")
            f.write("### Standardized Tables\n\n")
            f.write("| Table | Total Records | Unique Patients |\n")
            f.write("|-------|---------------|----------------|\n")
            
            for table in TABLES_TO_PROCESS:
                if table in stats:
                    table_stats = stats[table]
                    f.write(f"| {table} | {table_stats['total_records']:,} | {table_stats['unique_patients']:,} |\n")
            
            f.write("\n### Basic Tables\n\n")
            f.write("| Table | Total Records | Unique Patients |\n")
            f.write("|-------|---------------|----------------|\n")
            
            for table in BASIC_TABLES:
                if table in stats:
                    table_stats = stats[table]
                    f.write(f"| {table} | {table_stats['total_records']:,} | {table_stats['unique_patients']:,} |\n")
            
            f.write("\n## Directory Structure\n\n")
            f.write("```\n")
            f.write("output/5_extraction/\n")
            f.write("|-- patient_data/\n")
            f.write("|   |-- {prefix}/              # Patient ID prefix (first 9 digits)\n")
            f.write("|   |   |-- {person_id}/       # Individual patient folder\n")
            f.write("|   |   |   |-- icu_visit_summary_{person_id}.csv\n")
            f.write("|   |   |   |-- PERSON.csv\n")
            f.write("|   |   |   |-- DEATH.csv (if applicable)\n")
            f.write("|   |   |   |-- MEASUREMENT.csv\n")
            f.write("|   |   |   |-- OBSERVATION.csv\n")
            f.write("|   |   |   +-- ... (all other tables)\n")
            f.write("|   +-- 600000071/             # Special prefix with grouping\n")
            f.write("|       |-- 000000-000999/\n")
            f.write("|       |   +-- {person_id}/\n")
            f.write("|       +-- ...\n")
            f.write("|-- statistics/\n")
            f.write("|   |-- patient_before_icu_statistics.csv\n")
            f.write("|   +-- skipped_patients.csv\n")
            f.write("+-- extraction_report.md\n")
            f.write("```\n")
            
            if self.extraction_results['errors']:
                f.write("\n## Errors\n\n")
                for error in self.extraction_results['errors']:
                    f.write(f"- {error}\n")
        
        logging.info(f"Report saved to: {report_path}")
    
    def print_performance_breakdown(self, total_time):
        """Print detailed performance breakdown."""
        print("\n" + "="*60)
        print("PERFORMANCE BREAKDOWN - Parallel ICU Data Extraction")
        print("="*60)
        
        stats = self.extraction_results['statistics']
        phase_stats = self.phase_times
        
        # Calculate component times
        phase1_time = phase_stats.get('phase1', 0)  # Parallel table processing
        phase2_time = phase_stats.get('phase2', 0)  # MEASUREMENT processing
        phase3_time = phase_stats.get('phase3', 0)  # Basic tables
        phase4_time = phase_stats.get('phase4', 0)  # Pre-ICU statistics
        
        # Calculate CPU time (sum of all individual processing times)
        cpu_time = 0
        parallel_tasks = 0
        
        # Phase 1 CPU time (all parallel tasks)
        for table in TABLES_TO_PROCESS:
            if table != 'MEASUREMENT' and table in stats:
                table_time = stats[table].get('processing_time', 0)
                if table_time > 0:
                    cpu_time += table_time
                    parallel_tasks += 1
        
        # Add ICU extraction time
        icu_time = stats.get('icu_extraction_time', 0) 
        if icu_time > 0:
            cpu_time += icu_time
            parallel_tasks += 1
        
        # MEASUREMENT CPU time (6 parallel chunks)
        measurement_time = stats.get('MEASUREMENT', {}).get('processing_time', 0)
        if measurement_time > 0:
            # Estimate CPU time as chunk_time * num_chunks
            cpu_time += measurement_time * MEASUREMENT_CHUNKS
            parallel_tasks += MEASUREMENT_CHUNKS
        
        # Basic tables and pre-ICU are sequential
        basic_cpu = sum(stats.get(t, {}).get('processing_time', 0) for t in BASIC_TABLES)
        pre_icu_cpu = stats.get('pre_icu_calculation_time', 0)
        cpu_time += basic_cpu + pre_icu_cpu
        
        # Overall metrics
        if cpu_time > 0:
            speedup = cpu_time / total_time
            efficiency = (speedup / MAX_WORKERS) * 100
            
            print(f"\nOverall Performance:")
            print(f"  Total CPU time:        {cpu_time:.2f}s")
            print(f"  Wall clock time:       {total_time:.2f}s") 
            print(f"  Speedup:               {speedup:.2f}x")
            print(f"  Parallel efficiency:   {efficiency:.1f}%")
            print(f"  Parallel tasks:        {parallel_tasks}")
        
        # Phase breakdown
        print(f"\nPhase Breakdown:")
        if phase1_time > 0:
            print(f"  Phase 1 (Parallel Tables): {phase1_time:.2f}s ({phase1_time/total_time*100:.1f}%)")
        if phase2_time > 0:
            print(f"  Phase 2 (MEASUREMENT):      {phase2_time:.2f}s ({phase2_time/total_time*100:.1f}%)")
        if phase3_time > 0:
            print(f"  Phase 3 (Basic Tables):     {phase3_time:.2f}s ({phase3_time/total_time*100:.1f}%)")
        if phase4_time > 0:
            print(f"  Phase 4 (Pre-ICU Stats):    {phase4_time:.2f}s ({phase4_time/total_time*100:.1f}%)")
        
        # Individual table performance
        print(f"\nTable Processing Times:")
        table_times = []
        for table in TABLES_TO_PROCESS:
            if table in stats and 'processing_time' in stats[table]:
                table_times.append((table, stats[table]['processing_time']))
        
        table_times.sort(key=lambda x: x[1], reverse=True)
        
        for name, time_taken in table_times[:5]:  # Show top 5
            if time_taken > 0:
                records = stats[name].get('total_records', 0)
                if records > 0:
                    rate = records / time_taken
                    print(f"  {name:20s}: {time_taken:6.2f}s ({records:>10,} records, {rate:>10.0f} rec/s)")
                else:
                    print(f"  {name:20s}: {time_taken:6.2f}s")
        
        print("="*60)
    
    def run(self):
        """Run the complete extraction process with parallel optimization."""
        # Start timing
        start_time = time.time()
        
        # Simplified startup
        print(f"\nExtracting patient data for {len(TABLES_TO_PROCESS) + len(BASIC_TABLES)} tables...")
        print(f"ICU Concept IDs: {', '.join(map(str, ICU_CONCEPT_IDS))}")
        print(f"Using parallel processing with {MAX_WORKERS} workers")
        
        # Group tables by size for optimal parallel processing
        tables_without_measurement = [t for t in TABLES_TO_PROCESS if t != 'MEASUREMENT']
        small_tables, medium_tables, large_tables = group_tables_by_size(
            tables_without_measurement, standardized_dir
        )
        
        print(f"\nTable grouping:")
        print(f"  Small tables ({len(small_tables)}): {', '.join(small_tables) if small_tables else 'None'}")
        print(f"  Medium tables ({len(medium_tables)}): {', '.join(medium_tables) if medium_tables else 'None'}")
        print(f"  Large tables: MEASUREMENT")
        
        # Phase 1: Parallel processing of ICU summaries and tables
        print("\n=== Phase 1: Parallel Processing ===")
        phase1_start = time.time()
        
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            
            # Step 1: Submit ICU summaries extraction (independent task)
            print("\nStep 1/4: Extracting ICU visit summaries (parallel)...")
            futures['icu'] = executor.submit(self.extract_icu_summaries)
            
            # Step 2a: Submit small tables as batch (if any)
            if small_tables:
                print(f"\nStep 2a/4: Processing {len(small_tables)} small tables as batch...")
                futures['small_batch'] = executor.submit(
                    process_table_batch, small_tables, standardized_dir, patient_data_dir
                )
            
            # Step 2b: Submit medium tables individually
            if medium_tables:
                print(f"\nStep 2b/4: Processing {len(medium_tables)} medium tables in parallel...")
                for table in medium_tables:
                    futures[f'medium_{table}'] = executor.submit(
                        process_single_table_worker, table, standardized_dir, patient_data_dir
                    )
            
            # Collect parallel results
            icu_summaries = None
            completed_tasks = 0
            total_tasks = len(futures)
            
            for key, future in futures.items():
                try:
                    if key == 'icu':
                        icu_summaries = future.result(timeout=30)
                        completed_tasks += 1
                        print(f"  ✓ ICU summaries extracted: {len(icu_summaries) if icu_summaries else 0} patients")
                        self.extraction_results['statistics']['total_icu_patients'] = len(icu_summaries) if icu_summaries else 0
                    
                    elif key == 'small_batch':
                        batch_results = future.result(timeout=60)
                        completed_tasks += 1
                        print(f"  ✓ Small tables batch completed")
                        for table, stats in batch_results.items():
                            if 'error' not in stats:
                                self.extraction_results['statistics'][table] = stats
                                print(f"    - {table}: {stats.get('total_records', 0):,} records")
                    
                    elif key.startswith('medium_'):
                        table_name = key.replace('medium_', '')
                        stats = future.result(timeout=120)
                        completed_tasks += 1
                        if 'error' not in stats:
                            self.extraction_results['statistics'][table_name] = stats
                            print(f"  ✓ {table_name}: {stats.get('total_records', 0):,} records")
                        else:
                            print(f"  ✗ {table_name}: {stats['error']}")
                            self.extraction_results['errors'].append(f"{table_name}: {stats['error']}")
                
                except Exception as e:
                    logging.error(f"Task {key} failed: {e}")
                    print(f"  ✗ Task {key} failed: {e}")
                    self.extraction_results['errors'].append(f"Task {key}: {str(e)}")
        
        self.phase_times['phase1'] = time.time() - phase1_start
        
        # Phase 2: MEASUREMENT parallel chunk processing
        print("\n=== Phase 2: MEASUREMENT Parallel Processing ===")
        phase2_start = time.time()
        measurement_file = standardized_dir / "MEASUREMENT_standardized.csv"
        
        if measurement_file.exists():
            print(f"Processing MEASUREMENT with {MEASUREMENT_CHUNKS} parallel chunks...")
            measurement_start = time.time()
            
            with ProcessPoolExecutor(max_workers=MEASUREMENT_CHUNKS) as executor:
                # Submit all chunks
                chunk_futures = []
                for chunk_id in range(MEASUREMENT_CHUNKS):
                    future = executor.submit(
                        process_measurement_chunk,
                        (chunk_id, MEASUREMENT_CHUNKS, measurement_file, output_dir)
                    )
                    chunk_futures.append(future)
                
                # Collect chunk results
                chunk_results = []
                for i, future in enumerate(as_completed(chunk_futures)):
                    try:
                        chunk_id, patient_counts, temp_dir = future.result(timeout=120)
                        chunk_results.append((chunk_id, patient_counts, temp_dir))
                        print(f"  ✓ Chunk {chunk_id + 1}/{MEASUREMENT_CHUNKS} completed")
                    except Exception as e:
                        print(f"  ✗ Chunk {i} failed: {e}")
                        logging.error(f"MEASUREMENT chunk {i} failed: {e}")
            
            # Merge chunks
            if chunk_results:
                print("  Merging MEASUREMENT chunks...")
                merge_stats = merge_measurement_chunks(output_dir, patient_data_dir, MEASUREMENT_CHUNKS)
                
                measurement_time = time.time() - measurement_start
                self.extraction_results['statistics']['MEASUREMENT'] = {
                    'total_records': merge_stats['total_records'],
                    'unique_patients': merge_stats['unique_patients'],
                    'processing_time': measurement_time
                }
                print(f"  ✓ MEASUREMENT completed: {merge_stats['total_records']:,} records, "
                     f"{merge_stats['unique_patients']} patients in {measurement_time:.2f}s")
        else:
            print("  MEASUREMENT file not found, skipping...")
        
        self.phase_times['phase2'] = time.time() - phase2_start
        
        # Step 3: Process basic tables (still sequential, they're small)
        print(f"\n=== Phase 3: Basic Tables ===")
        print(f"Processing {len(BASIC_TABLES)} basic tables...")
        phase3_start = time.time()
        
        for idx, table in enumerate(BASIC_TABLES, 1):
            print(f"  [{idx}/{len(BASIC_TABLES)}] Processing {table}...")
            table_start = time.time()
            self.split_basic_table_by_patient(table)
            table_time = time.time() - table_start
            if table in self.extraction_results['statistics']:
                self.extraction_results['statistics'][table]['processing_time'] = table_time
                records = self.extraction_results['statistics'][table].get('total_records', 0)
                print(f"    ✓ Completed: {records:,} records in {table_time:.2f}s")
        
        self.phase_times['phase3'] = time.time() - phase3_start
        
        # Step 4: Calculate pre-ICU statistics
        print("\n=== Phase 4: Pre-ICU Statistics ===")
        phase4_start = time.time()
        
        if icu_summaries:
            print("Calculating pre-ICU statistics...")
            self.calculate_pre_icu_statistics(icu_summaries)
            stats_time = time.time() - phase4_start
            print(f"  ✓ Completed: {len(icu_summaries)} patients analyzed in {stats_time:.2f}s")
        else:
            print("Skipping pre-ICU statistics (no ICU patients found)")
        
        self.phase_times['phase4'] = time.time() - phase4_start
        
        # Calculate total time
        total_time = time.time() - start_time
        self.extraction_results['statistics']['total_extraction_time'] = total_time
        
        # Save extraction results
        results_path = output_dir / "extraction_results.json"
        with open(results_path, 'w') as f:
            json.dump(self.extraction_results, f, indent=2)
        
        # Generate report
        self.generate_report()
        
        # Final summary
        print("\n" + "="*80)
        print("EXTRACTION COMPLETED SUCCESSFULLY")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total execution time: {total_time:.2f} seconds")
        print(f"\nOutputs saved to: {output_dir}")
        print("="*80)
        
        # Performance breakdown
        self.print_performance_breakdown(total_time)


def main():
    """Main execution function."""
    extractor = PatientDataExtractor()
    extractor.run()


if __name__ == "__main__":
    main()
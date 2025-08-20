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


class PatientDataExtractor:
    def __init__(self):
        self.extraction_results = {
            "extraction_date": datetime.now().isoformat(),
            "dataset": "subdataset_1000",
            "icu_concept_ids": ICU_CONCEPT_IDS,
            "statistics": {},
            "errors": []
        }
        self.skipped_patients = []
        
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
        
        logging.info(f"Extracted ICU summaries for {total_icu_patients} patients")
        self.extraction_results['statistics']['total_icu_patients'] = total_icu_patients
        self.extraction_results['statistics']['icu_extraction_time'] = time.time() - t0
        
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
        self.extraction_results['statistics']['pre_icu_calculation_time'] = time.time() - t0
    
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
    
    def run(self):
        """Run the complete extraction process."""
        # Start timing
        start_time = time.time()
        
        # Simplified startup
        print(f"\nExtracting patient data for {len(TABLES_TO_PROCESS) + len(BASIC_TABLES)} tables...")
        print(f"ICU Concept IDs: {', '.join(map(str, ICU_CONCEPT_IDS))}")
        
        # Step 1: Extract ICU summaries
        print("\nStep 1/4: Extracting ICU visit summaries...")
        logging.info("Extracting ICU visit summaries...")
        icu_summaries = self.extract_icu_summaries()
        
        # Step 2: Split standardized tables by patient
        print(f"\nStep 2/4: Processing {len(TABLES_TO_PROCESS)} standardized tables")
        logging.info("Splitting standardized tables by patient...")
        for idx, table in enumerate(TABLES_TO_PROCESS, 1):
            print(f"\n[{idx}/{len(TABLES_TO_PROCESS)}] Processing {table}...")
            self.split_table_by_patient(table)
        
        # Step 3: Split basic tables by patient
        print(f"\nStep 3/4: Processing {len(BASIC_TABLES)} basic tables")
        logging.info("Processing basic tables...")
        for idx, table in enumerate(BASIC_TABLES, 1):
            print(f"\n[{idx}/{len(BASIC_TABLES)}] Processing {table}...")
            self.split_basic_table_by_patient(table)
        
        # Step 4: Calculate pre-ICU statistics
        if icu_summaries:
            print("\nStep 4/4: Calculating pre-ICU statistics")
            logging.info("Calculating pre-ICU statistics...")
            self.calculate_pre_icu_statistics(icu_summaries)
        else:
            print("\nStep 4/4: Skipping pre-ICU statistics (no ICU patients found)")
        
        # Save extraction results
        results_path = output_dir / "extraction_results.json"
        with open(results_path, 'w') as f:
            json.dump(self.extraction_results, f, indent=2)
        
        # Generate report
        self.generate_report()
        
        # Calculate total time
        total_time = time.time() - start_time
        self.extraction_results['statistics']['total_extraction_time'] = total_time
        
        print(f"\nExtraction completed. Results saved to: {output_dir}")
        print(f"Total execution time: {total_time:.2f} seconds")
        
        # Performance breakdown
        print("\n" + "="*50)
        print("PERFORMANCE BREAKDOWN - ICU Data Extraction")
        print("="*50)
        
        stats = self.extraction_results['statistics']
        
        # Calculate component times
        icu_time = stats.get('icu_extraction_time', 0)
        pre_icu_time = stats.get('pre_icu_calculation_time', 0)
        
        # Calculate table processing times
        standardized_time = sum(stats.get(t, {}).get('processing_time', 0) for t in TABLES_TO_PROCESS)
        basic_time = sum(stats.get(t, {}).get('processing_time', 0) for t in BASIC_TABLES)
        
        print(f"ICU summaries:         {icu_time:.2f}s ({icu_time/total_time*100:.1f}%)")
        print(f"Standardized tables:   {standardized_time:.2f}s ({standardized_time/total_time*100:.1f}%)")
        print(f"Basic tables:          {basic_time:.2f}s ({basic_time/total_time*100:.1f}%)")
        print(f"Pre-ICU statistics:    {pre_icu_time:.2f}s ({pre_icu_time/total_time*100:.1f}%)")
        
        # Find slowest tables
        table_times = []
        for table in TABLES_TO_PROCESS + BASIC_TABLES:
            if table in stats and 'processing_time' in stats[table]:
                table_times.append((table, stats[table]['processing_time']))
        
        table_times.sort(key=lambda x: x[1], reverse=True)
        
        print("\nSlowest tables:")
        for name, time_taken in table_times[:3]:
            if time_taken > 0:
                print(f"  {name}: {time_taken:.2f}s")
        
        print("="*50)


def main():
    """Main execution function."""
    extractor = PatientDataExtractor()
    extractor.run()


if __name__ == "__main__":
    main()
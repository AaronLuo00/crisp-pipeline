#!/usr/bin/env python
"""Extract patient data and organize by patient ID with ICU timeline information."""

import os
import csv
import json
import glob
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

# Add the current module directory to path for imports
import sys
from pathlib import Path as PathImport

module_dir = PathImport(__file__).parent
if str(module_dir) not in sys.path:
    sys.path.insert(0, str(module_dir))

# Add utils directory to path for file_splitter import
utils_dir = PathImport(__file__).parent.parent / "utils"
if str(utils_dir) not in sys.path:
    sys.path.insert(0, str(utils_dir))

# Import parallel processing functions
from parallel_extraction import (
    process_table_batch,
    process_single_table_worker,
    process_measurement_chunk,
    merge_measurement_chunks
)

# Import byte-offset utilities for MEASUREMENT chunking
from file_splitter import get_csv_byte_ranges, get_total_row_count_fast
from file_handle_cache import FileHandleCache

# Platform-specific settings for performance optimization
if platform.system() == 'Windows':
    PROGRESS_INTERVAL = 30.0  # Less frequent updates (reduce overhead)
    CHUNK_SIZE = 500000  # Larger chunks for Windows (better I/O performance)
    WRITE_BUFFER_SIZE = 50000  # 50K rows batch write
    FILE_BUFFER_SIZE = 2 * 1024 * 1024  # 2MB file buffer
else:
    PROGRESS_INTERVAL = 10.0  # Default for macOS/Linux
    CHUNK_SIZE = 200000  # Default for macOS/Linux
    WRITE_BUFFER_SIZE = 100000  # 100K rows batch write
    FILE_BUFFER_SIZE = 4 * 1024 * 1024  # 4MB file buffer

# Setup logging will be configured after output_dir is created

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

# Patient data goes to root level, statistics stays in output
patient_data_dir = project_root / "extracted_patient_data"
patient_data_dir = patient_data_dir.resolve()

# Create output directories
for dir_path in [output_dir, patient_data_dir]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Logging will be configured in main() to avoid multiple initialization in multiprocessing

# Configuration
TABLES_TO_PROCESS = [
    'MEASUREMENT', 'OBSERVATION', 'DRUG_EXPOSURE', 'CONDITION_OCCURRENCE', 
    'VISIT_OCCURRENCE', 'VISIT_DETAIL', 'PROCEDURE_OCCURRENCE', 
    'DEVICE_EXPOSURE', 'SPECIMEN', 'CONDITION_ERA', 'DRUG_ERA'
]

def estimate_file_rows(file_path, sample_size=1024*1024):
    """Estimate total rows by sampling first 1MB of file.
    
    For small files (< 100MB), uses get_total_row_count_fast for exact counts.
    For larger files, uses 1MB sampling to avoid full file scan on non-MEASUREMENT tables.
    (MEASUREMENT uses byte-offset chunking which doesn't need this function at all.)
    """
    file_size = file_path.stat().st_size
    
    if file_size == 0:
        return 0
    
    # For files under 100MB, use exact counting (fast enough)
    if file_size < 100 * 1024 * 1024:
        try:
            return get_total_row_count_fast(str(file_path))
        except Exception:
            pass  # Fall through to estimation
    
    # Read first 1MB (or entire file if smaller)
    actual_sample_size = min(sample_size, file_size)
    
    with open(file_path, 'rb') as f:
        sample = f.read(actual_sample_size)
    
    # Count lines in sample
    line_count = sample.count(b'\n')
    
    if line_count == 0:
        return 1  # At least header row
    
    # Estimate total lines based on sample
    if actual_sample_size < file_size and actual_sample_size > 0:
        # Extrapolate based on sample
        estimated_lines = int(line_count * (file_size / actual_sample_size))
    else:
        # We read the whole file
        estimated_lines = line_count
    
    return max(1, estimated_lines - 1)  # Subtract header row

# Sepsis-related concept IDs for detection
SEPSIS_CONCEPTS = {
    # Core sepsis diagnoses
    132797: "Sepsis",
    196236: "Septic shock",
    37394658: "Severe sepsis",
    4000938: "Bacterial sepsis",
    4029281: "Sepsis syndrome",
    36717263: "Sepsis without acute organ dysfunction",
    40487101: "Clinical sepsis",
    
    # Pathogen-specific sepsis
    40491960: "Sepsis caused by MRSA",
    40489907: "Sepsis caused by Staphylococcus aureus",
    40493038: "Sepsis caused by Gram negative bacteria",
    40487064: "Sepsis caused by Escherichia coli",
    40491961: "Sepsis caused by Pseudomonas",
    42539372: "Sepsis caused by Klebsiella pneumoniae",
    40486631: "Sepsis caused by anaerobic bacteria",
    
    # Source/context-specific sepsis
    36715430: "Sepsis due to urinary tract infection",
    44782822: "Postoperative sepsis",
    40484176: "Neutropenic sepsis",
}

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
MAX_WORKERS = max(1, mp.cpu_count() - 2)  # Reserve 2 cores for system
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
        """Extract ICU visit episodes for each patient."""
        t0 = time.time()
        logging.info("Extracting ICU visit episodes...")
        
        visit_detail_file = standardized_dir / "VISIT_DETAIL_standardized.csv"
        if not visit_detail_file.exists():
            logging.error(f"VISIT_DETAIL file not found: {visit_detail_file}")
            return {}
        
        # Dictionary to store all ICU episodes for each patient
        all_patient_episodes = {}
        total_icu_patients = 0
        total_icu_episodes = 0
        
        try:
            # Use sampling to estimate rows and chunks
            estimated_rows = estimate_file_rows(visit_detail_file)
            
            if estimated_rows > 0:
                estimated_chunks = max(1, (estimated_rows + CHUNK_SIZE - 1) // CHUNK_SIZE)
            else:
                logging.warning(f"VISIT_DETAIL file appears to be empty")
                return all_patient_episodes
            
            # Read in chunks for memory efficiency
            chunks = pd.read_csv(visit_detail_file, chunksize=CHUNK_SIZE, dtype={"person_id": str}, low_memory=False)
            
            # Only use progress bar if we have multiple chunks
            if estimated_chunks > 1:
                chunk_iterator = tqdm(chunks, total=estimated_chunks, 
                                     desc="Extracting ICU episodes", 
                                     leave=False, ascii=True, 
                                     miniters=1,
                                     mininterval=PROGRESS_INTERVAL)
            else:
                logging.info(f"Processing single chunk without progress bar")
                chunk_iterator = chunks
            
            # Collect all ICU visits first
            all_icu_visits = []
            for chunk in chunk_iterator:
                # Filter ICU visits
                icu_visits = chunk[chunk['visit_detail_concept_id'].isin(ICU_CONCEPT_IDS)].copy()
                if not icu_visits.empty:
                    all_icu_visits.append(icu_visits)
            
            # Combine all ICU visits
            if all_icu_visits:
                icu_df = pd.concat(all_icu_visits, ignore_index=True)
                
                # Parse datetime columns with explicit format for consistency
                # Use specific format since Module 4 now guarantees this format
                icu_df['visit_detail_start_datetime'] = pd.to_datetime(
                    icu_df['visit_detail_start_datetime'], 
                    format='%Y-%m-%d %H:%M:%S'
                )
                icu_df['visit_detail_end_datetime'] = pd.to_datetime(
                    icu_df['visit_detail_end_datetime'], 
                    format='%Y-%m-%d %H:%M:%S'
                )
                
                # Process each patient's ICU episodes
                for person_id in icu_df['person_id'].unique():
                    patient_icu = icu_df[icu_df['person_id'] == person_id].sort_values('visit_detail_start_datetime')
                    
                    # Identify separate ICU episodes
                    # Since Module 4 already merged visits within 60 minutes, 
                    # each row here represents a separate episode
                    episodes = []
                    for idx, (_, visit) in enumerate(patient_icu.iterrows(), 1):
                        # Calculate duration in hours
                        duration = (visit['visit_detail_end_datetime'] - visit['visit_detail_start_datetime']).total_seconds() / 3600
                        
                        episodes.append({
                            'episode_number': idx,
                            'episode_start': visit['visit_detail_start_datetime'],
                            'episode_end': visit['visit_detail_end_datetime'],
                            'duration_hours': round(duration, 2),
                            'is_first_icu': idx == 1,
                            'visit_detail_ids': str(visit['visit_detail_id'])
                        })
                    
                    # Store episodes for this patient
                    all_patient_episodes[person_id] = {
                        'episodes': episodes,
                        'first_icu_start': episodes[0]['episode_start'] if episodes else None,
                        'first_icu_end': episodes[0]['episode_end'] if episodes else None,
                        'total_episodes': len(episodes)
                    }
                    
                    # Create patient folder and save episodes
                    patient_path = self.get_patient_path(person_id)
                    patient_path.mkdir(parents=True, exist_ok=True)
                    
                    # Save ICU episodes to CSV
                    episodes_file = patient_path / "icu_episodes.csv"
                    episodes_df = pd.DataFrame(episodes)
                    episodes_df.to_csv(episodes_file, index=False)
                    
                    total_icu_patients += 1
                    total_icu_episodes += len(episodes)
                        
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logging.error(f"Error processing VISIT_DETAIL: {str(e)}")
            logging.error(f"Full traceback:\n{error_detail}")
            logging.error(f"File path: {visit_detail_file}")
            if visit_detail_file.exists():
                logging.error(f"File size: {visit_detail_file.stat().st_size} bytes")
            logging.error(f"CHUNK_SIZE: {CHUNK_SIZE}")
            if 'estimated_chunks' in locals():
                logging.error(f"Estimated chunks: {estimated_chunks}")
            self.extraction_results['errors'].append(f"VISIT_DETAIL processing: {str(e)}")
            # Re-raise the exception to be caught by the parent process
            raise
        
        extraction_time = time.time() - t0
        logging.info(f"Extracted {total_icu_episodes} ICU episodes for {total_icu_patients} patients in {extraction_time:.2f}s")
        self.extraction_results['statistics']['total_icu_patients'] = total_icu_patients
        self.extraction_results['statistics']['total_icu_episodes'] = total_icu_episodes
        self.extraction_results['statistics']['avg_episodes_per_patient'] = round(total_icu_episodes / total_icu_patients, 2) if total_icu_patients > 0 else 0
        self.extraction_results['statistics']['icu_extraction_time'] = extraction_time
        
        return all_patient_episodes
    
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
            # Use estimation for better performance on large files
            total_rows = estimate_file_rows(input_file)
            
            # Calculate number of chunks based on actual rows
            num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
            
            # Process in chunks
            reader = pd.read_csv(input_file, dtype={"person_id": str}, chunksize=CHUNK_SIZE)
            
            # Enable progress bar for tables with significant data (consistent with Module 4)
            show_progress = total_rows > 10000  # Show progress for tables > 10k rows
            
            # Create progress bar with row-based tracking (consistent with Module 4)
            pbar = tqdm(total=total_rows, desc=f"Processing {table_name}", ascii=True,
                       unit='rows',
                       miniters=max(100, total_rows//100 if total_rows > 0 else 100),  # Update every 1% or at least 100 rows
                       mininterval=PROGRESS_INTERVAL,  # Update at most once per 10 seconds
                       leave=False, ncols=100,
                       disable=not show_progress)
            
            # Use LRU file handle cache to avoid fd exhaustion with 500K+ patients
            file_cache = FileHandleCache(max_handles=5000, buffer_size=FILE_BUFFER_SIZE)
            
            try:
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
                        
                        # Get or create file handle via LRU cache
                        file_path = patient_path / f"{table_name}.csv"
                        handle, needs_header = file_cache.get_handle(str(file_path))
                        if needs_header:
                            group_df.to_csv(handle, index=False)
                        else:
                            group_df.to_csv(handle, header=False, index=False)
                        
                        patients_processed.add(person_id)
            finally:
                cache_stats = file_cache.get_stats()
                file_cache.close_all()
                logging.info(f"[{table_name}] FileHandleCache stats: {cache_stats}")
            
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
            raise  # Re-raise: silent partial data is worse than a loud failure
    
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
            
            for person_id, group_df in tqdm(grouped, desc=f"Processing {table_name}", ascii=True, 
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
            raise  # Re-raise: silent partial data is worse than a loud failure
    
    def generate_patient_labels(self, person_id: str, patient_path: Path, 
                                  episode_info: dict = None) -> dict:
        """
        Generate comprehensive patient labels including mortality, readmission, and LOS metrics.
        
        Args:
            person_id: Patient ID
            patient_path: Path to patient data directory
            episode_info: Dictionary containing ICU episodes information (None for non-ICU patients)
        
        Returns:
            Dictionary containing all labels for saving as patient_labels.json
        """
        # Check if patient has ICU episodes
        has_icu = episode_info is not None and episode_info.get('episodes') and len(episode_info.get('episodes', [])) > 0
        
        if has_icu:
            # 1. Get last ICU episode information
            episodes = episode_info['episodes']
            last_episode = episodes[-1]  # Last episode
            last_icu_start = pd.to_datetime(last_episode['episode_start'])
            last_icu_end = pd.to_datetime(last_episode['episode_end'])
        else:
            # Check if icu_episodes.csv exists (for cases where episode_info not passed)
            icu_episodes_file = patient_path / "icu_episodes.csv"
            if icu_episodes_file.exists():
                try:
                    icu_df = pd.read_csv(icu_episodes_file)
                    if not icu_df.empty:
                        # Convert to list of dicts format
                        episodes = icu_df.to_dict('records')
                        last_episode = episodes[-1]  # Last episode
                        last_icu_start = pd.to_datetime(last_episode['episode_start'])
                        last_icu_end = pd.to_datetime(last_episode['episode_end'])
                        has_icu = True
                    else:
                        episodes = []
                        last_episode = None
                        last_icu_start = None
                        last_icu_end = None
                except Exception:
                    episodes = []
                    last_episode = None
                    last_icu_start = None
                    last_icu_end = None
            else:
                # No ICU episodes
                episodes = []
                last_episode = None
                last_icu_start = None
                last_icu_end = None
        
        # 2. Read death information
        death_file = patient_path / "DEATH.csv"
        has_death = False
        death_datetime = None
        
        if death_file.exists():
            try:
                death_df = pd.read_csv(death_file)
                if not death_df.empty:
                    # Priority: death_datetime, then death_date
                    if 'death_datetime' in death_df.columns and pd.notna(death_df.iloc[0]['death_datetime']):
                        death_datetime = pd.to_datetime(death_df.iloc[0]['death_datetime'])
                    elif 'death_date' in death_df.columns and pd.notna(death_df.iloc[0]['death_date']):
                        death_datetime = pd.to_datetime(death_df.iloc[0]['death_date'])
                    
                    if death_datetime is not None:
                        has_death = True
            except Exception as e:
                logging.warning(f"Error reading death file for {person_id}: {e}")
        
        # 3. Build labels dictionary
        labels = {
            "patient_id": person_id,
            "has_icu_admission": 1 if has_icu else 0,
            "icu_info": {},
            "mortality": {},
            "readmission": {},
            "los": {},
            "sepsis": {}
        }
        
        # Fill ICU info based on whether patient has ICU episodes
        if has_icu:
            labels["icu_info"] = {
                "last_icu_start": last_episode['episode_start'],
                "last_icu_end": last_episode['episode_end'],
                "last_icu_duration_hours": last_episode['duration_hours'],
                "last_icu_duration_days": round(last_episode['duration_hours'] / 24, 2),
                "total_episodes": len(episodes),
                "last_episode_number": last_episode['episode_number']
            }
        else:
            labels["icu_info"] = {
                "last_icu_start": None,
                "last_icu_end": None,
                "last_icu_duration_hours": 0,
                "last_icu_duration_days": 0,
                "total_episodes": 0,
                "last_episode_number": 0
            }
        
        # 4. Calculate mortality labels
        if has_death and death_datetime is not None:
            if has_icu and last_icu_start is not None:
                # Calculate time from last ICU admission to death
                hours_from_last_admission = (death_datetime - last_icu_start).total_seconds() / 3600
                days_from_last_admission = hours_from_last_admission / 24
                
                labels["mortality"] = {
                    "mortality_icu_48h": int(hours_from_last_admission <= 48),
                    "mortality_icu_7day": int(days_from_last_admission <= 7),
                    "mortality_icu_30day": int(days_from_last_admission <= 30),
                    "days_to_death": round(days_from_last_admission, 2),
                    "death_after_discharge": int(death_datetime > last_icu_end),
                    "death_datetime": death_datetime.isoformat()
                }
            else:
                # Non-ICU patient with death record
                labels["mortality"] = {
                    "mortality_icu_48h": 0,  # No ICU admission
                    "mortality_icu_7day": 0,
                    "mortality_icu_30day": 0,
                    "days_to_death": -1,  # Cannot calculate without ICU admission
                    "death_after_discharge": 0,
                    "death_datetime": death_datetime.isoformat()
                }
        else:
            # No death record
            labels["mortality"] = {
                "mortality_icu_48h": 0,
                "mortality_icu_7day": 0,
                "mortality_icu_30day": 0,
                "days_to_death": -1,  # -1 indicates no death
                "death_after_discharge": 0,
                "death_datetime": None
            }
        
        # 5. Calculate readmission labels
        if len(episodes) > 1:
            # Days from first ICU discharge to second ICU admission
            first_episode = episodes[0]
            second_episode = episodes[1]
            first_end = pd.to_datetime(first_episode['episode_end'])
            second_start = pd.to_datetime(second_episode['episode_start'])
            
            days_between = (second_start - first_end).days
            labels["readmission"] = {
                "has_readmission": 1,
                "days_to_first_readmission": days_between,
                "readmission_within_7days": int(days_between <= 7),
                "readmission_within_30days": int(days_between <= 30),
                "readmission_within_90days": int(days_between <= 90)
            }
        else:
            # Only one ICU episode, no readmission
            labels["readmission"] = {
                "has_readmission": 0,
                "days_to_first_readmission": -1,
                "readmission_within_7days": 0,
                "readmission_within_30days": 0,
                "readmission_within_90days": 0
            }
        
        # 6. Calculate LOS labels
        if has_icu and last_episode:
            duration_hours = last_episode['duration_hours']
            duration_days = round(duration_hours / 24, 2)
            labels["los"] = {
                "los_greater_than_3days": int(duration_hours > 72),
                "los_greater_than_7days": int(duration_hours > 168),
                "last_icu_los_days": duration_days
            }
        else:
            labels["los"] = {
                "los_greater_than_3days": 0,
                "los_greater_than_7days": 0,
                "last_icu_los_days": 0
            }
        
        # 7. Calculate Sepsis labels
        condition_file = patient_path / "CONDITION_OCCURRENCE.csv"
        has_sepsis_after_icu = False
        sepsis_within_24h = False
        sepsis_within_48h = False
        sepsis_within_7days = False
        days_to_sepsis = -1
        sepsis_type = None
        
        if has_icu and last_icu_start and condition_file.exists():
            try:
                conditions_df = pd.read_csv(condition_file)
                
                # Filter for sepsis conditions
                sepsis_conditions = conditions_df[
                    conditions_df['condition_concept_id'].isin(SEPSIS_CONCEPTS.keys())
                ].copy()  # Create a copy to avoid SettingWithCopyWarning
                
                if not sepsis_conditions.empty:
                    # Parse condition dates
                    sepsis_conditions['condition_start_datetime'] = pd.to_datetime(
                        sepsis_conditions['condition_start_datetime'], errors='coerce'
                    )
                    
                    # Filter for sepsis AFTER last ICU admission
                    post_icu_sepsis = sepsis_conditions[
                        sepsis_conditions['condition_start_datetime'] > last_icu_start
                    ].copy()
                    
                    if not post_icu_sepsis.empty:
                        has_sepsis_after_icu = True
                        
                        # Find earliest post-ICU sepsis
                        earliest_sepsis = post_icu_sepsis['condition_start_datetime'].min()
                        hours_to_sepsis = (earliest_sepsis - last_icu_start).total_seconds() / 3600
                        days_to_sepsis = round(hours_to_sepsis / 24, 2)
                        
                        # Check timing categories
                        sepsis_within_24h = int(hours_to_sepsis <= 24)
                        sepsis_within_48h = int(hours_to_sepsis <= 48)
                        sepsis_within_7days = int(hours_to_sepsis <= 168)  # 7 days = 168 hours
                        
                        # Determine sepsis type (prioritize by severity)
                        sepsis_types_found = post_icu_sepsis['condition_concept_id'].unique()
                        
                        # Priority order: Septic shock > Severe sepsis > Sepsis > Bacterial/Clinical sepsis > Pathogen-specific > others
                        if 196236 in sepsis_types_found:  # Septic shock
                            sepsis_type = "Septic shock"
                        elif 37394658 in sepsis_types_found:  # Severe sepsis
                            sepsis_type = "Severe sepsis"
                        elif 132797 in sepsis_types_found:  # Sepsis
                            sepsis_type = "Sepsis"
                        elif 4000938 in sepsis_types_found:  # Bacterial sepsis
                            sepsis_type = "Bacterial sepsis"
                        elif 40487101 in sepsis_types_found:  # Clinical sepsis
                            sepsis_type = "Clinical sepsis"
                        elif 4029281 in sepsis_types_found:  # Sepsis syndrome
                            sepsis_type = "Sepsis syndrome"
                        else:
                            # Check for pathogen-specific or context-specific sepsis
                            sepsis_type = None
                            
                            # Prioritize pathogen-specific sepsis
                            pathogen_specific = [40491960, 40489907, 40493038, 40487064, 40491961, 42539372, 40486631]
                            for concept_id in pathogen_specific:
                                if concept_id in sepsis_types_found:
                                    sepsis_type = SEPSIS_CONCEPTS[concept_id]
                                    break
                            
                            # Then check context-specific sepsis
                            if not sepsis_type:
                                context_specific = [36715430, 44782822, 40484176]
                                for concept_id in context_specific:
                                    if concept_id in sepsis_types_found:
                                        sepsis_type = SEPSIS_CONCEPTS[concept_id]
                                        break
                            
                            # Finally, use the first available type
                            if not sepsis_type:
                                first_concept = sepsis_types_found[0]
                                sepsis_type = SEPSIS_CONCEPTS.get(first_concept, "Other sepsis")
                            
            except Exception as e:
                logging.warning(f"Error reading conditions for {person_id}: {e}")
        
        # Fill sepsis labels
        labels["sepsis"] = {
            "has_sepsis_after_icu": int(has_sepsis_after_icu),
            "sepsis_within_24h": int(sepsis_within_24h),
            "sepsis_within_48h": int(sepsis_within_48h),
            "sepsis_within_7days": int(sepsis_within_7days),
            "days_to_sepsis": days_to_sepsis,
            "sepsis_type": sepsis_type
        }
        
        # 8. Add generation timestamp
        labels["generated_at"] = datetime.now().isoformat()
        
        return labels
    
    def calculate_pre_icu_statistics(self, all_patient_episodes: dict):
        """Calculate statistics for records before first ICU admission and generate labels."""
        t0 = time.time()
        logging.info("Calculating pre-ICU statistics and generating labels...")
        
        results = []
        labels_generated = 0  # Counter for successfully generated labels
        
        # In-memory label accumulators — avoid re-reading 500K JSON files from disk
        label_agg = {
            'mortality_48h': 0, 'mortality_7day': 0, 'mortality_30day': 0,
            'readmission_total': 0, 'readmission_7day': 0, 'readmission_30day': 0,
            'readmission_90day': 0,
            'los_3day': 0, 'los_7day': 0,
            'sepsis_post_icu': 0, 'sepsis_24h': 0, 'sepsis_48h': 0, 'sepsis_7day': 0,
        }
        
        # Process each patient with ICU records
        if not all_patient_episodes:
            logging.info("No ICU patients found, skipping pre-ICU statistics")
            return
        
        # Use progress bar only for sufficient data
        if len(all_patient_episodes) >= 100:
            patient_iterator = tqdm(all_patient_episodes.items(), ascii=True, 
                                   desc="Calculating pre-ICU stats", 
                                   unit="patients", leave=False, 
                                   mininterval=PROGRESS_INTERVAL)
        else:
            # For small datasets, don't use progress bar
            logging.info(f"Processing {len(all_patient_episodes)} ICU patients without progress bar")
            patient_iterator = all_patient_episodes.items()
        
        for person_id, episode_info in patient_iterator:
            patient_path = self.get_patient_path(person_id)
            
            if not patient_path.exists():
                self.skipped_patients.append({
                    "patient_id": person_id,
                    "skip_reason": "Patient folder not found"
                })
                continue
            
            try:
                # Use only the FIRST ICU admission time
                first_icu_start = pd.to_datetime(episode_info['first_icu_start'])
                total_count = 0
                earliest_record = None
                latest_record = None
                
                # Count records in specified tables and find date range
                for table_name, time_col in PRE_ICU_STAT_TABLES.items():
                    table_path = patient_path / f"{table_name}.csv"
                    if not table_path.exists():
                        continue
                    
                    try:
                        df = pd.read_csv(table_path, parse_dates=[time_col], low_memory=False)
                        # Filter records before FIRST ICU admission
                        pre_icu_df = df[df[time_col] <= first_icu_start]
                        count = pre_icu_df.shape[0]
                        total_count += count
                        
                        # Track earliest and latest records
                        if not pre_icu_df.empty:
                            table_earliest = pre_icu_df[time_col].min()
                            table_latest = pre_icu_df[time_col].max()
                            
                            if earliest_record is None or table_earliest < earliest_record:
                                earliest_record = table_earliest
                            if latest_record is None or table_latest > latest_record:
                                latest_record = table_latest
                                
                    except Exception as e:
                        logging.warning(f"Error reading {table_path}: {e}")
                
                # Calculate days of data before ICU
                days_of_data = 0
                if earliest_record and latest_record:
                    days_of_data = (latest_record - earliest_record).days + 1
                
                # Read patient demographics
                person_path = patient_path / "PERSON.csv"
                age_at_icu = None
                gender = None
                race = None
                
                if person_path.exists():
                    try:
                        person_df = pd.read_csv(person_path, low_memory=False)
                        if not person_df.empty:
                            # Calculate age at first ICU admission
                            birth_year = person_df['year_of_birth'].iloc[0]
                            if pd.notna(birth_year):
                                age_at_icu = first_icu_start.year - int(birth_year)
                            
                            # Get gender
                            gender_id = person_df['gender_concept_id'].iloc[0]
                            if gender_id == 8507:
                                gender = 'Male'
                            elif gender_id == 8532:
                                gender = 'Female'
                            else:
                                gender = f'Other({gender_id})'
                            
                            # Get race
                            race_id = person_df['race_concept_id'].iloc[0]
                            race_mapping = {
                                8527: 'White',
                                8516: 'Black',
                                8515: 'Asian',
                                8657: 'Native American',
                                8557: 'Pacific Islander',
                                0: 'Unknown'
                            }
                            race = race_mapping.get(race_id, f'Other({race_id})')
                    except Exception as e:
                        logging.warning(f"Error reading person data for {person_id}: {e}")
                
                # Check death record
                death_path = patient_path / "DEATH.csv"
                has_death_record = death_path.exists() and death_path.stat().st_size > 0
                
                results.append({
                    "patient_id": person_id,
                    "count_before_icu": total_count,
                    "days_of_data_before_icu": days_of_data,
                    "age_at_icu_admission": age_at_icu,
                    "gender": gender,
                    "race": race,
                    "has_death_record": has_death_record
                })
                
                # Generate and save patient labels
                try:
                    labels = self.generate_patient_labels(person_id, patient_path, episode_info)
                    
                    # Save labels to JSON file
                    labels_path = patient_path / "patient_labels.json"
                    with open(labels_path, 'w') as f:
                        json.dump(labels, f, indent=2, default=str)
                    
                    labels_generated += 1
                    
                    # Accumulate label statistics in memory (no need to re-read from disk later)
                    label_agg['mortality_48h'] += labels['mortality']['mortality_icu_48h']
                    label_agg['mortality_7day'] += labels['mortality']['mortality_icu_7day']
                    label_agg['mortality_30day'] += labels['mortality']['mortality_icu_30day']
                    label_agg['readmission_total'] += labels['readmission']['has_readmission']
                    label_agg['readmission_7day'] += labels['readmission'].get('readmission_within_7days', 0)
                    label_agg['readmission_30day'] += labels['readmission']['readmission_within_30days']
                    label_agg['readmission_90day'] += labels['readmission'].get('readmission_within_90days', 0)
                    if 'los' in labels:
                        label_agg['los_3day'] += labels['los'].get('los_greater_than_3days', 0)
                        label_agg['los_7day'] += labels['los'].get('los_greater_than_7days', 0)
                    if 'sepsis' in labels:
                        label_agg['sepsis_post_icu'] += labels['sepsis'].get('has_sepsis_after_icu', 0)
                        label_agg['sepsis_24h'] += labels['sepsis'].get('sepsis_within_24h', 0)
                        label_agg['sepsis_48h'] += labels['sepsis'].get('sepsis_within_48h', 0)
                        label_agg['sepsis_7day'] += labels['sepsis'].get('sepsis_within_7days', 0)
                    
                    # Log only for patients with positive mortality labels or every 50 patients
                    if (labels['mortality']['mortality_icu_48h'] or 
                        labels['mortality']['mortality_icu_7day'] or 
                        labels['mortality']['mortality_icu_30day'] or
                        labels_generated % 50 == 0):
                        logging.debug(f"Generated labels for patient {person_id}: "
                                   f"mortality_icu_48h={labels['mortality']['mortality_icu_48h']}, "
                                   f"mortality_icu_7day={labels['mortality']['mortality_icu_7day']}, "
                                   f"mortality_icu_30day={labels['mortality']['mortality_icu_30day']}")
                    
                except Exception as label_error:
                    logging.warning(f"Could not generate labels for patient {person_id}: {label_error}")
                
            except Exception as e:
                logging.error(f"Error processing patient {person_id}: {e}")
                self.skipped_patients.append({
                    "patient_id": person_id,
                    "skip_reason": f"Processing error: {e}"
                })
        
        # Save results
        if results:
            results_df = pd.DataFrame(results)
            results_file = output_dir / "patient_cohort_statistics.csv"
            results_df.to_csv(results_file, index=False)
            
            # Calculate summary statistics
            self.extraction_results['statistics']['cohort_stats'] = {
                'total_patients': len(results),
                'avg_records_before_icu': float(results_df['count_before_icu'].mean()),
                'max_records_before_icu': int(results_df['count_before_icu'].max()),
                'min_records_before_icu': int(results_df['count_before_icu'].min()),
                'avg_days_of_data': float(results_df['days_of_data_before_icu'].mean()),
                'patients_with_death': int(results_df['has_death_record'].sum()),
                'avg_age_at_icu': float(results_df['age_at_icu_admission'].mean()) if results_df['age_at_icu_admission'].notna().any() else None
            }
            
            logging.info(f"Patient cohort statistics saved to: {results_file}")
        
        # Save skipped patients
        if self.skipped_patients:
            skipped_df = pd.DataFrame(self.skipped_patients)
            skipped_file = output_dir / "skipped_patients.csv"
            skipped_df.to_csv(skipped_file, index=False)
            logging.info(f"Skipped patients saved to: {skipped_file}")
        
        # Log label generation statistics from in-memory accumulators
        # (Previously re-read 500K+ JSON files from disk — now uses label_agg dict)
        if labels_generated > 0:
            logging.info(f"Generated labels for {labels_generated} ICU patients")
            
            n = labels_generated
            logging.info(f"Mortality summary - 48h: {label_agg['mortality_48h']}/{n} ({label_agg['mortality_48h']/n*100:.1f}%), "
                        f"7day: {label_agg['mortality_7day']}/{n} ({label_agg['mortality_7day']/n*100:.1f}%), "
                        f"30day: {label_agg['mortality_30day']}/{n} ({label_agg['mortality_30day']/n*100:.1f}%)")
            logging.info(f"Readmission summary - Total: {label_agg['readmission_total']}/{n} ({label_agg['readmission_total']/n*100:.1f}%), "
                        f"7day: {label_agg['readmission_7day']}/{n} ({label_agg['readmission_7day']/n*100:.1f}%), "
                        f"30day: {label_agg['readmission_30day']}/{n} ({label_agg['readmission_30day']/n*100:.1f}%), "
                        f"90day: {label_agg['readmission_90day']}/{n} ({label_agg['readmission_90day']/n*100:.1f}%)")
            logging.info(f"LOS summary - >3days: {label_agg['los_3day']}/{n} ({label_agg['los_3day']/n*100:.1f}%), "
                        f">7days: {label_agg['los_7day']}/{n} ({label_agg['los_7day']/n*100:.1f}%)")
            logging.info(f"Sepsis summary - Post-ICU: {label_agg['sepsis_post_icu']}/{n} ({label_agg['sepsis_post_icu']/n*100:.1f}%), "
                        f"24h: {label_agg['sepsis_24h']}/{n} ({label_agg['sepsis_24h']/n*100:.1f}%), "
                        f"48h: {label_agg['sepsis_48h']}/{n} ({label_agg['sepsis_48h']/n*100:.1f}%), "
                        f"7day: {label_agg['sepsis_7day']}/{n} ({label_agg['sepsis_7day']/n*100:.1f}%)")
        
        # Record processing time and label statistics
        processing_time = time.time() - t0
        self.extraction_results['statistics']['pre_icu_calculation_time'] = processing_time
        self.extraction_results['statistics']['labels_generated'] = labels_generated
    
    def generate_labels_for_all_patients(self, all_patient_episodes: dict):
        """Generate labels for all patients including those without ICU admissions."""
        t0 = time.time()
        logging.info("Generating labels for all patients (including non-ICU)...")
        
        # Get all extracted patients (handle both 2-level and 3-level structures)
        all_patients = set()
        if patient_data_dir.exists():
            # Find all PERSON.csv files and get their parent directory names as patient IDs
            for person_file in patient_data_dir.rglob("PERSON.csv"):
                patient_id = person_file.parent.name
                all_patients.add(patient_id)
        
        logging.info(f"Found {len(all_patients)} total extracted patients")
        
        # Get ICU patients
        icu_patients = set(all_patient_episodes.keys()) if all_patient_episodes else set()
        non_icu_patients = all_patients - icu_patients
        
        logging.info(f"ICU patients: {len(icu_patients)}, Non-ICU patients: {len(non_icu_patients)}")
        
        # Generate labels for non-ICU patients in parallel
        non_icu_labels_generated = 0
        
        # Function to process a single non-ICU patient
        def process_non_icu_patient(person_id):
            try:
                patient_path = self.get_patient_path(person_id)
                if not patient_path.exists():
                    return False
                
                # Generate labels for non-ICU patient
                labels = self.generate_patient_labels(person_id, patient_path, None)
                
                # Save labels to JSON file
                labels_path = patient_path / "patient_labels.json"
                with open(labels_path, 'w') as f:
                    json.dump(labels, f, indent=2, default=str)
                
                return True
                
            except Exception as e:
                logging.warning(f"Could not generate labels for non-ICU patient {person_id}: {e}")
                return False
        
        # Process non-ICU patients in parallel
        if non_icu_patients:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(process_non_icu_patient, pid): pid 
                          for pid in non_icu_patients}
                
                for future in as_completed(futures):
                    if future.result():
                        non_icu_labels_generated += 1
        
        # Log summary
        total_labels = len(icu_patients) + non_icu_labels_generated
        logging.info(f"Generated labels for {non_icu_labels_generated} non-ICU patients")
        logging.info(f"Total patients with labels: {total_labels}/{len(all_patients)}")
        
        # Update statistics
        self.extraction_results['statistics']['non_icu_labels_generated'] = non_icu_labels_generated
        self.extraction_results['statistics']['total_labels_generated'] = total_labels
        
        processing_time = time.time() - t0
        logging.info(f"Label generation for all patients completed in {processing_time:.2f}s")
    
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
            if 'total_icu_episodes' in stats:
                f.write(f"- **Total ICU episodes**: {stats['total_icu_episodes']:,}\n")
            if 'avg_episodes_per_patient' in stats:
                f.write(f"- **Average episodes per patient**: {stats['avg_episodes_per_patient']:.2f}\n")
            
            if 'cohort_stats' in stats:
                cohort_stats = stats['cohort_stats']
                f.write(f"- **Patients with pre-ICU analysis**: {cohort_stats['total_patients']:,}\n")
                f.write(f"- **Average records before ICU**: {cohort_stats['avg_records_before_icu']:.2f}\n")
                f.write(f"- **Max records before ICU**: {cohort_stats['max_records_before_icu']}\n")
                f.write(f"- **Min records before ICU**: {cohort_stats['min_records_before_icu']}\n")
                f.write(f"- **Average days of data before ICU**: {cohort_stats['avg_days_of_data']:.1f}\n")
                f.write(f"- **Patients with death records**: {cohort_stats['patients_with_death']}\n")
                if cohort_stats.get('avg_age_at_icu') is not None:
                    f.write(f"- **Average age at ICU admission**: {cohort_stats['avg_age_at_icu']:.1f} years\n")
            
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
            
            # Add output structure section
            f.write("\n## Output Data Structure\n\n")
            f.write("### Patient Data Location\n")
            f.write("The extracted patient data is stored in the project root directory:\n")
            f.write("- **Path**: `extracted_patient_data/`\n")
            f.write("- **Structure**: `extracted_patient_data/<patient_id>/<table_name>.csv`\n\n")
            
            f.write("### Data Format\n")
            f.write("Each patient folder contains:\n")
            f.write("- Individual CSV files for each OMOP table\n")
            f.write("- All records for that patient across all tables\n")
            f.write("- Timestamps preserved in original format\n")
            f.write("- Ready for direct use in ML pipelines\n\n")
            
            f.write("### Example Structure\n")
            f.write("```\n")
            f.write("extracted_patient_data/\n")
            f.write("|-- 400000000026076/\n")
            f.write("|   |-- MEASUREMENT.csv\n")
            f.write("|   |-- OBSERVATION.csv\n")
            f.write("|   |-- DRUG_EXPOSURE.csv\n")
            f.write("|   +-- ...\n")
            f.write("|-- 600000071123456/\n")
            f.write("|   +-- ...\n")
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
        logging.info("="*60)
        logging.info("PERFORMANCE BREAKDOWN - Parallel ICU Data Extraction")
        logging.info("="*60)
        
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
            logging.info("Overall Performance:")
            logging.info(f"  Total CPU time:        {cpu_time:.2f}s")
            logging.info(f"  Wall clock time:       {total_time:.2f}s")
            logging.info(f"  Speedup:               {speedup:.2f}x")
            logging.info(f"  Parallel efficiency:   {efficiency:.1f}%")
            logging.info(f"  Parallel tasks:        {parallel_tasks}")
        
        # Phase breakdown
        print(f"\nPhase Breakdown:")
        logging.info("Phase Breakdown:")
        if phase1_time > 0:
            print(f"  Phase 1 (Parallel Tables): {phase1_time:.2f}s ({phase1_time/total_time*100:.1f}%)")
            logging.info(f"  Phase 1 (Parallel Tables): {phase1_time:.2f}s ({phase1_time/total_time*100:.1f}%)")
        if phase2_time > 0:
            print(f"  Phase 2 (MEASUREMENT):      {phase2_time:.2f}s ({phase2_time/total_time*100:.1f}%)")
            logging.info(f"  Phase 2 (MEASUREMENT):      {phase2_time:.2f}s ({phase2_time/total_time*100:.1f}%)")
        if phase3_time > 0:
            print(f"  Phase 3 (Basic Tables):     {phase3_time:.2f}s ({phase3_time/total_time*100:.1f}%)")
            logging.info(f"  Phase 3 (Basic Tables):     {phase3_time:.2f}s ({phase3_time/total_time*100:.1f}%)")
        if phase4_time > 0:
            print(f"  Phase 4 (Pre-ICU Stats):    {phase4_time:.2f}s ({phase4_time/total_time*100:.1f}%)")
            logging.info(f"  Phase 4 (Pre-ICU Stats):    {phase4_time:.2f}s ({phase4_time/total_time*100:.1f}%)")
        phase5_time = self.phase_times.get('phase5', 0)
        if phase5_time > 0:
            print(f"  Phase 5 (All Labels):       {phase5_time:.2f}s ({phase5_time/total_time*100:.1f}%)")
            logging.info(f"  Phase 5 (All Labels):       {phase5_time:.2f}s ({phase5_time/total_time*100:.1f}%)")
        
        # Individual table performance
        print(f"\nTable Processing Times:")
        logging.info("Table Processing Times:")
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
                    logging.info(f"  {name:20s}: {time_taken:6.2f}s ({records:>10,} records, {rate:>10.0f} rec/s)")
                else:
                    print(f"  {name:20s}: {time_taken:6.2f}s")
                    logging.info(f"  {name:20s}: {time_taken:6.2f}s")
        
        print("="*60)
        logging.info("="*60)
    
    def run(self):
        """Run the complete extraction process with parallel optimization."""
        # Start timing
        start_time = time.time()
        
        # Simplified startup
        print(f"\nExtracting patient data for {len(TABLES_TO_PROCESS) + len(BASIC_TABLES)} tables...")
        print(f"ICU Concept IDs: {', '.join(map(str, ICU_CONCEPT_IDS))}")
        print(f"[PARALLEL PROCESSING]")
        print(f"  CPU cores available: {mp.cpu_count()}")
        print(f"  Using {MAX_WORKERS} workers (keeping 2 cores for system)")
        
        # Hard-coded table grouping for consistent processing
        # All tables will be processed regardless of size
        tables_without_measurement = [t for t in TABLES_TO_PROCESS if t != 'MEASUREMENT']
        
        # Process all non-MEASUREMENT tables individually in parallel
        # This ensures every table gets processed
        tables_to_process = []
        for table in tables_without_measurement:
            file_path = standardized_dir / f"{table}_standardized.csv"
            if file_path.exists():
                tables_to_process.append(table)
            else:
                logging.warning(f"Table {table} file not found: {file_path}")
        
        print(f"\nTables to process ({len(tables_to_process)}): {', '.join(tables_to_process)}")
        print(f"  (MEASUREMENT will be processed separately with parallel chunks)")
        
        # Phase 1: Parallel processing of ICU summaries and tables
        print("\n=== Phase 1: Parallel Processing ===")
        phase1_start = time.time()
        
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            
            # Step 1: Submit ICU summaries extraction (independent task)
            print("\nStep 1/3: Extracting ICU visit summaries (parallel)...")
            futures['icu'] = executor.submit(self.extract_icu_summaries)
            
            # Step 2: Submit all non-MEASUREMENT tables for parallel processing
            if tables_to_process:
                print(f"\nStep 2/3: Processing {len(tables_to_process)} tables in parallel...")
                for table in tables_to_process:
                    futures[f'table_{table}'] = executor.submit(
                        process_single_table_worker, table, standardized_dir, patient_data_dir
                    )
            
            # Collect parallel results
            all_patient_episodes = None
            completed_tasks = 0
            total_tasks = len(futures)
            
            for key, future in futures.items():
                try:
                    if key == 'icu':
                        all_patient_episodes = future.result()  # No timeout - let it complete
                        completed_tasks += 1
                        print(f"  [OK] ICU episodes extracted: {len(all_patient_episodes) if all_patient_episodes else 0} patients")
                        self.extraction_results['statistics']['total_icu_patients'] = len(all_patient_episodes) if all_patient_episodes else 0
                    
                    elif key.startswith('table_'):
                        table_name = key.replace('table_', '')
                        stats = future.result()  # No timeout
                        completed_tasks += 1
                        if 'error' not in stats:
                            self.extraction_results['statistics'][table_name] = stats
                            print(f"  [OK] {table_name}: {stats.get('total_records', 0):,} records")
                        else:
                            print(f"  [FAIL] {table_name}: {stats['error']}")
                            logging.error(f"Failed to extract {table_name}: {stats['error']}")
                            self.extraction_results['errors'].append(f"{table_name}: {stats['error']}")
                
                except Exception as e:
                    import traceback
                    error_detail = traceback.format_exc()
                    logging.error(f"Task {key} failed: {e}\nTraceback:\n{error_detail}")
                    print(f"  [FAIL] Task {key} failed: {e}")
                    if key == 'icu':
                        print(f"  ICU extraction error details: {str(e)}")
                    self.extraction_results['errors'].append(f"Task {key}: {str(e)}")
        
        self.phase_times['phase1'] = time.time() - phase1_start
        
        # Phase 2: MEASUREMENT parallel chunk processing (byte-offset seeking)
        print("\n=== Phase 2: MEASUREMENT Parallel Processing ===")
        print(f"Step 3/3: Processing MEASUREMENT table...")
        phase2_start = time.time()
        measurement_file = standardized_dir / "MEASUREMENT_standardized.csv"
        
        if measurement_file.exists():
            print(f"Processing MEASUREMENT with {MEASUREMENT_CHUNKS} parallel chunks...")
            print(f"  Computing byte-offset ranges (O(n_chunks) seeks, not full file scan)...")
            measurement_start = time.time()
            
            # Compute byte ranges ONCE — O(MEASUREMENT_CHUNKS) seeks, ~milliseconds
            # This replaces having each of 6 workers independently count all rows
            byte_ranges = get_csv_byte_ranges(str(measurement_file), MEASUREMENT_CHUNKS)
            actual_chunks = len(byte_ranges)
            
            if actual_chunks != MEASUREMENT_CHUNKS:
                print(f"  Note: File split into {actual_chunks} chunks (requested {MEASUREMENT_CHUNKS})")
            
            for i, (sb, eb, _) in enumerate(byte_ranges):
                size_mb = (eb - sb) / (1024 * 1024)
                print(f"  Chunk {i}: bytes {sb:,} - {eb:,} ({size_mb:.1f} MB)")
            
            with ProcessPoolExecutor(max_workers=actual_chunks) as executor:
                # Submit all chunks with pre-computed byte ranges
                chunk_futures = []
                for chunk_id, (start_byte, end_byte, fieldnames) in enumerate(byte_ranges):
                    future = executor.submit(
                        process_measurement_chunk,
                        (chunk_id, start_byte, end_byte, fieldnames, 
                         str(measurement_file), str(output_dir))
                    )
                    chunk_futures.append(future)
                
                # Collect chunk results
                chunk_results = []
                for i, future in enumerate(as_completed(chunk_futures)):
                    try:
                        chunk_id, patient_counts, temp_dir = future.result()  # No timeout - let it complete
                        chunk_results.append((chunk_id, patient_counts, temp_dir))
                        print(f"  [OK] Chunk {chunk_id + 1}/{actual_chunks} completed")
                    except Exception as e:
                        print(f"  [FAIL] Chunk {i} failed: {e}")
                        logging.error(f"MEASUREMENT chunk {i} failed: {e}")
            
            # Merge chunks
            if chunk_results:
                print("  Merging MEASUREMENT chunks...")
                merge_stats = merge_measurement_chunks(output_dir, patient_data_dir, actual_chunks)
                
                measurement_time = time.time() - measurement_start
                self.extraction_results['statistics']['MEASUREMENT'] = {
                    'total_records': merge_stats['total_records'],
                    'unique_patients': merge_stats['unique_patients'],
                    'processing_time': measurement_time
                }
                print(f"  [OK] MEASUREMENT completed: {merge_stats['total_records']:,} records, "
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
                print(f"    [OK] Completed: {records:,} records in {table_time:.2f}s")
        
        self.phase_times['phase3'] = time.time() - phase3_start
        
        # Step 4: Calculate pre-ICU statistics
        print("\n=== Phase 4: Pre-ICU Statistics ===")
        phase4_start = time.time()
        
        if all_patient_episodes:
            print("Calculating pre-ICU statistics...")
            self.calculate_pre_icu_statistics(all_patient_episodes)
            stats_time = time.time() - phase4_start
            print(f"  [OK] Completed: {len(all_patient_episodes)} patients analyzed in {stats_time:.2f}s")
        elif all_patient_episodes is None:
            raise RuntimeError("ICU episode extraction failed — cannot generate labels. Check Phase 1 errors above.")
        else:
            print("Skipping pre-ICU statistics (no ICU patients found)")
        
        self.phase_times['phase4'] = time.time() - phase4_start
        
        # Step 5: Generate labels for all patients (including non-ICU)
        print("\n=== Phase 5: Label Generation for All Patients ===")
        phase5_start = time.time()
        
        print("Generating labels for all patients (including non-ICU)...")
        self.generate_labels_for_all_patients(all_patient_episodes)
        labels_time = time.time() - phase5_start
        print(f"  [OK] Completed: Labels generated for all patients in {labels_time:.2f}s")
        
        self.phase_times['phase5'] = time.time() - phase5_start
        
        # Calculate total time
        total_time = time.time() - start_time
        
        # Add total_extraction_time to the beginning of results
        self.extraction_results = {
            "total_extraction_time": total_time,
            **self.extraction_results
        }
        
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
        
        # Detailed output locations
        print("\n[OUTPUT LOCATIONS]")
        print(f"  Reports & Statistics: {output_dir}")
        print(f"  Patient Data: {patient_data_dir}")
        
        # Count actual patients extracted by counting PERSON.csv files
        person_files = glob.glob(str(patient_data_dir / "**" / "PERSON.csv"), recursive=True)
        patient_count = len(person_files)
        
        print(f"    * Located at project root: extracted_patient_data/")
        print(f"    * Total patients extracted: {patient_count}")
        print(f"    * Structure: extracted_patient_data/<patient_id>/")
        print(f"    * Each patient folder contains:")
        print(f"       - MEASUREMENT.csv")
        print(f"       - OBSERVATION.csv") 
        print(f"       - DRUG_EXPOSURE.csv")
        print(f"       - CONDITION_OCCURRENCE.csv")
        print(f"       - And other OMOP tables for that patient")
        print("="*80)
        
        # Performance breakdown
        self.print_performance_breakdown(total_time)


def main():
    """Main execution function."""
    # Setup logging first (only in main process)
    log_dir = output_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"extraction_process_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file),  # Save to file
            logging.StreamHandler()  # Also output to console
        ],
        force=True
    )
    
    extractor = PatientDataExtractor()
    extractor.run()


if __name__ == "__main__":
    main()
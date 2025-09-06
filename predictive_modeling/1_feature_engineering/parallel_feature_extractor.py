#!/usr/bin/env python
"""
Parallel Feature Extraction Module for Time Series Features
Enables parallel processing of patient feature extraction
"""

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import pandas as pd
import numpy as np
from tqdm import tqdm
import json
import time
import logging
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract_patient_features_worker(args: Tuple) -> Tuple[str, Dict, float]:
    """
    Worker function to extract features for a single patient
    
    Args:
        args: Tuple containing (patient_id, patient_dir, icu_start, extractor_params)
    
    Returns:
        Tuple of (patient_id, features_dict, elapsed_time)
    """
    start_time = time.time()
    patient_id, patient_dir, icu_start, extractor_params = args
    
    try:
        # Import the extractor inside worker to avoid pickling issues
        from simple_time_series_extractor import SimpleTimeSeriesExtractor
        
        # Recreate extractor with parameters (verbose=False for workers)
        extractor = SimpleTimeSeriesExtractor(
            time_window=extractor_params['time_window'],
            minimum_observation_hours=extractor_params['minimum_observation_hours'],
            concepts_path=extractor_params['concepts_path'],
            aggregation=extractor_params.get('aggregation', 'median'),
            verbose=False  # Suppress output in worker processes
        )
        
        # Extract features for this patient
        features = extractor.extract_patient_features(patient_id, patient_dir, icu_start)
        
        elapsed = time.time() - start_time
        return patient_id, features, elapsed
        
    except Exception as e:
        logging.error(f"Error processing patient {patient_id}: {e}")
        return patient_id, None, time.time() - start_time


def extract_patient_chunk(args: Tuple) -> List[Tuple[str, Dict, float]]:
    """
    Process a chunk of patients
    
    Args:
        args: Tuple containing (chunk_id, patient_chunk, data_dir, extractor_params)
    
    Returns:
        List of (patient_id, features_dict, elapsed_time) tuples
    """
    chunk_id, patient_chunk, data_dir, extractor_params = args
    results = []
    
    # Import the extractor inside worker
    from simple_time_series_extractor import SimpleTimeSeriesExtractor
    
    # Create extractor instance for this worker (verbose=False to suppress output)
    extractor = SimpleTimeSeriesExtractor(
        time_window=extractor_params['time_window'],
        minimum_observation_hours=extractor_params['minimum_observation_hours'],
        concepts_path=extractor_params['concepts_path'],
        aggregation=extractor_params.get('aggregation', 'median'),
        verbose=False  # Suppress output in worker processes
    )
    
    for patient in patient_chunk:
        start_time = time.time()
        patient_id = patient['patient_id']
        
        try:
            # Find the correct patient directory based on patient_id structure
            patient_dir = None
            
            # Special handling for 6-prefix patients with 3-level directory structure
            if patient_id.startswith('6'):
                # For 6-prefix IDs, the structure is: 6XXXXXXXX/RANGE/patient_id
                # where RANGE is like "308000-308999"
                prefix_dir = Path(data_dir) / patient_id[:9]
                if prefix_dir.exists():
                    try:
                        numeric_part = int(patient_id[9:15])  # Get 6 digits after prefix
                        # Calculate range (floor to nearest 1000)
                        range_start = (numeric_part // 1000) * 1000
                        range_end = range_start + 999
                        range_dir = f"{range_start:06d}-{range_end:06d}"
                        
                        patient_dir = prefix_dir / range_dir / patient_id
                        if not patient_dir.exists():
                            patient_dir = None
                    except (ValueError, IndexError):
                        pass
            
            # If not found with special handling, try standard patterns
            if patient_dir is None:
                # The patient_id is actually a visit_id like "400000002029880"
                # The actual patient is the first part before the visit suffix
                actual_patient_id = patient_id[:9] if len(patient_id) > 9 else patient_id
                patient_dir = Path(data_dir) / actual_patient_id / patient_id
                
                if not patient_dir.exists():
                    # Try other patterns
                    patterns = [
                        Path(data_dir) / patient_id,
                        Path(data_dir) / patient_id[:10] / patient_id,
                        Path(data_dir) / patient_id[:11] / patient_id
                    ]
                    for pattern in patterns:
                        if pattern.exists():
                            patient_dir = pattern
                            break
            
            # Get ICU start time
            icu_info = patient.get('icu_info', {})
            icu_start = icu_info.get('last_icu_start')
            
            if patient_dir is None or not patient_dir.exists():
                logging.debug(f"Patient directory not found for: {patient_id}")
                results.append((patient_id, None, 0))
                continue
            
            if not icu_start:
                logging.debug(f"No ICU start time for patient {patient_id}")
                results.append((patient_id, None, 0))
                continue
            
            # Extract features
            features = extractor.extract_patient_features(patient_id, patient_dir, icu_start)
            
            if not features or features == {}:
                logging.debug(f"No features extracted for patient {patient_id}")
                results.append((patient_id, None, 0))
                continue
            
            # Add mortality labels from patient data (matching the old version format)
            features['mortality_48h'] = patient.get('mortality', {}).get('mortality_icu_48h', 0)
            features['mortality_7day'] = patient.get('mortality', {}).get('mortality_icu_7day', 0)
            features['mortality_30day'] = patient.get('mortality', {}).get('mortality_icu_30day', 0)
            
            elapsed = time.time() - start_time
            results.append((patient_id, features, elapsed))
            
        except Exception as e:
            logging.error(f"Error in chunk {chunk_id} for patient {patient_id}: {e}")
            results.append((patient_id, None, time.time() - start_time))
    
    return results


class ParallelFeatureExtractor:
    """Parallel feature extraction coordinator"""
    
    def __init__(self, time_window: int,
                 minimum_observation_hours: int,
                 concepts_path: str,
                 aggregation: str = 'median',
                 n_workers: Optional[int] = None):
        """
        Initialize parallel feature extractor
        
        Args:
            time_window: Time window in hours (2, 4, or 8)
            minimum_observation_hours: Minimum observation hours (24, 48, or 72)
            concepts_path: Path to concepts JSON
            aggregation: Aggregation method ('median', 'mean', 'max', 'min')
            n_workers: Number of parallel workers (default: CPU count - 2)
        """
        self.extractor_params = {
            'time_window': time_window,
            'minimum_observation_hours': minimum_observation_hours,
            'concepts_path': concepts_path,
            'aggregation': aggregation
        }
        
        # Set number of workers
        if n_workers is None:
            self.n_workers = max(1, mp.cpu_count() - 2)
        else:
            self.n_workers = n_workers
        
        logging.info(f"Initialized parallel extractor with {self.n_workers} workers")
    
    def extract_features_parallel(self, patient_labels: List[Dict],
                                 data_dir: Path,
                                 task: str = 'mortality',
                                 chunk_size: Optional[int] = None) -> Tuple[pd.DataFrame, float, float]:
        """
        Extract features for multiple patients in parallel
        
        Args:
            patient_labels: List of patient label dictionaries
            data_dir: Root directory of patient data
            task: Task name for filtering patients
            chunk_size: Number of patients per chunk (default: auto)
        
        Returns:
            Tuple of (DataFrame with extracted features, cpu_time, wall_time)
        """
        # Import here to avoid circular dependency
        from simple_time_series_extractor import SimpleTimeSeriesExtractor
        
        # Create a temporary extractor for filtering
        temp_extractor = SimpleTimeSeriesExtractor(
            time_window=self.extractor_params['time_window'],
            minimum_observation_hours=self.extractor_params['minimum_observation_hours'],
            concepts_path=self.extractor_params['concepts_path']
        )
        
        # Filter patients based on task (reuse existing logic)
        if task == 'mortality':
            patients = self._filter_mortality_patients(patient_labels)
        elif task == 'sepsis':
            patients = [p for p in patient_labels if p.get('sepsis_labels') is not None]
        else:
            patients = patient_labels
        
        if not patients:
            logging.warning("No patients to process after filtering")
            return pd.DataFrame(), 0.0, 0.0
        
        logging.info(f"Processing {len(patients)} patients with {self.n_workers} workers")
        
        # Determine chunk size
        if chunk_size is None:
            # Auto-determine chunk size based on number of patients and workers
            chunk_size = max(1, len(patients) // (self.n_workers * 4))
            chunk_size = min(chunk_size, 50)  # Cap at 50 patients per chunk
        
        # Split patients into chunks
        patient_chunks = []
        for i in range(0, len(patients), chunk_size):
            patient_chunks.append(patients[i:i+chunk_size])
        
        logging.info(f"Split into {len(patient_chunks)} chunks of ~{chunk_size} patients")
        
        # Process chunks in parallel
        all_features = []
        total_cpu_time = 0
        wall_start = time.time()
        
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            # Submit all chunk processing tasks
            futures = []
            for chunk_id, patient_chunk in enumerate(patient_chunks):
                future = executor.submit(
                    extract_patient_chunk,
                    (chunk_id, patient_chunk, data_dir, self.extractor_params)
                )
                futures.append((chunk_id, future))
            
            # Collect results with progress bar
            with tqdm(total=len(patients), desc="Extracting features") as pbar:
                for chunk_id, future in futures:
                    try:
                        chunk_results = future.result(timeout=300)  # 5 min timeout per chunk
                        
                        for patient_id, features, elapsed in chunk_results:
                            if features is not None:
                                all_features.append(features)
                            total_cpu_time += elapsed
                            pbar.update(1)
                        
                    except Exception as e:
                        logging.error(f"Chunk {chunk_id} failed: {e}")
                        pbar.update(len(patient_chunks[chunk_id]))
        
        wall_time = time.time() - wall_start
        
        # Convert to DataFrame
        if all_features:
            df = pd.DataFrame(all_features).fillna(0.0)  # Fill NaN with 0.0 to match old format
            
            # Add additional labels for other tasks if needed
            if task == 'mortality':
                df = self._add_additional_labels(df, patients)
            
            # Reorganize columns to match serial processing order
            df = self._reorganize_columns(df)
            
            logging.info(f"Extraction completed: {len(df)} samples, {df.shape[1]} features")
            logging.info(f"Total CPU time: {total_cpu_time:.2f}s")
            logging.info(f"Wall time: {wall_time:.2f}s")
            logging.info(f"Speedup: {total_cpu_time / max(0.001, wall_time):.2f}x")
            
            return df, total_cpu_time, wall_time
        else:
            logging.warning("No features extracted")
            return pd.DataFrame(), total_cpu_time, wall_time
    
    def _filter_mortality_patients(self, patient_labels: List[Dict]) -> List[Dict]:
        """Filter patients for mortality prediction task"""
        patients = []
        
        # Track filtering statistics
        stats = {
            'total': len(patient_labels),
            'no_icu': 0,
            'missing_icu_times': 0,
            'icu_too_short': 0,
            'died_too_early': 0,
            'passed': 0
        }
        
        for p in patient_labels:
            if p.get('has_icu_admission') != 1:
                stats['no_icu'] += 1
                continue
            
            # Check ICU length of stay
            icu_info = p.get('icu_info', {})
            icu_start = icu_info.get('last_icu_start')
            icu_end = icu_info.get('last_icu_end')
            
            if not icu_start or not icu_end:
                stats['missing_icu_times'] += 1
                continue
            
            # Parse dates and calculate ICU duration
            from datetime import datetime
            icu_start_dt = datetime.fromisoformat(icu_start.replace('Z', '+00:00'))
            icu_end_dt = datetime.fromisoformat(icu_end.replace('Z', '+00:00'))
            icu_duration_hours = (icu_end_dt - icu_start_dt).total_seconds() / 3600
            
            # Skip if ICU stay < minimum_observation_hours
            min_hours = self.extractor_params['minimum_observation_hours']
            if icu_duration_hours < min_hours:
                stats['icu_too_short'] += 1
                continue
            
            # Skip if patient died within minimum_observation_hours of ICU admission
            death_time = p.get('mortality', {}).get('death_time')
            if death_time:
                death_dt = datetime.fromisoformat(death_time.replace('Z', '+00:00'))
                hours_to_death = (death_dt - icu_start_dt).total_seconds() / 3600
                if hours_to_death < min_hours:
                    stats['died_too_early'] += 1
                    continue
            
            stats['passed'] += 1
            patients.append(p)
        
        # Log filtering statistics
        logging.info(f"Mortality patient filtering statistics:")
        logging.info(f"  Total patients: {stats['total']}")
        logging.info(f"  No ICU admission: {stats['no_icu']}")
        logging.info(f"  Missing ICU times: {stats['missing_icu_times']}")
        logging.info(f"  ICU stay < {min_hours}h: {stats['icu_too_short']}")
        logging.info(f"  Died within {min_hours}h: {stats['died_too_early']}")
        logging.info(f"  Passed all filters: {stats['passed']}")
        
        return patients
    
    def _reorganize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorganize columns: patient_id, demographics, static, windowed, labels
        This matches the order used in simple_time_series_extractor.py"""
        # Identify column types
        patient_id_cols = ['patient_id']
        
        # Demographic features (should come first after patient_id)
        # IMPORTANT: Order is age_at_icu, gender, race, ethnicity
        demo_cols = ['age_at_icu', 'gender', 'race', 'ethnicity']
        demo_cols = [col for col in demo_cols if col in df.columns]
        
        # Static features: conditions and procedures (no time component)
        static_cols = []
        for col in df.columns:
            if col == 'patient_id' or col in demo_cols:
                continue
            # Static features are those without '_t' suffix (time window)
            if '_t' not in col and not any(col.startswith(p) for p in ['mortality_', 'readmission_', 'los_', 'sepsis_']):
                static_cols.append(col)
        
        # Windowed features: those with _t0, _t1, etc.
        windowed_cols = []
        for col in df.columns:
            if '_t' in col and not any(col.startswith(p) for p in ['mortality_', 'readmission_', 'los_', 'sepsis_']):
                windowed_cols.append(col)
        
        # Sort windowed features by time window then concept ID
        windowed_cols.sort(key=lambda x: (int(x.split('_t')[-1]), int(x.split('_t')[0])))
        
        # Label columns - maintain specific order to match old file
        # Order: mortality_30day, mortality_48h, mortality_7day, 
        #        readmission_7days, readmission_30days, readmission_90days,
        #        los_greater_3days, los_greater_7days
        label_order = [
            'mortality_30day', 'mortality_48h', 'mortality_7day',
            'readmission_7days', 'readmission_30days', 'readmission_90days',
            'los_greater_3days', 'los_greater_7days',
            'sepsis_within_24h', 'sepsis_within_48h', 'sepsis_during_icu'
        ]
        label_cols = [col for col in label_order if col in df.columns]
        
        # Reorganize: patient_id, demographics, static, windowed, labels
        new_column_order = patient_id_cols + demo_cols + sorted(static_cols) + windowed_cols + label_cols
        
        # Only include columns that exist
        new_column_order = [col for col in new_column_order if col in df.columns]
        
        return df[new_column_order]
    
    def _add_additional_labels(self, df: pd.DataFrame, patients: List[Dict]) -> pd.DataFrame:
        """Add readmission and LOS labels to the dataframe"""
        # Create a mapping of patient_id to patient data
        patient_map = {p['patient_id']: p for p in patients}
        
        # Add labels for each patient
        for idx, row in df.iterrows():
            patient_id = row['patient_id']
            if patient_id in patient_map:
                patient = patient_map[patient_id]
                
                # Add readmission labels
                df.loc[idx, 'readmission_7days'] = patient['readmission']['readmission_within_7days']
                df.loc[idx, 'readmission_30days'] = patient['readmission']['readmission_within_30days']
                df.loc[idx, 'readmission_90days'] = patient['readmission']['readmission_within_90days']
                
                # Add LOS labels
                df.loc[idx, 'los_greater_3days'] = patient['los']['los_greater_than_3days']
                df.loc[idx, 'los_greater_7days'] = patient['los']['los_greater_than_7days']
        
        return df


def benchmark_parallel_extraction(patient_labels: List[Dict],
                                 data_dir: Path,
                                 time_window: int = 4,
                                 min_observation: int = 24,
                                 concepts_path: str = '../config/concepts.json'):
    """
    Benchmark parallel vs sequential extraction
    
    Args:
        patient_labels: Patient label list
        data_dir: Data directory
        time_window: Time window in hours
        min_observation: Minimum observation hours
        concepts_path: Path to concepts file
    """
    print("\n" + "="*60)
    print("BENCHMARKING PARALLEL FEATURE EXTRACTION")
    print("="*60)
    
    # Test with subset of patients
    test_size = min(100, len(patient_labels))
    test_patients = patient_labels[:test_size]
    
    print(f"\nTest configuration:")
    print(f"  Patients: {test_size}")
    print(f"  Time window: {time_window}h")
    print(f"  Min observation: {min_observation}h")
    
    # Test parallel extraction with different worker counts
    worker_counts = [1, 2, 4, max(1, mp.cpu_count() - 2)]
    
    for n_workers in worker_counts:
        print(f"\nTesting with {n_workers} workers...")
        
        extractor = ParallelFeatureExtractor(
            time_window=time_window,
            minimum_observation_hours=min_observation,
            concepts_path=concepts_path,
            n_workers=n_workers
        )
        
        start_time = time.time()
        df = extractor.extract_features_parallel(
            test_patients,
            data_dir,
            task='mortality'
        )
        elapsed = time.time() - start_time
        
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Samples: {len(df)}")
        print(f"  Throughput: {len(df)/elapsed:.2f} patients/sec")
    
    print("\n" + "="*60)
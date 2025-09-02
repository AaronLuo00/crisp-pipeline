#!/usr/bin/env python
"""
Simplified Time Series Feature Extractor for ICU Patient Data
Uses unified time windows and aggregation for all data types
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import json
import yaml
from typing import Dict, List, Optional
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')


class SimpleTimeSeriesExtractor:
    """Simplified time series feature extractor with unified configuration"""
    
    def __init__(self, config_path: str = '../config/time_series_config.yaml',
                 concepts_path: str = '../config/concepts.json',
                 time_window: Optional[int] = None,
                 minimum_observation_hours: Optional[int] = None):
        """
        Initialize the simplified extractor
        
        Args:
            config_path: Path to time series configuration
            concepts_path: Path to concepts JSON
            time_window: Override time window from config (must be 2, 4, or 8)
            minimum_observation_hours: Override minimum observation from config (must be 24, 48, or 72)
        """
        # Load configurations
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        with open(concepts_path, 'r') as f:
            self.concepts = json.load(f)
        
        # Setup parameters with validation
        # Time window validation
        if time_window is not None:
            if time_window not in [2, 4, 8]:
                raise ValueError(f"time_window must be 2, 4, or 8 hours, got {time_window}")
            self.time_window = time_window
        else:
            self.time_window = self.config['time_window']
            if self.time_window not in [2, 4, 8]:
                raise ValueError(f"time_window in config must be 2, 4, or 8 hours, got {self.time_window}")
        
        # Minimum observation hours validation
        if minimum_observation_hours is not None:
            if minimum_observation_hours not in [24, 48, 72]:
                raise ValueError(f"minimum_observation_hours must be 24, 48, or 72 hours, got {minimum_observation_hours}")
            self.minimum_observation_hours = minimum_observation_hours
        else:
            self.minimum_observation_hours = self.config.get('minimum_observation_hours', 24)
            if self.minimum_observation_hours not in [24, 48, 72]:
                raise ValueError(f"minimum_observation_hours must be 24, 48, or 72 hours, got {self.minimum_observation_hours}")
        
        # Set total_hours based on minimum_observation_hours (extract data for the minimum period)
        self.total_hours = self.minimum_observation_hours
        
        self.aggregation = self.config['aggregation']
        self.fill_strategy = self.config['fill_strategy']
        
        # Calculate number of time windows
        self.n_windows = self.total_hours // self.time_window
        
        print(f"Initialized extractor with:")
        print(f"  - Time window: {self.time_window} hours")
        print(f"  - Minimum observation: {self.minimum_observation_hours} hours")
        print(f"  - Total windows: {self.n_windows}")
        print(f"  - Aggregation: {self.aggregation}")
        print(f"  - Total concepts: {sum(len(v) for v in self.concepts.values())}")
    
    def extract_patient_features(self, patient_id: str, patient_dir: Path, 
                                icu_start: str) -> Dict:
        """
        Extract time series features for a single patient
        
        Args:
            patient_id: Patient identifier
            patient_dir: Path to patient data directory
            icu_start: ICU admission datetime string
        
        Returns:
            Dictionary of features
        """
        features = {'patient_id': patient_id}
        icu_start_dt = pd.to_datetime(icu_start)
        icu_end_dt = icu_start_dt + timedelta(hours=self.total_hours)
        
        # Extract demographic features first (will appear at the beginning of CSV)
        demo_features = self._extract_demographic_features(patient_dir, icu_start_dt)
        features.update(demo_features)
        
        # Extract dense static features (CONDITIONS and PROCEDURES during ICU)
        static_features = self._extract_dense_static_features(patient_dir, icu_start_dt, icu_end_dt)
        features.update(static_features)
        
        # Extract time-windowed features from measurement/observation/drug tables
        for table_name, concept_list in self.concepts.items():
            # Skip static tables as they're handled separately now
            if table_name in ['CONDITION_OCCURRENCE', 'PROCEDURE_OCCURRENCE']:
                continue
            table_features = self._extract_table_features(
                patient_dir, table_name, concept_list, icu_start_dt, icu_end_dt
            )
            features.update(table_features)
        
        return features
    
    def _extract_demographic_features(self, patient_dir: Path, icu_start: datetime) -> Dict:
        """
        Extract demographic features from PERSON table
        
        Args:
            patient_dir: Path to patient data directory
            icu_start: ICU admission datetime
        
        Returns:
            Dictionary of demographic features
        """
        features = {}
        person_file = patient_dir / 'PERSON.csv'
        
        if person_file.exists():
            try:
                person_df = pd.read_csv(person_file)
                if len(person_df) > 0:
                    row = person_df.iloc[0]
                    
                    # Gender: 0=unknown, 1=male, 2=female
                    gender_id = row.get('gender_concept_id', 0)
                    if gender_id == 8507:
                        features['gender'] = 1
                    elif gender_id == 8532:
                        features['gender'] = 2
                    else:
                        features['gender'] = 0
                    
                    # Race: 0=unknown, 1=white, 2=black, 3=asian, 4=pacific_islander, 5=mixed, 6=other
                    race_id = row.get('race_concept_id', 0)
                    race_mapping = {
                        0: 0,        # No information
                        8552: 0,     # Unknown racial group
                        8527: 1,     # White
                        8516: 2,     # Black or African American
                        8515: 3,     # Asian
                        8657: 4,     # Native Hawaiian or Other Pacific Islander
                        4212311: 5,  # Mixed race
                        3213494: 5,  # Multiracial
                        38003574: 5, # More than one race
                    }
                    features['race'] = race_mapping.get(race_id, 6)  # 6 for other/unmapped
                    
                    # Ethnicity: 0=unknown, 1=hispanic, 2=not_hispanic
                    eth_id = row.get('ethnicity_concept_id', 0)
                    if eth_id == 38003563:
                        features['ethnicity'] = 1
                    elif eth_id == 38003564:
                        features['ethnicity'] = 2
                    else:
                        features['ethnicity'] = 0
                    
                    # Age at ICU admission
                    if pd.notna(row.get('birth_datetime')):
                        birth_date = pd.to_datetime(row['birth_datetime'])
                        age = (icu_start - birth_date).days / 365.25
                        features['age_at_icu'] = max(0, age)  # Ensure non-negative
                    else:
                        features['age_at_icu'] = 0
                        
            except Exception as e:
                print(f"Error extracting demographic features: {e}")
                # Set default values if error
                features['gender'] = 0
                features['race'] = 0
                features['ethnicity'] = 0
                features['age_at_icu'] = 0
        else:
            # Set default values if PERSON.csv doesn't exist
            features['gender'] = 0
            features['race'] = 0
            features['ethnicity'] = 0
            features['age_at_icu'] = 0
            
        return features
    
    def _extract_dense_static_features(self, patient_dir: Path, icu_start: datetime, icu_end: datetime) -> Dict:
        """
        Extract dense static features (all CONDITIONS and PROCEDURES as 0/1)
        Used for time series feature extraction
        """
        features = {}
        
        # Extract CONDITIONS (50 concepts, dense)
        if 'CONDITION_OCCURRENCE' in self.concepts:
            cond_file = patient_dir / 'CONDITION_OCCURRENCE.csv'
            icu_conditions = set()
            
            if cond_file.exists():
                try:
                    df = pd.read_csv(cond_file)
                    if 'condition_start_datetime' in df.columns:
                        df['condition_start_datetime'] = pd.to_datetime(df['condition_start_datetime'])
                        # Get conditions during ICU stay
                        icu_cond_df = df[(df['condition_start_datetime'] >= icu_start) & 
                                        (df['condition_start_datetime'] <= icu_end)]
                        icu_conditions = set(icu_cond_df['condition_concept_id'].values)
                except Exception as e:
                    print(f"Error reading conditions: {e}")
            
            # Create dense features for top 50 conditions
            for concept in self.concepts['CONDITION_OCCURRENCE'][:50]:
                concept_id = concept['id']
                features[str(concept_id)] = int(concept_id in icu_conditions)
        
        # Extract PROCEDURES (50 concepts, dense)
        if 'PROCEDURE_OCCURRENCE' in self.concepts:
            proc_file = patient_dir / 'PROCEDURE_OCCURRENCE.csv'
            icu_procedures = set()
            
            if proc_file.exists():
                try:
                    df = pd.read_csv(proc_file)
                    if 'procedure_datetime' in df.columns:
                        df['procedure_datetime'] = pd.to_datetime(df['procedure_datetime'])
                        # Get procedures during ICU stay
                        icu_proc_df = df[(df['procedure_datetime'] >= icu_start) & 
                                        (df['procedure_datetime'] <= icu_end)]
                        icu_procedures = set(icu_proc_df['procedure_concept_id'].values)
                except Exception as e:
                    print(f"Error reading procedures: {e}")
            
            # Create dense features for top 50 procedures
            for concept in self.concepts['PROCEDURE_OCCURRENCE'][:50]:
                concept_id = concept['id']
                features[str(concept_id)] = int(concept_id in icu_procedures)
        
        return features
    
    def extract_static_features_for_sepsis(self, patient_id: str, patient_dir: Path, 
                                          icu_start: str) -> Dict:
        """
        Extract only static features for sepsis prediction
        Includes demographics and pre-ICU conditions and procedures
        """
        features = {'patient_id': patient_id}
        
        # Convert icu_start string to datetime (same as extract_patient_features)
        icu_start_dt = pd.to_datetime(icu_start)
        
        # Extract demographic features using the datetime object
        demo_features = self._extract_demographic_features(patient_dir, icu_start_dt)
        features.update(demo_features)
        
        # Extract pre-ICU conditions (comorbidities) - dense, 50 concepts
        if 'CONDITION_OCCURRENCE' in self.concepts:
            conditions_file = patient_dir / 'CONDITION_OCCURRENCE.csv'
            pre_icu_conditions = set()
            
            if conditions_file.exists():
                try:
                    df = pd.read_csv(conditions_file)
                    if 'condition_start_datetime' in df.columns:
                        df['condition_start_datetime'] = pd.to_datetime(df['condition_start_datetime'])
                        # Get conditions before ICU admission
                        pre_icu_df = df[df['condition_start_datetime'] < icu_start_dt]
                        pre_icu_conditions = set(pre_icu_df['condition_concept_id'].values)
                except Exception as e:
                    print(f"Error extracting condition features: {e}")
            
            # Create dense features for top 50 conditions
            for concept in self.concepts['CONDITION_OCCURRENCE'][:50]:
                concept_id = concept['id']
                # Use string concept_id without prefix to match time series format
                features[str(concept_id)] = int(concept_id in pre_icu_conditions)
        
        # Extract pre-ICU procedures - dense, 50 concepts
        if 'PROCEDURE_OCCURRENCE' in self.concepts:
            procedures_file = patient_dir / 'PROCEDURE_OCCURRENCE.csv'
            pre_icu_procedures = set()
            
            if procedures_file.exists():
                try:
                    df = pd.read_csv(procedures_file)
                    if 'procedure_datetime' in df.columns:
                        df['procedure_datetime'] = pd.to_datetime(df['procedure_datetime'])
                        # Get procedures before ICU admission
                        pre_icu_df = df[df['procedure_datetime'] < icu_start_dt]
                        pre_icu_procedures = set(pre_icu_df['procedure_concept_id'].values)
                except Exception as e:
                    print(f"Error extracting procedure features: {e}")
            
            # Create dense features for top 50 procedures
            for concept in self.concepts['PROCEDURE_OCCURRENCE'][:50]:
                concept_id = concept['id']
                # Use string concept_id without prefix to match time series format
                features[str(concept_id)] = int(concept_id in pre_icu_procedures)
        
        return features
    
    def _extract_table_features(self, patient_dir: Path, table_name: str,
                               concept_list: List[Dict], icu_start: datetime,
                               icu_end: datetime) -> Dict:
        """Extract features from a specific OMOP table"""
        features = {}
        
        # Map table names to file names and columns
        table_mapping = {
            'MEASUREMENT': {
                'file': 'MEASUREMENT.csv',
                'datetime_col': 'measurement_datetime',
                'concept_col': 'measurement_concept_id',
                'value_col': 'value_as_number'
            },
            'OBSERVATION': {
                'file': 'OBSERVATION.csv',
                'datetime_col': 'observation_datetime',
                'concept_col': 'observation_concept_id',
                'value_col': 'value_as_number'
            },
            'DRUG_EXPOSURE': {
                'file': 'DRUG_EXPOSURE.csv',
                'datetime_col': 'drug_exposure_start_datetime',
                'concept_col': 'drug_concept_id',
                'value_col': None  # Binary presence
            },
            'CONDITION_OCCURRENCE': {
                'file': 'CONDITION_OCCURRENCE.csv',
                'datetime_col': 'condition_start_datetime',
                'concept_col': 'condition_concept_id',
                'value_col': None  # Binary presence
            },
            'PROCEDURE_OCCURRENCE': {
                'file': 'PROCEDURE_OCCURRENCE.csv',
                'datetime_col': 'procedure_datetime',
                'concept_col': 'procedure_concept_id',
                'value_col': None  # Binary presence
            }
        }
        
        if table_name not in table_mapping:
            return features
        
        mapping = table_mapping[table_name]
        data_file = patient_dir / mapping['file']
        
        if not data_file.exists():
            return features
        
        try:
            # Read data
            df = pd.read_csv(data_file)
            if len(df) == 0:
                return features
            
            # Parse datetime if column exists
            if mapping['datetime_col'] in df.columns:
                df[mapping['datetime_col']] = pd.to_datetime(df[mapping['datetime_col']])
                # Filter to ICU window
                df = df[(df[mapping['datetime_col']] >= icu_start) & 
                       (df[mapping['datetime_col']] <= icu_end)]
            
            if len(df) == 0:
                return features
            
            # Extract features for each concept
            for concept in concept_list:
                concept_id = concept['id']
                concept_data = df[df[mapping['concept_col']] == concept_id]
                
                if len(concept_data) == 0:
                    continue
                
                # Extract windowed features (static tables are now handled separately)
                for window_idx in range(self.n_windows):
                    window_start = icu_start + timedelta(hours=window_idx * self.time_window)
                    window_end = window_start + timedelta(hours=self.time_window)
                    
                    window_data = concept_data[
                        (concept_data[mapping['datetime_col']] >= window_start) &
                        (concept_data[mapping['datetime_col']] < window_end)
                    ]
                    
                    feature_name = f"{concept_id}_t{window_idx}"
                    
                    if len(window_data) > 0:
                        if mapping['value_col'] and mapping['value_col'] in window_data.columns:
                            # Numeric value - use aggregation
                            values = window_data[mapping['value_col']].dropna()
                            if len(values) > 0:
                                if self.aggregation == 'median':
                                    features[feature_name] = values.median()
                                elif self.aggregation == 'mean':
                                    features[feature_name] = values.mean()
                                elif self.aggregation == 'max':
                                    features[feature_name] = values.max()
                                elif self.aggregation == 'min':
                                    features[feature_name] = values.min()
                                else:
                                    features[feature_name] = values.median()
                            else:
                                features[feature_name] = 1  # Presence indicator
                        else:
                            # Binary presence for drugs
                            features[feature_name] = 1
                    else:
                        # Handle missing windows
                        features[feature_name] = self._handle_missing(
                            features, table_name, concept_id, window_idx
                        )
        
        except Exception as e:
            print(f"Error processing {table_name}: {e}")
        
        return features
    
    def _handle_missing(self, features: Dict, table_name: str, 
                       concept_id: int, window_idx: int) -> float:
        """Handle missing values with forward fill or zero"""
        if window_idx == 0:
            return 0  # First window - use zero
        
        # Try forward fill from previous window
        prev_feature = f"{concept_id}_t{window_idx-1}"
        if prev_feature in features:
            return features[prev_feature]
        
        return 0
    
    def extract_features_batch(self, patient_labels: List[Dict], 
                              data_dir: Path, task: str = 'mortality',
                              output_dir: Path = None) -> pd.DataFrame:
        """
        Extract features for multiple patients
        
        Args:
            patient_labels: List of patient label dictionaries
            data_dir: Root directory of patient data
            task: Task name for filtering patients
            output_dir: Optional output directory
        
        Returns:
            DataFrame with extracted features
        """
        # Filter patients based on task
        if task == 'mortality':
            # For mortality prediction: exclude patients who died within 24h or ICU stay < 24h
            patients = []
            for p in patient_labels:
                if p.get('has_icu_admission') != 1:
                    continue
                    
                # Check ICU length of stay
                icu_info = p.get('icu_info', {})
                icu_start = icu_info.get('last_icu_start')
                icu_end = icu_info.get('last_icu_end')
                
                if not icu_start or not icu_end:
                    continue
                    
                # Parse dates and calculate ICU duration
                from datetime import datetime
                icu_start_dt = datetime.fromisoformat(icu_start.replace('Z', '+00:00'))
                icu_end_dt = datetime.fromisoformat(icu_end.replace('Z', '+00:00'))
                icu_duration_hours = (icu_end_dt - icu_start_dt).total_seconds() / 3600
                
                # Skip if ICU stay < minimum_observation_hours
                if icu_duration_hours < self.minimum_observation_hours:
                    continue
                
                # Skip if patient died within minimum_observation_hours of ICU admission
                death_time = p.get('mortality', {}).get('death_time')
                if death_time:
                    death_dt = datetime.fromisoformat(death_time.replace('Z', '+00:00'))
                    hours_to_death = (death_dt - icu_start_dt).total_seconds() / 3600
                    if hours_to_death < self.minimum_observation_hours:
                        continue
                
                patients.append(p)
                
        elif task == 'los':
            # For LOS prediction: exclude patients who died within 24h or ICU stay < 24h
            patients = []
            for p in patient_labels:
                if p.get('has_icu_admission') != 1:
                    continue
                    
                # Check ICU length of stay
                icu_info = p.get('icu_info', {})
                icu_start = icu_info.get('last_icu_start')
                icu_end = icu_info.get('last_icu_end')
                
                if not icu_start or not icu_end:
                    continue
                    
                # Parse dates and calculate ICU duration
                from datetime import datetime
                icu_start_dt = datetime.fromisoformat(icu_start.replace('Z', '+00:00'))
                icu_end_dt = datetime.fromisoformat(icu_end.replace('Z', '+00:00'))
                icu_duration_hours = (icu_end_dt - icu_start_dt).total_seconds() / 3600
                
                # Skip if ICU stay < minimum_observation_hours
                if icu_duration_hours < self.minimum_observation_hours:
                    continue
                
                # Skip if patient died within minimum_observation_hours of ICU admission
                death_time = p.get('mortality', {}).get('death_time')
                if death_time:
                    death_dt = datetime.fromisoformat(death_time.replace('Z', '+00:00'))
                    hours_to_death = (death_dt - icu_start_dt).total_seconds() / 3600
                    if hours_to_death < self.minimum_observation_hours:
                        continue
                
                patients.append(p)
                
        elif task == 'readmission':
            # For readmission: exclude patients who died within 24h or ICU stay < 24h (same as other tasks)
            patients = []
            for p in patient_labels:
                if p.get('has_icu_admission') != 1:
                    continue
                    
                # Check ICU length of stay
                icu_info = p.get('icu_info', {})
                icu_start = icu_info.get('last_icu_start')
                icu_end = icu_info.get('last_icu_end')
                
                if not icu_start or not icu_end:
                    continue
                    
                # Parse dates and calculate ICU duration
                from datetime import datetime
                icu_start_dt = datetime.fromisoformat(icu_start.replace('Z', '+00:00'))
                icu_end_dt = datetime.fromisoformat(icu_end.replace('Z', '+00:00'))
                icu_duration_hours = (icu_end_dt - icu_start_dt).total_seconds() / 3600
                
                # Skip if ICU stay < minimum_observation_hours
                if icu_duration_hours < self.minimum_observation_hours:
                    continue
                
                # Skip if patient died within minimum_observation_hours of ICU admission
                death_time = p.get('mortality', {}).get('death_time')
                if death_time:
                    death_dt = datetime.fromisoformat(death_time.replace('Z', '+00:00'))
                    hours_to_death = (death_dt - icu_start_dt).total_seconds() / 3600
                    if hours_to_death < self.minimum_observation_hours:
                        continue
                
                patients.append(p)
                
        elif task == 'sepsis':
            # For sepsis prediction: include all ICU patients (no 24h filtering)
            patients = [p for p in patient_labels if p.get('has_icu_admission') == 1]
        else:
            patients = patient_labels
        
        print(f"Extracting features for {len(patients)} patients...")
        
        all_features = []
        for patient in tqdm(patients):
            patient_id = patient['patient_id']
            patient_dir = self._get_patient_dir(patient_id, data_dir)
            
            if not patient_dir:
                continue
            
            icu_start = patient.get('icu_info', {}).get('last_icu_start')
            if not icu_start:
                continue
            
            # Extract features based on task
            if task == 'sepsis':
                # For sepsis, use only static features
                features = self.extract_static_features_for_sepsis(patient_id, patient_dir, icu_start)
            else:
                # For other tasks, use time series features
                features = self.extract_patient_features(patient_id, patient_dir, icu_start)
            
            # Add labels based on task
            if task == 'mortality':
                features.update({
                    'mortality_48h': patient['mortality']['mortality_icu_48h'],
                    'mortality_7day': patient['mortality']['mortality_icu_7day'],
                    'mortality_30day': patient['mortality']['mortality_icu_30day']
                })
            elif task == 'readmission':
                features.update({
                    'readmission_7days': patient['readmission']['readmission_within_7days'],
                    'readmission_30days': patient['readmission']['readmission_within_30days'],
                    'readmission_90days': patient['readmission']['readmission_within_90days']
                })
            elif task == 'los':
                features.update({
                    'los_greater_3days': patient['los']['los_greater_than_3days'],
                    'los_greater_7days': patient['los']['los_greater_than_7days']
                })
            elif task == 'sepsis':
                features.update({
                    'has_sepsis_after_icu': patient['sepsis']['has_sepsis_after_icu'],
                    'sepsis_within_48h': patient['sepsis']['sepsis_within_48h'],
                    'sepsis_within_7days': patient['sepsis']['sepsis_within_7days']
                })
            
            all_features.append(features)
        
        # Create DataFrame
        df = pd.DataFrame(all_features).fillna(0)
        
        # Reorganize columns: patient_id, static features, windowed features, labels
        df = self._reorganize_columns(df)
        
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f'{task}_features_{self.time_window}h_window.csv'
            df.to_csv(output_file, index=False)
            print(f"Saved {len(df)} samples with {len(df.columns)-1} features to {output_file}")
        
        return df
    
    def _reorganize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorganize columns: patient_id, demographics, static, windowed, labels"""
        # Identify column types
        patient_id_cols = ['patient_id']
        
        # Demographic features (should come first after patient_id)
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
        
        # Label columns
        label_cols = [col for col in df.columns if any(
            col.startswith(p) for p in ['mortality_', 'readmission_', 'los_', 'sepsis_']
        )]
        
        # Reorganize: patient_id, demographics, static, windowed, labels
        new_column_order = patient_id_cols + demo_cols + sorted(static_cols) + windowed_cols + sorted(label_cols)
        
        # Only include columns that exist
        new_column_order = [col for col in new_column_order if col in df.columns]
        
        return df[new_column_order]
    
    def _get_patient_dir(self, patient_id: str, data_dir: Path) -> Optional[Path]:
        """Find patient directory"""
        patterns = [
            data_dir / patient_id,
            data_dir / patient_id[:9] / patient_id,
            data_dir / patient_id[:10] / patient_id,
            data_dir / patient_id[:11] / patient_id
        ]
        
        # Special handling for 6-prefix patients with 3-level directory structure
        if patient_id.startswith('6'):
            # For 6-prefix IDs, the structure is: 6XXXXXXXX/RANGE/patient_id
            # where RANGE is like "308000-308999"
            prefix_dir = data_dir / patient_id[:9]
            if prefix_dir.exists():
                # Get the numeric part after the prefix for range calculation
                try:
                    numeric_part = int(patient_id[9:15])  # Get 6 digits after prefix
                    # Calculate range (floor to nearest 1000)
                    range_start = (numeric_part // 1000) * 1000
                    range_end = range_start + 999
                    range_dir = f"{range_start:06d}-{range_end:06d}"
                    
                    patient_path = prefix_dir / range_dir / patient_id
                    if patient_path.exists():
                        return patient_path
                except (ValueError, IndexError):
                    pass
        
        for pattern in patterns:
            if pattern.exists():
                return pattern
        
        return None
    
    def get_feature_summary(self, df: pd.DataFrame) -> Dict:
        """Get summary statistics about extracted features"""
        # Separate features by type with new naming convention
        windowed_features = [col for col in df.columns if '_t' in col and not any(
            col.startswith(p) for p in ['mortality_', 'readmission_', 'los_', 'sepsis_']
        )]
        static_features = [col for col in df.columns if col != 'patient_id' and '_t' not in col and not any(
            col.startswith(p) for p in ['mortality_', 'readmission_', 'los_', 'sepsis_']
        )]
        label_features = [col for col in df.columns if any(
            col.startswith(prefix) for prefix in ['mortality_', 'readmission_', 'los_', 'sepsis_']
        )]
        
        # Calculate statistics
        summary = {
            'n_samples': len(df),
            'n_features': len(df.columns) - 1,  # Exclude patient_id
            'n_windowed_features': len(windowed_features),
            'n_static_features': len(static_features),
            'n_labels': len(label_features),
            'data_completeness': (df != 0).mean().mean(),
            'features_by_table': {}
        }
        
        # Count features by table (by matching concept IDs with our concepts dict)
        for table, concepts in self.concepts.items():
            concept_ids = [str(c['id']) for c in concepts]
            table_features = [col for col in df.columns if col.split('_')[0] in concept_ids]
            summary['features_by_table'][table] = len(table_features)
        
        return summary


if __name__ == '__main__':
    # Example usage
    print("\n" + "="*60)
    print("Simple Time Series Feature Extractor")
    print("="*60)
    
    print("\nConfiguration Options:")
    print("  Time Windows: 2h, 4h, 8h")
    print("  Min Observation: 24h, 48h, 72h")
    
    print("\nFeature Naming Convention:")
    print("  Windowed: {concept_id}_t{window}")
    print("    Example: 3024171_t0 (Respiratory rate, window 0)")
    print("  Static: condition_{concept_id}")
    print("    Example: condition_320128 (Essential hypertension)")
    
    print("\nUsage Examples:")
    print("  # Default configuration (4h windows, 24h observation)")
    print("  extractor = SimpleTimeSeriesExtractor()")
    print()
    print("  # Custom configuration")
    print("  extractor = SimpleTimeSeriesExtractor(time_window=2, minimum_observation_hours=48)")
    
    print("\nReady for feature extraction!")
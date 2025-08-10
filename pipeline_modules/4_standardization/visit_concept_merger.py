#!/usr/bin/env python
"""
Visit Concept Merger Module
Merge visit records with same concept_id and close temporal proximity
Author: CRISP Team
Date: 2025-08-08
"""

import pandas as pd
import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VisitConceptMerger:
    """
    Merge visit records based on concept_id and temporal proximity
    """
    
    def __init__(self, threshold_minutes: int = 60, output_dir: Optional[Path] = None):
        """
        Initialize visit merger
        
        Args:
            threshold_minutes: Time threshold for merging (default 60)
            output_dir: Output directory for results
        """
        self.threshold_minutes = threshold_minutes
        self.output_dir = output_dir or Path.cwd() / "output"
        self.merge_mappings = []
        self.statistics = {
            'total_records': 0,
            'merged_episodes': 0,
            'unchanged_records': 0,
            'records_merged': 0
        }
    
    def merge_visits_for_patient(self, df: pd.DataFrame, patient_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Merge visits for a single patient based on concept_id
        
        Args:
            df: DataFrame with visit records
            patient_id: Patient identifier
            
        Returns:
            tuple: (merged_records_df, mapping_df)
        """
        # Filter patient visits
        patient_visits = df[df['person_id'] == patient_id].copy()
        
        if patient_visits.empty:
            logging.warning(f"No visits found for patient {patient_id}")
            return pd.DataFrame(), pd.DataFrame()
        
        # Determine table type and columns
        if 'visit_detail_id' in patient_visits.columns:
            table_type = 'VISIT_DETAIL'
            id_col = 'visit_detail_id'
            concept_col = 'visit_detail_concept_id'
            start_col = 'visit_detail_start_datetime'
            end_col = 'visit_detail_end_datetime'
        else:
            table_type = 'VISIT_OCCURRENCE'
            id_col = 'visit_occurrence_id'
            concept_col = 'visit_concept_id'
            start_col = 'visit_start_datetime'
            end_col = 'visit_end_datetime'
        
        # Convert datetime columns with error handling
        patient_visits[start_col] = pd.to_datetime(patient_visits[start_col], errors='coerce')
        patient_visits[end_col] = pd.to_datetime(patient_visits[end_col], errors='coerce')
        
        all_merged_records = []
        all_unchanged_records = []
        patient_mappings = []
        
        # Process by concept_id groups
        for concept_id, concept_group in patient_visits.groupby(concept_col):
            # Sort by start time
            concept_group = concept_group.sort_values(start_col).reset_index(drop=True)
            
            # Identify episodes to merge
            episodes = self.identify_merge_episodes(concept_group, start_col, end_col)
            
            # Process each episode
            for episode_idx, episode_indices in enumerate(episodes):
                episode_records = concept_group.iloc[episode_indices]
                
                if len(episode_indices) > 1:
                    # Create merged record
                    merged_record = self.create_merged_record(
                        episode_records, 
                        patient_id, 
                        concept_id, 
                        episode_idx,
                        id_col, 
                        start_col, 
                        end_col
                    )
                    all_merged_records.append(merged_record)
                    
                    # Create mapping entries
                    for _, original_record in episode_records.iterrows():
                        mapping = {
                            'original_visit_id': original_record[id_col],
                            'merged_episode_id': merged_record['episode_id'],
                            'person_id': patient_id,
                            'concept_id': concept_id,
                            'table_type': table_type
                        }
                        patient_mappings.append(mapping)
                        self.merge_mappings.append(mapping)
                    
                    self.statistics['records_merged'] += len(episode_indices)
                    self.statistics['merged_episodes'] += 1
                else:
                    # Keep unchanged
                    unchanged = episode_records.iloc[0].to_dict()
                    unchanged['merge_flag'] = 'N'
                    unchanged['episode_id'] = f"{patient_id}_{concept_id}_{episode_idx}"
                    unchanged['original_visit_ids'] = json.dumps([unchanged[id_col]])
                    unchanged['visits_merged_count'] = 1
                    all_unchanged_records.append(unchanged)
                    self.statistics['unchanged_records'] += 1
        
        # Combine results
        result_records = all_merged_records + all_unchanged_records
        
        if result_records:
            result_df = pd.DataFrame(result_records)
        else:
            result_df = pd.DataFrame()
        
        mapping_df = pd.DataFrame(patient_mappings) if patient_mappings else pd.DataFrame()
        
        return result_df, mapping_df
    
    def identify_merge_episodes(self, concept_group: pd.DataFrame, 
                               start_col: str, end_col: str) -> List[List[int]]:
        """
        Identify episodes within concept group based on time proximity
        
        Args:
            concept_group: DataFrame of visits with same concept_id
            start_col: Name of start datetime column
            end_col: Name of end datetime column
            
        Returns:
            list: List of episode groups (each group is list of indices)
        """
        if len(concept_group) == 0:
            return []
        
        if len(concept_group) == 1:
            return [[0]]
        
        episodes = []
        current_episode = [0]
        
        for i in range(1, len(concept_group)):
            current_start = concept_group.iloc[i][start_col]
            previous_end = concept_group.iloc[i-1][end_col]
            
            # Calculate interval in minutes
            if pd.notna(current_start) and pd.notna(previous_end):
                interval_minutes = (current_start - previous_end).total_seconds() / 60
                
                if interval_minutes <= self.threshold_minutes:
                    # Add to current episode
                    current_episode.append(i)
                else:
                    # Start new episode
                    episodes.append(current_episode)
                    current_episode = [i]
            else:
                # If datetime is missing, start new episode
                episodes.append(current_episode)
                current_episode = [i]
        
        # Add last episode
        episodes.append(current_episode)
        
        return episodes
    
    def create_merged_record(self, episode_records: pd.DataFrame, 
                            patient_id: str, concept_id: int, episode_idx: int,
                            id_col: str, start_col: str, end_col: str) -> Dict:
        """
        Create single merged record from episode visits
        
        Args:
            episode_records: DataFrame of visits to merge
            patient_id: Patient identifier
            concept_id: Concept identifier
            episode_idx: Episode index within concept
            id_col: Name of ID column
            start_col: Name of start datetime column
            end_col: Name of end datetime column
            
        Returns:
            dict: Merged record with traceability info
        """
        # Take first record as base
        merged = episode_records.iloc[0].to_dict()
        
        # Update with merge information
        merged['merge_flag'] = 'Y'
        merged['episode_id'] = f"{patient_id}_{concept_id}_{episode_idx}"
        merged['original_visit_ids'] = json.dumps(episode_records[id_col].tolist())
        merged['visits_merged_count'] = len(episode_records)
        
        # Update temporal boundaries
        merged['merged_start_datetime'] = episode_records[start_col].min()
        merged['merged_end_datetime'] = episode_records[end_col].max()
        
        # Calculate merged duration
        duration_hours = (merged['merged_end_datetime'] - merged['merged_start_datetime']).total_seconds() / 3600
        merged['merged_duration_hours'] = duration_hours
        
        # Keep original start/end for reference
        merged[f'original_{start_col}'] = merged[start_col]
        merged[f'original_{end_col}'] = merged[end_col]
        
        # Update main datetime fields
        merged[start_col] = merged['merged_start_datetime']
        merged[end_col] = merged['merged_end_datetime']
        
        return merged
    
    def process_table(self, input_file: Path, table_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Process entire table for all patients
        
        Args:
            input_file: Path to input CSV file
            table_name: Name of table (VISIT_OCCURRENCE or VISIT_DETAIL)
            
        Returns:
            tuple: (merged_visits_df, mappings_df)
        """
        logging.info(f"Processing {table_name} from {input_file}")
        
        # Read input file
        df = pd.read_csv(input_file, low_memory=False)
        
        # Get unique patients
        patients = df['person_id'].unique()
        logging.info(f"Found {len(patients)} unique patients")
        
        all_results = []
        all_mappings = []
        
        # Process each patient
        from tqdm import tqdm
        for patient_id in tqdm(patients, desc=f"Processing {table_name}"):
            patient_results, patient_mappings = self.merge_visits_for_patient(df, patient_id)
            
            if not patient_results.empty:
                all_results.append(patient_results)
            
            if not patient_mappings.empty:
                all_mappings.append(patient_mappings)
        
        # Combine results
        if all_results:
            merged_df = pd.concat(all_results, ignore_index=True)
        else:
            merged_df = pd.DataFrame()
        
        if all_mappings:
            mapping_df = pd.concat(all_mappings, ignore_index=True)
        else:
            mapping_df = pd.DataFrame()
        
        # Log statistics
        logging.info(f"Completed {table_name}:")
        logging.info(f"  - Original records: {len(df)}")
        logging.info(f"  - Output records: {len(merged_df)}")
        logging.info(f"  - Merged episodes: {self.statistics['merged_episodes']}")
        logging.info(f"  - Unchanged records: {self.statistics['unchanged_records']}")
        
        return merged_df, mapping_df
    
    def save_results(self, merged_df: pd.DataFrame, mapping_df: pd.DataFrame, 
                    table_name: str, output_dir: Optional[Path] = None):
        """
        Save merged results and mappings to files
        
        Args:
            merged_df: DataFrame with merged visits
            mapping_df: DataFrame with merge mappings
            table_name: Name of table
            output_dir: Output directory (optional)
        """
        output_dir = output_dir or self.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save merged visits
        merged_file = output_dir / f"{table_name}_merged.csv"
        merged_df.to_csv(merged_file, index=False)
        logging.info(f"Saved merged visits to {merged_file}")
        
        # Save mappings
        if not mapping_df.empty:
            mapping_file = output_dir / f"{table_name}_merge_mapping.csv"
            mapping_df.to_csv(mapping_file, index=False)
            logging.info(f"Saved merge mappings to {mapping_file}")
        
        # Save statistics
        stats_file = output_dir / f"{table_name}_merge_statistics.json"
        with open(stats_file, 'w') as f:
            json.dump(self.statistics, f, indent=2)
        logging.info(f"Saved statistics to {stats_file}")
    
    def validate_merge(self, original_df: pd.DataFrame, merged_df: pd.DataFrame, 
                      mapping_df: pd.DataFrame) -> bool:
        """
        Validate merge results for data integrity
        
        Args:
            original_df: Original DataFrame
            merged_df: Merged DataFrame
            mapping_df: Mapping DataFrame
            
        Returns:
            bool: True if validation passes
        """
        # Check 1: No data loss
        original_ids = set(original_df.get('visit_occurrence_id', 
                                          original_df.get('visit_detail_id', [])))
        
        # Extract all original IDs from merged records
        traced_ids = set()
        for _, row in merged_df.iterrows():
            if row.get('merge_flag') == 'Y':
                ids = json.loads(row['original_visit_ids'])
                traced_ids.update(ids)
            else:
                traced_ids.add(row.get('visit_occurrence_id', 
                                      row.get('visit_detail_id')))
        
        missing_ids = original_ids - traced_ids
        if missing_ids:
            logging.error(f"Missing {len(missing_ids)} IDs after merge")
            return False
        
        # Check 2: Mapping completeness
        if not mapping_df.empty:
            mapped_ids = set(mapping_df['original_visit_id'])
            merged_ids = set()
            for _, row in merged_df[merged_df['merge_flag'] == 'Y'].iterrows():
                ids = json.loads(row['original_visit_ids'])
                merged_ids.update(ids)
            
            if mapped_ids != merged_ids:
                logging.error("Mapping inconsistency detected")
                return False
        
        logging.info("Validation passed successfully")
        return True


def main():
    """
    Example usage of VisitConceptMerger
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Merge visit records by concept ID')
    parser.add_argument('--input', type=str, required=True, 
                       help='Input CSV file path')
    parser.add_argument('--table', type=str, required=True,
                       choices=['VISIT_OCCURRENCE', 'VISIT_DETAIL'],
                       help='Table type')
    parser.add_argument('--output', type=str, default='output/merged',
                       help='Output directory')
    parser.add_argument('--threshold', type=int, default=60,
                       help='Time threshold in minutes (default: 60)')
    
    args = parser.parse_args()
    
    # Initialize merger
    merger = VisitConceptMerger(
        threshold_minutes=args.threshold,
        output_dir=Path(args.output)
    )
    
    # Process table
    input_file = Path(args.input)
    merged_df, mapping_df = merger.process_table(input_file, args.table)
    
    # Save results
    merger.save_results(merged_df, mapping_df, args.table)
    
    # Validate
    original_df = pd.read_csv(input_file)
    merger.validate_merge(original_df, merged_df, mapping_df)


if __name__ == "__main__":
    main()
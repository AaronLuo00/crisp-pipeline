#!/usr/bin/env python
"""
Process concept mapping files by removing already-mapped concepts and standardizing format.

This script:
1. For tables with SNOMED mappings: removes concepts that have been mapped to SNOMED
2. For all tables: removes Domain column, keeps Frequency column
3. Outputs standardized concept_mapping files
"""

import csv
from pathlib import Path
from typing import Set, Dict

# Setup paths
base_dir = Path(__file__).parent.parent  # mapping_resources directory
original_dir = base_dir / "original_mappings"
processed_dir = Path(__file__).parent  # processed_mappings directory

# Tables that have SNOMED mappings
MAPPED_TABLES = {
    'DEVICE_EXPOSURE': 'DEVICE_EXPOSURE_device_concept_id_analysis.csv',
    'MEASUREMENT': 'MEASUREMENT_measurement_concept_id_analysis_complete.csv',
    'OBSERVATION': 'OBSERVATION_observation_concept_id_analysis.csv',
    'PROCEDURE_OCCURRENCE': 'PROCEDURE_OCCURRENCE_procedure_concept_id_analysis_complete.csv'
}

# Output columns (removing Domain but keeping Frequency)
OUTPUT_COLUMNS = ['Id', 'Code', 'Name', 'Standard Class', 'Vocab', 'Validity', 'Concept', 'Frequency']


def get_default_domain(table_name: str) -> str:
    """Get default domain based on table name."""
    domain_map = {
        'DEVICE_EXPOSURE': 'Device',
        'MEASUREMENT': 'Measurement', 
        'OBSERVATION': 'Observation',
        'PROCEDURE_OCCURRENCE': 'Procedure'
    }
    return domain_map.get(table_name, 'Observation')


def load_mapped_concept_ids(table_name: str) -> Set[str]:
    """Load original concept IDs that have been mapped to SNOMED."""
    mapped_ids = set()
    reference_file = processed_dir / f"{table_name}_mapping_reference.csv"
    
    if reference_file.exists():
        with open(reference_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mapped_ids.add(row['original_concept_id'])
        print(f"  Loaded {len(mapped_ids)} mapped concept IDs from {reference_file.name}")
    else:
        print(f"  No mapping reference file found for {table_name}")
    
    return mapped_ids


def load_frequency_aggregation_mapping(table_name: str) -> Dict[str, int]:
    """Build a mapping of SNOMED concept IDs to aggregated frequencies."""
    frequency_map = {}
    reference_file = processed_dir / f"{table_name}_mapping_reference.csv"
    
    if reference_file.exists():
        with open(reference_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                snomed_id = row['snomed_concept_id']
                frequency = int(row.get('frequency', 0))
                
                # Aggregate frequencies for each SNOMED concept
                if snomed_id in frequency_map:
                    frequency_map[snomed_id] += frequency
                else:
                    frequency_map[snomed_id] = frequency
        
        print(f"  Built frequency aggregation for {len(frequency_map)} SNOMED concepts")
    
    return frequency_map


def load_snomed_replacements(table_name: str) -> Dict[str, Dict]:
    """Load SNOMED concepts that should replace mapped concepts."""
    replacements = {}
    
    # First load basic mapping info from reference file
    reference_file = processed_dir / f"{table_name}_mapping_reference.csv"
    snomed_mapped_file = processed_dir / f"{table_name}_snomed_mapped.csv"
    
    if reference_file.exists():
        # Build a map of original_id -> snomed_id from reference file
        original_to_snomed = {}
        with open(reference_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                original_id = row['original_concept_id']
                snomed_id = row['snomed_concept_id']
                original_to_snomed[original_id] = snomed_id
        
        # Now load complete SNOMED concept info from snomed_mapped file
        if snomed_mapped_file.exists():
            snomed_info_map = {}
            with open(snomed_mapped_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Only process rows where mapping was applied
                    if row.get('Mapping_Applied') == 'Yes' and row.get('Original_Id'):
                        orig_id = row['Original_Id']
                        if orig_id in original_to_snomed:
                            # Store complete SNOMED concept information
                            snomed_info_map[orig_id] = {
                                'snomed_id': row['Id'],
                                'snomed_code': row['Code'],
                                'snomed_name': row['Name'],
                                'standard_class': row.get('Standard Class', ''),
                                'domain': row.get('Domain', get_default_domain(table_name)),
                                'validity': row.get('Validity', ''),
                                'concept': row.get('Concept', ''),
                                'frequency': int(row.get('Frequency', 0)),
                                'mapping_relationship': row.get('Mapping_Relationship', 'Maps to')
                            }
            
            # Use the complete info if available, otherwise fall back to basic info
            for original_id, snomed_id in original_to_snomed.items():
                if original_id in snomed_info_map:
                    replacements[original_id] = snomed_info_map[original_id]
                else:
                    # Fallback to basic info from reference file (shouldn't happen normally)
                    replacements[original_id] = {
                        'snomed_id': snomed_id,
                        'snomed_code': '',
                        'snomed_name': '',
                        'standard_class': '',
                        'domain': get_default_domain(table_name),
                        'validity': '',
                        'concept': '',
                        'frequency': 0,
                        'mapping_relationship': 'Maps to'
                    }
        else:
            print(f"  Warning: {snomed_mapped_file.name} not found, using limited info")
            # Fall back to reference file only
            with open(reference_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    original_id = row['original_concept_id']
                    replacements[original_id] = {
                        'snomed_id': row['snomed_concept_id'],
                        'snomed_code': row['snomed_concept_code'],
                        'snomed_name': row['snomed_concept_name'],
                        'standard_class': '',
                        'domain': get_default_domain(table_name),
                        'validity': '',
                        'concept': '',
                        'frequency': int(row.get('frequency', 0)),
                        'mapping_relationship': row.get('mapping_relationship', 'Maps to')
                    }
        
        print(f"  Loaded {len(replacements)} SNOMED replacement mappings")
    
    return replacements


def process_table_with_mappings(table_name: str, filename: str) -> Dict:
    """Process a table that has SNOMED mappings."""
    print(f"\nProcessing {table_name} (with SNOMED mappings)...")
    
    # Load mapped concept IDs, frequency aggregation, and SNOMED replacements
    mapped_ids = load_mapped_concept_ids(table_name)
    frequency_aggregation = load_frequency_aggregation_mapping(table_name)
    snomed_replacements = load_snomed_replacements(table_name)
    
    # Read original file
    original_file = original_dir / filename
    output_file = base_dir / f"{table_name}_concept_mapping.csv"
    
    original_count = 0
    kept_count = 0
    removed_count = 0
    added_snomed_count = 0
    
    # Track existing concept IDs to avoid duplicates
    existing_ids = set()
    rows_to_write = []
    
    # First pass: process original file
    with open(original_file, 'r') as infile:
        reader = csv.DictReader(infile)
        
        for row in reader:
            original_count += 1
            existing_ids.add(row['Id'])
            
            # Skip if this concept has been mapped to SNOMED
            if row['Id'] in mapped_ids:
                removed_count += 1
                continue
            
            # Keep only the desired columns
            output_row = {col: row.get(col, '') for col in OUTPUT_COLUMNS}
            
            # Check if this is a SNOMED concept that received mappings from other vocabularies
            # If so, aggregate the frequencies (handles many-to-one mappings)
            if row['Id'] in frequency_aggregation:
                # Add the aggregated frequency from mapped concepts
                original_freq = int(row.get('Frequency', 0))
                aggregated_freq = frequency_aggregation[row['Id']]
                output_row['Frequency'] = str(original_freq + aggregated_freq)
            elif 'Frequency' not in output_row:
                # Ensure Frequency column exists even if empty
                output_row['Frequency'] = row.get('Frequency', '0')
            
            rows_to_write.append(output_row)
            kept_count += 1
    
    # Second pass: add new SNOMED concepts that don't exist in original file
    # This ensures the pipeline knows about SNOMED concepts that replaced mapped concepts
    # Group replacements by SNOMED ID to handle many-to-one mappings (multiple original concepts -> one SNOMED)
    unique_snomed_concepts = {}
    for _, snomed_info in snomed_replacements.items():
        snomed_id = snomed_info['snomed_id']
        
        # Only process if this SNOMED concept doesn't already exist in the original file
        if snomed_id not in existing_ids:
            # Aggregate frequency for many-to-one mappings
            if snomed_id in unique_snomed_concepts:
                # Add frequency from this mapping
                existing_freq = int(unique_snomed_concepts[snomed_id]['Frequency'])
                new_freq = int(snomed_info['frequency'])
                unique_snomed_concepts[snomed_id]['Frequency'] = str(existing_freq + new_freq)
            else:
                # First time seeing this SNOMED concept - use actual data from snomed_info
                new_row = {
                    'Id': snomed_id,
                    'Code': snomed_info.get('snomed_code', ''),
                    'Name': snomed_info.get('snomed_name', ''),
                    'Standard Class': snomed_info.get('standard_class', ''),
                    'Vocab': 'SNOMED',
                    'Validity': snomed_info.get('validity', ''),
                    'Concept': snomed_info.get('concept', ''),
                    'Frequency': str(snomed_info.get('frequency', 0))
                }
                
                unique_snomed_concepts[snomed_id] = new_row
    
    # Add unique SNOMED concepts to output
    for snomed_concept in unique_snomed_concepts.values():
        rows_to_write.append(snomed_concept)
        added_snomed_count += 1
    
    # Write all rows to output file
    with open(output_file, 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows_to_write)
    
    print(f"  Original concepts: {original_count:,}")
    print(f"  Removed (mapped to SNOMED): {removed_count:,}")
    print(f"  Kept concepts: {kept_count:,}")
    print(f"  Added new SNOMED concepts: {added_snomed_count:,}")
    print(f"  Total output concepts: {len(rows_to_write):,}")
    print(f"  Output: {output_file.name}")
    
    return {
        'table': table_name,
        'original': original_count,
        'removed': removed_count,
        'kept': kept_count,
        'added_snomed': added_snomed_count
    }


def process_table_without_mappings(filename: str) -> Dict:
    """Process a table that doesn't have SNOMED mappings."""
    # Extract table name from filename
    table_name = filename.replace('_analysis.csv', '').replace('_complete.csv', '')
    table_name = table_name.replace('_concept_id', '').replace('_id', '')
    
    print(f"\nProcessing {table_name} (no SNOMED mappings)...")
    
    original_file = original_dir / filename
    output_file = base_dir / f"{table_name}_concept_mapping.csv"
    
    row_count = 0
    
    with open(original_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        
        # Check which output columns exist in this file
        available_columns = [col for col in OUTPUT_COLUMNS if col in reader.fieldnames]
        
        writer = csv.DictWriter(outfile, fieldnames=available_columns)
        writer.writeheader()
        
        for row in reader:
            row_count += 1
            # Keep only the available columns
            output_row = {col: row.get(col, '') for col in available_columns}
            writer.writerow(output_row)
    
    print(f"  Total concepts: {row_count:,}")
    print(f"  Output: {output_file.name}")
    
    return {
        'table': table_name,
        'original': row_count,
        'removed': 0,
        'kept': row_count
    }


def main():
    """Main processing function."""
    print("="*70)
    print("CONCEPT MAPPING PROCESSOR")
    print("="*70)
    print(f"Original mappings directory: {original_dir}")
    print(f"Processed mappings directory: {processed_dir}")
    print(f"Output directory: {base_dir}")
    
    stats = []
    
    # Process tables with SNOMED mappings
    print("\n" + "="*70)
    print("PROCESSING TABLES WITH SNOMED MAPPINGS")
    print("="*70)
    
    for table_name, filename in MAPPED_TABLES.items():
        if (original_dir / filename).exists():
            stats.append(process_table_with_mappings(table_name, filename))
        else:
            print(f"\nWarning: {filename} not found in original_mappings/")
    
    # Process tables without SNOMED mappings
    print("\n" + "="*70)
    print("PROCESSING TABLES WITHOUT SNOMED MAPPINGS")
    print("="*70)
    
    # Get all other analysis files
    all_files = list(original_dir.glob("*_analysis*.csv"))
    mapped_files = set(MAPPED_TABLES.values())
    
    for file_path in all_files:
        filename = file_path.name
        if filename not in mapped_files:
            stats.append(process_table_without_mappings(filename))
    
    # Print summary statistics
    print("\n" + "="*70)
    print("SUMMARY STATISTICS")
    print("="*70)
    print(f"\n{'Table':<35} {'Original':>10} {'Removed':>10} {'Total_Rows':>10} {'Kept':>10} {'New_SNOMED':>10}")
    print("-"*110)
    
    total_original = 0
    total_removed = 0
    total_kept = 0
    total_added = 0
    total_final = 0
    
    for stat in stats:
        added = stat.get('added_snomed', 0)
        final_rows = stat['kept'] + added  # Calculate total rows in output file
        print(f"{stat['table']:<35} {stat['original']:>10,} {stat['removed']:>10,} {final_rows:>10,} {stat['kept']:>10,} {added:>10,}")
        total_original += stat['original']
        total_removed += stat['removed']
        total_kept += stat['kept']
        total_added += added
        total_final += final_rows
    
    print("-"*110)
    print(f"{'TOTAL':<35} {total_original:>10,} {total_removed:>10,} {total_final:>10,} {total_kept:>10,} {total_added:>10,}")
    
    print("\n" + "="*70)
    print("PROCESSING COMPLETE")
    print("="*70)
    print(f"Generated {len(stats)} concept mapping files in {base_dir}")
    print("\nThese files can be used by the pipeline for concept mapping,")
    print("with already-mapped concepts removed to avoid duplication.")


if __name__ == "__main__":
    main()
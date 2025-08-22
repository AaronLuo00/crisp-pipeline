#!/usr/bin/env python
"""
Process concept mapping files by removing already-mapped concepts and standardizing format.

This script:
1. For tables with SNOMED mappings: removes concepts that have been mapped to SNOMED
2. For all tables: removes Domain and Frequency columns
3. Outputs standardized concept_mapping files
"""

import csv
import os
from pathlib import Path
from typing import Set, Dict, List

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

# Output columns (removing Domain and Frequency)
OUTPUT_COLUMNS = ['Id', 'Code', 'Name', 'Standard Class', 'Vocab', 'Validity', 'Concept']


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


def process_table_with_mappings(table_name: str, filename: str) -> Dict:
    """Process a table that has SNOMED mappings."""
    print(f"\nProcessing {table_name} (with SNOMED mappings)...")
    
    # Load mapped concept IDs
    mapped_ids = load_mapped_concept_ids(table_name)
    
    # Read original file
    original_file = original_dir / filename
    output_file = base_dir / f"{table_name}_concept_mapping.csv"
    
    original_count = 0
    kept_count = 0
    removed_count = 0
    
    with open(original_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        
        for row in reader:
            original_count += 1
            
            # Skip if this concept has been mapped to SNOMED
            if row['Id'] in mapped_ids:
                removed_count += 1
                continue
            
            # Keep only the desired columns
            output_row = {col: row.get(col, '') for col in OUTPUT_COLUMNS}
            writer.writerow(output_row)
            kept_count += 1
    
    print(f"  Original concepts: {original_count:,}")
    print(f"  Removed (mapped to SNOMED): {removed_count:,}")
    print(f"  Kept concepts: {kept_count:,}")
    print(f"  Output: {output_file.name}")
    
    return {
        'table': table_name,
        'original': original_count,
        'removed': removed_count,
        'kept': kept_count
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
    print(f"\n{'Table':<35} {'Original':>10} {'Removed':>10} {'Kept':>10}")
    print("-"*70)
    
    total_original = 0
    total_removed = 0
    total_kept = 0
    
    for stat in stats:
        print(f"{stat['table']:<35} {stat['original']:>10,} {stat['removed']:>10,} {stat['kept']:>10,}")
        total_original += stat['original']
        total_removed += stat['removed']
        total_kept += stat['kept']
    
    print("-"*70)
    print(f"{'TOTAL':<35} {total_original:>10,} {total_removed:>10,} {total_kept:>10,}")
    
    print("\n" + "="*70)
    print("PROCESSING COMPLETE")
    print("="*70)
    print(f"Generated {len(stats)} concept mapping files in {base_dir}")
    print("\nThese files can be used by the pipeline for concept mapping,")
    print("with already-mapped concepts removed to avoid duplication.")


if __name__ == "__main__":
    main()
#!/usr/bin/env python
"""Process DEVICE_EXPOSURE mappings to create SNOMED-mapped versions."""

import csv
from pathlib import Path

# Setup paths
base_dir = Path(__file__).parent.parent
processed_dir = Path(__file__).parent

# Input files
original_file = base_dir / "DEVICE_EXPOSURE_device_concept_id_analysis.csv"
mappings_file = base_dir / "non_snomed_concepts" / "mapping_results" / "device_exposure_snomed_mappings.csv"

# Output files
mapped_output = processed_dir / "DEVICE_EXPOSURE_snomed_mapped.csv"
reference_output = processed_dir / "DEVICE_EXPOSURE_mapping_reference.csv"

def load_mappings():
    """Load SNOMED mappings from device_exposure_snomed_mappings.csv"""
    mappings = {}
    
    with open(mappings_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['mapping_status'] == 'Mapped' and row['snomed_concept_id']:
                original_id = row['Id']
                mappings[original_id] = {
                    'snomed_concept_id': row['snomed_concept_id'],
                    'snomed_concept_code': row['snomed_concept_code'],
                    'snomed_concept_name': row['snomed_concept_name'].strip(),
                    'mapping_relationship': row['mapping_relationship'],
                    'original_code': row['Code'],
                    'original_name': row['Name'],
                    'original_vocab': row['Vocab']
                }
    
    print(f"Loaded {len(mappings)} SNOMED mappings")
    return mappings

def process_device_exposure():
    """Process DEVICE_EXPOSURE file with SNOMED mappings."""
    mappings = load_mappings()
    
    # Read original file
    rows = []
    fieldnames = []
    
    with open(original_file, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames + ['Original_Id', 'Original_Code', 'Original_Name', 
                                          'Original_Vocab', 'Mapping_Applied', 'Mapping_Relationship']
        
        for row in reader:
            rows.append(row)
    
    print(f"Original file has {len(rows)} concepts")
    
    # Track mapping statistics
    stats = {
        'total_concepts': len(rows),
        'already_snomed': 0,
        'mapped_to_snomed': 0,
        'unmapped': 0
    }
    
    # Process each row
    processed_rows = []
    mapped_rows = []
    
    for row in rows:
        # Create a copy with additional fields
        new_row = row.copy()
        new_row['Original_Id'] = row['Id']
        new_row['Original_Code'] = row['Code']
        new_row['Original_Name'] = row['Name']
        new_row['Original_Vocab'] = row['Vocab']
        new_row['Mapping_Applied'] = 'No'
        new_row['Mapping_Relationship'] = ''
        
        concept_id = row['Id']
        
        if row['Vocab'] == 'SNOMED':
            # Already SNOMED - no change needed
            stats['already_snomed'] += 1
        elif concept_id in mappings:
            # Apply SNOMED mapping
            mapping = mappings[concept_id]
            
            # Replace with SNOMED concept info
            new_row['Id'] = mapping['snomed_concept_id']
            new_row['Code'] = mapping['snomed_concept_code']
            new_row['Name'] = mapping['snomed_concept_name']
            new_row['Standard Class'] = 'Procedure'  # Most mappings are to procedures
            new_row['Domain'] = 'Procedure'  # Update domain as these are procedures
            new_row['Vocab'] = 'SNOMED'
            new_row['Mapping_Applied'] = 'Yes'
            new_row['Mapping_Relationship'] = mapping['mapping_relationship']
            
            stats['mapped_to_snomed'] += 1
            
            # Add to mapped rows for reference file
            mapped_rows.append({
                'original_concept_id': new_row['Original_Id'],
                'original_code': new_row['Original_Code'],
                'original_name': new_row['Original_Name'],
                'original_vocab': new_row['Original_Vocab'],
                'snomed_concept_id': new_row['Id'],
                'snomed_concept_code': new_row['Code'],
                'snomed_concept_name': new_row['Name'],
                'snomed_vocab': 'SNOMED',
                'mapping_relationship': new_row['Mapping_Relationship'],
                'frequency': new_row['Frequency']
            })
        else:
            # No mapping available
            stats['unmapped'] += 1
        
        processed_rows.append(new_row)
    
    # Save processed file
    with open(mapped_output, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(processed_rows)
    
    print(f"\nSaved processed file to: {mapped_output}")
    
    # Save reference file with only mapped concepts
    if mapped_rows:
        reference_fieldnames = ['original_concept_id', 'original_code', 'original_name', 'original_vocab',
                               'snomed_concept_id', 'snomed_concept_code', 'snomed_concept_name', 
                               'snomed_vocab', 'mapping_relationship', 'frequency']
        
        with open(reference_output, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=reference_fieldnames)
            writer.writeheader()
            writer.writerows(mapped_rows)
        
        print(f"Saved reference file to: {reference_output}")
    
    # Print statistics
    print("\nMapping Statistics:")
    print(f"  Total concepts: {stats['total_concepts']}")
    print(f"  Already SNOMED: {stats['already_snomed']} ({stats['already_snomed']/stats['total_concepts']*100:.1f}%)")
    print(f"  Mapped to SNOMED: {stats['mapped_to_snomed']} ({stats['mapped_to_snomed']/stats['total_concepts']*100:.1f}%)")
    print(f"  Unmapped: {stats['unmapped']} ({stats['unmapped']/stats['total_concepts']*100:.1f}%)")
    
    return stats

if __name__ == "__main__":
    print("Processing DEVICE_EXPOSURE mappings...")
    print("="*60)
    process_device_exposure()
    print("="*60)
    print("Processing complete!")
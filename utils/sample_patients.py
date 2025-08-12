#!/usr/bin/env python3
"""
Sample patients and optionally extract all related OMOP CDM data.

This script can:
1. Sample patients from PERSON.csv based on ID prefixes
2. Extract all related records from OMOP CDM tables for sampled patients
"""

import pandas as pd
import numpy as np
from tqdm import tqdm
import argparse
import os
import sys
from pathlib import Path

# OMOP CDM tables to process
OMOP_TABLES = [
    'PERSON',
    'DEATH',
    'OBSERVATION_PERIOD', 
    'VISIT_OCCURRENCE',
    'VISIT_DETAIL',
    'CONDITION_OCCURRENCE',
    'PROCEDURE_OCCURRENCE',
    'DRUG_EXPOSURE',
    'MEASUREMENT',
    'OBSERVATION',
    'DEVICE_EXPOSURE',
    'SPECIMEN',
    'CONDITION_ERA',
    'DRUG_ERA'
]

def sample_patients(input_file, output_file, sample_size=1000, prefixes=None, random_seed=42):
    """
    Sample patients from PERSON.csv using stratified sampling by prefix
    
    Args:
        input_file: Path to input PERSON.csv
        output_file: Path to output sampled PERSON.csv
        sample_size: Number of patients to sample
        prefixes: List of person_id prefixes to filter (optional)
        random_seed: Random seed for reproducibility (default: 42)
    
    Returns:
        set: Set of sampled patient IDs
    """
    print(f"Reading PERSON.csv from: {input_file}")
    
    try:
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df):,} patients")
    except Exception as e:
        print(f"Error reading file: {e}")
        return None
    
    # Set random seed for reproducibility
    np.random.seed(random_seed)
    
    # If prefixes specified, filter by them
    if prefixes:
        df['person_id_str'] = df['person_id'].astype(str)
        
        # Filter by prefixes
        masks = []
        for prefix in prefixes:
            masks.append(df['person_id_str'].str.startswith(str(prefix)))
        
        if masks:
            combined_mask = pd.concat(masks, axis=1).any(axis=1)
            df = df[combined_mask]
            print(f"Filtered to {len(df):,} patients with prefixes: {prefixes}")
        
        df = df.drop('person_id_str', axis=1)
    
    # Perform stratified sampling by prefix (first 9 digits)
    if len(df) <= sample_size:
        print(f"Dataset has {len(df)} patients, using all")
        sampled_df = df
    else:
        print(f"\nPerforming stratified sampling of {sample_size} patients from {len(df):,}")
        print(f"Random seed: {random_seed}")
        
        # Add prefix column for stratification (first 9 digits)
        df['prefix'] = df['person_id'].astype(str).str[:9]
        
        # Calculate samples per prefix
        total_patients = len(df)
        prefix_groups = df.groupby('prefix')
        
        sampled_dfs = []
        print("\nSampling by prefix:")
        
        for prefix, group in prefix_groups:
            group_size = len(group)
            # Calculate proportional sample size
            sample_count = max(1, int(np.round(sample_size * group_size / total_patients)))
            
            # Adjust if group is smaller than calculated sample size
            if group_size < sample_count:
                sample_count = group_size
            
            # Sample from this prefix
            sampled = group.sample(n=sample_count, replace=False, random_state=random_seed)
            sampled_dfs.append(sampled)
            
            percentage = group_size / total_patients * 100
            print(f"  Prefix {prefix}: {sample_count} from {group_size:,} ({percentage:.1f}%)")
        
        # Combine all samples
        sampled_df = pd.concat(sampled_dfs, ignore_index=True)
        
        # If we're slightly under target due to rounding, add more from largest groups
        current_size = len(sampled_df)
        if current_size < sample_size:
            remaining = sample_size - current_size
            already_sampled = set(sampled_df['person_id'])
            
            for prefix in df['prefix'].value_counts().index:
                if remaining <= 0:
                    break
                group = df[df['prefix'] == prefix]
                available = group[~group['person_id'].isin(already_sampled)]
                if len(available) > 0:
                    additional = available.sample(n=min(remaining, len(available)), random_state=random_seed)
                    sampled_df = pd.concat([sampled_df, additional], ignore_index=True)
                    remaining -= len(additional)
        
        # Remove the prefix column before saving
        if 'prefix' in sampled_df.columns:
            sampled_df = sampled_df.drop('prefix', axis=1)
        
        print(f"\nTotal sampled: {len(sampled_df)} patients")
    
    # Save sampled patients
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
    sampled_df.to_csv(output_file, index=False)
    print(f"Saved {len(sampled_df)} sampled patients to: {output_file}")
    
    # Return patient IDs for extraction
    return set(sampled_df['person_id'].values)

def extract_table_data(table_name, input_dir, output_dir, patient_ids, chunk_size=200000, disable_progress=False):
    """
    Extract records for specific patients from an OMOP table
    
    Args:
        table_name: Name of the OMOP table
        input_dir: Directory containing input CSV files
        output_dir: Directory for output files
        patient_ids: Set of patient IDs to extract
        chunk_size: Number of rows to process at once
    """
    input_file = os.path.join(input_dir, f"{table_name}.csv")
    output_file = os.path.join(output_dir, f"{table_name}.csv")
    
    # Check if file exists
    if not os.path.exists(input_file):
        print(f"  ⚠️  {table_name}.csv not found, skipping...")
        return
    
    # Check if already extracted
    if os.path.exists(output_file):
        existing_size = os.path.getsize(output_file)
        if existing_size > 0:
            print(f"  ✅ {table_name}.csv already extracted ({existing_size/1024:.1f} KB), skipping...")
            return
    
    # Get file size
    file_size = os.path.getsize(input_file) / (1024 * 1024)  # MB
    print(f"  File: {table_name}.csv ({file_size:.1f} MB)")
    
    # For very large files, use larger chunk size and write incrementally
    if file_size > 10000:  # If file is larger than 10GB
        chunk_size = 500000  # Use larger chunks
        print(f"  Note: Large file detected, using chunk size of {chunk_size:,}")
    
    matched_rows = []
    total_rows = 0
    total_matches = 0
    batch_size = 50  # Write to disk every 50 chunks for large files
    
    try:
        # Estimate total chunks for progress bar
        # For small files, count actual lines; for large files, estimate
        total_chunks = None
        if file_size < 100:  # Less than 100MB, count actual lines
            with open(input_file, 'r') as f:
                total_lines = sum(1 for _ in f) - 1  # Subtract header
            total_chunks = (total_lines // chunk_size) + (1 if total_lines % chunk_size else 0)
            print(f"  Total rows: {total_lines:,}, Chunks: {total_chunks}")
        else:
            # For large files, estimate based on sampling actual file lines
            print(f"  Estimating total rows...")
            with open(input_file, 'rb') as f:
                # Read first 1MB or 10% of file (whichever is smaller) to estimate
                sample_size_bytes = min(int(file_size * 1024 * 1024 * 0.1), 1024 * 1024)  # Max 1MB sample
                sample_data = f.read(sample_size_bytes)
                line_count = sample_data.count(b'\n')
                
                if line_count > 0:
                    avg_bytes_per_line = sample_size_bytes / line_count
                    estimated_rows = int(file_size * 1024 * 1024 / avg_bytes_per_line) - 1  # Subtract header
                else:
                    # Fallback if no newlines found in sample
                    estimated_rows = 1000000  # Default estimate
            
            total_chunks = (estimated_rows // chunk_size) + 1
            print(f"  Estimated rows: ~{estimated_rows:,}, Chunks: ~{total_chunks}")
        
        # Create progress bar with known total
        pbar = tqdm(total=total_chunks, desc=f"  {table_name}", unit="chunk", 
                   leave=False, ascii="123456789#", ncols=80, disable=disable_progress)
        
        # First pass: write header
        first_chunk = True
        
        # Process chunks
        chunks = pd.read_csv(input_file, chunksize=chunk_size, low_memory=False)
        for i, chunk in enumerate(chunks):
            total_rows += len(chunk)
            
            # Find matching records
            if 'person_id' in chunk.columns:
                mask = chunk['person_id'].isin(patient_ids)
                matched = chunk[mask]
                
                if not matched.empty:
                    matched_rows.append(matched)
                    total_matches += len(matched)
            
            # Update progress
            pbar.update(1)
            pbar.set_postfix({'Rows': f'{total_rows:,}', 'Matches': total_matches})
            
            # For large files, write incrementally to avoid memory issues
            if file_size > 10000 and len(matched_rows) >= batch_size:
                if matched_rows:
                    batch_result = pd.concat(matched_rows, ignore_index=True)
                    if first_chunk:
                        batch_result.to_csv(output_file, index=False, mode='w')
                        first_chunk = False
                    else:
                        batch_result.to_csv(output_file, index=False, mode='a', header=False)
                    matched_rows = []  # Clear memory
        
        # Close progress bar
        pbar.close()
        
        # Save remaining results
        if matched_rows:
            result = pd.concat(matched_rows, ignore_index=True)
            if first_chunk:
                result.to_csv(output_file, index=False, mode='w')
            else:
                result.to_csv(output_file, index=False, mode='a', header=False)
            print(f"  ✓ Extracted {total_matches:,} records from {total_rows:,} total")
        elif total_matches == 0:
            # Create empty file with headers if no matches
            empty_df = pd.read_csv(input_file, nrows=0)
            empty_df.to_csv(output_file, index=False)
            print(f"  ✓ No matching records found (created empty file with headers)")
        else:
            print(f"  ✓ Extracted {total_matches:,} records from {total_rows:,} total")
            
    except Exception as e:
        print(f"  ✗ Error processing {table_name}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(
        description='Sample patients and extract related OMOP CDM data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Simple sampling (backward compatible)
  %(prog)s --input /path/to/PERSON.csv --output data/PERSON.csv --sample-size 1000
  
  # Sample and extract all related data
  %(prog)s --input-dir /path/to/OMOP_data/ --output-dir data/ --sample-size 1000 --extract-all
        """
    )
    
    # Sampling options
    parser.add_argument('--input', help='Path to input PERSON.csv (for simple sampling)')
    parser.add_argument('--output', help='Path to output PERSON.csv (for simple sampling)')
    parser.add_argument('--sample-size', type=int, default=1000, help='Number of patients to sample (default: 1000)')
    parser.add_argument('--prefixes', nargs='+', help='Person ID prefixes to filter (e.g., 4 6 7 9)')
    parser.add_argument('--random-seed', type=int, default=42, help='Random seed for reproducibility (default: 42)')
    
    # Full extraction options
    parser.add_argument('--input-dir', help='Directory containing all OMOP CSV files')
    parser.add_argument('--output-dir', default='data/', help='Directory for output files (default: data/)')
    parser.add_argument('--extract-all', action='store_true', help='Extract all related OMOP tables')
    parser.add_argument('--chunk-size', type=int, default=200000, help='Chunk size for processing (default: 200000)')
    parser.add_argument('--resume', action='store_true', help='Resume extraction (skip already extracted tables)')
    parser.add_argument('--no-progress', action='store_true', help='Disable progress bars (use simple output)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.extract_all:
        if not args.input_dir:
            parser.error("--extract-all requires --input-dir")
        
        # Safety check: warn if input and output directories are the same
        input_dir_abs = os.path.abspath(args.input_dir)
        output_dir_abs = os.path.abspath(args.output_dir)
        
        if input_dir_abs == output_dir_abs:
            print("\n⚠️  WARNING: Input and output directories are the same!")
            print("This will overwrite your original data files.")
            response = input("Are you ABSOLUTELY sure you want to continue? (yes/no): ")
            if response.lower() != 'yes':
                print("Operation cancelled for safety.")
                sys.exit(0)
        
        # Full extraction mode
        print("="*70)
        print("OMOP CDM Data Extraction")
        print("="*70)
        
        # Step 1: Sample patients or load existing
        print("\nStep 1: Preparing patient list")
        print("-"*30)
        
        person_input = os.path.join(args.input_dir, 'PERSON.csv')
        person_output = os.path.join(args.output_dir, 'PERSON.csv')
        
        if args.resume and os.path.exists(person_output):
            # Load existing patient IDs
            print(f"Resume mode: Loading existing patient IDs from {person_output}")
            existing_df = pd.read_csv(person_output)
            patient_ids = set(existing_df['person_id'].values)
            print(f"Loaded {len(patient_ids)} existing patient IDs")
        else:
            # Sample new patients
            patient_ids = sample_patients(
                person_input, 
                person_output, 
                args.sample_size,
                args.prefixes,
                args.random_seed
            )
            
            if patient_ids is None:
                print("Failed to sample patients")
                sys.exit(1)
        
        # Step 2: Extract related data from all tables
        print(f"\nStep 2: Extracting data for {len(patient_ids)} patients")
        print("-"*30)
        
        tables_to_extract = OMOP_TABLES[1:]  # Skip PERSON as it's already done
        
        # Process each table with progress counter (not nested progress bars)
        for idx, table in enumerate(tables_to_extract, 1):
            print(f"\n[{idx}/{len(tables_to_extract)}] Processing {table}...")
            extract_table_data(
                table, 
                args.input_dir, 
                args.output_dir, 
                patient_ids,
                args.chunk_size,
                args.no_progress
            )
        
        print("\n" + "="*70)
        print("✅ Data extraction complete!")
        print(f"All files saved to: {args.output_dir}")
        print("="*70)
        
    else:
        # Simple sampling mode (backward compatible)
        if not args.input or not args.output:
            parser.error("Simple sampling requires --input and --output")
        
        print("Sampling patients...")
        patient_ids = sample_patients(
            args.input,
            args.output,
            args.sample_size,
            args.prefixes,
            args.random_seed
        )
        
        if patient_ids:
            print(f"\n✅ Successfully sampled {len(patient_ids)} patients")
        else:
            print("\n✗ Sampling failed")
            sys.exit(1)

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""
Validate OMOP CDM data format and quality for CRISP pipeline.

Performs essential validation:
1. Table existence check
2. Schema validation  
3. Data quality check
"""

import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime
import sys

class OMOPValidator:
    """OMOP CDM Data Validator for CRISP Pipeline"""
    
    # Based on CRISP pipeline requirements
    REQUIRED_TABLES = [
        'PERSON', 'VISIT_OCCURRENCE', 'CONDITION_OCCURRENCE',
        'PROCEDURE_OCCURRENCE', 'DRUG_EXPOSURE', 'MEASUREMENT',
        'OBSERVATION', 'DEATH', 'DEVICE_EXPOSURE', 'SPECIMEN',
        'VISIT_DETAIL', 'DRUG_ERA', 'CONDITION_ERA', 'OBSERVATION_PERIOD'
    ]
    
    OPTIONAL_TABLES = [
        'LOCATION', 'PROVIDER', 'CARE_SITE'
    ]
    
    # OMOP CDM v5.3 required columns (simplified)
    REQUIRED_COLUMNS = {
        'PERSON': ['person_id', 'gender_concept_id', 'year_of_birth', 
                   'race_concept_id', 'ethnicity_concept_id'],
        'VISIT_OCCURRENCE': ['visit_occurrence_id', 'person_id', 
                             'visit_concept_id', 'visit_start_date', 'visit_end_date'],
        'CONDITION_OCCURRENCE': ['condition_occurrence_id', 'person_id', 
                                  'condition_concept_id', 'condition_start_date'],
        'PROCEDURE_OCCURRENCE': ['procedure_occurrence_id', 'person_id', 
                                  'procedure_concept_id', 'procedure_date'],
        'DRUG_EXPOSURE': ['drug_exposure_id', 'person_id', 
                          'drug_concept_id', 'drug_exposure_start_date'],
        'MEASUREMENT': ['measurement_id', 'person_id', 
                        'measurement_concept_id', 'measurement_date'],
        'OBSERVATION': ['observation_id', 'person_id', 
                        'observation_concept_id', 'observation_date'],
        'DEATH': ['person_id', 'death_date'],
        'DEVICE_EXPOSURE': ['device_exposure_id', 'person_id', 
                            'device_concept_id', 'device_exposure_start_date'],
        'SPECIMEN': ['specimen_id', 'person_id', 'specimen_concept_id'],
        'VISIT_DETAIL': ['visit_detail_id', 'person_id', 'visit_detail_concept_id'],
        'DRUG_ERA': ['drug_era_id', 'person_id', 'drug_concept_id'],
        'CONDITION_ERA': ['condition_era_id', 'person_id', 'condition_concept_id'],
        'OBSERVATION_PERIOD': ['observation_period_id', 'person_id']
    }
    
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self.errors = []
        self.warnings = []
        self.info = []
        self.table_stats = {}
        
    def validate_all(self):
        """Run validation suite"""
        print("\n" + "="*60)
        print("OMOP CDM Data Validation for CRISP Pipeline")
        print("="*60)
        print(f"Data directory: {self.data_dir}")
        
        # Step 1: Check table existence
        print("\n[Step 1/3] Checking table existence...")
        print("-"*40)
        self.check_tables()
        
        # Step 2: Validate schemas
        print("\n[Step 2/3] Validating table schemas...")
        print("-"*40)
        self.validate_schemas()
        
        # Step 3: Check data quality
        print("\n[Step 3/3] Checking data quality...")
        print("-"*40)
        self.check_data_quality()
        
        # Generate report
        self.generate_report()
        
    def check_tables(self):
        """Step 1: Check for required and optional tables"""
        missing_required = []
        found_tables = []
        
        # Check required tables
        for table in self.REQUIRED_TABLES:
            file_path = self.data_dir / f"{table}.csv"
            if not file_path.exists():
                missing_required.append(table)
                self.errors.append(f"Required table missing: {table}.csv")
            else:
                # Get file stats
                size_mb = file_path.stat().st_size / (1024*1024)
                # Quick row count (excluding header)
                with open(file_path, 'r') as f:
                    rows = sum(1 for _ in f) - 1
                
                self.table_stats[table] = {
                    'rows': rows, 
                    'size_mb': round(size_mb, 2)
                }
                found_tables.append(table)
                print(f"  [OK] {table}: {rows:,} rows, {size_mb:.1f} MB")
        
        # Report missing required tables
        if missing_required:
            print(f"\n  [ERROR] Missing {len(missing_required)} required tables:")
            for table in missing_required:
                print(f"     - {table}.csv")
        
        # Check optional tables
        print("\n  Optional tables:")
        for table in self.OPTIONAL_TABLES:
            file_path = self.data_dir / f"{table}.csv"
            if file_path.exists():
                size_mb = file_path.stat().st_size / (1024*1024)
                print(f"  [INFO] {table}.csv found ({size_mb:.1f} MB)")
                self.info.append(f"Optional table found: {table}.csv")
            else:
                print(f"  ○  {table}.csv not found (optional)")
        
        print(f"\n  Summary: {len(found_tables)}/{len(self.REQUIRED_TABLES)} required tables found")
        
    def validate_schemas(self):
        """Step 2: Validate table schemas against OMOP CDM v5.3"""
        tables_checked = 0
        schema_errors = 0
        
        for table, required_cols in self.REQUIRED_COLUMNS.items():
            file_path = self.data_dir / f"{table}.csv"
            if not file_path.exists():
                continue
            
            tables_checked += 1
            
            try:
                # Read just the header
                df_sample = pd.read_csv(file_path, nrows=0)
                actual_cols = set(df_sample.columns)
                required_cols_set = set(required_cols)
                
                missing_cols = required_cols_set - actual_cols
                
                if missing_cols:
                    self.errors.append(f"{table} missing required columns: {missing_cols}")
                    print(f"  [ERROR] {table}: missing columns {missing_cols}")
                    schema_errors += 1
                else:
                    print(f"  [OK] {table}: all required columns present")
                    
            except Exception as e:
                self.errors.append(f"Error reading {table}: {str(e)}")
                print(f"  [ERROR] Error reading {table}: {str(e)}")
                schema_errors += 1
        
        print(f"\n  Summary: {tables_checked - schema_errors}/{tables_checked} tables have valid schemas")
    
    def check_data_quality(self):
        """Step 3: Check basic data quality"""
        
        # Focus on PERSON table as it's the most critical
        person_file = self.data_dir / "PERSON.csv"
        if not person_file.exists():
            print("  [WARNING] Cannot check data quality - PERSON.csv not found")
            return
        
        print(f"  Checking PERSON table quality...")
        
        try:
            # Read PERSON table
            person_df = pd.read_csv(person_file)
            total_persons = len(person_df)
            
            # 1. Check for duplicate person_ids
            duplicates = person_df['person_id'].duplicated().sum()
            if duplicates > 0:
                self.errors.append(f"Found {duplicates} duplicate person_ids in PERSON table")
                print(f"    [ERROR] Found {duplicates} duplicate person_ids")
            else:
                print(f"    [OK] No duplicate person_ids (checked {total_persons:,} records)")
            
            # 2. Check for nulls in critical fields
            critical_fields = ['person_id', 'gender_concept_id', 'year_of_birth']
            null_summary = []
            
            for col in critical_fields:
                if col in person_df.columns:
                    null_count = person_df[col].isna().sum()
                    if null_count > 0:
                        null_pct = (null_count / total_persons) * 100
                        self.warnings.append(f"{null_count} null values in PERSON.{col} ({null_pct:.1f}%)")
                        null_summary.append(f"{col}: {null_count} nulls")
                        print(f"    [WARNING] {col}: {null_count:,} null values ({null_pct:.1f}%)")
                    else:
                        print(f"    [OK] {col}: no null values")
            
            # 3. Basic statistics
            print(f"\n  PERSON table statistics:")
            print(f"    • Total persons: {total_persons:,}")
            
            if 'gender_concept_id' in person_df.columns:
                gender_counts = person_df['gender_concept_id'].value_counts().head(3)
                print(f"    • Top gender concepts: {dict(gender_counts)}")
            
            if 'year_of_birth' in person_df.columns:
                min_year = person_df['year_of_birth'].min()
                max_year = person_df['year_of_birth'].max()
                print(f"    • Birth year range: {min_year} - {max_year}")
                
        except Exception as e:
            self.errors.append(f"Error checking data quality: {str(e)}")
            print(f"  [ERROR] Error checking data quality: {str(e)}")
    
    def generate_report(self):
        """Generate comprehensive validation summary"""
        print("\n" + "="*60)
        print("VALIDATION SUMMARY")
        print("="*60)
        
        # Overall statistics
        total_rows = sum(stats['rows'] for stats in self.table_stats.values())
        total_size = sum(stats['size_mb'] for stats in self.table_stats.values())
        
        print(f"\n[STATS] Dataset Overview:")
        print(f"  • Total tables found: {len(self.table_stats)}/{len(self.REQUIRED_TABLES)}")
        print(f"  • Total records: {total_rows:,}")
        print(f"  • Total data size: {total_size:.1f} MB")
        
        if self.table_stats:
            # Find largest tables
            sorted_tables = sorted(self.table_stats.items(), 
                                 key=lambda x: x[1]['rows'], reverse=True)
            print(f"\n📈 Largest tables by record count:")
            for table, stats in sorted_tables[:3]:
                print(f"  • {table}: {stats['rows']:,} records ({stats['size_mb']:.1f} MB)")
        
        # Validation results
        print(f"\n[CHECK] Validation Results:")
        print(f"  • Schema validation: {'[PASSED]' if not any('schema' in e for e in self.errors) else '[FAILED]'}")
        print(f"  • Data quality: {'[PASSED]' if not any('duplicate' in e or 'quality' in e for e in self.errors) else '[FAILED]'}")
        print(f"  • Required tables: {'[ALL PRESENT]' if len(self.table_stats) == len(self.REQUIRED_TABLES) else f'[{len(self.REQUIRED_TABLES) - len(self.table_stats)} MISSING]'}")
        
        # Determine if data is ready
        is_ready = len(self.errors) == 0
        
        print("\n" + "-"*60)
        
        if is_ready:
            print("\n[SUCCESS] DATA VALIDATION PASSED")
            print("\nYour OMOP CDM data meets all requirements for the CRISP pipeline.")
            print("You can proceed with running the pipeline modules.")
            
            print("\n[TIP] Next steps:")
            print("  1. Run complete pipeline: python pipeline_modules/run_all_module.py")
            print("  2. Or run individual modules:")
            print("     • python pipeline_modules/1_eda/run_eda_analysis.py")
            print("     • python pipeline_modules/2_cleaning/run_data_cleaning.py")
            print("     • etc.")
        else:
            print(f"\n[FAILED] VALIDATION FAILED")
            print(f"\nFound {len(self.errors)} critical error(s) that must be fixed before running CRISP pipeline:")
            
            # Group errors by type
            table_errors = [e for e in self.errors if 'missing:' in e or 'Missing' in e]
            schema_errors = [e for e in self.errors if 'columns' in e]
            quality_errors = [e for e in self.errors if 'duplicate' in e or 'null' in e]
            
            if table_errors:
                print("\n📁 Missing Tables:")
                for error in table_errors:
                    print(f"  • {error}")
            
            if schema_errors:
                print("\n[REPORT] Schema Issues:")
                for error in schema_errors:
                    print(f"  • {error}")
            
            if quality_errors:
                print("\n[WARNING] Data Quality Issues:")
                for error in quality_errors:
                    print(f"  • {error}")
            
            print("\n🔧 Resolution:")
            print("  Please fix the above issues before running the CRISP pipeline.")
            print("  Refer to OMOP CDM v5.3 documentation for table requirements.")
        
        if self.warnings:
            print(f"\n[WARNING] {len(self.warnings)} warning(s) (non-critical):")
            for i, warning in enumerate(self.warnings[:3], 1):
                print(f"  {i}. {warning}")
            if len(self.warnings) > 3:
                print(f"  ... and {len(self.warnings)-3} more")
            print("\n  Note: Warnings won't prevent pipeline execution but may affect results.")
        
        print("\n" + "="*60)
        
        return is_ready

def main():
    parser = argparse.ArgumentParser(
        description='Validate OMOP CDM data for CRISP pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate data in default directory
  python data_preparation/validate_data.py
  
  # Validate data in specific directory
  python data_preparation/validate_data.py --data-dir /path/to/omop/data/
        """
    )
    parser.add_argument('--data-dir', default='data/', 
                        help='Directory containing OMOP CDM CSV files (default: data/)')
    
    args = parser.parse_args()
    
    # Check if directory exists
    data_path = Path(args.data_dir)
    if not data_path.exists():
        print(f"[ERROR] Data directory '{data_path}' does not exist")
        sys.exit(1)
    
    # Run validation
    validator = OMOPValidator(args.data_dir)
    is_valid = validator.validate_all()
    
    # Exit with appropriate code
    sys.exit(0 if is_valid else 1)

if __name__ == '__main__':
    main()
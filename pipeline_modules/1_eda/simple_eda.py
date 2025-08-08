#!/usr/bin/env python
"""Simple EDA analysis using only standard library."""

import csv
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Setup
base_dir = Path(__file__).parent
project_root = base_dir.parent.parent  # Go up to crisp_pipeline_code
data_dir = project_root / "data"
output_dir = project_root / "output" / "1_eda"
output_dir.mkdir(parents=True, exist_ok=True)

print("Starting simple EDA analysis on subdataset_1000...")
print(f"Output directory: {output_dir}")

# Find all CSV files
data_files = list(data_dir.glob("*.csv"))
print(f"\nFound {len(data_files)} data files")

# Store results
eda_results = {
    "analysis_date": datetime.now().isoformat(),
    "dataset": "subdataset_1000",
    "tables": {}
}

# Analyze each table
for file_path in sorted(data_files):
    table_name = file_path.stem
    print(f"\nAnalyzing {table_name}...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Initialize counters
        row_count = 0
        columns = None
        id_unique_values = defaultdict(set)
        missing_counts = defaultdict(int)
        icu_visits = 0
        icu_patients = set()
        
        # Process rows
        for row in reader:
            row_count += 1
            
            if columns is None:
                columns = list(row.keys())
                
            # Count missing values and collect unique IDs
            for col, value in row.items():
                if not value or value.strip() == '':
                    missing_counts[col] += 1
                    
                if col.endswith('_id') and value:
                    id_unique_values[col].add(value)
            
            # Special handling for VISIT_DETAIL
            if table_name == "VISIT_DETAIL" and row.get('visit_detail_concept_id') == '581379':
                icu_visits += 1
                if row.get('person_id'):
                    icu_patients.add(row['person_id'])
        
        # Calculate statistics
        stats = {
            "file_name": file_path.name,
            "total_records": row_count,
            "total_columns": len(columns) if columns else 0,
            "columns": columns or [],
            "missing_counts": dict(missing_counts),
            "missing_percentage": {col: (count / row_count * 100) if row_count > 0 else 0 
                                 for col, count in missing_counts.items()}
        }
        
        # Unique counts
        stats["unique_counts"] = {col: len(values) for col, values in id_unique_values.items()}
        
        # Table-specific stats
        if table_name == "PERSON":
            stats["unique_patients"] = len(id_unique_values.get('person_id', set()))
            
        elif table_name == "VISIT_DETAIL":
            stats["icu_visits"] = {
                "total": icu_visits,
                "unique_patients": len(icu_patients)
            }
            
        elif table_name == "DEATH":
            stats["total_deaths"] = row_count
            stats["unique_patients_died"] = len(id_unique_values.get('person_id', set()))
        
        eda_results["tables"][table_name] = stats

# Overall summary
print("\n" + "="*60)
print("OVERALL SUMMARY")
print("="*60)

total_records = sum(stats["total_records"] for stats in eda_results["tables"].values())
print(f"Total records across all tables: {total_records:,}")
print(f"Total tables analyzed: {len(eda_results['tables'])}")

# Patient summary
if "PERSON" in eda_results["tables"]:
    person_stats = eda_results["tables"]["PERSON"]
    print(f"\nPatient Statistics:")
    print(f"  - Total patients: {person_stats['unique_patients']:,}")

# ICU summary
if "VISIT_DETAIL" in eda_results["tables"]:
    visit_stats = eda_results["tables"]["VISIT_DETAIL"]
    if "icu_visits" in visit_stats:
        print(f"\nICU Statistics:")
        print(f"  - Total ICU visits: {visit_stats['icu_visits']['total']:,}")
        print(f"  - Unique ICU patients: {visit_stats['icu_visits']['unique_patients']:,}")

# Death summary
if "DEATH" in eda_results["tables"]:
    death_stats = eda_results["tables"]["DEATH"]
    person_count = person_stats['unique_patients'] if 'person_stats' in locals() else 1000
    print(f"\nMortality Statistics:")
    print(f"  - Total deaths: {death_stats['total_deaths']:,}")
    print(f"  - Death rate: {death_stats['total_deaths'] / person_count * 100:.1f}%")

# Save results
results_path = output_dir / "eda_results.json"
with open(results_path, 'w') as f:
    json.dump(eda_results, f, indent=2)
print(f"\nDetailed results saved to: {results_path}")

# Generate text report
report_path = output_dir / "eda_report.txt"
with open(report_path, 'w') as f:
    f.write("EDA REPORT - subdataset_1000\n")
    f.write("="*60 + "\n\n")
    f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    
    f.write("DATASET OVERVIEW\n")
    f.write("-"*40 + "\n")
    f.write(f"Total Tables: {len(eda_results['tables'])}\n")
    f.write(f"Total Records: {total_records:,}\n")
    if "PERSON" in eda_results["tables"]:
        f.write(f"Total Patients: {person_stats['unique_patients']:,}\n")
    f.write("\n")
    
    f.write("TABLE SUMMARY (sorted by record count)\n")
    f.write("-"*40 + "\n")
    
    # Sort tables by record count
    sorted_tables = sorted(eda_results["tables"].items(), 
                         key=lambda x: x[1]["total_records"], reverse=True)
    
    for name, stats in sorted_tables:
        f.write(f"\n{name}:\n")
        f.write(f"  Records: {stats['total_records']:,}\n")
        f.write(f"  Columns: {stats['total_columns']}\n")
        
        # Show unique counts for ID columns
        if stats["unique_counts"]:
            f.write("  Unique values:\n")
            for col, count in sorted(stats["unique_counts"].items()):
                f.write(f"    {col}: {count:,}\n")
                
        # Show missing data summary
        missing_cols = [col for col, pct in stats["missing_percentage"].items() if pct > 0]
        if missing_cols:
            f.write("  Columns with missing data:\n")
            for col in sorted(missing_cols, 
                            key=lambda x: stats["missing_percentage"][x], reverse=True)[:5]:
                f.write(f"    {col}: {stats['missing_percentage'][col]:.1f}%\n")
                
        # Table-specific info
        if name == "VISIT_DETAIL" and "icu_visits" in stats:
            f.write(f"  ICU visits: {stats['icu_visits']['total']:,}\n")
            f.write(f"  ICU patients: {stats['icu_visits']['unique_patients']:,}\n")
        elif name == "DEATH":
            f.write(f"  Deaths recorded: {stats['total_deaths']:,}\n")

print(f"Text report saved to: {report_path}")
print("\nEDA analysis completed!")
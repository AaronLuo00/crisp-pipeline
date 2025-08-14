#!/usr/bin/env python
"""Run EDA analysis on subdataset_1000 with progress bars and chunked reading."""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import numpy as np

class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle numpy types."""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

# Configuration
CHUNK_SIZE = 200000  # Default chunk size for reading large files

# Setup
base_dir = Path(__file__).parent
project_root = base_dir.parent.parent  # Go up to crisp_pipeline_code
data_dir = project_root / "data"
output_dir = project_root / "output" / "1_eda"
output_dir.mkdir(parents=True, exist_ok=True)

print("Starting EDA analysis on subdataset_1000...")
print(f"Output directory: {output_dir}")

# Load all CSV files
data_files = list(data_dir.glob("*.csv"))
print(f"\nFound {len(data_files)} data files")
print(f"Using chunk size: {CHUNK_SIZE:,} rows")

# Store statistics
eda_results = {
    "analysis_date": datetime.now().isoformat(),
    "dataset": "subdataset_1000",
    "chunk_size": CHUNK_SIZE,
    "tables": {}
}

def get_file_row_count(file_path):
    """Get the number of rows in a CSV file (excluding header)."""
    with open(file_path, 'r') as f:
        return sum(1 for _ in f) - 1

def process_table_chunked(file_path, chunk_size=CHUNK_SIZE):
    """Process a table in chunks and return aggregated statistics."""
    table_name = file_path.stem
    
    # Get total rows for progress bar
    total_rows = get_file_row_count(file_path)
    
    # Initialize statistics
    stats = {
        "file_name": file_path.name,
        "total_records": 0,
        "total_columns": 0,
        "columns": None,
        "memory_usage_mb": 0,
        "missing_values": {},
        "data_types": None,
        "chunks_processed": 0
    }
    
    # For aggregating unique values
    unique_trackers = {}
    id_columns = []
    datetime_columns = []
    
    # Table-specific data collection
    table_data = {
        "gender_counts": {},
        "birth_years": [],
        "concept_distributions": {},
        "death_count": 0,
        "icu_patients": set(),
        "all_patients": set()
    }
    
    # Process chunks with progress bar
    with tqdm(total=total_rows, desc=f"Reading {table_name}", 
              unit="rows", leave=False,
              disable=total_rows < 10000,  # Don't show progress for small files
              miniters=max(1, total_rows//20) if total_rows > 0 else 1,  # Update every 5%
              mininterval=2.0,  # At least 2 seconds interval
              position=1,  # Nested position
              ncols=80) as pbar:
        for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size, low_memory=False)):
            # First chunk: initialize column-based statistics
            if chunk_num == 0:
                stats["columns"] = list(chunk.columns)
                stats["total_columns"] = len(chunk.columns)
                stats["data_types"] = chunk.dtypes.astype(str).to_dict()
                
                # Initialize missing values counter
                for col in chunk.columns:
                    stats["missing_values"][col] = 0
                
                # Identify column types
                id_columns = [col for col in chunk.columns if col.endswith('_id')]
                datetime_columns = [col for col in chunk.columns 
                                  if 'date' in col.lower() or 'time' in col.lower()]
                
                # Initialize unique value trackers for ID columns
                for col in id_columns:
                    unique_trackers[col] = set()
            
            # Update basic statistics
            stats["total_records"] += len(chunk)
            stats["memory_usage_mb"] += chunk.memory_usage(deep=True).sum() / 1024 / 1024
            
            # Update missing values
            for col in chunk.columns:
                stats["missing_values"][col] += chunk[col].isnull().sum()
            
            # Track unique values for ID columns
            for col in id_columns:
                unique_trackers[col].update(chunk[col].dropna().unique())
            
            # Table-specific processing
            if table_name == "PERSON":
                table_data["all_patients"].update(chunk['person_id'].unique())
                # Gender distribution
                gender_counts = chunk['gender_concept_id'].value_counts().to_dict()
                for gender, count in gender_counts.items():
                    table_data["gender_counts"][int(gender)] = table_data["gender_counts"].get(int(gender), 0) + int(count)
                # Birth years
                table_data["birth_years"].extend(chunk['year_of_birth'].tolist())
                
            elif table_name == "VISIT_DETAIL":
                # Track all patients
                table_data["all_patients"].update(chunk['person_id'].unique())
                # Concept distribution
                concept_counts = chunk['visit_detail_concept_id'].value_counts().to_dict()
                for concept, count in concept_counts.items():
                    table_data["concept_distributions"][int(concept)] = \
                        table_data["concept_distributions"].get(int(concept), 0) + int(count)
                # ICU patients
                icu_concept_ids = [581379, 32037]
                icu_chunk = chunk[chunk['visit_detail_concept_id'].isin(icu_concept_ids)]
                table_data["icu_patients"].update(icu_chunk['person_id'].unique())
                
            elif table_name == "DEATH":
                table_data["death_count"] += len(chunk)
                table_data["all_patients"].update(chunk['person_id'].unique())
            
            stats["chunks_processed"] += 1
            pbar.update(len(chunk))
    
    # Calculate final statistics
    # Missing percentages
    stats["missing_percentage"] = {
        col: (stats["missing_values"][col] / stats["total_records"] * 100) 
        for col in stats["columns"]
    }
    
    # Unique counts for ID columns
    stats["unique_counts"] = {col: len(unique_trackers[col]) for col in id_columns}
    
    # Date ranges (process a sample for efficiency)
    if datetime_columns:
        stats["date_ranges"] = {}
        # Read first and last chunks for date ranges
        first_chunk = pd.read_csv(file_path, nrows=1000)
        last_chunk = pd.read_csv(file_path, skiprows=range(1, max(1, total_rows - 1000)))
        
        for col in datetime_columns:
            try:
                first_dates = pd.to_datetime(first_chunk[col], errors='coerce')
                last_dates = pd.to_datetime(last_chunk[col], errors='coerce')
                all_dates = pd.concat([first_dates, last_dates])
                all_dates = all_dates.dropna()
                if len(all_dates) > 0:
                    stats["date_ranges"][col] = {
                        "min": str(all_dates.min()),
                        "max": str(all_dates.max())
                    }
            except:
                pass
    
    # Add table-specific statistics
    if table_name == "PERSON":
        stats["unique_patients"] = len(table_data["all_patients"])
        stats["gender_distribution"] = table_data["gender_counts"]
        if table_data["birth_years"]:
            stats["birth_year_range"] = {
                "min": int(min(table_data["birth_years"])),
                "max": int(max(table_data["birth_years"]))
            }
            
    elif table_name == "VISIT_DETAIL":
        icu_concept_ids = [581379, 32037]
        icu_visit_count = sum(table_data["concept_distributions"].get(cid, 0) 
                             for cid in icu_concept_ids)
        stats["icu_visits"] = {
            "total": icu_visit_count,
            "unique_patients": len(table_data["icu_patients"]),
            "concept_distribution": table_data["concept_distributions"],
            "icu_concept_ids": icu_concept_ids
        }
        
    elif table_name == "DEATH":
        stats["total_deaths"] = table_data["death_count"]
        stats["unique_patients_died"] = len(table_data["all_patients"])
    
    return stats

# Analyze each table with overall progress bar
print("\nAnalyzing tables...")
for file_path in tqdm(sorted(data_files), desc="Processing tables", 
                     unit="table", position=0, leave=True):
    table_name = file_path.stem
    
    # Process table
    stats = process_table_chunked(file_path, CHUNK_SIZE)
    
    # Store results
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
    # print(f"  - Birth year range: {person_stats['birth_year_range']['min']} - {person_stats['birth_year_range']['max']}")

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
    print(f"\nMortality Statistics:")
    print(f"  - Total deaths: {death_stats['total_deaths']:,}")
    print(f"  - Death rate: {death_stats['total_deaths'] / person_stats['unique_patients'] * 100:.1f}%")

# Save results
results_path = output_dir / "eda_results.json"
with open(results_path, 'w') as f:
    json.dump(eda_results, f, indent=2, cls=NumpyEncoder)
print(f"\nDetailed results saved to: {results_path}")

# Generate visualizations
print("\nGenerating visualizations...")

# 1. Table size distribution
plt.figure(figsize=(12, 6))
table_sizes = [(name, stats["total_records"]) for name, stats in eda_results["tables"].items()]
table_sizes.sort(key=lambda x: x[1], reverse=True)
tables, sizes = zip(*table_sizes)

plt.bar(range(len(tables)), sizes)
plt.xticks(range(len(tables)), tables, rotation=45, ha='right')
plt.ylabel('Number of Records')
plt.title('Record Count by Table')
plt.yscale('log')
plt.tight_layout()
plt.savefig(output_dir / 'table_sizes.png', dpi=300)
plt.close()

# 2. Missing data heatmap (for top tables)
top_tables = ['MEASUREMENT', 'OBSERVATION', 'DRUG_EXPOSURE', 'CONDITION_OCCURRENCE']
fig, axes = plt.subplots(2, 2, figsize=(15, 12))
axes = axes.flatten()

for i, table_name in enumerate(top_tables):
    if table_name in eda_results["tables"]:
        missing_pct = eda_results["tables"][table_name]["missing_percentage"]
        
        # Select columns with any missing data
        missing_cols = {k: v for k, v in missing_pct.items() if v > 0}
        
        if missing_cols:
            ax = axes[i]
            cols = list(missing_cols.keys())
            values = list(missing_cols.values())
            
            ax.barh(cols, values)
            ax.set_xlabel('Missing %')
            ax.set_title(f'{table_name} - Missing Data')
            ax.set_xlim(0, 100)
        else:
            axes[i].text(0.5, 0.5, f'{table_name}\nNo missing data', 
                        ha='center', va='center', transform=axes[i].transAxes)
            axes[i].set_xticks([])
            axes[i].set_yticks([])

plt.tight_layout()
plt.savefig(output_dir / 'missing_data_analysis.png', dpi=300)
plt.close()

print("EDA analysis completed!")

# Generate markdown report
report_path = output_dir / "eda_report.md"
with open(report_path, 'w') as f:
    f.write("# EDA Report - subdataset_1000\n\n")
    f.write(f"**Analysis Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    
    f.write("## Dataset Overview\n\n")
    f.write(f"- **Total Tables**: {len(eda_results['tables'])}\n")
    f.write(f"- **Total Records**: {total_records:,}\n")
    f.write(f"- **Total Patients**: {person_stats['unique_patients']:,}\n\n")
    
    f.write("## Table Summary\n\n")
    f.write("| Table | Records | Columns | Size (MB) | Chunks |\n")
    f.write("|-------|---------|---------|-----------|--------|\n")
    
    for name, stats in sorted(eda_results["tables"].items(), 
                             key=lambda x: x[1]["total_records"], reverse=True):
        f.write(f"| {name} | {stats['total_records']:,} | "
               f"{stats['total_columns']} | {stats['memory_usage_mb']:.2f} | "
               f"{stats['chunks_processed']} |\n")
    
    f.write("\n## Key Findings\n\n")
    
    if "VISIT_DETAIL" in eda_results["tables"]:
        visit_stats = eda_results["tables"]["VISIT_DETAIL"]
        if "icu_visits" in visit_stats:
            f.write("### ICU Cohort\n")
            f.write(f"- ICU visits: {visit_stats['icu_visits']['total']:,}\n")
            f.write(f"- Unique ICU patients: {visit_stats['icu_visits']['unique_patients']:,}\n")
            f.write(f"- ICU patient percentage: "
                   f"{visit_stats['icu_visits']['unique_patients'] / person_stats['unique_patients'] * 100:.1f}%\n\n")
    
    if "DEATH" in eda_results["tables"]:
        death_stats = eda_results["tables"]["DEATH"]
        f.write("### Mortality\n")
        f.write(f"- Total deaths: {death_stats['total_deaths']:,}\n")
        f.write(f"- Overall mortality rate: "
               f"{death_stats['total_deaths'] / person_stats['unique_patients'] * 100:.1f}%\n")

print(f"Report saved to: {report_path}")
#!/usr/bin/env python
"""Run EDA analysis on subdataset_1000 with progress bars and chunked reading."""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import os
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import numpy as np
import platform

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
if platform.system() == 'Windows':
    CHUNK_SIZE = 500000  # Larger chunks for Windows (better I/O performance)
else:
    CHUNK_SIZE = 100000  # Default for macOS/Linux

# Setup
base_dir = Path(__file__).parent
project_root = base_dir.parent.parent  # Go up to crisp_pipeline_code
data_dir = project_root / "data"
output_dir = project_root / "output" / "1_eda"
output_dir.mkdir(parents=True, exist_ok=True)

print("Starting EDA analysis on subdataset_1000...")
print(f"Output directory: {output_dir}")

# Start timing
start_time = time.time()

# Load all CSV files
data_files = list(data_dir.glob("*.csv"))
print(f"Found {len(data_files)} data files")
print(f"Using chunk size: {CHUNK_SIZE:,} rows")

# Store statistics
eda_results = {
    "analysis_date": datetime.now().isoformat(),
    "dataset": "subdataset_1000",
    "chunk_size": CHUNK_SIZE,
    "tables": {}
}

def get_file_row_count(file_path):
    """Get the number of rows in a CSV file with caching."""
    # Cache file path
    cache_file = data_dir / ".row_counts_cache.json"
    
    # Try to load cache
    cache = {}
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                cache = json.load(f)
        except:
            cache = {}
    
    # Check if cache is valid
    file_name = file_path.name
    file_mtime = os.path.getmtime(file_path)
    
    if file_name in cache and cache[file_name].get('mtime') == file_mtime:
        # Use cached row count
        return cache[file_name]['row_count']
    
    # Calculate row count (using fast method)
    def blocks(file, size=1024*1024):  # 1MB buffer
        while True:
            b = file.read(size)
            if not b: break
            yield b
    
    with open(file_path, "r", encoding="utf-8", errors='ignore') as f:
        row_count = sum(bl.count("\n") for bl in blocks(f)) - 1
    
    # Update cache
    cache[file_name] = {
        'row_count': row_count,
        'mtime': file_mtime
    }
    
    # Save cache
    with open(cache_file, 'w') as f:
        json.dump(cache, f, indent=2)
    
    return row_count

def process_table_chunked(file_path, chunk_size=CHUNK_SIZE):
    """Process a table in chunks and return aggregated statistics."""
    table_name = file_path.stem
    
    # Initialize time statistics
    time_stats = {
        'total': 0,
        'row_counting': 0,
        'chunk_reading': 0,
        'missing_calc': 0,
        'unique_tracking': 0,
        'date_range': 0
    }
    table_start = time.time()
    
    # Get total rows for progress bar
    t0 = time.time()
    total_rows = get_file_row_count(file_path)
    time_stats['row_counting'] = time.time() - t0
    
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
    
    # Process chunks (progress bar disabled for cleaner output)
    t0 = time.time()
    with tqdm(total=total_rows, desc=f"Reading {table_name}", 
              unit="rows", leave=False,
              disable=True,  # Disable to avoid nested progress bars
              miniters=max(1, total_rows//20) if total_rows > 0 else 1,  # Update every 5%
              mininterval=60.0,  # At least 60 seconds interval
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
            
            # Update missing values - vectorized
            t1 = time.time()
            missing_counts = chunk.isnull().sum().to_dict()
            for col, count in missing_counts.items():
                stats["missing_values"][col] += count
            time_stats['missing_calc'] += time.time() - t1
            
            # Track unique values for ID columns
            t1 = time.time()
            for col in id_columns:
                if col in chunk.columns:
                    unique_trackers[col].update(chunk[col].dropna().unique())
            time_stats['unique_tracking'] += time.time() - t1
            
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
    
    time_stats['chunk_reading'] = time.time() - t0
    
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
        t0 = time.time()
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
        time_stats['date_range'] = time.time() - t0
    
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
    
    # Add time statistics
    time_stats['total'] = time.time() - table_start
    stats["time_stats"] = time_stats
    
    return stats

# Analyze each table (simplified output)
print("\nAnalyzing tables...")
for i, file_path in enumerate(sorted(data_files), 1):
    table_name = file_path.stem
    print(f"  [{i}/{len(data_files)}] Processing {table_name}...", end="", flush=True)
    
    # Process table
    stats = process_table_chunked(file_path, CHUNK_SIZE)
    
    # Store results
    eda_results["tables"][table_name] = stats
    
    # Display time details
    if "time_stats" in stats:
        ts = stats["time_stats"]
        table_time = ts['total']  # Use the time from process_table_chunked
        print(f" Done ({table_time:.2f}s)")
        print(f"    - Row counting: {ts['row_counting']:.3f}s")
        print(f"    - Chunk reading: {ts['chunk_reading']:.3f}s")
        print(f"    - Missing calc: {ts['missing_calc']:.3f}s")
        print(f"    - Unique tracking: {ts['unique_tracking']:.3f}s")
        if ts.get('date_range', 0) > 0:
            print(f"    - Date range: {ts['date_range']:.3f}s")
    else:
        print(f" Done")

# Overall summary
print("\nOVERALL SUMMARY")
print("-"*40)

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
print("Generating visualizations...")

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

# Display total execution time
total_time = time.time() - start_time
print(f"\nTotal execution time: {total_time:.2f} seconds")

# Display performance breakdown
print("\n" + "="*50)
print("PERFORMANCE BREAKDOWN")
print("="*50)

# Sum up time spent in each category
total_row_counting = sum(s.get('time_stats', {}).get('row_counting', 0) 
                        for s in eda_results["tables"].values())
total_chunk_reading = sum(s.get('time_stats', {}).get('chunk_reading', 0) 
                         for s in eda_results["tables"].values())
total_missing_calc = sum(s.get('time_stats', {}).get('missing_calc', 0) 
                        for s in eda_results["tables"].values())
total_unique_tracking = sum(s.get('time_stats', {}).get('unique_tracking', 0) 
                           for s in eda_results["tables"].values())
total_date_range = sum(s.get('time_stats', {}).get('date_range', 0) 
                      for s in eda_results["tables"].values())

# Calculate the sum of all table processing times
total_table_time = sum(s.get('time_stats', {}).get('total', 0) 
                      for s in eda_results["tables"].values())

print(f"Row counting:     {total_row_counting:.2f}s ({total_row_counting/total_time*100:.1f}%)")
print(f"Chunk reading:    {total_chunk_reading:.2f}s ({total_chunk_reading/total_time*100:.1f}%)")
print(f"Missing calc:     {total_missing_calc:.2f}s ({total_missing_calc/total_time*100:.1f}%)")
print(f"Unique tracking:  {total_unique_tracking:.2f}s ({total_unique_tracking/total_time*100:.1f}%)")
print(f"Date range:       {total_date_range:.2f}s ({total_date_range/total_time*100:.1f}%)")

# Calculate overhead (difference between wall time and sum of table times)
overhead = total_time - total_table_time
print(f"Overhead:         {overhead:.2f}s ({overhead/total_time*100:.1f}%)")

# Verify timing consistency
print(f"\nTiming verification:")
print(f"  Wall clock time: {total_time:.2f}s")
print(f"  Sum of table times: {total_table_time:.2f}s")
print(f"  Difference (overhead): {overhead:.2f}s")

# Find slowest tables
print("\nSlowest tables:")
table_times = [(name, stats.get('time_stats', {}).get('total', 0)) 
               for name, stats in eda_results["tables"].items()]
table_times.sort(key=lambda x: x[1], reverse=True)
for name, time_taken in table_times[:3]:
    if time_taken > 0:
        print(f"  {name}: {time_taken:.2f}s")
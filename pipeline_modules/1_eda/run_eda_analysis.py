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
from concurrent.futures import ProcessPoolExecutor, as_completed

# Check pandas version for compatibility
PANDAS_VERSION = tuple(map(int, pd.__version__.split('.')[:2]))
USE_INFER_FORMAT = PANDAS_VERSION < (2, 0)

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

# Parallel processing configuration
PARALLEL_EDA = os.environ.get('PARALLEL_EDA', 'true').lower() == 'true'
MAX_WORKERS = min(os.cpu_count(), 6)  # Limit to 4 workers max
MEASUREMENT_SPLITS = int(os.environ.get('MEASUREMENT_SPLITS', '6'))  # Split MEASUREMENT table

def get_file_row_count(file_path, silent=False):
    """Get the number of rows in a CSV file with caching."""
    # Cache file path - use data directory from file path
    data_dir = file_path.parent
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
        if not silent:
            print(f"  Using cached row count for {file_name}: {cache[file_name]['row_count']:,} rows")
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

def process_table_partial(file_path, start_row, end_row, chunk_size=CHUNK_SIZE, part_suffix=""):
    """Process a partial range of a table and return aggregated statistics."""
    table_name = file_path.stem
    part_name = f"{table_name}{part_suffix}" if part_suffix else table_name
    
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
    
    # Calculate actual rows to process
    total_file_rows = get_file_row_count(file_path, silent=True)
    if end_row == -1:
        end_row = total_file_rows
    actual_rows = end_row - start_row
    
    # Initialize statistics
    stats = {
        "file_name": file_path.name,
        "part_name": part_name,
        "start_row": start_row,
        "end_row": end_row,
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
    
    # Process chunks with row range limitation
    t0 = time.time()
    rows_read = 0
    chunk_num = 0
    
    # Skip to start position if needed
    skip_rows = list(range(1, start_row + 1)) if start_row > 0 else None
    
    for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False, skiprows=skip_rows):
        # Check if we've read enough rows
        if end_row != -1 and rows_read + len(chunk) > actual_rows:
            # Truncate the last chunk
            remaining_rows = actual_rows - rows_read
            chunk = chunk.iloc[:remaining_rows]
        
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
        rows_read += len(chunk)
        chunk_num += 1
        
        # Break if we've read all required rows
        if end_row != -1 and rows_read >= actual_rows:
            break
    
    time_stats['chunk_reading'] = time.time() - t0
    
    # Calculate final statistics
    # Missing percentages
    stats["missing_percentage"] = {
        col: (stats["missing_values"][col] / stats["total_records"] * 100) 
        for col in stats["columns"]
    }
    
    # Unique counts for ID columns
    stats["unique_counts"] = {col: len(unique_trackers[col]) for col in id_columns}
    
    # Date ranges (sample from this part only)
    if datetime_columns and stats["total_records"] > 0:
        t0 = time.time()
        stats["date_ranges"] = {}
        # Read a sample from this part for date ranges
        sample_size = min(1000, stats["total_records"])
        if start_row > 0:
            sample_chunk = pd.read_csv(file_path, nrows=sample_size, skiprows=list(range(1, start_row + 1)))
        else:
            sample_chunk = pd.read_csv(file_path, nrows=sample_size)
        
        for col in datetime_columns:
            try:
                # Pandas 2.0+ automatically infers format, older versions need explicit parameter
                if USE_INFER_FORMAT:
                    dates = pd.to_datetime(sample_chunk[col], 
                                          infer_datetime_format=True,
                                          errors='coerce').dropna()
                else:
                    dates = pd.to_datetime(sample_chunk[col], 
                                          errors='coerce').dropna()
                
                if len(dates) > 0:
                    stats["date_ranges"][col] = {
                        "min": str(dates.min()),
                        "max": str(dates.max())
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
                # Pandas 2.0+ automatically infers format, older versions need explicit parameter
                if USE_INFER_FORMAT:
                    first_dates = pd.to_datetime(first_chunk[col], 
                                                infer_datetime_format=True,
                                                errors='coerce')
                    last_dates = pd.to_datetime(last_chunk[col], 
                                               infer_datetime_format=True,
                                               errors='coerce')
                else:
                    first_dates = pd.to_datetime(first_chunk[col], 
                                                errors='coerce')
                    last_dates = pd.to_datetime(last_chunk[col], 
                                               errors='coerce')
                
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

def merge_table_part_stats(part_stats_list, table_name):
    """Merge statistics from multiple table parts into a single table stats."""
    if not part_stats_list:
        return {}
    
    # Use first part as template
    merged_stats = part_stats_list[0].copy()
    merged_stats["file_name"] = f"{table_name}.csv"
    
    # Remove part-specific fields
    merged_stats.pop("part_name", None)
    merged_stats.pop("start_row", None) 
    merged_stats.pop("end_row", None)
    
    # Aggregate numeric fields
    merged_stats["total_records"] = sum(stats["total_records"] for stats in part_stats_list)
    merged_stats["memory_usage_mb"] = sum(stats["memory_usage_mb"] for stats in part_stats_list)
    merged_stats["chunks_processed"] = sum(stats["chunks_processed"] for stats in part_stats_list)
    
    # Merge missing values
    merged_missing = {}
    for col in merged_stats["columns"]:
        merged_missing[col] = sum(stats["missing_values"].get(col, 0) for stats in part_stats_list)
    merged_stats["missing_values"] = merged_missing
    
    # Recalculate missing percentages
    merged_stats["missing_percentage"] = {
        col: (merged_missing[col] / merged_stats["total_records"] * 100) 
        for col in merged_stats["columns"]
    }
    
    # Merge unique counts by combining sets (approximate)
    if "unique_counts" in merged_stats:
        # For unique counts, we take the maximum from all parts (this is an approximation)
        # In reality, there might be overlaps between parts, but this gives us a reasonable estimate
        merged_unique = {}
        for col in merged_stats["unique_counts"]:
            merged_unique[col] = max(stats["unique_counts"].get(col, 0) for stats in part_stats_list)
        merged_stats["unique_counts"] = merged_unique
    
    # Merge date ranges
    if "date_ranges" in merged_stats:
        merged_date_ranges = {}
        for col in merged_stats.get("date_ranges", {}):
            all_dates = []
            for stats in part_stats_list:
                if "date_ranges" in stats and col in stats["date_ranges"]:
                    all_dates.extend([
                        pd.to_datetime(stats["date_ranges"][col]["min"]),
                        pd.to_datetime(stats["date_ranges"][col]["max"])
                    ])
            if all_dates:
                merged_date_ranges[col] = {
                    "min": str(min(all_dates)),
                    "max": str(max(all_dates))
                }
        merged_stats["date_ranges"] = merged_date_ranges
    
    # Merge table-specific statistics
    if table_name == "PERSON":
        # Merge gender distribution
        merged_gender = {}
        for stats in part_stats_list:
            if "gender_distribution" in stats:
                for gender, count in stats["gender_distribution"].items():
                    merged_gender[gender] = merged_gender.get(gender, 0) + count
        merged_stats["gender_distribution"] = merged_gender
        
        # Merge birth year range
        all_birth_years = []
        for stats in part_stats_list:
            if "birth_year_range" in stats:
                all_birth_years.extend([stats["birth_year_range"]["min"], stats["birth_year_range"]["max"]])
        if all_birth_years:
            merged_stats["birth_year_range"] = {
                "min": min(all_birth_years),
                "max": max(all_birth_years)
            }
        
        # Note: unique_patients is already handled by unique_counts approximation
        
    elif table_name == "VISIT_DETAIL":
        # Merge concept distribution
        merged_concepts = {}
        for stats in part_stats_list:
            if "icu_visits" in stats and "concept_distribution" in stats["icu_visits"]:
                for concept, count in stats["icu_visits"]["concept_distribution"].items():
                    merged_concepts[concept] = merged_concepts.get(concept, 0) + count
        
        # Calculate ICU visits
        icu_concept_ids = [581379, 32037]
        icu_visit_total = sum(merged_concepts.get(cid, 0) for cid in icu_concept_ids)
        icu_patients_estimate = max(stats.get("icu_visits", {}).get("unique_patients", 0) for stats in part_stats_list)
        
        merged_stats["icu_visits"] = {
            "total": icu_visit_total,
            "unique_patients": icu_patients_estimate,
            "concept_distribution": merged_concepts,
            "icu_concept_ids": icu_concept_ids
        }
        
    elif table_name == "DEATH":
        merged_stats["total_deaths"] = sum(stats.get("total_deaths", 0) for stats in part_stats_list)
        merged_stats["unique_patients_died"] = max(stats.get("unique_patients_died", 0) for stats in part_stats_list)
    
    # Merge time statistics - for parallel processing, use the maximum time from all parts
    # This represents the actual wall clock time since parts run in parallel
    merged_time_stats = {
        'total': max(stats["time_stats"]["total"] for stats in part_stats_list),
        'row_counting': max(stats["time_stats"].get("row_counting", 0) for stats in part_stats_list),
        'chunk_reading': max(stats["time_stats"].get("chunk_reading", 0) for stats in part_stats_list),
        'missing_calc': max(stats["time_stats"].get("missing_calc", 0) for stats in part_stats_list),
        'unique_tracking': max(stats["time_stats"].get("unique_tracking", 0) for stats in part_stats_list),
        'date_range': max(stats["time_stats"].get("date_range", 0) for stats in part_stats_list)
    }
    merged_stats["time_stats"] = merged_time_stats
    
    return merged_stats

if __name__ == '__main__':
    # Setup
    base_dir = Path(__file__).parent.absolute()
    project_root = base_dir.parent.parent  # Go up to crisp_pipeline_code
    data_dir = project_root / "data"
    output_dir = project_root / "output" / "1_eda"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Starting EDA analysis on subdataset_1000...")
    print(f"Output directory: {output_dir}")
    print(f"Parallel processing: {'enabled' if PARALLEL_EDA else 'disabled'}")
    if PARALLEL_EDA:
        print(f"Max workers: {MAX_WORKERS}")
        print(f"MEASUREMENT splits: {MEASUREMENT_SPLITS}")

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

    # Analyze each table
    print("\nAnalyzing tables...")
    
    if PARALLEL_EDA and len(data_files) > 1:
        # Parallel processing
        
        # Calculate total tasks (accounting for MEASUREMENT splits)
        total_tasks = len(data_files)
        measurement_file = None
        for file_path in data_files:
            if file_path.stem == "MEASUREMENT":
                measurement_file = file_path
                total_tasks = total_tasks - 1 + MEASUREMENT_SPLITS  # Remove 1, add MEASUREMENT_SPLITS
                break
        
        print(f"Processing {len(data_files)} tables ({total_tasks} tasks) in parallel with {MAX_WORKERS} workers...")
        
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_info = {}
            
            for file_path in sorted(data_files):
                table_name = file_path.stem
                
                if table_name == "MEASUREMENT" and MEASUREMENT_SPLITS > 1:
                    # Split MEASUREMENT table
                    total_rows = get_file_row_count(file_path)
                    rows_per_split = total_rows // MEASUREMENT_SPLITS
                    
                    for i in range(MEASUREMENT_SPLITS):
                        start_row = i * rows_per_split
                        end_row = (i + 1) * rows_per_split if i < MEASUREMENT_SPLITS - 1 else total_rows
                        part_suffix = f"_part{i+1}"
                        
                        future = executor.submit(
                            process_table_partial, file_path, start_row, end_row, CHUNK_SIZE, part_suffix
                        )
                        future_to_info[future] = (table_name, i+1, MEASUREMENT_SPLITS)
                else:
                    # Normal table processing
                    future = executor.submit(process_table_chunked, file_path, CHUNK_SIZE)
                    future_to_info[future] = (table_name, 0, 0)
            
            # Collect results as they complete
            completed = 0
            measurement_parts = []
            
            for future in as_completed(future_to_info):
                table_name, part_num, num_parts = future_to_info[future]
                completed += 1
                
                try:
                    stats = future.result()
                    
                    if num_parts > 0:
                        # This is a MEASUREMENT part
                        measurement_parts.append(stats)
                        print(f"  [{completed}/{total_tasks}] {table_name} part {part_num}/{num_parts} completed ({stats['time_stats']['total']:.2f}s)")
                        
                        # Check if all MEASUREMENT parts are done
                        if len(measurement_parts) == MEASUREMENT_SPLITS:
                            # Merge MEASUREMENT parts
                            merged_stats = merge_table_part_stats(measurement_parts, "MEASUREMENT")
                            eda_results["tables"]["MEASUREMENT"] = merged_stats
                            print(f"  MEASUREMENT table merged from {MEASUREMENT_SPLITS} parts")
                    else:
                        # Normal table
                        eda_results["tables"][table_name] = stats
                        
                        # Display completion
                        if "time_stats" in stats:
                            table_time = stats["time_stats"]['total']
                            print(f"  [{completed}/{total_tasks}] {table_name} completed ({table_time:.2f}s)")
                        else:
                            print(f"  [{completed}/{total_tasks}] {table_name} completed")
                        
                except Exception as exc:
                    print(f"  [{completed}/{total_tasks}] {table_name} part {part_num if num_parts > 0 else ''} failed: {exc}")
    else:
        # Sequential processing (fallback)
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
                table_time = ts['total']
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

    # Save column analysis for data cleaning module
    column_analysis = {}
    for table_name, stats in eda_results["tables"].items():
        if "missing_percentage" in stats:
            columns_to_remove = []
            column_stats = {}
            
            for col, missing_pct in stats["missing_percentage"].items():
                should_remove = missing_pct > 95.0
                if should_remove:
                    columns_to_remove.append(col)
                
                column_stats[col] = {
                    "missing_percentage": missing_pct,
                    "missing_count": stats["missing_values"].get(col, 0),
                    "total_records": stats["total_records"],
                    "data_type": stats["data_types"].get(col, "unknown"),
                    "unique_count": stats.get("unique_counts", {}).get(col, -1),
                    "should_remove": should_remove
                }
            
            column_analysis[table_name] = {
                "columns_to_remove": columns_to_remove,
                "column_stats": column_stats,
                "total_columns": stats["total_columns"],
                "columns": stats["columns"]
            }

    # Save column analysis
    column_analysis_path = output_dir / "column_analysis.json"
    with open(column_analysis_path, 'w') as f:
        json.dump(column_analysis, f, indent=2)
    print(f"Column analysis saved to: {column_analysis_path}")

    # Generate visualizations
    print("Generating visualizations...")

    # 1. Table size distribution
    plt.figure(figsize=(12, 6))
    table_sizes = [(name, stats["total_records"]) for name, stats in eda_results["tables"].items()]
    if table_sizes:
        table_sizes.sort(key=lambda x: x[1], reverse=True)
        tables, sizes = zip(*table_sizes)
    else:
        tables, sizes = [], []

    if tables:
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
        if "PERSON" in eda_results["tables"]:
            person_stats = eda_results["tables"]["PERSON"]
            f.write(f"- **Total Patients**: {person_stats['unique_patients']:,}\n\n")
        else:
            f.write("\n")
        
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
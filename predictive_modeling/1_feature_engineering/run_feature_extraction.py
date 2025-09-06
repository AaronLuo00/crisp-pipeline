#!/usr/bin/env python
"""
Unified Feature Extraction for Predictive Modeling
Generates time series features for mortality/readmission/LOS and static features for sepsis
Supports both sequential and parallel processing
"""

from simple_time_series_extractor import SimpleTimeSeriesExtractor
from pathlib import Path
import json
from datetime import datetime
import argparse
import os
import sys
import time
import multiprocessing as mp
from tqdm import tqdm
from typing import Dict, Any

def main(args):
    # Initialize performance tracking
    performance_stats = {
        'start_time': time.time(),
        'phases': {},
        'details': {}
    }
    
    print("="*80)
    print("FEATURE EXTRACTION FOR PREDICTIVE MODELING")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Phase 1: Load patient labels
    print("\n1. Loading patient labels...")
    phase1_start = time.time()
    data_path = Path(args.data_dir)
    patient_labels = []
    
    for label_file in data_path.rglob('patient_labels.json'):
        with open(label_file, 'r') as f:
            labels = json.load(f)
            patient_labels.append(labels)
    
    performance_stats['phases']['data_loading'] = time.time() - phase1_start
    performance_stats['details']['total_patients'] = len(patient_labels)
    
    print(f"   Loaded {len(patient_labels)} patients")
    icu_patients = sum(1 for p in patient_labels if p.get('has_icu_admission') == 1)
    print(f"   ICU patients: {icu_patients}")
    performance_stats['details']['icu_patients'] = icu_patients
    
    # Phase 2: Initialize extractor
    print("\n2. Initializing feature extractor...")
    phase2_start = time.time()
    # Determine the base directory for configs
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    config_dir = script_dir.parent / 'config'
    
    # Choose between parallel and sequential processing
    if args.parallel:
        from parallel_feature_extractor import ParallelFeatureExtractor
        n_workers = args.n_workers if args.n_workers else max(1, mp.cpu_count() - 2)
        print(f"  Using PARALLEL processing with {n_workers} workers")
        extractor = ParallelFeatureExtractor(
            time_window=args.time_window,
            minimum_observation_hours=args.min_observation,
            concepts_path=str(config_dir / 'concepts.json'),
            n_workers=args.n_workers
        )
        performance_stats['details']['processing_mode'] = 'parallel'
        performance_stats['details']['n_workers'] = n_workers
    else:
        print("  Using SEQUENTIAL processing")
        extractor = SimpleTimeSeriesExtractor(
            time_window=args.time_window,
            minimum_observation_hours=args.min_observation,
            concepts_path=str(config_dir / 'concepts.json')
        )
        performance_stats['details']['processing_mode'] = 'sequential'
        performance_stats['details']['n_workers'] = 1
    
    performance_stats['phases']['initialization'] = time.time() - phase2_start
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("\n3. Extracting features...")
    print("-"*80)
    
    # Phase 3: Extract time series features
    phase3_start = time.time()
    
    # Extract unified time series features for mortality/readmission/los
    print("\n[TIME SERIES FEATURES] For mortality, readmission, and LOS prediction:")
    print("-"*40)
    
    # Use mortality task to get the filtered patients and features
    if args.parallel:
        result = extractor.extract_features_parallel(
            patient_labels,
            data_path,
            task='mortality',  # This applies the 24h filtering
            chunk_size=args.chunk_size
        )
        # Handle both tuple return (with timing) and single DataFrame return
        if isinstance(result, tuple):
            df_time_series, cpu_time, wall_time = result
            performance_stats['details']['ts_cpu_time'] = cpu_time
            performance_stats['details']['ts_wall_time'] = wall_time
        else:
            df_time_series = result
            performance_stats['details']['ts_cpu_time'] = 0
            performance_stats['details']['ts_wall_time'] = time.time() - phase3_start
    else:
        extraction_start = time.time()
        df_time_series = extractor.extract_features_batch(
            patient_labels,
            data_path,
            task='mortality',  # This applies the 24h filtering
            output_dir=None
        )
        extraction_time = time.time() - extraction_start
        performance_stats['details']['ts_cpu_time'] = extraction_time
        performance_stats['details']['ts_wall_time'] = extraction_time
    
    performance_stats['phases']['time_series_extraction'] = time.time() - phase3_start
    
    # Phase 4: Add labels for different tasks
    phase4_start = time.time()
    
    # Add all labels for different tasks
    # The labels were already added for mortality, now add the others
    
    # Check if dataframe is empty first
    if df_time_series.empty or 'patient_id' not in df_time_series.columns:
        print("  Warning: No time series features extracted, skipping label addition")
    else:
        print("  Adding labels for readmission and LOS tasks...")
        
        # Check if we're in pipeline mode
        is_terminal = sys.stdout.isatty() and not os.environ.get('PIPELINE_MODE')
        
        if is_terminal:
            iterator = tqdm(patient_labels, desc="  Adding labels", leave=False)
        else:
            iterator = patient_labels
            print("  Adding readmission and LOS labels...")
            
        for i, patient in enumerate(iterator):
            # Show progress in pipeline mode (every 5% or every 100 patients)
            if not is_terminal and i > 0:
                progress_interval = min(max(1, len(patient_labels) // 20), 100)  # Every 5% or every 100 patients
                if i % progress_interval == 0:
                    print(f"    Progress: {i}/{len(patient_labels)} ({i*100//len(patient_labels)}%)")
                
            patient_id = patient['patient_id']
            if patient_id in df_time_series['patient_id'].values:
                idx = df_time_series[df_time_series['patient_id'] == patient_id].index[0]
                
                # Add readmission labels
                df_time_series.loc[idx, 'readmission_7days'] = patient['readmission']['readmission_within_7days']
                df_time_series.loc[idx, 'readmission_30days'] = patient['readmission']['readmission_within_30days']
                df_time_series.loc[idx, 'readmission_90days'] = patient['readmission']['readmission_within_90days']
                
                # Add LOS labels
                df_time_series.loc[idx, 'los_greater_3days'] = patient['los']['los_greater_than_3days']
                df_time_series.loc[idx, 'los_greater_7days'] = patient['los']['los_greater_than_7days']
    
    performance_stats['phases']['label_addition'] = time.time() - phase4_start
    
    # Phase 5: Save time series features
    phase5_start = time.time()
    
    # Save unified time series features
    window_length = args.time_window if args.parallel else extractor.time_window
    time_series_file = output_dir / f'time_series_features_{window_length}h_window.csv'
    
    if not df_time_series.empty:
        # Convert numeric columns to float to match old file format (all zeros as 0.0)
        # Keep patient_id as string
        patient_id_col = df_time_series['patient_id']
        numeric_cols = df_time_series.select_dtypes(include=['int64', 'int32', 'float64', 'float32']).columns
        for col in numeric_cols:
            if col != 'patient_id':
                df_time_series[col] = df_time_series[col].astype(float)
        df_time_series['patient_id'] = patient_id_col  # Restore patient_id as string
        
        df_time_series.to_csv(time_series_file, index=False)
        
        # Get summary for time series features
        if args.parallel:
            # Create temp extractor for summary if using parallel
            temp_extractor = SimpleTimeSeriesExtractor(
                time_window=args.time_window,
                minimum_observation_hours=args.min_observation,
                concepts_path=str(config_dir / 'concepts.json'),
                verbose=False  # Suppress duplicate output
            )
            ts_summary = temp_extractor.get_feature_summary(df_time_series)
        else:
            ts_summary = extractor.get_feature_summary(df_time_series)
        print(f"  Samples: {ts_summary['n_samples']}")
        print(f"  Features: {ts_summary['n_features']}")
        print(f"  Data completeness: {ts_summary['data_completeness']:.2%}")
        print(f"  Saved to: {time_series_file}")
    else:
        print("  Warning: No time series features to save")
        # Create empty summary for report
        ts_summary = {
            'n_samples': 0,
            'n_features': 0,
            'n_time_features': 0,
            'n_static_features': 0,
            'n_windowed_features': 0,
            'data_completeness': 0,
            'features_by_table': {}
        }
    
    # Phase 6: Extract sepsis static features
    print("\n[STATIC FEATURES] For sepsis prediction:")
    print("-"*40)
    
    phase6_start = time.time()
    if args.parallel:
        # For sepsis, we still need to use the sequential extractor
        # since it's different feature extraction logic
        sepsis_extractor = SimpleTimeSeriesExtractor(
            time_window=args.time_window,
            minimum_observation_hours=args.min_observation,
            concepts_path=str(config_dir / 'concepts.json')
        )
        df_sepsis = sepsis_extractor.extract_features_batch(
            patient_labels,
            data_path,
            task='sepsis',  # No 24h filtering, uses static features only
            output_dir=None
        )
    else:
        df_sepsis = extractor.extract_features_batch(
            patient_labels,
            data_path,
            task='sepsis',  # No 24h filtering, uses static features only
            output_dir=None
        )
    
    sepsis_extraction_time = time.time() - phase6_start
    performance_stats['phases']['sepsis_extraction'] = sepsis_extraction_time
    performance_stats['details']['sepsis_cpu_time'] = sepsis_extraction_time
    performance_stats['details']['sepsis_wall_time'] = sepsis_extraction_time
    performance_stats['details']['sepsis_samples'] = len(df_sepsis)
    performance_stats['details']['sepsis_features'] = df_sepsis.shape[1] if not df_sepsis.empty else 0
    
    # Save sepsis features
    sepsis_file = output_dir / 'sepsis_static_features.csv'
    
    if not df_sepsis.empty:
        # Convert numeric columns to float to match old file format (all zeros as 0.0)
        # Keep patient_id as string
        patient_id_col = df_sepsis['patient_id']
        numeric_cols = df_sepsis.select_dtypes(include=['int64', 'int32', 'float64', 'float32']).columns
        for col in numeric_cols:
            if col != 'patient_id':
                df_sepsis[col] = df_sepsis[col].astype(float)
        df_sepsis['patient_id'] = patient_id_col  # Restore patient_id as string
        
        df_sepsis.to_csv(sepsis_file, index=False)
        
        # Get summary for sepsis features
        if args.parallel:
            # Use the sepsis_extractor created above
            sepsis_summary = sepsis_extractor.get_feature_summary(df_sepsis) if 'sepsis_extractor' in locals() else temp_extractor.get_feature_summary(df_sepsis)
        else:
            sepsis_summary = extractor.get_feature_summary(df_sepsis)
        print(f"  Samples: {sepsis_summary['n_samples']}")
        print(f"  Features: {sepsis_summary['n_features']}")
        print(f"  Data completeness: {sepsis_summary['data_completeness']:.2%}")
        print(f"  Saved to: {sepsis_file}")
    else:
        print("  Warning: No sepsis features to save")
        # Create empty summary for report
        sepsis_summary = {
            'n_samples': 0,
            'n_features': 0,
            'n_time_features': 0,
            'n_static_features': 0,
            'n_windowed_features': 0,
            'data_completeness': 0,
            'features_by_table': {}
        }
    
    performance_stats['phases']['saving_output'] = time.time() - phase5_start
    
    # Phase 7: Generate summary report
    print("\n4. Generating summary report...")
    phase7_start = time.time()
    generate_summary_report(ts_summary, sepsis_summary, output_dir, 
                          args.time_window, args.min_observation)
    performance_stats['phases']['report_generation'] = time.time() - phase7_start
    
    # Calculate total time
    performance_stats['total_time'] = time.time() - performance_stats['start_time']
    
    print("\n" + "="*80)
    print("FEATURE EXTRACTION COMPLETED")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    print(f"\nGenerated files:")
    print(f"  1. {time_series_file.name} - Unified features for mortality/readmission/LOS")
    print(f"  2. {sepsis_file.name} - Static features for sepsis prediction")
    print(f"  3. feature_extraction_summary.md - Summary report")
    
    # Print performance breakdown
    print_performance_breakdown(performance_stats)

def print_performance_breakdown(stats: Dict[str, Any]):
    """Print detailed performance breakdown similar to pipeline modules"""
    
    print("\n" + "="*60)
    print("PERFORMANCE BREAKDOWN - Feature Extraction")
    print("="*60)
    
    total_time = stats['total_time']
    phases = stats['phases']
    details = stats['details']
    
    # Overall execution
    print(f"\nTotal execution time: {total_time:.2f}s ({total_time/60:.1f} minutes)")
    
    # Processing configuration
    print(f"\nProcessing Configuration:")
    print(f"  Mode: {details.get('processing_mode', 'unknown').upper()}")
    if details.get('processing_mode') == 'parallel':
        print(f"  Workers: {details.get('n_workers', 'N/A')}")
        print(f"  CPU cores available: {mp.cpu_count()}")
    
    # Data statistics
    print(f"\nData Statistics:")
    print(f"  Total patients loaded: {details.get('total_patients', 0):,}")
    print(f"  ICU patients: {details.get('icu_patients', 0):,}")
    print(f"  Time series samples: {details.get('time_series_samples', 0):,}")
    print(f"  Time series features: {details.get('time_series_features', 0):,}")
    print(f"  Sepsis samples: {details.get('sepsis_samples', 0):,}")
    print(f"  Sepsis features: {details.get('sepsis_features', 0):,}")
    
    # Phase breakdown
    print(f"\nPhase Breakdown:")
    print(f"  {'Phase':<35} {'Time (s)':<12} {'Percentage':<10}")
    print("  " + "-"*57)
    
    phase_names = {
        'data_loading': 'Data Loading',
        'initialization': 'Extractor Initialization',
        'time_series_extraction': 'Time Series Feature Extraction',
        'label_addition': 'Label Addition',
        'sepsis_extraction': 'Sepsis Feature Extraction',
        'saving_output': 'Saving Output Files',
        'report_generation': 'Report Generation'
    }
    
    for phase_key, phase_time in phases.items():
        phase_name = phase_names.get(phase_key, phase_key)
        percentage = (phase_time / total_time * 100) if total_time > 0 else 0
        print(f"  {phase_name:<35} {phase_time:<12.2f} {percentage:<10.1f}%")
    
    # Performance metrics
    if details.get('processing_mode') == 'parallel':
        # Calculate actual speedup from CPU time vs wall time
        ts_cpu_time = details.get('ts_cpu_time', 0)
        ts_wall_time = details.get('ts_wall_time', 0)
        sepsis_cpu_time = details.get('sepsis_cpu_time', 0)
        sepsis_wall_time = details.get('sepsis_wall_time', 0)
        
        total_cpu_time = ts_cpu_time + sepsis_cpu_time
        total_wall_time = ts_wall_time + sepsis_wall_time
        
        actual_speedup = total_cpu_time / max(0.001, total_wall_time)
        
        print(f"\nParallel Processing Performance:")
        print(f"  Time Series Extraction:")
        print(f"    CPU time: {ts_cpu_time:.2f}s")
        print(f"    Wall time: {ts_wall_time:.2f}s")
        if ts_wall_time > 0 and ts_cpu_time > 0:
            print(f"    Speedup: {ts_cpu_time / ts_wall_time:.2f}x")
        elif ts_wall_time > 0:
            print(f"    Speedup: N/A (no data processed)")
        print(f"  Sepsis Extraction (sequential):")
        print(f"    Time: {sepsis_wall_time:.2f}s")
        print(f"  Total CPU time: {total_cpu_time:.2f}s")
        print(f"  Total wall time: {total_wall_time:.2f}s")
        print(f"  Overall speedup: {actual_speedup:.2f}x")
        
        extraction_time = total_wall_time
        if extraction_time > 0:
            print(f"  Throughput: {(details.get('time_series_samples', 0) + details.get('sepsis_samples', 0)) / extraction_time:.1f} samples/sec")
    else:
        extraction_time = phases.get('time_series_extraction', 0) + phases.get('sepsis_extraction', 0)
        print(f"\nSequential Processing Performance:")
        print(f"  Feature extraction time: {extraction_time:.2f}s")
        print(f"  Throughput: {details.get('time_series_samples', 0) / extraction_time if extraction_time > 0 else 0:.1f} samples/sec")
    
    # Success summary
    print(f"\nSuccess Summary:")
    total_samples = details.get('time_series_samples', 0) + details.get('sepsis_samples', 0)
    total_features = details.get('time_series_features', 0) + details.get('sepsis_features', 0)
    print(f"  Total samples generated: {total_samples:,}")
    print(f"  Total features extracted: {total_features:,}")
    print(f"  Average time per sample: {total_time / total_samples if total_samples > 0 else 0:.3f}s")
    
    print("="*60)


def generate_summary_report(ts_summary, sepsis_summary, output_dir, time_window, min_observation):
    """Generate a summary report of all extracted features"""
    
    n_windows = min_observation // time_window
    
    report_lines = [
        "# Feature Extraction Summary",
        f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "\n## Time Series Features (Unified)",
        "Used for: Mortality, Readmission, and Length-of-Stay prediction",
        "",
        "### Configuration",
        f"- Time window: {time_window} hours",
        f"- Total observation: {min_observation} hours ({n_windows} windows)",
        "- Aggregation: median",
        f"- Minimum ICU stay: {min_observation} hours",
        f"- Excluded: Patients who died within {min_observation}h",
        "",
        "### Statistics",
        f"- Samples: {ts_summary['n_samples']}",
        f"- Total features: {ts_summary['n_features']}",
        f"- Windowed features: {ts_summary['n_windowed_features']}",
        f"- Static features: {ts_summary['n_static_features']}",
        f"- Data completeness: {ts_summary['data_completeness']:.1%}",
        "",
        "### Features by table:",
    ]
    
    for table, count in ts_summary['features_by_table'].items():
        if count > 0:
            report_lines.append(f"  - {table}: {count}")
    
    report_lines.extend([
        "",
        "### Labels included:",
        "- Mortality: mortality_48h, mortality_7day, mortality_30day",
        "- Readmission: readmission_7days, readmission_30days, readmission_90days",
        "- Length of Stay: los_greater_3days, los_greater_7days",
        "",
        "## Static Features (Sepsis)",
        "Used for: Sepsis prediction",
        "",
        "### Configuration",
        "- Only demographic and pre-ICU conditions",
        "- No time windowing",
        "- Includes all ICU patients",
        "",
        "### Statistics",
        f"- Samples: {sepsis_summary['n_samples']}",
        f"- Total features: {sepsis_summary['n_features']}",
        f"- Data completeness: {sepsis_summary['data_completeness']:.1%}",
        "",
        "### Features:",
        "- Demographics: age_at_icu, gender, race, ethnicity",
        "- Pre-ICU conditions: Top 50 comorbidity indicators",
        "",
        "### Labels:",
        "- sepsis_within_24h",
        "- sepsis_within_48h",
        "- sepsis_during_icu",
        "",
        "## Feature Naming Convention",
        "",
        "**Windowed features**: `{concept_id}_t{window}`",
        "- Example: `3024171_t0` (Respiratory rate at window 0)",
        "",
        "**Static features**: `{concept_id}` or `condition_{concept_id}`",
        "- Example: `condition_320128` (Essential hypertension)",
    ])
    
    report_file = output_dir / 'feature_extraction_summary.md'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"   Summary report saved to: {report_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract features for predictive modeling')
    
    # Determine default paths based on script location
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    default_data_dir = script_dir.parent.parent / 'extracted_patient_data'
    default_output_dir = script_dir.parent / 'modeling_results' / 'features'
    
    parser.add_argument('--data-dir', type=str,
                       default=str(default_data_dir),
                       help='Directory containing patient data')
    
    parser.add_argument('--output-dir', type=str,
                       default=str(default_output_dir),
                       help='Output directory for features')
    
    parser.add_argument('--time-window', type=int,
                       choices=[2, 4, 8],
                       default=4,
                       help='Time window in hours (2, 4, or 8). Default: 4 hours.')
    
    parser.add_argument('--min-observation', type=int,
                       choices=[24, 48, 72],
                       default=24,
                       help='Minimum observation hours (24, 48, or 72). Default: 24 hours.')
    
    # Parallel processing arguments
    parser.add_argument('--no-parallel', action='store_true',
                       help='Disable parallel feature extraction (use sequential processing)')
    
    parser.add_argument('--n-workers', type=int, default=None,
                       help='Number of parallel workers (default: CPU count - 2)')
    
    parser.add_argument('--chunk-size', type=int, default=None,
                       help='Number of patients per chunk (default: auto)')
    
    args = parser.parse_args()
    
    # Set parallel flag (default is True, unless --no-parallel is specified)
    args.parallel = not args.no_parallel
    
    # Print processing mode
    if args.parallel:
        n_workers = args.n_workers if args.n_workers else max(1, mp.cpu_count() - 2)
        print(f"\nParallel processing enabled with {n_workers} workers (default)")
        print("Use --no-parallel to disable parallel processing")
    else:
        print("\nSequential processing mode (--no-parallel specified)")
    
    main(args)
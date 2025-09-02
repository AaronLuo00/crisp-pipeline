#!/usr/bin/env python
"""
Unified Feature Extraction for Predictive Modeling
Generates time series features for mortality/readmission/LOS and static features for sepsis
"""

from simple_time_series_extractor import SimpleTimeSeriesExtractor
from pathlib import Path
import json
from datetime import datetime
import argparse
import os

def main(args):
    print("="*80)
    print("FEATURE EXTRACTION FOR PREDICTIVE MODELING")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Load all patient labels
    print("\n1. Loading patient labels...")
    data_path = Path(args.data_dir)
    patient_labels = []
    
    for label_file in data_path.rglob('patient_labels.json'):
        with open(label_file, 'r') as f:
            labels = json.load(f)
            patient_labels.append(labels)
    
    print(f"   Loaded {len(patient_labels)} patients")
    icu_patients = sum(1 for p in patient_labels if p.get('has_icu_admission') == 1)
    print(f"   ICU patients: {icu_patients}")
    
    # Initialize extractor
    print("\n2. Initializing feature extractor...")
    # Determine the base directory for configs
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    config_dir = script_dir.parent / 'config'
    
    extractor = SimpleTimeSeriesExtractor(
        time_window=args.time_window,
        minimum_observation_hours=args.min_observation,
        concepts_path=str(config_dir / 'concepts.json')
    )
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("\n3. Extracting features...")
    print("-"*80)
    
    # Extract unified time series features for mortality/readmission/los
    print("\n[TIME SERIES FEATURES] For mortality, readmission, and LOS prediction:")
    print("-"*40)
    
    # Use mortality task to get the filtered patients and features
    df_time_series = extractor.extract_features_batch(
        patient_labels,
        data_path,
        task='mortality',  # This applies the 24h filtering
        output_dir=None
    )
    
    # Add all labels for different tasks
    # The labels were already added for mortality, now add the others
    for patient in patient_labels:
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
    
    # Save unified time series features
    window_length = extractor.time_window
    time_series_file = output_dir / f'time_series_features_{window_length}h_window.csv'
    df_time_series.to_csv(time_series_file, index=False)
    
    # Get summary for time series features
    ts_summary = extractor.get_feature_summary(df_time_series)
    print(f"  Samples: {ts_summary['n_samples']}")
    print(f"  Features: {ts_summary['n_features']}")
    print(f"  Data completeness: {ts_summary['data_completeness']:.2%}")
    print(f"  Saved to: {time_series_file}")
    
    # Extract sepsis static features separately
    print("\n[STATIC FEATURES] For sepsis prediction:")
    print("-"*40)
    
    df_sepsis = extractor.extract_features_batch(
        patient_labels,
        data_path,
        task='sepsis',  # No 24h filtering, uses static features only
        output_dir=None
    )
    
    # Save sepsis features
    sepsis_file = output_dir / 'sepsis_static_features.csv'
    df_sepsis.to_csv(sepsis_file, index=False)
    
    # Get summary for sepsis features
    sepsis_summary = extractor.get_feature_summary(df_sepsis)
    print(f"  Samples: {sepsis_summary['n_samples']}")
    print(f"  Features: {sepsis_summary['n_features']}")
    print(f"  Data completeness: {sepsis_summary['data_completeness']:.2%}")
    print(f"  Saved to: {sepsis_file}")
    
    # Generate summary report
    print("\n4. Generating summary report...")
    generate_summary_report(ts_summary, sepsis_summary, output_dir, 
                          extractor.time_window, extractor.minimum_observation_hours)
    
    print("\n" + "="*80)
    print("FEATURE EXTRACTION COMPLETED")
    print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    print(f"\nGenerated files:")
    print(f"  1. {time_series_file.name} - Unified features for mortality/readmission/LOS")
    print(f"  2. {sepsis_file.name} - Static features for sepsis prediction")
    print(f"  3. feature_extraction_summary.md - Summary report")

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
    with open(report_file, 'w') as f:
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
    
    args = parser.parse_args()
    main(args)
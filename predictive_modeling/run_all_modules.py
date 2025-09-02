#!/usr/bin/env python
"""
Predictive Modeling Pipeline Orchestrator
Run all modules: feature extraction, model training, and evaluation
Supports configuration-based execution
"""

import sys
import subprocess
import json
import os
from pathlib import Path
from datetime import datetime
import argparse
import time

def run_module(module_name: str, module_path: str, args: list = None) -> dict:
    """Run a single module with real-time output"""
    print(f"\n{'='*80}")
    print(f"Running: {module_name}")
    print(f"{'='*80}")
    
    start_time = time.time()
    
    # Use unbuffered Python for better real-time output
    cmd = ['python', '-u', module_path]  # -u for unbuffered output
    if args:
        cmd.extend(args)
    
    try:
        # Set environment variables for better output control
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'  # Unbuffered output
        env['PIPELINE_MODE'] = '1'     # Tell child processes they're in pipeline mode
        
        # Use Popen for real-time output streaming
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Combine stderr with stdout
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True,
            encoding='utf-8',  # Add UTF-8 encoding for Windows compatibility
            env=env  # Pass environment variables
        )
        
        # Stream output in real-time
        output_lines = []
        for line in process.stdout:
            print(line, end='', flush=True)  # Print in real-time with flush
            output_lines.append(line)
        
        # Wait for process to complete
        return_code = process.wait()
        duration = time.time() - start_time
        
        if return_code == 0:
            return {
                'status': 'success',
                'duration': duration,
                'module': module_name,
                'output': ''.join(output_lines)
            }
        else:
            return {
                'status': 'failed',
                'duration': duration,
                'module': module_name,
                'error': f"Process exited with code {return_code}",
                'output': ''.join(output_lines)
            }
    
    except Exception as e:
        duration = time.time() - start_time
        
        print(f"ERROR in {module_name}: {str(e)}")
        
        return {
            'status': 'failed',
            'duration': duration,
            'module': module_name,
            'error': str(e)
        }


def check_dependencies():
    """Check if required directories and files exist"""
    # Get the script's directory to build correct paths
    script_dir = Path(__file__).parent
    
    required_dirs = [
        Path('extracted_patient_data'),  # Changed from ../extracted_patient_data
        script_dir / '1_feature_engineering',
        script_dir / '2_model_training',
        script_dir / '3_evaluation'
    ]
    
    missing = []
    for dir_path in required_dirs:
        if not dir_path.exists():
            missing.append(str(dir_path))
    
    if missing:
        print("ERROR: Missing required directories:")
        for m in missing:
            print(f"  - {m}")
        return False
    
    # Check for patient data
    patient_data_dir = Path('extracted_patient_data')
    if patient_data_dir.exists():
        patient_files = list(patient_data_dir.rglob('patient_labels.json'))
        if not patient_files:
            print("ERROR: No patient_labels.json files found in extracted_patient_data/")
            return False
        print(f"Found {len(patient_files)} patients with labels")
    else:
        print("WARNING: extracted_patient_data directory not found, checking for existing features...")
        # Check if features already exist
        features_dir = Path('predictive_modeling/modeling_results/features')
        if features_dir.exists() and list(features_dir.glob('*.csv')):
            print(f"Found existing features in {features_dir}")
        else:
            print("ERROR: No patient data or existing features found")
            return False
    return True

def generate_report(results: list, output_dir: Path):
    """Generate pipeline execution report"""
    report_lines = [
        "# Predictive Modeling Pipeline Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Execution Summary",
        ""
    ]
    
    total_duration = sum(r['duration'] for r in results)
    successful = sum(1 for r in results if r['status'] == 'success')
    
    report_lines.extend([
        f"- Total modules: {len(results)}",
        f"- Successful: {successful}/{len(results)}",
        f"- Total duration: {total_duration:.1f} seconds",
        "",
        "## Module Results",
        ""
    ])
    
    for result in results:
        status_icon = "[SUCCESS]" if result['status'] == 'success' else "[FAILED]"
        report_lines.extend([
            f"### {result['module']} {status_icon}",
            f"- Status: {result['status']}",
            f"- Duration: {result['duration']:.1f} seconds"
        ])
        
        if result['status'] == 'failed' and 'error' in result:
            report_lines.append(f"- Error: {result['error']}")
        
        report_lines.append("")
    
    # Add performance summary if evaluation completed
    eval_results_file = output_dir / 'evaluation' / 'evaluation_results.json'
    if eval_results_file.exists():
        with open(eval_results_file, 'r') as f:
            eval_results = json.load(f)
        
        report_lines.extend([
            "## Model Performance Summary",
            ""
        ])
        
        for task_name, task_results in eval_results.items():
            if task_results:
                aurocs = []
                for target, result in task_results.items():
                    if result and 'test_performance' in result:
                        aurocs.append(result['test_performance']['auroc'])
                
                if aurocs:
                    report_lines.extend([
                        f"### {task_name.upper()}",
                        f"- Average AUROC: {sum(aurocs)/len(aurocs):.3f}",
                        f"- Models evaluated: {len(aurocs)}",
                        ""
                    ])
    
    report_lines.extend([
        "## Output Structure",
        "",
        "```",
        "modeling_results/",
        "+-- features/           # Extracted features for each task",
        "+-- models/            # Trained models and results",
        "+-- evaluation/        # Evaluation metrics and plots",
        "|   +-- plots/        # ROC, PR curves",
        "|   +-- *_report.md   # Performance reports",
        "+-- pipeline_report.md # This report",
        "```"
    ])
    
    # Save report
    report_file = output_dir / 'pipeline_report.md'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"\nPipeline report saved: {report_file}")

def main(args):
    """Main pipeline orchestrator"""
    print("="*80)
    print("PREDICTIVE MODELING PIPELINE")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    # Configuration via command-line args only
    
    # Check dependencies
    print("\nChecking dependencies...")
    if not check_dependencies():
        print("\nPipeline aborted due to missing dependencies")
        return 1
    
    # Setup output directory
    output_dir = Path('predictive_modeling/modeling_results')
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # Define modules to run with correct paths
    script_dir = Path(__file__).parent
    modules = [
        {
            'name': 'Feature Extraction',
            'path': str(script_dir / '1_feature_engineering' / 'run_feature_extraction.py'),
            'args': ['--output-dir', 'predictive_modeling/modeling_results/features']
        },
        {
            'name': 'Model Training (Traditional)',
            'path': str(script_dir / '2_model_training' / 'run_traditional_models.py'),
            'args': ['--input-dir', 'predictive_modeling/modeling_results/features', 
                    '--output-dir', 'predictive_modeling/modeling_results/models/traditional']
        }
    ]
    
    # Add time window parameter if provided
    if args.time_window:
        for module in modules:
            if 'Feature Extraction' in module['name'] or 'Model Training' in module['name']:
                module['args'].extend(['--time-window', str(args.time_window)])
    
    # Add deep learning module if requested
    if args.include_dl:
        dl_module = {
            'name': 'Model Training (Deep Learning)',
            'path': str(script_dir / '2_model_training' / 'run_DL_models.py'),
            'args': ['--input-dir', 'predictive_modeling/modeling_results/features', 
                    '--output-dir', 'predictive_modeling/modeling_results/models/deep_learning']
        }
        modules.append(dl_module)
    
    # Add evaluation module
    eval_module = {
        'name': 'Model Evaluation',
        'path': str(script_dir / '3_evaluation' / 'run_evaluation.py'),
        'args': ['--model-dir', 'predictive_modeling/modeling_results/models', 
                '--feature-dir', 'predictive_modeling/modeling_results/features',
                '--output-dir', 'predictive_modeling/modeling_results/evaluation']
    }
    if args.time_window:
        eval_module['args'].extend(['--time-window', str(args.time_window)])
    modules.append(eval_module)
    
    # Skip modules if requested
    if args.skip_modules:
        skip_names = args.skip_modules.split(',')
        modules = [m for m in modules if not any(skip in m['name'].lower() 
                                                 for skip in skip_names)]
        print(f"\nSkipping modules: {skip_names}")
    
    # Start from specific module if requested
    if args.start_from:
        start_idx = None
        for i, m in enumerate(modules):
            if args.start_from.lower() in m['name'].lower():
                start_idx = i
                break
        
        if start_idx is not None:
            modules = modules[start_idx:]
            print(f"\nStarting from: {modules[0]['name']}")
    
    # Run modules
    results = []
    
    for module in modules:
        result = run_module(module['name'], module['path'], module.get('args'))
        results.append(result)
        
        # Stop on failure if requested
        if result['status'] == 'failed' and not args.continue_on_error:
            print(f"\nPipeline stopped due to failure in {module['name']}")
            break
    
    # Generate report
    generate_report(results, output_dir)
    
    # Print summary
    print("\n" + "="*80)
    print("PIPELINE SUMMARY")
    print("="*80)
    
    successful = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] == 'failed')
    
    print(f"\nModules executed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    if failed == 0:
        print("\n[SUCCESS] Pipeline completed successfully!")
        
        # Show key outputs
        print("\nKey outputs:")
        print("  - Features: predictive_modeling/modeling_results/features/")
        print("  - Models: predictive_modeling/modeling_results/models/")
        print("  - Evaluation: predictive_modeling/modeling_results/evaluation/")
        print("  - Reports: predictive_modeling/modeling_results/evaluation/evaluation_report.md")
        print("  - Plots: predictive_modeling/modeling_results/evaluation/plots/")
        
        return 0
    else:
        print(f"\n[WARNING] Pipeline completed with {failed} failures")
        return 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run predictive modeling pipeline'
    )
    parser.add_argument('--time-window', type=int,
                       choices=[2, 4, 8],
                       default=4,
                       help='Time window in hours for feature extraction (2, 4, or 8)')
    parser.add_argument('--include-dl', action='store_true',
                       help='Include deep learning models in training')
    parser.add_argument('--skip-modules', type=str,
                       help='Comma-separated list of modules to skip')
    parser.add_argument('--start-from', type=str,
                       help='Start from specific module')
    parser.add_argument('--continue-on-error', action='store_true',
                       help='Continue pipeline even if a module fails')
    
    args = parser.parse_args()
    sys.exit(main(args))
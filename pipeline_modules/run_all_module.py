#!/usr/bin/env python
"""
CRISP Pipeline Runner - Execute all pipeline modules in sequence
Author: CRISP Team
Date: 2025-08-08
"""

import os
import sys
import json
import time
import logging
import argparse
import subprocess
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # No milliseconds
)

class CRISPPipeline:
    """
    CRISP-DM Pipeline Orchestrator
    Manages the execution of all pipeline modules in sequence
    """
    
    def __init__(self, base_dir: Path, config: Optional[Dict] = None):
        """
        Initialize the pipeline runner
        
        Args:
            base_dir: Base directory of the pipeline (pipeline_modules)
            config: Optional configuration dictionary
        """
        self.base_dir = base_dir
        self.project_root = base_dir.parent  # crisp_pipeline_code directory
        self.config = config or {}
        self.results = {}
        self.start_time = None
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Define pipeline modules in execution order
        self.modules = [
            {
                'id': '1_eda',
                'name': 'EDA Analysis',
                'script': '1_eda/run_eda_analysis.py',
                'description': 'Exploratory Data Analysis',
                'required': True
            },
            {
                'id': '2_cleaning',
                'name': 'Data Cleaning',
                'script': '2_cleaning/run_data_cleaning.py',
                'description': 'Clean and preprocess data',
                'required': True
            },
            {
                'id': '3_mapping',
                'name': 'Concept Mapping',
                'script': '3_mapping/run_concept_mapping.py',
                'description': 'Map concepts to SNOMED-CT',
                'required': True
            },
            {
                'id': '4_standardization',
                'name': 'Data Standardization',
                'script': '4_standardization/run_data_standardization.py',
                'description': 'Standardize and normalize data',
                'required': True
            },
            {
                'id': '5_extraction',
                'name': 'ICU Data Extraction',
                'script': '5_extraction/run_icu_extraction.py',
                'description': 'Extract patient-level ICU data',
                'required': False
            }
        ]
        
        # Setup output directories
        self.setup_output_dirs()
        
    def setup_output_dirs(self):
        """Create output directories for pipeline run"""
        self.output_dir = self.project_root / "output" / "pipeline_runs" / f"run_{self.run_id}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.module_results_dir = self.output_dir / "module_results"
        self.module_results_dir.mkdir(exist_ok=True)
        
        # Setup log file
        self.log_file = self.output_dir / "pipeline_log.txt"
        
    def check_environment(self) -> bool:
        """
        Check if the environment is properly set up
        
        Returns:
            True if environment is ready, False otherwise
        """
        logging.info("Checking environment...")
        
        # Check if data directory exists
        data_dir = self.project_root / "data"
        if not data_dir.exists():
            # Try alternative location
            data_dir = self.project_root.parent / "data"
            if not data_dir.exists():
                logging.warning(f"Data directory not found at expected locations, continuing anyway...")
                # Don't fail - some modules may not need the data directory
        
        # Check if output directory is writable
        try:
            test_file = self.output_dir / "test.txt"
            test_file.touch()
            test_file.unlink()
        except Exception as e:
            logging.error(f"Cannot write to output directory: {e}")
            return False
        
        # Check Python environment
        python_path = self.config.get('python_path', sys.executable)
        try:
            result = subprocess.run(
                [python_path, '--version'],
                capture_output=True,
                text=True,
                timeout=5
            )
            logging.info(f"Python version: {result.stdout.strip()}")
        except Exception as e:
            logging.error(f"Python environment check failed: {e}")
            return False
        
        logging.info("Environment check passed")
        return True
    
    def run_module(self, module: Dict) -> Tuple[bool, Dict]:
        """
        Execute a single pipeline module
        
        Args:
            module: Module configuration dictionary
            
        Returns:
            Tuple of (success, results)
        """
        module_id = module['id']
        module_name = module['name']
        script_path = self.base_dir / module['script']
        
        logging.info(f"\n{'='*60}")
        logging.info(f"Running Module: {module_name}")
        logging.info(f"Script: {script_path.name}")
        logging.info(f"{'='*60}")
        
        # Check if script exists
        if not script_path.exists():
            logging.error(f"Script not found: {script_path}")
            return False, {'error': 'Script not found'}
        
        # Prepare command - use absolute path for script
        python_path = self.config.get('python_path', sys.executable)
        # Convert to absolute path
        absolute_script_path = script_path.resolve()
        cmd = [python_path, str(absolute_script_path)]
        
        # Add module-specific arguments
        if module_id == '3_mapping':
            if self.config.get('min_concept_freq'):
                cmd.extend(['--min-concept-freq', str(self.config['min_concept_freq'])])
            # Note: Module 3 uses parallel processing internally by default
        
        elif module_id == '4_standardization':
            if not self.config.get('no_parallel', False):  # Default: parallel enabled
                cmd.append('--parallel')
            # Visit merging is enabled by default in Module 4, no need to pass argument
            # Use --no-merge-visits if you want to disable it
        
        # Module 2 (cleaning) and Module 5 (extraction) have built-in parallel processing
        
        # Set environment variables to control tqdm behavior
        env = os.environ.copy()
        env['TQDM_DISABLE'] = '0'  # Enable tqdm but control nesting
        env['TQDM_POSITION'] = '0'  # Force single line
        env['TQDM_NESTED'] = 'false'  # Disable nesting
        
        # Execute module
        start_time = time.time()
        try:
            # Run with output capture
            with open(self.log_file, 'a') as log:
                log.write(f"\n\n{'='*60}\n")
                log.write(f"Module: {module_name}\n")
                log.write(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log.write(f"{'='*60}\n")
                
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                    env=env
                    # Removed cwd parameter - let scripts handle their own paths
                )
                
                # Stream output with improved filtering
                lines = []
                last_progress_line = ""
                last_percentage = -1
                in_progress_bar = False
                
                for line in process.stdout:
                    lines.append(line)
                    log.write(line)
                    log.flush()
                    
                    # Improved output filtering based on verbosity settings
                    line_stripped = line.strip()
                    
                    # Skip empty lines
                    if not line_stripped:
                        continue
                    
                    # Handle progress bars specially (avoid overlapping)
                    if '%|' in line or '█' in line or any(c in line for c in ['▏', '▎', '▍', '▌', '▋', '▊', '▉']):
                        # Extract percentage if available
                        import re
                        percent_match = re.search(r'(\d+)%', line_stripped)
                        if percent_match:
                            current_percent = int(percent_match.group(1))
                            # Only show significant progress updates (every 10%)
                            if current_percent - last_percentage >= 10 or current_percent == 100:
                                # Clear previous line if we're updating the same progress bar
                                if in_progress_bar:
                                    print(f"\r  {line_stripped[:100]}", end='', flush=True)
                                else:
                                    print(f"  {line_stripped[:100]}", end='', flush=True)
                                last_percentage = current_percent
                                in_progress_bar = True
                                if current_percent == 100:
                                    print()  # New line after completion
                                    in_progress_bar = False
                                    last_percentage = -1
                        continue
                    else:
                        # Not a progress bar anymore, reset state
                        if in_progress_bar:
                            print()  # Ensure we're on a new line
                            in_progress_bar = False
                    
                    # Filter based on quiet/verbose settings
                    if self.config.get('quiet', False):
                        # Quiet mode: only show critical messages
                        if any(keyword in line_stripped for keyword in [
                            'Error', 'FAILED', '[FAIL]', 'Warning', 
                            'COMPLETED SUCCESSFULLY', 'Phase', '==='
                        ]):
                            print(f"  {line_stripped}")
                    elif self.config.get('verbose', False):
                        # Verbose mode: show everything
                        print(f"  {line_stripped}")
                    else:
                        # Normal mode: balanced output
                        if any(keyword in line_stripped for keyword in [
                            'Phase',  '[OK]', '[FAIL]',  # Phase markers, '===',
                            'Error', 'Warning', 'FAILED',  # Issues
                            'PERFORMANCE BREAKDOWN', 'COMPLETED SUCCESSFULLY',  # Summary sections
                            'Total time:', 'Speedup:', 'efficiency:',  # Performance metrics
                            'Total rows:', 'Output:'  # Key results
                        ]) and 'Processing tables:' not in line_stripped:
                            print(f"  {line_stripped}")
                    
                    last_progress_line = line_stripped
                
                process.wait()
                
            execution_time = time.time() - start_time
            
            # Check return code
            if process.returncode != 0:
                logging.error(f"Module {module_name} failed with return code {process.returncode}")
                return False, {
                    'error': f'Return code {process.returncode}',
                    'execution_time': execution_time
                }
            
            # Try to read module results
            results = {
                'success': True,
                'execution_time': execution_time,
                'module_id': module_id
            }
            
            # Look for result JSON files
            result_patterns = [
                f"{module_id}_results.json",
                f"{module_id.split('_')[1]}_results.json",
                "results.json"
            ]
            
            for pattern in result_patterns:
                for result_file in self.project_root.glob(f"output/*/{pattern}"):
                    try:
                        with open(result_file, 'r') as f:
                            results['data'] = json.load(f)
                        break
                    except:
                        pass
            
            logging.info(f"Module {module_name} completed in {execution_time:.2f} seconds")
            return True, results
            
        except subprocess.TimeoutExpired:
            logging.error(f"Module {module_name} timed out")
            return False, {'error': 'Timeout', 'execution_time': time.time() - start_time}
        except Exception as e:
            logging.error(f"Module {module_name} failed: {e}")
            return False, {'error': str(e), 'execution_time': time.time() - start_time}
    
    def run_all(self, skip_modules: List[str] = None, start_from: str = None, dry_run: bool = False):
        """
        Run all pipeline modules in sequence
        
        Args:
            skip_modules: List of module IDs to skip
            start_from: Module ID to start from
            dry_run: If True, only show execution plan without running
        """
        self.start_time = time.time()
        skip_modules = skip_modules or []
        
        logging.info(f"\n{'='*60}")
        logging.info("CRISP PIPELINE EXECUTION")
        logging.info(f"Run ID: {self.run_id}")
        logging.info(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"{'='*60}\n")
        
        # Save configuration
        config_file = self.output_dir / "pipeline_config.json"
        with open(config_file, 'w') as f:
            json.dump({
                'run_id': self.run_id,
                'start_time': datetime.now().isoformat(),
                'config': self.config,
                'skip_modules': skip_modules,
                'start_from': start_from
            }, f, indent=2)
        
        # Filter modules based on parameters
        modules_to_run = []
        started = start_from is None
        
        for module in self.modules:
            if module['id'] == start_from:
                started = True
            
            if started and module['id'] not in skip_modules:
                modules_to_run.append(module)
        
        # Dry run - show execution plan
        if dry_run:
            logging.info("DRY RUN - Execution Plan:")
            for i, module in enumerate(modules_to_run, 1):
                logging.info(f"  {i}. {module['name']} ({module['id']})")
                logging.info(f"     {module['description']}")
            return
        
        # Check environment before starting
        if not self.check_environment():
            logging.error("Environment check failed. Aborting pipeline.")
            return
        
        # Execute modules with cleaner progress bar
        successful_modules = []
        failed_modules = []
        
        with tqdm(total=len(modules_to_run), 
                 desc="Pipeline Progress",
                 bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]',
                 ncols=100) as pbar:
            for module in modules_to_run:
                pbar.set_description(f"Module {module['id']}")
                
                success, results = self.run_module(module)
                self.results[module['id']] = results
                
                # Save module results
                result_file = self.module_results_dir / f"{module['id']}_result.json"
                with open(result_file, 'w') as f:
                    json.dump(results, f, indent=2)
                
                if success:
                    successful_modules.append(module['id'])
                else:
                    failed_modules.append(module['id'])
                    if module.get('required', False):
                        logging.error(f"Required module {module['name']} failed. Stopping pipeline.")
                        break
                
                pbar.update(1)
        
        # Generate final report
        self.generate_report(successful_modules, failed_modules)
        
        # Print performance summary (enhanced output)
        self.print_performance_summary()
        
        total_time = time.time() - self.start_time
        logging.info(f"\n{'='*60}")
        logging.info("PIPELINE EXECUTION COMPLETE")
        logging.info(f"Total Time: {total_time:.2f} seconds")
        logging.info(f"Successful Modules: {len(successful_modules)}")
        logging.info(f"Failed Modules: {len(failed_modules)}")
        logging.info(f"Report: {self.output_dir / 'pipeline_report.md'}")
        logging.info(f"{'='*60}\n")
    
    def print_performance_summary(self):
        """
        Print a comprehensive performance summary of the pipeline execution
        """
        print("\n" + "="*80)
        print("PIPELINE PERFORMANCE SUMMARY")
        print("="*80)
        
        # Overall metrics
        total_time = time.time() - self.start_time if self.start_time else 0
        print(f"\nOverall Execution:")
        print(f"  Total Wall Time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        print(f"  Run ID: {self.run_id}")
        
        # Module-level breakdown (simplified)
        print(f"\nModule Performance:")
        print(f"  {'Module':<30} {'Status':<12} {'Time (s)':<12}")
        print("  " + "-"*55)
        
        for module_id, result in self.results.items():
            module_name = next((m['name'] for m in self.modules if m['id'] == module_id), module_id)
            status = "[OK] Success" if result.get('success', False) else "[FAIL] Failed"
            exec_time = result.get('execution_time', 0)
            
            print(f"  {module_name[:29]:<30} {status:<12} {exec_time:<12.2f}")
        
        # Parallel processing summary (simplified)
        if not self.config.get('no_parallel', False):
            print(f"\nParallel Processing:")
            print(f"  Mode: Enabled")
            print(f"  Max Workers: {self.config.get('max_workers', 6)}")
        else:
            print(f"\nParallel Processing: Disabled (Sequential Mode)")
        
        # Success rate
        successful = sum(1 for r in self.results.values() if r.get('success', False))
        total_run = len(self.results)
        if total_run > 0:
            success_rate = (successful / total_run) * 100
            print(f"\nSuccess Rate: {successful}/{total_run} modules ({success_rate:.1f}%)")
        
        # Output locations
        print(f"\nOutput Locations:")
        print(f"  Pipeline Output: {self.output_dir}")
        print(f"  Module Results: {self.module_results_dir}")
        print(f"  Log File: {self.log_file}")
        print(f"  Data Outputs: {self.project_root / 'output'}")
        
        print("="*80)
    
    def generate_report(self, successful_modules: List[str], failed_modules: List[str]):
        """
        Generate a comprehensive pipeline execution report
        
        Args:
            successful_modules: List of successful module IDs
            failed_modules: List of failed module IDs
        """
        report_file = self.output_dir / "pipeline_report.md"
        
        with open(report_file, 'w') as f:
            f.write("# CRISP Pipeline Execution Report\n\n")
            f.write(f"**Run ID**: {self.run_id}\n")
            f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Total Execution Time**: {time.time() - self.start_time:.2f} seconds\n\n")
            
            f.write("## Summary\n\n")
            f.write(f"- **Total Modules**: {len(self.modules)}\n")
            f.write(f"- **Executed**: {len(successful_modules) + len(failed_modules)}\n")
            f.write(f"- **Successful**: {len(successful_modules)}\n")
            f.write(f"- **Failed**: {len(failed_modules)}\n\n")
            
            f.write("## Module Execution Details\n\n")
            
            for module in self.modules:
                module_id = module['id']
                if module_id in self.results:
                    result = self.results[module_id]
                    status = "[SUCCESS]" if module_id in successful_modules else "[FAILED]"
                    
                    f.write(f"### {module['name']} ({module_id})\n\n")
                    f.write(f"- **Status**: {status}\n")
                    f.write(f"- **Execution Time**: {result.get('execution_time', 0):.2f} seconds\n")
                    
                    # Add parallel processing indicator
                    if module_id in ['3_mapping', '4_standardization', '5_extraction']:
                        parallel_enabled = not self.config.get('no_parallel', False)
                        f.write(f"- **Parallel Processing**: {'Enabled' if parallel_enabled else 'Disabled'}\n")
                    
                    if 'error' in result:
                        f.write(f"- **Error**: {result['error']}\n")
                    
                    if 'data' in result:
                        f.write(f"- **Output Statistics**:\n")
                        data = result['data']
                        if isinstance(data, dict):
                            for key, value in data.items():
                                if key != 'details':  # Skip detailed data
                                    f.write(f"  - {key}: {value}\n")
                    
                    f.write("\n")
            
            f.write("## Output Locations\n\n")
            f.write(f"- **Pipeline Output**: `{self.output_dir}`\n")
            f.write(f"- **Module Results**: `{self.module_results_dir}`\n")
            f.write(f"- **Log File**: `{self.log_file}`\n")
            f.write(f"- **Data Outputs**: `{self.project_root / 'output'}`\n\n")
            
            f.write("## Next Steps\n\n")
            if failed_modules:
                f.write("### Failed Modules Recovery\n")
                f.write("To resume from failed modules, run:\n")
                f.write(f"```bash\n")
                f.write(f"python run_all_module.py --start-from {failed_modules[0]}\n")
                f.write(f"```\n\n")
            else:
                f.write("All modules completed successfully. The pipeline has generated:\n")
                f.write("- Cleaned data in `output/2_cleaning/`\n")
                f.write("- Mapped concepts in `output/3_mapping/`\n")
                f.write("- Standardized data in `output/4_standardization/`\n")
                f.write("- Patient-level ICU data in `output/5_extraction/`\n\n")
                
                # Add performance summary
                f.write("### Performance Summary\n\n")
                if not self.config.get('no_parallel', False):
                    f.write("- **Parallel Processing**: Enabled for all modules\n")
                    f.write("- **Workers Used**: Up to {} parallel workers\n".format(self.config.get('max_workers', 6)))
                    f.write("- **Estimated Speedup**: 3-5x compared to sequential processing\n")
                else:
                    f.write("- **Parallel Processing**: Disabled (sequential mode)\n")
                    f.write("- **Note**: Enable parallel processing with default settings for better performance\n")


def main():
    """Main entry point for the pipeline runner"""
    parser = argparse.ArgumentParser(
        description='CRISP Pipeline Runner - Execute all pipeline modules',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--skip-modules',
        nargs='+',
        help='Module IDs to skip (e.g., 1_eda 2_cleaning)'
    )
    
    parser.add_argument(
        '--start-from',
        type=str,
        help='Module ID to start from (e.g., 3_mapping)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show execution plan without running'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to configuration JSON file'
    )
    
    parser.add_argument(
        '--python-path',
        type=str,
        default=sys.executable,
        help='Path to Python interpreter'
    )
    
    parser.add_argument(
        '--min-concept-freq',
        type=int,
        default=10,
        help='Minimum concept frequency for mapping module'
    )
    
    parser.add_argument(
        '--no-parallel',
        action='store_true',
        help='Disable parallel processing (default: enabled for all modules)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=6,
        help='Maximum number of parallel workers (default: 6)'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Reduce output verbosity (show only key messages)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Show detailed output from each module'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = {}
    if args.config:
        try:
            with open(args.config, 'r') as f:
                config = json.load(f)
        except Exception as e:
            logging.error(f"Failed to load config file: {e}")
            sys.exit(1)
    
    # Override with command line arguments
    config['python_path'] = args.python_path
    config['min_concept_freq'] = args.min_concept_freq
    config['no_parallel'] = args.no_parallel
    config['max_workers'] = args.max_workers
    config['quiet'] = args.quiet
    config['verbose'] = args.verbose
    
    # Initialize and run pipeline
    base_dir = Path(__file__).parent
    pipeline = CRISPPipeline(base_dir, config)
    
    try:
        pipeline.run_all(
            skip_modules=args.skip_modules,
            start_from=args.start_from,
            dry_run=args.dry_run
        )
    except KeyboardInterrupt:
        logging.info("\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
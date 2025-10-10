#!/usr/bin/env python
"""
Traditional Model Training Module
Train traditional ML models (Logistic Regression, Random Forest, XGBoost, etc.) 
for each prediction task using extracted features
"""

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import json
import pickle
from pathlib import Path
from datetime import datetime
import argparse
from typing import Dict, List, Any

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, average_precision_score
from imblearn.over_sampling import SMOTE

def load_features(task_name: str, input_dir: Path, time_window: int = 4) -> pd.DataFrame:
    """Load features for a specific task"""
    
    # Determine which feature file to use
    if task_name == 'sepsis':
        feature_file = input_dir / 'sepsis_static_features.csv'
    else:
        # For mortality, readmission, los tasks
        feature_file = input_dir / f'time_series_features_{time_window}h_window.csv'
    
    if not feature_file.exists():
        raise FileNotFoundError(f"Feature file not found: {feature_file}")
    
    df = pd.read_csv(feature_file)
    print(f"  Loaded {len(df)} samples with {len(df.columns)-1} features from {feature_file.name}")
    return df

def prepare_data(df: pd.DataFrame, target_col: str, test_size: float = 0.2,
                 use_smote: bool = False, smote_threshold: float = 0.3,
                 random_state: int = 42, loso_test_site: str = None) -> tuple:
    """Prepare data for training"""
    # Check if target column exists
    if target_col not in df.columns:
        return None, None, None, None, None, None
    
    # Separate features and target
    feature_cols = [col for col in df.columns if col not in ['patient_id', target_col] 
                   and not col.startswith('mortality_') 
                   and not col.startswith('readmission_')
                   and not col.startswith('los_')
                   and not col.startswith('has_sepsis')
                   and not col.startswith('sepsis_within')]
    
    X = df[feature_cols]
    y = df[target_col]

    # Handle missing values
    X = X.fillna(X.median())

    # Split data
    if loso_test_site is not None:
        # LOSO: site-based split
        site = df['patient_id'].astype(str).str[0]
        test_mask = (site == str(loso_test_site))
        X_train, X_test = X[~test_mask], X[test_mask]
        y_train, y_test = y[~test_mask], y[test_mask]

        train_sites = sorted(set(site[~test_mask].unique()))
        print(f"  LOSO split: Train on sites {train_sites}, Test on site {loso_test_site}")
        print(f"  Train: {len(X_train)} samples, Test: {len(X_test)} samples")
    else:
        # Original: random split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state, stratify=y
        )
    
    # Apply SMOTE if requested and imbalanced
    if use_smote and y_train.mean() < smote_threshold:
        print(f"  Applying SMOTE (positive rate: {y_train.mean():.2%})")
        smote = SMOTE(random_state=random_state)
        X_train, y_train = smote.fit_resample(X_train, y_train)
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    return X_train_scaled, X_test_scaled, y_train, y_test, scaler, feature_cols

def get_models(task_type: str, model_types: List[str]) -> Dict[str, Any]:
    """Get models based on task type and requested models"""
    models = {}
    
    # Initialize requested models
    if 'LogisticRegression' in model_types:
        models['LogisticRegression'] = LogisticRegression(
            max_iter=1000,
            class_weight='balanced',
            random_state=42,
            solver='lbfgs',
            C=1.0
        )
    
    if 'RandomForest' in model_types:
        models['RandomForest'] = RandomForestClassifier(
            n_estimators=100,
            max_depth=10 if 'sepsis' not in task_type else 8,
            min_samples_split=5,
            class_weight='balanced',
            random_state=42,
            n_jobs=-1
        )
    
    if 'GradientBoosting' in model_types:
        models['GradientBoosting'] = GradientBoostingClassifier(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42
        )
    
    if 'XGBoost' in model_types:
        models['XGBoost'] = XGBClassifier(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42,
            use_label_encoder=False,
            eval_metric='logloss',
            verbosity=0,
            scale_pos_weight=10 if 'sepsis' in task_type else 1
        )
    
    return models

def train_models(X_train, X_test, y_train, y_test, models: Dict, cv_folds: int = 5) -> Dict:
    """Train and evaluate models"""
    results = {}
    
    for model_name, model in models.items():
        print(f"    Training {model_name}...")
        
        # Train
        model.fit(X_train, y_train)
        
        # Predict
        y_pred_proba = model.predict_proba(X_test)[:, 1]
        
        # Evaluate
        auroc = roc_auc_score(y_test, y_pred_proba)
        auprc = average_precision_score(y_test, y_pred_proba)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X_train, y_train, 
                                   cv=cv_folds, scoring='roc_auc', n_jobs=-1)
        
        results[model_name] = {
            'model': model,
            'auroc': auroc,
            'auprc': auprc,
            'cv_mean': cv_scores.mean(),
            'cv_std': cv_scores.std(),
            'predictions': y_pred_proba
        }
        
        print(f"      Test AUROC: {auroc:.3f}, Test AUPRC: {auprc:.3f} | Train CV: {cv_scores.mean():.3f}+/-{cv_scores.std():.3f}")
    
    return results

def save_models(task_name: str, target: str, results: Dict, scaler, 
                feature_cols: List, output_dir: Path):
    """Save trained models organized by model type"""
    
    # Save each model in its own directory
    for model_name, result in results.items():
        # Create directory: output_dir/model_type/
        model_dir = output_dir / model_name.lower().replace(' ', '_')
        model_dir.mkdir(exist_ok=True, parents=True)
        
        # Include task name in filename for clarity
        # e.g., mortality_mortality_icu_48h_model.pkl
        model_file = model_dir / f'{task_name}_{target}_model.pkl'
        with open(model_file, 'wb') as f:
            pickle.dump({
                'model': result['model'],
                'scaler': scaler,
                'feature_cols': feature_cols,
                'model_type': model_name
            }, f)
        
        # Save metadata separately
        metadata_file = model_dir / f'{task_name}_{target}_metadata.pkl'
        with open(metadata_file, 'wb') as f:
            pickle.dump({
                'scaler': scaler,
                'feature_cols': feature_cols,
                'model_type': model_name
            }, f)
        
        # Save performance metrics as JSON for easy access
        metrics_file = model_dir / f'{task_name}_{target}_metrics.json'
        with open(metrics_file, 'w', encoding='utf-8') as f:
            json.dump({
                'auroc': result['auroc'],
                'auprc': result['auprc'],
                'cv_mean': result['cv_mean'],
                'cv_std': result['cv_std']
            }, f, indent=2)
    
    # Find and return best model
    best_model_name = max(results.keys(), key=lambda k: results[k]['auroc'])
    best_result = results[best_model_name]
    
    return best_model_name, best_result

def train_task_models(task_name: str, targets: List[str], input_dir: Path, 
                      output_dir: Path, args):
    """Train models for all targets in a task"""
    print(f"\n{task_name.upper()} MODELS")
    print("-" * 60)
    
    # Load features
    df = load_features(task_name, input_dir, args.time_window)
    
    task_results = {}
    
    for target in targets:
        if target not in df.columns:
            print(f"  {target}: Not found in features")
            continue
        
        # Check if enough positive samples
        n_positive = int(df[target].sum())
        if n_positive < 10:
            print(f"  {target}: Skipped (only {n_positive} positive samples)")
            continue
        
        print(f"\n  Target: {target}")
        print(f"  Positive rate: {df[target].mean():.2%} ({n_positive}/{len(df)})")
        
        # Prepare data
        data = prepare_data(
            df, target,
            test_size=args.test_size,
            use_smote=args.use_smote,
            smote_threshold=args.smote_threshold,
            random_state=42,
            loso_test_site=getattr(args, 'loso_site', None)
        )
        
        if data[0] is None:
            print(f"  {target}: Failed to prepare data")
            continue
            
        X_train, X_test, y_train, y_test, scaler, feature_cols = data
        
        # Get models
        model_types = args.models.split(',')
        models = get_models(f"{task_name}_{target}", model_types)
        
        if not models:
            print(f"  No models configured for training")
            continue
        
        # Train models
        results = train_models(X_train, X_test, y_train, y_test, models, args.cv_folds)
        
        # Save models
        best_model, best_result = save_models(
            task_name, target, results, scaler, feature_cols, output_dir
        )
        
        task_results[target] = {
            'best_model': best_model,
            'auroc': best_result['auroc'],
            'auprc': best_result['auprc'],
            'positive_rate': df[target].mean()
        }
    
    return task_results

def main(args):
    """Main training function"""
    # Generate experiment name
    if args.exp_name is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if args.loso_site:
            exp_name = f'loso_site{args.loso_site}_{timestamp}'
        else:
            exp_name = f'random_split_{timestamp}'
    else:
        exp_name = args.exp_name

    print("=" * 80)
    print("TRADITIONAL MODEL TRAINING MODULE")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Experiment: {exp_name}")
    print(f"Time window: {args.time_window} hours")
    print(f"Models: {args.models}")
    print(f"Tasks: {args.tasks}")
    print("=" * 80)

    # Setup paths
    input_dir = Path(args.input_dir)
    base_output_dir = Path(args.output_dir)
    output_dir = base_output_dir / exp_name
    output_dir.mkdir(exist_ok=True, parents=True)

    print(f"Output directory: {output_dir}")
    print("=" * 80)
    
    # Define default targets for each task
    default_targets = {
        'mortality': ['mortality_7day', 'mortality_30day'],  # Only 7-day and 30-day mortality
        'readmission': ['readmission_7days', 'readmission_30days', 'readmission_90days'],
        'los': ['los_greater_3days', 'los_greater_7days'],
        'sepsis': ['has_sepsis_after_icu', 'sepsis_within_48h', 'sepsis_within_7days']
    }
    
    # Parse tasks
    tasks_to_run = args.tasks.split(',')
    
    all_results = {}
    
    # Train models for each task
    for task_name in tasks_to_run:
        if task_name not in default_targets:
            print(f"\nSkipping {task_name} (unknown task)")
            continue
        
        targets = default_targets[task_name]
        
        task_results = train_task_models(
            task_name, targets, input_dir, output_dir, args
        )
        all_results[task_name] = task_results
    
    # Save summary
    print("\n" + "=" * 80)
    print("TRAINING SUMMARY")
    print("=" * 80)
    
    summary = {
        'experiment_name': exp_name,
        'experiment_type': 'loso' if args.loso_site else 'random_split',
        'loso_test_site': args.loso_site,
        'run_time': datetime.now().isoformat(),
        'hyperparameters': {
            'models': args.models,
            'time_window': args.time_window,
            'tasks': args.tasks,
            'test_size': args.test_size,
            'use_smote': args.use_smote,
            'smote_threshold': args.smote_threshold,
            'cv_folds': args.cv_folds
        },
        'tasks': all_results,
        'statistics': {}
    }
    
    for task_name, task_results in all_results.items():
        if task_results:
            avg_auroc = np.mean([r['auroc'] for r in task_results.values()])
            avg_auprc = np.mean([r['auprc'] for r in task_results.values()])
            
            summary['statistics'][task_name] = {
                'avg_auroc': avg_auroc,
                'avg_auprc': avg_auprc,
                'n_targets': len(task_results)
            }
            
            print(f"\n{task_name.upper()}:")
            print(f"  Targets trained: {len(task_results)}")
            print(f"  Average AUROC: {avg_auroc:.3f}")
            print(f"  Average AUPRC: {avg_auprc:.3f}")
            
            best_target = max(task_results.items(), key=lambda x: x[1]['auroc'])
            print(f"  Best: {best_target[0]} (AUROC={best_target[1]['auroc']:.3f})")
    
    # Save summary
    summary_file = output_dir / 'training_summary.json'
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\nModels saved to: {output_dir}")
    print(f"Summary saved to: {summary_file}")
    
    print("\n" + "=" * 80)
    print("Training completed successfully!")
    print("=" * 80)
    
    return summary

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Train traditional ML models for prediction tasks')
    
    # Data paths
    parser.add_argument('--input-dir', type=str, 
                       default='predictive_modeling/modeling_results/features',
                       help='Directory containing extracted features')
    parser.add_argument('--output-dir', type=str,
                       default='predictive_modeling/modeling_results/models/traditional',
                       help='Directory to save trained models')
    
    # Feature parameters
    parser.add_argument('--time-window', type=int,
                       choices=[2, 4, 8],
                       default=4,
                       help='Time window in hours for time series features (2, 4, or 8)')
    
    # Task configuration
    parser.add_argument('--tasks', type=str,
                       default='mortality,readmission,los,sepsis',
                       help='Comma-separated list of tasks to train')
    
    # Model configuration
    parser.add_argument('--models', type=str,
                       default='LogisticRegression,RandomForest,GradientBoosting,XGBoost',
                       help='Comma-separated list of models to train')
    
    # Training parameters
    parser.add_argument('--test-size', type=float, default=0.2,
                       help='Test set size (default: 0.2)')
    parser.add_argument('--use-smote', action='store_true',
                       help='Use SMOTE for imbalanced data')
    parser.add_argument('--smote-threshold', type=float, default=0.3,
                       help='Apply SMOTE if positive rate below this threshold (default: 0.3)')
    parser.add_argument('--cv-folds', type=int, default=5,
                       help='Number of cross-validation folds (default: 5)')

    # LOSO parameters
    parser.add_argument('--loso-site', type=str, choices=['4', '6', '7', '9'],
                       help='Leave-one-site-out: test on this site (4/6/7/9), train on others')

    # Experiment naming
    parser.add_argument('--exp-name', type=str, default=None,
                       help='Experiment name (default: auto-generated from timestamp and params)')

    args = parser.parse_args()
    main(args)
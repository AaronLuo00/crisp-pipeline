#!/usr/bin/env python
"""
Deep Learning Model Training Module
Train deep learning models (LSTM, GRU, Transformer, etc.) 
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
from typing import Dict, List, Tuple

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, average_precision_score
from imblearn.over_sampling import SMOTE

# Set device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

class TabularDataset(Dataset):
    """Dataset for tabular data"""
    def __init__(self, X, y):
        self.X = torch.FloatTensor(X)
        self.y = torch.FloatTensor(y)
    
    def __len__(self):
        return len(self.y)
    
    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]

class MLP(nn.Module):
    """Multi-Layer Perceptron for tabular data"""
    def __init__(self, input_dim, hidden_dims=[256, 128, 64], dropout=0.3):
        super(MLP, self).__init__()
        layers = []
        prev_dim = input_dim
        
        for hidden_dim in hidden_dims:
            layers.append(nn.Linear(prev_dim, hidden_dim))
            layers.append(nn.BatchNorm1d(hidden_dim))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout))
            prev_dim = hidden_dim
        
        layers.append(nn.Linear(prev_dim, 1))
        layers.append(nn.Sigmoid())
        
        self.model = nn.Sequential(*layers)
    
    def forward(self, x):
        return self.model(x).squeeze()

class ResidualBlock(nn.Module):
    """Residual block for deep networks"""
    def __init__(self, dim, dropout=0.3):
        super(ResidualBlock, self).__init__()
        self.block = nn.Sequential(
            nn.Linear(dim, dim),
            nn.BatchNorm1d(dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(dim, dim),
            nn.BatchNorm1d(dim)
        )
        self.relu = nn.ReLU()
    
    def forward(self, x):
        residual = x
        out = self.block(x)
        out += residual
        return self.relu(out)

class ResNet(nn.Module):
    """Residual Network for tabular data"""
    def __init__(self, input_dim, hidden_dim=256, num_blocks=4, dropout=0.3):
        super(ResNet, self).__init__()
        
        # Input projection
        self.input_proj = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.BatchNorm1d(hidden_dim),
            nn.ReLU()
        )
        
        # Residual blocks
        self.blocks = nn.ModuleList([
            ResidualBlock(hidden_dim, dropout) for _ in range(num_blocks)
        ])
        
        # Output layer
        self.output = nn.Sequential(
            nn.Linear(hidden_dim, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x):
        x = self.input_proj(x)
        for block in self.blocks:
            x = block(x)
        return self.output(x).squeeze()

class AttentionBlock(nn.Module):
    """Self-attention block for tabular data"""
    def __init__(self, dim, num_heads=8):
        super(AttentionBlock, self).__init__()
        self.attention = nn.MultiheadAttention(dim, num_heads, batch_first=True)
        self.norm = nn.LayerNorm(dim)
        self.ffn = nn.Sequential(
            nn.Linear(dim, dim * 4),
            nn.ReLU(),
            nn.Linear(dim * 4, dim)
        )
        self.norm2 = nn.LayerNorm(dim)
    
    def forward(self, x):
        # Self-attention
        x = x.unsqueeze(1)  # Add sequence dimension
        attn_out, _ = self.attention(x, x, x)
        x = self.norm(x + attn_out)
        
        # Feed-forward
        ffn_out = self.ffn(x)
        x = self.norm2(x + ffn_out)
        return x.squeeze(1)

class TransformerModel(nn.Module):
    """Transformer model for tabular data"""
    def __init__(self, input_dim, hidden_dim=256, num_blocks=4, num_heads=8, dropout=0.3):
        super(TransformerModel, self).__init__()
        
        # Input projection
        self.input_proj = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.LayerNorm(hidden_dim),
            nn.ReLU(),
            nn.Dropout(dropout)
        )
        
        # Attention blocks
        self.blocks = nn.ModuleList([
            AttentionBlock(hidden_dim, num_heads) for _ in range(num_blocks)
        ])
        
        # Output layer
        self.output = nn.Sequential(
            nn.Linear(hidden_dim, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x):
        x = self.input_proj(x)
        for block in self.blocks:
            x = block(x)
        return self.output(x).squeeze()

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
                 random_state: int = 42) -> Tuple:
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
    
    return X_train_scaled, X_test_scaled, y_train.values, y_test.values, scaler, feature_cols

def get_model(model_type: str, input_dim: int, config: Dict) -> nn.Module:
    """Get deep learning model based on type"""
    
    if model_type == 'MLP':
        hidden_dims = config.get('hidden_dims', [256, 128, 64])
        dropout = config.get('dropout', 0.3)
        return MLP(input_dim, hidden_dims, dropout)
    
    elif model_type == 'ResNet':
        hidden_dim = config.get('hidden_dim', 256)
        num_blocks = config.get('num_blocks', 4)
        dropout = config.get('dropout', 0.3)
        return ResNet(input_dim, hidden_dim, num_blocks, dropout)
    
    elif model_type == 'Transformer':
        hidden_dim = config.get('hidden_dim', 256)
        num_blocks = config.get('num_blocks', 4)
        num_heads = config.get('num_heads', 8)
        dropout = config.get('dropout', 0.3)
        return TransformerModel(input_dim, hidden_dim, num_blocks, num_heads, dropout)
    
    else:
        raise ValueError(f"Unknown model type: {model_type}")

def train_model(model: nn.Module, train_loader: DataLoader, val_loader: DataLoader,
                epochs: int = 100, lr: float = 0.001, patience: int = 10) -> Dict:
    """Train deep learning model"""
    
    model = model.to(device)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=5, factor=0.5)
    
    best_val_auroc = 0
    best_model_state = None
    patience_counter = 0
    
    train_losses = []
    val_aurocs = []
    
    for epoch in range(epochs):
        # Training
        model.train()
        train_loss = 0
        for batch_X, batch_y in train_loader:
            batch_X, batch_y = batch_X.to(device), batch_y.to(device)
            
            optimizer.zero_grad()
            outputs = model(batch_X)
            loss = criterion(outputs, batch_y)
            loss.backward()
            optimizer.step()
            
            train_loss += loss.item()
        
        train_loss /= len(train_loader)
        train_losses.append(train_loss)
        
        # Validation
        model.eval()
        val_preds = []
        val_labels = []
        
        with torch.no_grad():
            for batch_X, batch_y in val_loader:
                batch_X = batch_X.to(device)
                outputs = model(batch_X)
                val_preds.extend(outputs.cpu().numpy())
                val_labels.extend(batch_y.numpy())
        
        val_auroc = roc_auc_score(val_labels, val_preds)
        val_aurocs.append(val_auroc)
        
        # Learning rate scheduling
        scheduler.step(val_auroc)
        
        # Early stopping
        if val_auroc > best_val_auroc:
            best_val_auroc = val_auroc
            best_model_state = model.state_dict().copy()
            patience_counter = 0
        else:
            patience_counter += 1
        
        if (epoch + 1) % 10 == 0:
            print(f"      Epoch {epoch+1}/{epochs} - Loss: {train_loss:.4f}, Val AUROC: {val_auroc:.3f}")
        
        if patience_counter >= patience:
            print(f"      Early stopping at epoch {epoch+1}")
            break
    
    # Load best model
    model.load_state_dict(best_model_state)
    
    return {
        'model': model,
        'train_losses': train_losses,
        'val_aurocs': val_aurocs,
        'best_val_auroc': best_val_auroc
    }

def evaluate_model(model: nn.Module, test_loader: DataLoader) -> Tuple[float, float]:
    """Evaluate model on test set"""
    model.eval()
    predictions = []
    labels = []
    
    with torch.no_grad():
        for batch_X, batch_y in test_loader:
            batch_X = batch_X.to(device)
            outputs = model(batch_X)
            predictions.extend(outputs.cpu().numpy())
            labels.extend(batch_y.numpy())
    
    auroc = roc_auc_score(labels, predictions)
    auprc = average_precision_score(labels, predictions)
    
    return auroc, auprc, np.array(predictions)

def save_model(task_name: str, target: str, model: nn.Module, scaler,
               feature_cols: List, performance: Dict, output_dir: Path, model_type: str):
    """Save trained model and metadata organized by model type"""
    # Create directory: output_dir/model_type/
    model_dir = output_dir / model_type.lower()
    model_dir.mkdir(exist_ok=True, parents=True)
    
    # Include task name in filename for clarity
    # e.g., mortality_icu_48h_model.pt
    model_file = model_dir / f'{task_name}_{target}_model.pt'
    torch.save({
        'model_state_dict': model.state_dict(),
        'model_class': model.__class__.__name__,
        'input_dim': len(feature_cols)
    }, model_file)
    
    # Save metadata with same naming convention
    metadata_file = model_dir / f'{task_name}_{target}_metadata.pkl'
    with open(metadata_file, 'wb') as f:
        pickle.dump({
            'scaler': scaler,
            'feature_cols': feature_cols,
            'model_type': model.__class__.__name__,
            'performance': performance
        }, f)
    
    # Also save performance metrics separately for easy access
    metrics_file = model_dir / f'{task_name}_{target}_metrics.json'
    with open(metrics_file, 'w') as f:
        json.dump(performance, f, indent=2)

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
            random_state=42
        )
        
        if data[0] is None:
            print(f"  {target}: Failed to prepare data")
            continue
            
        X_train, X_test, y_train, y_test, scaler, feature_cols = data
        
        # Create data loaders
        train_dataset = TabularDataset(X_train, y_train)
        test_dataset = TabularDataset(X_test, y_test)
        
        train_loader = DataLoader(train_dataset, batch_size=args.batch_size, shuffle=True)
        test_loader = DataLoader(test_dataset, batch_size=args.batch_size, shuffle=False)
        
        # Split train into train/val
        val_size = int(0.2 * len(train_dataset))
        train_size = len(train_dataset) - val_size
        train_subset, val_subset = torch.utils.data.random_split(
            train_dataset, [train_size, val_size]
        )
        
        train_loader = DataLoader(train_subset, batch_size=args.batch_size, shuffle=True)
        val_loader = DataLoader(val_subset, batch_size=args.batch_size, shuffle=False)
        
        # Get model
        input_dim = X_train.shape[1]
        model_config = {
            'dropout': args.dropout,
            'hidden_dims': [int(d) for d in args.hidden_dims.split(',')],
            'hidden_dim': int(args.hidden_dims.split(',')[0]),
            'num_blocks': args.num_blocks,
            'num_heads': args.num_heads
        }
        
        model = get_model(args.model_type, input_dim, model_config)
        print(f"    Training {args.model_type} model...")
        
        # Train model
        train_results = train_model(
            model, train_loader, val_loader,
            epochs=args.epochs, lr=args.lr, patience=args.patience
        )
        
        # Evaluate on test set
        auroc, auprc, _ = evaluate_model(train_results['model'], test_loader)
        
        print(f"    Test AUROC: {auroc:.3f}, Test AUPRC: {auprc:.3f}")
        
        # Save model
        performance = {
            'auroc': auroc,
            'auprc': auprc,
            'val_auroc': train_results['best_val_auroc']
        }
        
        save_model(task_name, target, train_results['model'], scaler,
                  feature_cols, performance, output_dir, args.model_type)
        
        task_results[target] = {
            'model_type': args.model_type,
            'auroc': auroc,
            'auprc': auprc,
            'positive_rate': df[target].mean()
        }
    
    return task_results

def main(args):
    """Main training function"""
    print("=" * 80)
    print("DEEP LEARNING MODEL TRAINING MODULE")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Device: {device}")
    print(f"Model type: {args.model_type}")
    print(f"Time window: {args.time_window} hours")
    print(f"Tasks: {args.tasks}")
    print("=" * 80)
    
    # Setup paths
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    
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
        'run_time': datetime.now().isoformat(),
        'model_type': args.model_type,
        'time_window': args.time_window,
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
    summary_file = output_dir / f'deep_learning_summary_{args.model_type.lower()}.json'
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\nModels saved to: {output_dir}")
    print(f"Summary saved to: {summary_file}")
    
    print("\n" + "=" * 80)
    print("Training completed successfully!")
    print("=" * 80)
    
    return summary

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Train deep learning models for prediction tasks')
    
    # Data paths
    parser.add_argument('--input-dir', type=str, 
                       default='predictive_modeling/modeling_results/features',
                       help='Directory containing extracted features')
    parser.add_argument('--output-dir', type=str,
                       default='predictive_modeling/modeling_results/models/deep_learning',
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
    parser.add_argument('--model-type', type=str,
                       choices=['MLP', 'ResNet', 'Transformer'],
                       default='MLP',
                       help='Type of deep learning model to train')
    parser.add_argument('--hidden-dims', type=str,
                       default='256,128,64',
                       help='Comma-separated hidden dimensions for MLP')
    parser.add_argument('--num-blocks', type=int, default=4,
                       help='Number of residual/attention blocks')
    parser.add_argument('--num-heads', type=int, default=8,
                       help='Number of attention heads for Transformer')
    parser.add_argument('--dropout', type=float, default=0.3,
                       help='Dropout rate')
    
    # Training parameters
    parser.add_argument('--epochs', type=int, default=100,
                       help='Number of training epochs')
    parser.add_argument('--batch-size', type=int, default=32,
                       help='Batch size')
    parser.add_argument('--lr', type=float, default=0.001,
                       help='Learning rate')
    parser.add_argument('--patience', type=int, default=10,
                       help='Early stopping patience')
    parser.add_argument('--test-size', type=float, default=0.2,
                       help='Test set size (default: 0.2)')
    parser.add_argument('--use-smote', action='store_true',
                       help='Use SMOTE for imbalanced data')
    parser.add_argument('--smote-threshold', type=float, default=0.3,
                       help='Apply SMOTE if positive rate below this threshold')
    
    args = parser.parse_args()
    main(args)
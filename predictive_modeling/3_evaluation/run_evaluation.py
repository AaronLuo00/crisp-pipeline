#!/usr/bin/env python
"""
Model Evaluation Module
Comprehensive evaluation of trained models with clinical metrics
Supports both traditional ML and deep learning models
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
from typing import Dict, List, Tuple, Optional

from sklearn.metrics import (
    roc_auc_score, average_precision_score, roc_curve,
    precision_recall_curve, confusion_matrix
)
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

# Try to import torch for deep learning models
try:
    import torch
    import torch.nn as nn
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    print("Warning: PyTorch not available. Deep learning model evaluation will be skipped.")

def reshape_temporal_features(df, time_window=4):
    """
    Reshape flattened temporal features to sequential format
    
    Args:
        df: DataFrame with columns like concept_t0, concept_t1, etc.
        time_window: Time window in hours (2, 4, or 8)
    
    Returns:
        X_static: Static features array
        X_temporal: Temporal features array [batch, seq_len, features]
        feature_cols: List of feature column names
    """
    # Identify static columns (demographics and conditions/procedures without _t suffix)
    static_cols = ['age_at_icu', 'gender', 'race', 'ethnicity']
    
    # Find condition and procedure columns (those without _t suffix)
    for col in df.columns:
        if col not in static_cols and not col.startswith('mortality') and \
           not col.startswith('readmission') and not col.startswith('los') and \
           not col.startswith('sepsis') and not col.startswith('has_sepsis') and \
           not col.startswith('patient_id') and '_t' not in col:
            static_cols.append(col)
    
    # Identify temporal columns (those with _t0, _t1, etc.)
    temporal_cols = [col for col in df.columns if '_t' in col]
    
    # Extract unique concept IDs and time steps
    concept_time_map = {}
    for col in temporal_cols:
        parts = col.rsplit('_t', 1)
        if len(parts) == 2:
            concept_id = parts[0]
            time_step = int(parts[1])
            if concept_id not in concept_time_map:
                concept_time_map[concept_id] = []
            concept_time_map[concept_id].append((time_step, col))
    
    # Determine sequence length
    if concept_time_map:
        max_time_step = max([max([t for t, _ in times]) for times in concept_time_map.values()]) + 1
        seq_length = max_time_step
    else:
        # No temporal features found - use dummy sequence
        seq_length = 1
    
    # Extract static features
    X_static = df[static_cols].values
    
    # Build temporal features array
    n_samples = len(df)
    if concept_time_map:
        n_temporal_features = len(concept_time_map)
        X_temporal = np.zeros((n_samples, seq_length, n_temporal_features))
        
        # Fill temporal array
        for feat_idx, (concept_id, time_cols) in enumerate(concept_time_map.items()):
            for time_step, col_name in time_cols:
                if col_name in df.columns:
                    X_temporal[:, time_step, feat_idx] = df[col_name].values
    else:
        # No temporal features - create dummy temporal data
        X_temporal = np.zeros((n_samples, 1, 1))
    
    return X_static, X_temporal, static_cols

def load_model(model_path: Path, model_category: str = 'traditional') -> Dict:
    """Load trained model and metadata
    
    Args:
        model_path: Path to model file
        model_category: 'traditional' or 'deep_learning'
    """
    if model_category == 'traditional':
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        return model_data
    elif model_category == 'deep_learning' and TORCH_AVAILABLE:
        # Load PyTorch model
        checkpoint = torch.load(model_path, map_location='cpu')
        
        # Load metadata
        metadata_path = model_path.with_suffix('').with_name(
            model_path.stem.replace('_model', '_metadata') + '.pkl'
        )
        with open(metadata_path, 'rb') as f:
            metadata = pickle.load(f)
        
        # Reconstruct model
        model = reconstruct_dl_model(checkpoint, metadata)
        
        return {
            'model': model,
            'scaler': metadata['scaler'],
            'feature_cols': metadata['feature_cols'],
            'model_type': metadata['model_type']
        }
    else:
        raise ValueError(f"Unsupported model category: {model_category}")

def reconstruct_dl_model(checkpoint: Dict, metadata: Dict = None):
    """Reconstruct deep learning model from checkpoint"""
    if not TORCH_AVAILABLE:
        return None
    
    input_dim = checkpoint['input_dim']
    model_class = checkpoint['model_class']
    
    # Define model architectures
    class MLP(nn.Module):
        def __init__(self, input_dim):
            super(MLP, self).__init__()
            self.model = nn.Sequential(
                nn.Linear(input_dim, 256),
                nn.BatchNorm1d(256),
                nn.ReLU(),
                nn.Dropout(0.3),
                nn.Linear(256, 128),
                nn.BatchNorm1d(128),
                nn.ReLU(),
                nn.Dropout(0.3),
                nn.Linear(128, 64),
                nn.BatchNorm1d(64),
                nn.ReLU(),
                nn.Dropout(0.3),
                nn.Linear(64, 1),
                nn.Sigmoid()
            )
        
        def forward(self, x):
            return self.model(x).squeeze()
    
    class LSTMPredictor(nn.Module):
        """LSTM model with static and temporal feature fusion"""
        def __init__(self, static_dim=104, temporal_dim=477, hidden_dim=128, 
                     num_layers=2, dropout=0.3):
            super(LSTMPredictor, self).__init__()
            
            # Static feature processing
            self.static_net = nn.Sequential(
                nn.Linear(static_dim, 64),
                nn.BatchNorm1d(64),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(64, 32),
                nn.BatchNorm1d(32),
                nn.ReLU()
            )
            
            # LSTM for temporal features
            self.lstm = nn.LSTM(
                input_size=temporal_dim,
                hidden_size=hidden_dim,
                num_layers=num_layers,
                batch_first=True,
                dropout=dropout if num_layers > 1 else 0,
                bidirectional=True
            )
            
            # Fusion and output layers
            fusion_dim = 32 + (hidden_dim * 2)  # Static + bidirectional LSTM
            self.fusion = nn.Sequential(
                nn.Linear(fusion_dim, 128),
                nn.BatchNorm1d(128),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(128, 64),
                nn.BatchNorm1d(64),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(64, 1),
                nn.Sigmoid()
            )
        
        def forward(self, x_static, x_temporal):
            # Process static features
            static_out = self.static_net(x_static)
            
            # Process temporal features with LSTM
            lstm_out, (h_n, c_n) = self.lstm(x_temporal)
            
            # Use the last hidden state from both directions
            h_forward = h_n[-2, :, :]  # Last layer, forward direction
            h_backward = h_n[-1, :, :]  # Last layer, backward direction
            temporal_out = torch.cat([h_forward, h_backward], dim=1)
            
            # Fusion
            combined = torch.cat([static_out, temporal_out], dim=1)
            output = self.fusion(combined)
            
            return output.squeeze()
    
    class TCNBlock(nn.Module):
        """Temporal Convolutional Block"""
        def __init__(self, in_channels, out_channels, kernel_size, dilation, dropout=0.3):
            super(TCNBlock, self).__init__()
            padding = (kernel_size - 1) * dilation // 2
            
            self.conv1 = nn.Conv1d(in_channels, out_channels, kernel_size,
                                   padding=padding, dilation=dilation)
            self.bn1 = nn.BatchNorm1d(out_channels)
            self.relu1 = nn.ReLU()
            self.dropout1 = nn.Dropout(dropout)
            
            self.conv2 = nn.Conv1d(out_channels, out_channels, kernel_size,
                                   padding=padding, dilation=dilation)
            self.bn2 = nn.BatchNorm1d(out_channels)
            self.relu2 = nn.ReLU()
            self.dropout2 = nn.Dropout(dropout)
            
            # Residual connection
            self.residual = nn.Conv1d(in_channels, out_channels, 1) if in_channels != out_channels else None
        
        def forward(self, x):
            residual = x
            
            out = self.conv1(x)
            out = self.bn1(out)
            out = self.relu1(out)
            out = self.dropout1(out)
            
            out = self.conv2(out)
            out = self.bn2(out)
            
            if self.residual is not None:
                residual = self.residual(residual)
            
            out = self.relu2(out + residual)
            out = self.dropout2(out)
            
            return out
    
    class TCNPredictor(nn.Module):
        """Temporal Convolutional Network with static and temporal feature fusion"""
        def __init__(self, static_dim=104, temporal_dim=477, num_channels=[64, 128, 256], 
                     kernel_size=3, dropout=0.3):
            super(TCNPredictor, self).__init__()
            
            # Static feature processing
            self.static_net = nn.Sequential(
                nn.Linear(static_dim, 64),
                nn.BatchNorm1d(64),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(64, 32),
                nn.BatchNorm1d(32),
                nn.ReLU()
            )
            
            # TCN for temporal features
            layers = []
            num_levels = len(num_channels)
            for i in range(num_levels):
                in_channels = temporal_dim if i == 0 else num_channels[i-1]
                out_channels = num_channels[i]
                dilation = 2 ** i
                layers.append(TCNBlock(in_channels, out_channels, kernel_size, dilation, dropout))
            
            self.tcn = nn.Sequential(*layers)
            
            # Global pooling
            self.global_pool = nn.AdaptiveAvgPool1d(1)
            
            # Fusion and output layers
            fusion_dim = 32 + num_channels[-1]  # Static + TCN output
            self.fusion = nn.Sequential(
                nn.Linear(fusion_dim, 128),
                nn.BatchNorm1d(128),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(128, 64),
                nn.BatchNorm1d(64),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(64, 1),
                nn.Sigmoid()
            )
        
        def forward(self, x_static, x_temporal):
            # Process static features
            static_out = self.static_net(x_static)
            
            # Process temporal features with TCN
            # TCN expects [batch, channels, sequence_length]
            x_temporal = x_temporal.transpose(1, 2)
            tcn_out = self.tcn(x_temporal)
            
            # Global pooling to get fixed size representation
            tcn_out = self.global_pool(tcn_out).squeeze(-1)
            
            # Fusion
            combined = torch.cat([static_out, tcn_out], dim=1)
            output = self.fusion(combined)
            
            return output.squeeze()

    class TemporalAttentionBlock(nn.Module):
        """Temporal self-attention block for sequential data"""
        def __init__(self, dim, num_heads=8, dropout=0.3):
            super(TemporalAttentionBlock, self).__init__()
            self.attention = nn.MultiheadAttention(dim, num_heads, batch_first=True, dropout=dropout)
            self.norm1 = nn.LayerNorm(dim)
            self.ffn = nn.Sequential(
                nn.Linear(dim, dim * 4),
                nn.GELU(),
                nn.Dropout(dropout),
                nn.Linear(dim * 4, dim),
                nn.Dropout(dropout)
            )
            self.norm2 = nn.LayerNorm(dim)
            self.dropout = nn.Dropout(dropout)

        def forward(self, x):
            # Self-attention with residual connection
            attn_out, _ = self.attention(x, x, x)
            x = self.norm1(x + self.dropout(attn_out))

            # Feed-forward with residual connection
            ffn_out = self.ffn(x)
            x = self.norm2(x + ffn_out)
            return x

    class SequentialTransformer(nn.Module):
        """Transformer model with static and temporal feature fusion for sequential data"""
        def __init__(self, static_dim, temporal_dim, hidden_dim=128, num_blocks=3, num_heads=8, dropout=0.3):
            super(SequentialTransformer, self).__init__()

            # Static feature processing
            self.static_net = nn.Sequential(
                nn.Linear(static_dim, 64),
                nn.BatchNorm1d(64),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(64, 32),
                nn.BatchNorm1d(32),
                nn.ReLU()
            )

            # Temporal feature projection
            self.temporal_proj = nn.Sequential(
                nn.Linear(temporal_dim, hidden_dim),
                nn.LayerNorm(hidden_dim),
                nn.Dropout(dropout)
            )

            # Temporal Transformer blocks
            self.transformer_blocks = nn.ModuleList([
                TemporalAttentionBlock(hidden_dim, num_heads, dropout)
                for _ in range(num_blocks)
            ])

            # Global pooling for temporal features
            self.temporal_pool = nn.AdaptiveAvgPool1d(1)

            # Fusion and output layers
            fusion_dim = 32 + hidden_dim  # Static + Transformer output
            self.fusion = nn.Sequential(
                nn.Linear(fusion_dim, 128),
                nn.BatchNorm1d(128),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(128, 64),
                nn.BatchNorm1d(64),
                nn.ReLU(),
                nn.Dropout(dropout),
                nn.Linear(64, 1),
                nn.Sigmoid()
            )

        def forward(self, x_static, x_temporal):
            # Process static features
            static_out = self.static_net(x_static)

            # Process temporal features with Transformer
            x = self.temporal_proj(x_temporal)

            # Apply Transformer blocks
            for block in self.transformer_blocks:
                x = block(x)

            # Global average pooling over time dimension
            x = x.transpose(1, 2)
            temporal_out = self.temporal_pool(x).squeeze(-1)

            # Fusion
            combined = torch.cat([static_out, temporal_out], dim=1)
            output = self.fusion(combined)

            return output.squeeze()

    # Create model instance
    if model_class == 'MLP':
        model = MLP(input_dim)
    elif model_class == 'LSTMPredictor':
        # Infer dimensions from checkpoint weights
        state_dict = checkpoint['model_state_dict']

        # Get static_dim from first layer weight
        static_dim = state_dict['static_net.0.weight'].shape[1]

        # Get temporal_dim from LSTM weight
        # lstm.weight_ih_l0 shape: [hidden_size * 4, temporal_dim] for bidirectional
        temporal_dim = state_dict['lstm.weight_ih_l0'].shape[1]

        # Infer hidden_dim from LSTM weight
        # For bidirectional LSTM: weight shape is [4 * hidden_dim, input_dim]
        hidden_dim = state_dict['lstm.weight_ih_l0'].shape[0] // 4

        model = LSTMPredictor(static_dim=static_dim, temporal_dim=temporal_dim,
                            hidden_dim=hidden_dim, num_layers=2, dropout=0.3)

    elif model_class == 'TCNPredictor':
        # Infer dimensions from checkpoint weights
        state_dict = checkpoint['model_state_dict']

        # Get static_dim from first layer weight
        static_dim = state_dict['static_net.0.weight'].shape[1]

        # Get temporal_dim from first TCN conv layer
        # tcn.0.conv1.weight shape: [out_channels, in_channels, kernel_size]
        temporal_dim = state_dict['tcn.0.conv1.weight'].shape[1]

        # Get num_channels from TCN layers
        num_channels = []
        i = 0
        while f'tcn.{i}.conv1.weight' in state_dict:
            out_channels = state_dict[f'tcn.{i}.conv1.weight'].shape[0]
            num_channels.append(out_channels)
            i += 1

        # Get kernel_size from first conv layer
        kernel_size = state_dict['tcn.0.conv1.weight'].shape[2]

        model = TCNPredictor(static_dim=static_dim, temporal_dim=temporal_dim,
                           num_channels=num_channels, kernel_size=kernel_size, dropout=0.3)
    elif model_class == 'SequentialTransformer':
        # Infer dimensions from checkpoint weights
        state_dict = checkpoint['model_state_dict']

        # Get static_dim from first layer weight
        static_dim = state_dict['static_net.0.weight'].shape[1]

        # Get temporal_dim from temporal projection weight
        temporal_dim = state_dict['temporal_proj.0.weight'].shape[1]

        # Get hidden_dim from temporal projection output
        hidden_dim = state_dict['temporal_proj.0.weight'].shape[0]

        # Count number of transformer blocks
        num_blocks = sum(1 for key in state_dict.keys() if 'transformer_blocks' in key and 'attention.in_proj_weight' in key)

        # Get num_heads from attention layer shape
        # in_proj_weight shape: [3 * hidden_dim, hidden_dim] for Q, K, V
        num_heads = 8  # Default, can't easily infer from weights

        model = SequentialTransformer(static_dim=static_dim, temporal_dim=temporal_dim,
                                     hidden_dim=hidden_dim, num_blocks=num_blocks,
                                     num_heads=num_heads, dropout=0.3)
    else:
        raise ValueError(f"Unknown model class: {model_class}")
    
    # Load state dict
    model.load_state_dict(checkpoint['model_state_dict'])
    model.eval()
    
    return model

def load_test_data(task_name: str, target: str, feature_dir: Path, time_window: int = 4) -> Tuple:
    """Load test data for evaluation"""
    
    # Determine which feature file to use
    if task_name == 'sepsis':
        feature_file = feature_dir / 'sepsis_static_features.csv'
    else:
        # For mortality, readmission, los tasks
        feature_file = feature_dir / f'time_series_features_{time_window}h_window.csv'
    
    if not feature_file.exists():
        raise FileNotFoundError(f"Feature file not found: {feature_file}")
    
    df = pd.read_csv(feature_file)
    
    # Filter to columns needed
    feature_cols = [col for col in df.columns if col not in ['patient_id', target]
                   and not col.startswith('mortality_') 
                   and not col.startswith('readmission_')
                   and not col.startswith('los_')
                   and not col.startswith('has_sepsis')
                   and not col.startswith('sepsis_within')]
    
    X = df[feature_cols]
    y = df[target]
    patient_ids = df['patient_id']
    
    # Handle missing values
    X = X.fillna(X.median())
    
    # Split data - keep consistent with training (20% test set, same random_state)
    _, X_test, _, y_test, _, patient_ids_test = train_test_split(
        X, y, patient_ids,
        test_size=0.2,
        random_state=42,
        stratify=y
    )
    
    return X_test, y_test, patient_ids_test

def calculate_metrics(y_true, y_pred_proba, threshold=0.5) -> Dict:
    """Calculate essential evaluation metrics"""

    # Convert to numpy arrays
    y_true = np.array(y_true)
    y_pred_proba = np.array(y_pred_proba)

    # Filter out NaN predictions
    valid_mask = ~np.isnan(y_pred_proba)
    if not valid_mask.all():
        n_nan = (~valid_mask).sum()
        print(f"      Warning: {n_nan} NaN predictions detected, filtering them out")
        y_pred_proba = y_pred_proba[valid_mask]
        y_true = y_true[valid_mask]

    # Check if we have enough valid predictions
    if len(y_pred_proba) == 0:
        print(f"      Error: All predictions are NaN, returning default metrics")
        return {
            'auroc': 0.5,
            'auprc': 0.0,
            'sensitivity': 0.0,
            'specificity': 0.0,
            'ppv': 0.0,
            'accuracy': 0.0,
            'tp': 0,
            'tn': 0,
            'fp': 0,
            'fn': 0
        }

    y_pred = (y_pred_proba >= threshold).astype(int)

    # Confusion matrix
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    # Core metrics only
    metrics = {
        'auroc': roc_auc_score(y_true, y_pred_proba),
        'auprc': average_precision_score(y_true, y_pred_proba),
        'sensitivity': tp / (tp + fn) if (tp + fn) > 0 else 0,
        'specificity': tn / (tn + fp) if (tn + fp) > 0 else 0,
        'ppv': tp / (tp + fp) if (tp + fp) > 0 else 0,
        'accuracy': (tp + tn) / (tp + tn + fp + fn),
        'tp': int(tp),
        'tn': int(tn),
        'fp': int(fp),
        'fn': int(fn)
    }

    return metrics

def plot_roc_curve(y_true, y_pred_proba, model_name: str, task_name: str, 
                   target: str, output_dir: Path):
    """Plot and save ROC curve"""
    fpr, tpr, _ = roc_curve(y_true, y_pred_proba)
    auroc = roc_auc_score(y_true, y_pred_proba)
    
    plt.figure(figsize=(8, 6))
    plt.plot(fpr, tpr, label=f'ROC (AUROC = {auroc:.3f})', linewidth=2)
    plt.plot([0, 1], [0, 1], 'k--', linewidth=1)
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title(f'ROC Curve - {model_name} - {task_name.title()} - {target}')
    plt.legend(loc="lower right")
    plt.grid(True, alpha=0.3)
    
    plot_dir = output_dir / 'plots' / model_name.lower() / task_name
    plot_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(plot_dir / f'{target}_roc.png', dpi=100, bbox_inches='tight')
    plt.close()

def plot_pr_curve(y_true, y_pred_proba, model_name: str, task_name: str, 
                  target: str, output_dir: Path):
    """Plot and save Precision-Recall curve"""
    precision, recall, _ = precision_recall_curve(y_true, y_pred_proba)
    auprc = average_precision_score(y_true, y_pred_proba)
    
    plt.figure(figsize=(8, 6))
    plt.plot(recall, precision, label=f'PR (AUPRC = {auprc:.3f})', linewidth=2)
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title(f'Precision-Recall Curve - {model_name} - {task_name.title()} - {target}')
    plt.legend(loc="lower left")
    plt.grid(True, alpha=0.3)
    
    plot_dir = output_dir / 'plots' / model_name.lower() / task_name
    plot_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(plot_dir / f'{target}_pr.png', dpi=100, bbox_inches='tight')
    plt.close()

def evaluate_model(task_name: str, target: str, model_name: str, model_category: str,
                   model_dir: Path, feature_dir: Path, output_dir: Path, 
                   time_window: int = 4) -> Optional[Dict]:
    """Evaluate a single model
    
    Args:
        task_name: Task name (mortality, readmission, etc.)
        target: Target variable name
        model_name: Model name (logisticregression, randomforest, mlp, etc.)
        model_category: 'traditional' or 'deep_learning'
        model_dir: Base directory for models
        feature_dir: Directory containing features
        output_dir: Output directory for results
        time_window: Time window for features
    """
    
    # Construct model path based on new structure
    if model_category == 'traditional':
        model_path = model_dir / 'traditional' / model_name / f'{task_name}_{target}_model.pkl'
    else:
        model_path = model_dir / 'deep_learning' / model_name / f'{task_name}_{target}_model.pt'
    
    if not model_path.exists():
        print(f"    Model not found: {model_path}")
        return None
    
    # Load model
    try:
        model_data = load_model(model_path, model_category)
    except Exception as e:
        print(f"    Error loading model: {e}")
        return None
    
    model = model_data['model']
    scaler = model_data['scaler']
    feature_cols = model_data['feature_cols']
    
    # Load test data
    try:
        X, y, _ = load_test_data(task_name, target, feature_dir, time_window)
    except FileNotFoundError as e:
        print(f"    {e}")
        return None
    
    # Ensure we have the right features in the right order
    X = X[feature_cols]

    # Clip extreme values BEFORE scaling to prevent numerical instability
    # Values > 1e10 are data errors (e.g., WBC count of 1e21)
    extreme_threshold = 1e10
    n_extreme = (X.abs() > extreme_threshold).sum().sum()
    if n_extreme > 0:
        print(f"      Clipping {n_extreme} extreme values (> {extreme_threshold:.0e})")
        X = X.clip(-extreme_threshold, extreme_threshold)

    # Scale features
    X_scaled = scaler.transform(X)
    
    # Make predictions
    if model_category == 'traditional':
        y_pred_proba = model.predict_proba(X_scaled)[:, 1]
    elif model_category == 'deep_learning' and TORCH_AVAILABLE:
        model.eval()
        with torch.no_grad():
            # Check if this is a sequential model (LSTM/TCN/Transformer)
            model_type = model_data.get('model_type', model_name)
            if model_type in ['LSTMPredictor', 'TCNPredictor', 'SequentialTransformer'] or model_name in ['lstm', 'tcn', 'transformer']:
                # For sequential models, reshape data
                df_features = pd.DataFrame(X_scaled, columns=feature_cols)
                X_static, X_temporal, _ = reshape_temporal_features(df_features, time_window)

                # Convert to tensors
                X_static_tensor = torch.FloatTensor(X_static)
                X_temporal_tensor = torch.FloatTensor(X_temporal)

                # Make predictions with dual inputs
                y_pred_proba = model(X_static_tensor, X_temporal_tensor).cpu().numpy()
            else:
                # For non-sequential models (MLP, etc.)
                X_tensor = torch.FloatTensor(X_scaled)
                y_pred_proba = model(X_tensor).cpu().numpy()
    else:
        return None
    
    # Calculate metrics
    metrics = calculate_metrics(y, y_pred_proba)
    
    # Add model info
    metrics['model_name'] = model_name
    # Use specific model name as category for traditional models
    if model_category == 'traditional':
        # Use the actual model name as the category
        if model_name in ['logisticregression', 'randomforest', 'gradientboosting', 'xgboost']:
            metrics['model_category'] = model_name
        else:
            metrics['model_category'] = model_category
    else:
        metrics['model_category'] = model_category
    metrics['model_type'] = model_data.get('model_type', model_name)
    metrics['n_samples'] = len(y)
    metrics['positive_rate'] = float(y.mean())
    
    # Generate plots
    plot_roc_curve(y, y_pred_proba, model_name, task_name, target, output_dir)
    plot_pr_curve(y, y_pred_proba, model_name, task_name, target, output_dir)
    
    return metrics

def discover_available_models(model_dir: Path, task_name: str, target: str) -> Dict[str, List[str]]:
    """Automatically discover available models in the model directory
    
    Returns:
        Dict with keys 'traditional' and 'deep_learning', each containing list of model names
    """
    available_models = {'traditional': [], 'deep_learning': []}
    
    # Check traditional models directory
    traditional_dir = model_dir / 'traditional'
    if traditional_dir.exists():
        for model_subdir in traditional_dir.iterdir():
            if model_subdir.is_dir():
                # Check if this model has files for the given task and target
                model_file = model_subdir / f'{task_name}_{target}_model.pkl'
                if model_file.exists():
                    available_models['traditional'].append(model_subdir.name)
    
    # Check deep learning models directory
    dl_dir = model_dir / 'deep_learning'
    if dl_dir.exists() and TORCH_AVAILABLE:
        for model_subdir in dl_dir.iterdir():
            if model_subdir.is_dir():
                # Check if this model has files for the given task and target
                model_file = model_subdir / f'{task_name}_{target}_model.pt'
                if model_file.exists():
                    available_models['deep_learning'].append(model_subdir.name)
    
    return available_models

def evaluate_all_models_for_task(task_name: str, targets: List[str], model_dir: Path,
                                 feature_dir: Path, output_dir: Path, 
                                 time_window: int = 4,
                                 model_types: Optional[List[str]] = None) -> Dict:
    """Evaluate all available models for a task"""
    
    print(f"\n{task_name.upper()} EVALUATION")
    print("-" * 60)
    
    task_results = {}
    
    # If model_types specified, use those; otherwise discover available models
    if model_types is not None:
        # Parse model types from argument (backward compatibility)
        traditional_models = [m for m in model_types if m in
                            ['logisticregression', 'randomforest', 'gradientboosting', 'xgboost']]
        dl_models = [m for m in model_types if m in ['mlp', 'transformer', 'lstm', 'tcn']]
        
        # For each target, evaluate specified models
        for target in targets:
            print(f"\n  Target: {target}")
            target_results = {}
            
            # Evaluate traditional models
            for model_name in traditional_models:
                print(f"    Evaluating {model_name}...")
                metrics = evaluate_model(
                    task_name, target, model_name, 'traditional',
                    model_dir, feature_dir, output_dir, time_window
                )
                if metrics:
                    target_results[model_name] = metrics
                    print(f"      AUROC: {metrics['auroc']:.3f}, AUPRC: {metrics['auprc']:.3f}")
            
            # Evaluate deep learning models
            for model_name in dl_models:
                print(f"    Evaluating {model_name}...")
                metrics = evaluate_model(
                    task_name, target, model_name, 'deep_learning',
                    model_dir, feature_dir, output_dir, time_window
                )
                if metrics:
                    target_results[model_name] = metrics
                    print(f"      AUROC: {metrics['auroc']:.3f}, AUPRC: {metrics['auprc']:.3f}")
            
            if target_results:
                # Find best model for this target
                best_model = max(target_results.items(), key=lambda x: x[1]['auroc'])
                print(f"    Best model: {best_model[0]} (AUROC: {best_model[1]['auroc']:.3f})")
                task_results[target] = target_results
    else:
        # Auto-discover models for each target
        for target in targets:
            print(f"\n  Target: {target}")
            
            # Discover available models for this task and target
            available_models = discover_available_models(model_dir, task_name, target)
            
            if not available_models['traditional'] and not available_models['deep_learning']:
                print(f"    No models found for {task_name}_{target}")
                continue
            
            print(f"    Found models: Traditional={available_models['traditional']}, "
                  f"Deep Learning={available_models['deep_learning']}")
            
            target_results = {}
            
            # Evaluate discovered traditional models
            for model_name in available_models['traditional']:
                print(f"    Evaluating {model_name} (traditional)...")
                metrics = evaluate_model(
                    task_name, target, model_name, 'traditional',
                    model_dir, feature_dir, output_dir, time_window
                )
                if metrics:
                    target_results[model_name] = metrics
                    print(f"      AUROC: {metrics['auroc']:.3f}, AUPRC: {metrics['auprc']:.3f}")
            
            # Evaluate discovered deep learning models
            for model_name in available_models['deep_learning']:
                print(f"    Evaluating {model_name} (deep learning)...")
                metrics = evaluate_model(
                    task_name, target, model_name, 'deep_learning',
                    model_dir, feature_dir, output_dir, time_window
                )
                if metrics:
                    target_results[model_name] = metrics
                    print(f"      AUROC: {metrics['auroc']:.3f}, AUPRC: {metrics['auprc']:.3f}")
            
            if target_results:
                # Find best model for this target
                best_model = max(target_results.items(), key=lambda x: x[1]['auroc'])
                print(f"    Best model: {best_model[0]} (AUROC: {best_model[1]['auroc']:.3f})")
                task_results[target] = target_results
    
    return task_results

def main(args):
    """Main evaluation function"""
    print("=" * 80)
    print("MODEL EVALUATION MODULE")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Time window: {args.time_window} hours")
    print(f"Tasks: {args.tasks}")
    if args.models:
        print(f"Models: {args.models}")
    print("=" * 80)
    
    # Setup paths
    model_dir = Path(args.model_dir)
    feature_dir = Path(args.feature_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)
    
    # Define default targets for each task
    default_targets = {
        'mortality': ['mortality_7day', 'mortality_30day'],  # Only 7-day and 30-day mortality
        'readmission': ['readmission_7days', 'readmission_30days', 'readmission_90days'],
        'los': ['los_greater_3days', 'los_greater_7days'],
        'sepsis': ['has_sepsis_after_icu', 'sepsis_within_48h', 'sepsis_within_7days']
    }
    
    # Parse tasks and models
    tasks_to_evaluate = args.tasks.split(',')
    model_types = args.models.split(',') if args.models else None
    
    all_results = {}
    
    # Evaluate each task
    for task_name in tasks_to_evaluate:
        if task_name not in default_targets:
            print(f"\nSkipping {task_name} (unknown task)")
            continue
        
        targets = default_targets[task_name]
        
        task_results = evaluate_all_models_for_task(
            task_name, targets, model_dir, feature_dir, output_dir,
            args.time_window, model_types
        )
        all_results[task_name] = task_results
    
    # Save comprehensive report
    print("\n" + "=" * 80)
    print("EVALUATION SUMMARY")
    print("=" * 80)
    
    summary = {
        'run_time': datetime.now().isoformat(),
        'time_window': args.time_window,
        'tasks': all_results,
        'statistics': {}
    }
    
    for task_name, task_targets in all_results.items():
        if task_targets:
            # Calculate average metrics across all targets and models
            all_aurocs = []
            all_auprcs = []
            best_models = {}
            
            for target, models in task_targets.items():
                if models:
                    # Find best model for this target
                    best = max(models.items(), key=lambda x: x[1]['auroc'])
                    best_models[target] = {
                        'model': best[0],
                        'auroc': best[1]['auroc'],
                        'auprc': best[1]['auprc']
                    }
                    
                    # Collect all metrics
                    for model_metrics in models.values():
                        all_aurocs.append(model_metrics['auroc'])
                        all_auprcs.append(model_metrics['auprc'])
            
            if all_aurocs:
                summary['statistics'][task_name] = {
                    'avg_auroc': np.mean(all_aurocs),
                    'avg_auprc': np.mean(all_auprcs),
                    'best_auroc': max(all_aurocs),
                    'best_auprc': max(all_auprcs),
                    'n_models_evaluated': len(all_aurocs),
                    'best_models': best_models
                }
                
                print(f"\n{task_name.upper()}:")
                print(f"  Models evaluated: {len(all_aurocs)}")
                print(f"  Average AUROC: {np.mean(all_aurocs):.3f}")
                print(f"  Best AUROC: {max(all_aurocs):.3f}")
                
                # Show best model for each target
                for target, info in best_models.items():
                    print(f"  {target}: {info['model']} (AUROC={info['auroc']:.3f})")
    
    # Save summary
    summary_file = output_dir / 'evaluation_summary.json'
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)
    
    # Generate markdown report
    report_file = output_dir / 'evaluation_report.md'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("# Model Evaluation Report\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"Time Window: {args.time_window} hours\n\n")
        
        for task_name, task_targets in all_results.items():
            if not task_targets:
                continue
                
            f.write(f"## {task_name.title()}\n\n")
            
            for target, models in task_targets.items():
                if not models:
                    continue
                    
                f.write(f"### {target}\n\n")
                f.write("| Model | Category | AUROC | AUPRC | Sensitivity | Specificity | PPV |\n")
                f.write("|-------|----------|-------|-------|-------------|-------------|-----|\n")
                
                # Sort models by AUROC
                sorted_models = sorted(models.items(), key=lambda x: x[1]['auroc'], reverse=True)
                
                for model_name, metrics in sorted_models:
                    f.write(f"| {model_name} | {metrics['model_category']} | "
                           f"{metrics['auroc']:.3f} | {metrics['auprc']:.3f} | "
                           f"{metrics['sensitivity']:.3f} | {metrics['specificity']:.3f} | "
                           f"{metrics['ppv']:.3f} |\n")
                
                f.write("\n")
    
    print(f"\nEvaluation results saved to: {output_dir}")
    print(f"Summary saved to: {summary_file}")
    print(f"Report saved to: {report_file}")
    
    print("\n" + "=" * 80)
    print("Evaluation completed successfully!")
    print("=" * 80)
    
    return summary

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Evaluate trained models')
    
    # Data paths
    parser.add_argument('--model-dir', type=str,
                       default='predictive_modeling/modeling_results/models',
                       help='Directory containing trained models')
    parser.add_argument('--feature-dir', type=str,
                       default='predictive_modeling/modeling_results/features',
                       help='Directory containing feature files')
    parser.add_argument('--output-dir', type=str,
                       default='predictive_modeling/modeling_results/evaluation',
                       help='Directory to save evaluation results')
    
    # Feature parameters
    parser.add_argument('--time-window', type=int,
                       choices=[2, 4, 8],
                       default=4,
                       help='Time window in hours for time series features (2, 4, or 8)')
    
    # Task and model configuration
    parser.add_argument('--tasks', type=str,
                       default='mortality,readmission,los,sepsis',
                       help='Comma-separated list of tasks to evaluate')
    parser.add_argument('--models', type=str,
                       default=None,
                       help='Comma-separated list of models to evaluate (e.g., logisticregression,randomforest,mlp)')
    
    args = parser.parse_args()
    main(args)
# Predictive Modeling Pipeline

Machine learning pipeline for ICU outcome prediction using OMOP CDM data.

## Overview

Predicts 4 clinical outcomes:
- **Mortality**: 7-day and 30-day ICU mortality
- **Readmission**: 7-day, 30-day, and 90-day readmission  
- **Length of Stay**: >3 days, >7 days
- **Sepsis**: Post-ICU sepsis detection (24h, 48h, during ICU)

## Structure

```
predictive_modeling/
├── 1_feature_engineering/     # Feature extraction with time windows
│   ├── simple_time_series_extractor.py
│   └── run_feature_extraction.py
├── 2_model_training/          # Separated traditional & deep learning
│   ├── run_traditional_models.py
│   └── run_DL_models.py
├── 3_evaluation/              # Comprehensive model evaluation
│   └── run_evaluation.py
├── config/                    # Configuration files (JSON format)
├── notebooks/                 # Jupyter notebooks
├── modeling_results/          # Generated outputs
│   ├── features/             # Extracted features
│   ├── models/               # Trained models
│   └── evaluation/           # Evaluation results
└── run_all_modules.py         # Pipeline orchestrator
```

## Quick Start

```bash
# Run complete pipeline (traditional ML models only)
python predictive_modeling/run_all_modules.py

# Run with specific time window (2, 4, or 8 hours)
python predictive_modeling/run_all_modules.py --time-window 8

# Include deep learning models 
python predictive_modeling/run_all_modules.py --include-dl

# Run individual modules
python predictive_modeling/1_feature_engineering/run_feature_extraction.py --time-window 4
python predictive_modeling/2_model_training/run_traditional_models.py
python predictive_modeling/3_evaluation/run_evaluation.py
```

## Modules

### 1. Feature Engineering (`1_feature_engineering/`)

**Configurable Time Series Features:**
- **Time Windows**: 2H, 4H, or 8H configurable windows
- **Observation Period**: 24H, 48H, or 72H minimum ICU stay
- **Unified Features**: Single feature file for mortality/readmission/LOS tasks
- **Static Features**: Separate file for sepsis prediction (demographics + conditions)

**Feature Types:**
- Demographics: age, gender, race, ethnicity  
- Time Series: vital signs, lab values (median aggregation per window)
- Static: Pre-ICU conditions and procedures (top 50 each)
- Labels: All prediction targets included

### 2. Model Training (`2_model_training/`)

**Traditional Models (`run_traditional_models.py`):**
- Logistic Regression, Random Forest, Gradient Boosting, XGBoost
- Automatic hyperparameter optimization
- SMOTE for imbalanced classes
- Cross-validation with early stopping

**Deep Learning Models (`run_DL_models.py`):**
- Multi-Layer Perceptron (MLP)
- Residual Networks (ResNet) 
- Transformer models
- PyTorch implementation with GPU support

### 3. Model Evaluation (`3_evaluation/`)

**Comprehensive Evaluation:**
- **Metrics**: AUROC, AUPRC, Sensitivity, Specificity
- **Visualizations**: ROC curves, PR curves, calibration plots
- **Automatic Model Discovery**: Finds all trained models
- **Comparative Analysis**: Performance across all tasks and models
- **Detailed Reports**: Markdown reports with results summary

## Data Flow

```text
extracted_patient_data/ → Feature Extraction → modeling_results/features/
                                                      ↓
                                              Model Training → modeling_results/models/
                                                                      ↓
                                                              Model Evaluation → modeling_results/evaluation/
```

## Command Line Interface

### Pipeline Orchestrator (`run_all_modules.py`)

**Basic Usage:**
```bash
# Default pipeline (4H windows, 24H observation, traditional models)
python predictive_modeling/run_all_modules.py

# Custom time window and observation period
python predictive_modeling/run_all_modules.py --time-window 8

# Include deep learning models
python predictive_modeling/run_all_modules.py --include-dl

# Skip specific modules
python predictive_modeling/run_all_modules.py --skip-modules evaluation

# Continue on errors
python predictive_modeling/run_all_modules.py --continue-on-error
```

**Module-Specific Options:**

**Feature Extraction:**
```bash
python predictive_modeling/1_feature_engineering/run_feature_extraction.py \
  --time-window 4 \
  --min-observation 24 \
  --output-dir predictive_modeling/modeling_results/features
```

**Traditional Model Training:**
```bash
python predictive_modeling/2_model_training/run_traditional_models.py \
  --time-window 4 \
  --tasks mortality,readmission,los,sepsis \
  --models LogisticRegression,RandomForest,XGBoost \
  --use-smote
```

**Deep Learning Training:**
```bash
python predictive_modeling/2_model_training/run_DL_models.py \
  --time-window 4 \
  --model-type MLP \
  --epochs 100 \
  --batch-size 32
```

## Configuration

**Time Windows & Observation Periods:**
- **Time Windows**: 2H, 4H, 8H (feature aggregation intervals)
- **Observation Periods**: 24H, 48H, 72H (minimum ICU stay)
- **Default**: 4H windows with 24H observation period

**Prediction Tasks:**
- `mortality`: 7-day, 30-day mortality
- `readmission`: 7-day, 30-day, 90-day readmission  
- `los`: Length of stay >3 days, >7 days
- `sepsis`: 24H, 48H, during ICU sepsis

**Model Types:**
- **Traditional**: LogisticRegression, RandomForest, GradientBoosting, XGBoost
- **Deep Learning**: MLP, ResNet, Transformer

## Requirements

```bash
# Core dependencies
pip install pandas numpy scikit-learn xgboost imbalanced-learn 
pip install matplotlib seaborn tqdm

# Deep learning (optional)
pip install torch torchvision

# Jupyter notebooks (optional) 
pip install jupyter ipykernel
```

**Note**: `pathlib` is included in Python 3.4+ standard library.

## Output Structure

```text
modeling_results/
├── features/
│   ├── time_series_features_4h_window.csv    # Unified features
│   ├── sepsis_static_features.csv            # Static features
│   └── feature_extraction_summary.md
├── models/
│   ├── traditional/
│   │   ├── logisticregression/
│   │   ├── randomforest/
│   │   └── xgboost/
│   └── deep_learning/
│       ├── mlp/
│       └── resnet/
├── evaluation/
│   ├── plots/
│   │   ├── mortality_roc_curves.png
│   │   └── readmission_pr_curves.png
│   ├── evaluation_results.json
│   └── evaluation_report.md
└── pipeline_report.md
```

## Data Requirements

- **Input**: Patient data in `extracted_patient_data/*/patient_labels.json`
- **OMOP Tables**: PERSON, VISIT_OCCURRENCE, MEASUREMENT, etc.
- **Labels**: Pre-computed outcome labels for all tasks

## Windows Compatibility

All code uses ASCII-compatible characters and cross-platform paths. No Unicode symbols that might cause issues on Windows systems.

## Support

For issues or questions, please open an issue in the repository.
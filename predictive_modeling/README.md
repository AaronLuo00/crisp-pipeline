# Predictive Modeling Pipeline

Machine learning pipeline for ICU outcome prediction using OMOP CDM data.

## Overview

Predicts 4 clinical outcomes:
- **Mortality**: 48-hour, 7-day, and 30-day ICU mortality
- **Readmission**: 7-day, 30-day, and 90-day readmission
- **Length of Stay**: >3 days, >7 days
- **Sepsis**: Post-ICU sepsis detection

## Structure

```
predictive_modeling/
├── 1_feature_engineering/     # Feature extraction
├── 2_model_training/          # Model training
├── 3_evaluation/              # Model evaluation
├── config/                    # Configuration files
├── notebooks/                 # Jupyter notebooks
└── run_all_modules.py         # Pipeline orchestrator
```

## Quick Start

```bash
# Run complete pipeline (all traditional ML models)
python run_all_modules.py

# Run only logistic regression
python run_all_modules.py --config config/logistic_only.yaml

# Run individual modules
python 1_feature_engineering/run_feature_extraction.py
python 2_model_training/run_model_training.py --config config/logistic_only.yaml
python 3_evaluation/run_evaluation.py
```

## Modules

### 1. Feature Extraction
Extracts features from patient data:
- Demographics, ICU information
- Vital signs, laboratory values
- Medications, conditions, procedures

### 2. Model Training
Trains ML models:
- Logistic Regression, Random Forest
- Gradient Boosting, XGBoost
- Handles class imbalance with SMOTE

### 3. Model Evaluation
Evaluates performance:
- AUROC, AUPRC, Sensitivity, Specificity
- ROC and PR curves
- Modeling reports

## Data Flow

```
Patient Data → Feature Extraction → Feature Files
                                          ↓
                                    Model Training → Trained Models
                                                           ↓
                                                    Model Evaluation → Results
```

## Configuration

### Available Configurations
- `config/default_config.yaml` - All traditional models
- `config/logistic_only.yaml` - Only logistic regression
- `config/traditional_only.yaml` - All traditional ML
- `config/full_pipeline.yaml` - Traditional + deep learning

### Config Structure
```yaml
models:
  traditional:
    enabled: true
    types: [LogisticRegression, RandomForest, GradientBoosting, XGBoost]

training:
  test_size: 0.2
  use_smote: true

tasks:
  mortality: true
  readmission: true
```

## Requirements

```bash
pip install pandas numpy scikit-learn xgboost imbalanced-learn matplotlib seaborn pyyaml
```

## Data Requirements

- Patient labels in `../extracted_patient_data/*/patient_labels.json`
- OMOP CDM tables from extraction module

## Support

For issues or questions, please open an issue in the repository.
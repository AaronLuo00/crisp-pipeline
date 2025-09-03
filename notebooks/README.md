# CRISP Pipeline Notebooks

This directory contains interactive Jupyter notebooks demonstrating key functionalities of the CRISP pipeline.

## Available Notebooks

### 1. Data Quality Exploration (`01_data_quality_exploration.ipynb`)

**Purpose**: Explore and assess data quality of OMOP CDM datasets

**Key Features**:
- Data quality checks for OMOP CDM tables
- Statistical summaries and distributions
- Missing data analysis
- Temporal coverage assessment
- Data consistency validation

**Usage**:
- Start with this notebook to understand your data
- Identify potential data quality issues before modeling
- Generate data quality reports

### 2. Predictive Modeling Pipeline (`02_ml_modeling_example.ipynb`)

**Purpose**: Complete end-to-end predictive modeling pipeline demonstration

**What This Notebook Does**:
1. **Feature Extraction** - Extract time series and static features from OMOP data
2. **Traditional Model Training** - Train LogisticRegression, RandomForest, GradientBoosting, XGBoost
3. **Deep Learning Training** - Train MLP, LSTM, and TCN models
4. **Model Evaluation** - Generate performance metrics and visualizations
5. **Results Analysis** - Compare models and provide recommendations

**Clinical Prediction Tasks**:
- **Mortality**: 7-day, 30-day ICU mortality
- **Readmission**: 7-day, 30-day, 90-day readmission
- **Length of Stay**: >3 days, >7 days
- **Sepsis**: 24h, 48h, during ICU sepsis

**Key Features**:
- Interactive step-by-step execution
- Real-time progress monitoring
- Comprehensive performance visualizations
- Error handling with helpful messages

## Getting Started

### Prerequisites

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure you have patient data extracted:
```bash
# Run the CRISP pipeline modules 1-5 first
# Data should be in ../extracted_patient_data/
```

## Output Structure

The predictive modeling notebook generates outputs in:

```
../predictive_modeling/modeling_results/
├── features/                    # Extracted features
├── models/                      # Trained models
│   ├── traditional/            # Traditional ML models
│   └── deep_learning/          # Deep learning models
├── evaluation/                  # Performance metrics
│   ├── plots/                  # ROC/PR curves
│   └── evaluation_report.md    # Detailed report
└── pipeline_report.md          # Pipeline execution summary
```

## Additional Resources

- [CRISP Pipeline Documentation](../README.md)
- [Predictive Modeling Module](../predictive_modeling/README.md)
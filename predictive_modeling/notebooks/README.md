# Predictive Modeling Notebooks

Interactive Jupyter notebook for demonstrating the complete CRISP predictive modeling pipeline.

## Notebook Overview

### `predictive_modeling_pipeline.ipynb`
**End-to-End Pipeline Demonstration**

A comprehensive, interactive walkthrough of the entire predictive modeling process from data to deployment.

**What it demonstrates:**
- **Data Verification** - Checks patient data availability
- **Feature Extraction** - Time series (4H windows) + static features  
- **Traditional ML Training** - LogisticRegression, RandomForest, GradientBoosting, XGBoost
- **Deep Learning Training** - MLP (Multi-Layer Perceptron) models
- **Model Evaluation** - AUROC/AUPRC metrics, ROC curves, reports
- **Results Analysis** - Performance comparison and recommendations

**Clinical prediction tasks:**
- **Mortality**: 7-day, 30-day ICU mortality
- **Readmission**: 7-day, 30-day, 90-day readmission
- **Length of Stay**: >3 days, >7 days
- **Sepsis**: 24h, 48h, during ICU sepsis

## Usage

### Running the Notebook

1. **Start Jupyter**:
   ```bash
   cd predictive_modeling/notebooks
   jupyter notebook
   ```

2. **Open the notebook**: `predictive_modeling_pipeline.ipynb`

3. **Execute cells sequentially**: 
   - Use "Run All" for complete pipeline execution
   - Or run cells step-by-step to see each stage

### Interactive Features

**Real-time Pipeline Execution:**
- Each step executes the actual pipeline modules
- Displays progress and results as they happen
- Shows error messages if steps fail
- Provides skip logic if dependencies missing

**Comprehensive Analysis:**
- Automatic results loading and comparison
- Performance visualizations (box plots, scatter plots)
- Top model rankings with recommendations
- Clinical deployment strategy

**Visual Outputs:**
- Model performance comparisons
- AUROC vs AUPRC scatter plots  
- Task-specific performance analysis
- Pipeline execution summary

## Key Features

### End-to-End Execution
The notebook mirrors `run_all_modules.py` functionality but with:
- **Interactive visualization** of each step
- **Error handling** with helpful messages

### Step-by-Step Walkthrough
1. **Data Check**: Verifies patient data availability
2. **Feature Extraction**: Runs feature engineering modules
3. **Traditional Training**: Trains 4 traditional ML models
4. **Deep Learning**: Trains MLP neural network models
5. **Evaluation**: Generates comprehensive performance metrics
6. **Analysis**: Compares results and provides recommendations
7. **Summary**: Shows complete pipeline execution status

## Output Structure

The notebook generates the same outputs as the command-line pipeline:

```
../modeling_results/
├── features/                    # Extracted features
├── models/                      # Trained models
│   ├── traditional/            # Traditional ML models
│   └── deep_learning/          # Deep learning models
├── evaluation/                  # Performance metrics
│   ├── plots/                  # ROC/PR curves
│   └── evaluation_report.md    # Detailed report
└── pipeline_comparison_summary.csv # Results comparison
```


# Configuration Files

This directory contains configuration files for the CRISP pipeline.

## Environment Setup Files

### requirements.txt
Python package dependencies for pip installation.

**Usage:**
```bash
pip install -r config/requirements.txt
```

### environment.yml
Conda/Mamba environment specification with all dependencies.

**Usage:**
```bash
conda env create -f config/environment.yml
conda activate crisp-pipeline
```

## Python Version

The project uses **Python 3.8.13** (specified in `.python-version` in the root directory).

## Package Dependencies

Core packages include:
- **pandas** (>=1.3.0): Data manipulation
- **numpy** (>=1.21.0): Numerical computing
- **tqdm** (>=4.62.0): Progress bars
- **matplotlib** (>=3.4.0): Plotting
- **seaborn** (>=0.11.0): Statistical visualization
- **scipy** (>=1.7.0): Scientific computing
- **scikit-learn** (>=1.0.0): Machine learning
- **jupyter**: Notebook support
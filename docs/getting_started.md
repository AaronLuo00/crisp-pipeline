# Getting Started with CRISP Pipeline

This guide covers the basics of setting up and running CRISP.

## Requirements

- Python 3.8+ (tested with Python 3.8.13)
- 16GB RAM recommended

## Quick Start

### 1. Installation

```bash
git clone https://github.com/AaronLuo00/crisp-pipeline.git
cd crisp-pipeline
pip install -r requirements.txt
```

### 2. Data Setup

> **Important**: We strongly recommend testing with a small sample first, even if you have your full dataset ready.

**Decision: Do you already have OMOP data in the data/ directory?**

**NO - Starting fresh (Recommended path):**
```bash
# Create data directory
mkdir data

# Sample from your existing OMOP data location
python data_preparation/sample_patients.py \
    --input-dir /path/to/your/OMOP_data/ \
    --output-dir data/ \
    --sample-size 1000 \
    --extract-all
```

**YES - Already have full dataset in data/:**
```bash
# We still recommend testing with a sample first:
python data_preparation/sample_patients.py \
    --input-dir data/ \
    --output-dir data_sample/ \
    --sample-size 1000 \
    --extract-all

# After successful test, you can run on full dataset in data/
```

**Note**: Processing large datasets (>100GB) without testing can lead to hours of wasted computation if configuration is incorrect.

### 3. Validate Data (Recommended)

```bash
# Validate your OMOP CDM data before running pipeline
python data_preparation/validate_data.py --data-dir data/
```

This checks:
- Required tables existence
- OMOP CDM v5.3 schema compliance
- Basic data quality

### 4. Run Pipeline

```bash
# Option 1: Run complete pipeline
python pipeline_modules/run_all_module.py

# Option 2: Run individual modules
python pipeline_modules/1_eda/run_eda_analysis.py
python pipeline_modules/2_cleaning/run_data_cleaning.py
# ... etc
```

## Output Structure

```
output/
├── 1_eda/           # Data exploration results
├── 2_cleaning/      # Cleaned data
├── 3_mapping/       # Concept-mapped data
├── 4_standardization/ # Standardized data
└── 5_extraction/    # ML-ready datasets
```

## Common Issues

- **Memory errors**: Reduce chunk size or use larger machine
- **Missing mappings**: Check vocabulary tables
- **Slow processing**: Use SSD storage

## Next Steps

See [Pipeline Guide](pipeline_guide.md) for detailed documentation.
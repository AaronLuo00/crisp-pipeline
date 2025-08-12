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

```bash
# Create data directory
mkdir data

# Option 1: Sample from your existing OMOP data location (recommended)
python utils/sample_patients.py \
    --input /path/to/your/OMOP_data/PERSON.csv \
    --output data/PERSON.csv \
    --sample-size 1000

# Option 2: For full dataset (not recommended for initial testing)
# Copy all OMOP CDM files directly to data/ directory
```

### 3. Run Pipeline

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
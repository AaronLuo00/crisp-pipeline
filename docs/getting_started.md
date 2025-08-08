# Getting Started with CRISP Pipeline

This guide covers the basics of setting up and running CRISP.

## Requirements

- Python 3.8+
- 16GB RAM recommended

## Quick Start

### 1. Installation

```bash
git clone https://github.com/yourusername/crisp-pipeline.git
cd crisp-pipeline
pip install pandas numpy tqdm matplotlib seaborn
```

### 2. Data Setup

```bash
# Create directories
mkdir raw_data data

# Place your OMOP CDM files in raw_data/
# Files needed: PERSON.csv, MEASUREMENT.csv, etc.
```

### 3. Run Pipeline

```bash
# Option 1: Run complete pipeline
python main/run_pipeline.py

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
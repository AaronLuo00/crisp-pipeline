# Data Preparation Tools

This directory contains utilities for preparing and validating OMOP CDM data before running the CRISP pipeline.

## Available Tools

### 1. sample_patients.py
Stratified patient sampling with automatic extraction of related records from all OMOP tables.

**Features:**
- Proportional sampling across demographics (age, gender, race)
- Automatic extraction of all related medical records
- Maintains referential integrity across tables
- Memory-efficient processing for large datasets

**Usage:**
```bash
# Sample 1000 patients and extract all related records
python data_preparation/sample_patients.py \
  --input-dir /path/to/OMOP/data/ \
  --output-dir data/ \
  --sample-size 1000 \
  --extract-all
```

### 2. validate_data.py
Comprehensive validation tool for OMOP CDM data quality and compliance.

**Features:**
- Table existence verification (14 required tables)
- Schema validation against OMOP CDM v5.3
- Data quality checks (duplicates, nulls, temporal consistency)
- Detailed professional reporting

**Usage:**
```bash
# Validate data in the data/ directory
python data_preparation/validate_data.py --data-dir data/
```

**Validation Steps:**
1. **Table Existence**: Checks for all required OMOP tables
2. **Schema Validation**: Verifies required columns are present
3. **Data Quality**: Checks for duplicates, null values, and data consistency

## Recommended Workflow

1. **Sample your data** using `sample_patients.py`
2. **Validate the data** using `validate_data.py`
3. **Run the pipeline** once validation passes

## Important Notes

- Always validate data before running the pipeline
- Use small samples (100-1000 patients) for initial testing
- The tools handle both sampled and full datasets
- All tools use memory-efficient processing for large files
# CRISP Pipeline Modules

This directory contains the 5-stage CRISP-DM pipeline modules for processing OMOP CDM data.

## Module Overview

### 1. EDA Module (`1_eda/`)
**Purpose**: Exploratory Data Analysis
- `run_eda_analysis.py` - Main EDA script with comprehensive analysis
- `simple_eda.py` - Simplified EDA for quick overview

### 2. Cleaning Module (`2_cleaning/`)
**Purpose**: Data cleaning and quality assurance
- `run_data_cleaning.py` - Remove duplicates, validate data integrity

### 3. Mapping Module (`3_mapping/`)
**Purpose**: Concept mapping and vocabulary standardization
- `run_concept_mapping.py` - Main concept mapping script
- `mapping_frequency/` - Analysis files and processed mappings
  - `*_id_analysis.csv` - Concept frequency analysis for each table
  - `processed_mappings/` - Pre-processed mapping references

### 4. Standardization Module (`4_standardization/`)
**Purpose**: Data normalization and standardization
- `run_data_standardization.py` - Standardize formats, remove outliers

### 5. Extraction Module (`5_extraction/`)
**Purpose**: Feature extraction and cohort selection
- `run_icu_extraction.py` - Extract ICU cohort data

## Usage

### Run Individual Modules
```bash
# EDA Analysis
python pipeline_modules/1_eda/run_eda_analysis.py

# Data Cleaning
python pipeline_modules/2_cleaning/run_data_cleaning.py

# Concept Mapping
python pipeline_modules/3_mapping/run_concept_mapping.py

# Data Standardization
python pipeline_modules/4_standardization/run_data_standardization.py

# ICU Extraction
python pipeline_modules/5_extraction/run_icu_extraction.py
```

### Run Complete Pipeline
```bash
python pipeline_modules/run_all_module.py
```

## Requirements

- Python 3.8+
- pandas, numpy, matplotlib, seaborn, tqdm
- Sufficient memory for processing large OMOP datasets
- Input data should be in `data/` directory

## Output

Each module generates results in the `output/` directory with:
- Processed data files
- Analysis reports
- Visualization plots
- Summary statistics
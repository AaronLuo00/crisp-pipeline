# CRISP Pipeline Modules

This directory contains the 5-stage CRISP-DM pipeline modules for processing OMOP CDM data.

## Module Overview

### 1. EDA Module (`1_eda/`)
**Purpose**: Exploratory Data Analysis
- `run_eda_analysis.py` - Comprehensive statistical analysis and data profiling
- `simple_eda.py` - Quick data overview and basic statistics

### 2. Cleaning Module (`2_cleaning/`)
**Purpose**: Data cleaning and quality assurance
- `run_data_cleaning.py` - Remove duplicates, validate data integrity, handle missing concept values

### 3. Mapping Module (`3_mapping/`)
**Purpose**: Concept mapping and vocabulary standardization
- `run_concept_mapping.py` - Map concepts to SNOMED-CT vocabulary
- `mapping_frequency/` - Concept frequency analysis and mapping references
  - `*_analysis.csv` - Frequency analysis for each OMOP table
  - `processed_mappings/` - Pre-processed SNOMED mapping references

### 4. Standardization Module (`4_standardization/`)
**Purpose**: Data normalization and standardization
- `run_data_standardization.py` - Standardize formats, remove outliers, calculate statistics
- `parallel_tdigest.py` - Efficient percentile calculation for large datasets
- `parallel_standardization.py` - Distributed processing for standardization tasks
- `visit_concept_merger.py` - Merge consecutive visit records

### 5. Extraction Module (`5_extraction/`)
**Purpose**: Feature extraction and cohort selection
- `run_icu_extraction.py` - Extract ICU cohort data

## Performance Optimizations

The pipeline has been optimized with parallel processing capabilities:
- **Parallel Processing**: All modules support concurrent execution for improved performance
- **Memory Optimization**: Chunk-based processing reduces memory footprint from O(n) to O(chunk_size)
- **T-Digest Algorithm**: Memory-efficient percentile calculation for statistical analysis

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
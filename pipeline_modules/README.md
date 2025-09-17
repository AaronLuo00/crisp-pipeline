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
- Uses mapping resources from `mapping_resources/` (at project root)
  - `mapping_resources/original_mappings/` - Frequency analysis for each OMOP table
  - `mapping_resources/processed_mappings/` - Pre-processed SNOMED mapping references

### 4. Standardization Module (`4_standardization/`)
**Purpose**: Data normalization and standardization
- `run_data_standardization.py` - Standardize formats, remove outliers, calculate statistics
- `parallel_tdigest.py` - Efficient percentile calculation for large datasets
- `parallel_standardization.py` - Distributed processing for standardization tasks
- `visit_concept_merger.py` - Merge consecutive visit records

### 5. Extraction Module (`5_extraction/`)
**Purpose**: Feature extraction and cohort selection
- `run_icu_extraction.py` - Extract ICU cohort data
- **Output**: Patient data saved to `extracted_patient_data/` at project root
- **Reports**: Statistics and summaries saved to `output/5_extraction/`

**Note on Visit Timestamps**: The extracted data contains visit records where start time equals end time. This originates from the raw OMOP CDM data. These records are preserved as they still represent valid medical events.

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
# Run from project root directory
python pipeline_modules/run_all_module.py

# Optional parameters
python pipeline_modules/run_all_module.py --skip-modules 3_mapping 4_standardization
python pipeline_modules/run_all_module.py --start-from 3_mapping
python pipeline_modules/run_all_module.py --quiet  # Reduce output verbosity
```

## Requirements

- Python 3.8+
- pandas, numpy, matplotlib, seaborn, tqdm
- Sufficient memory for processing large OMOP datasets
- Input data should be in `data/` directory

## Output Structure

### Pipeline Execution Outputs

When running the complete pipeline with `run_all_module.py`, outputs are organized in a unified structure:

```text
output/pipeline_runs/run_YYYYMMDD_HHMMSS/
├── pipeline_config.json       # Pipeline configuration
├── pipeline_log.txt           # Combined execution log
├── pipeline_report.md         # Execution summary report
├── module_results/            # Module result JSON files
│   ├── 1_eda_result.json
│   ├── 2_cleaning_result.json
│   ├── 3_mapping_result.json
│   ├── 4_standardization_result.json
│   └── 5_extraction_result.json
└── module_logs/               # Individual module log files
    ├── 1_eda.log
    ├── 2_cleaning.log
    ├── 3_mapping.log
    ├── 4_standardization.log
    └── 5_extraction.log
```

This unified structure provides:

- **Traceability**: Each run is timestamped and self-contained
- **Centralized Logs**: All logs and results in one location
- **Easy Debugging**: Module-specific logs alongside combined pipeline log

### Module-Specific Outputs

Each module also generates outputs in its respective directory:

#### Reports and Analytics (`output/`)

Each module generates results in its respective subdirectory:

- Processed intermediate data files
- Statistical analysis reports  
- Visualization plots

#### Patient-Level Data (`extracted_patient_data/`)

The extraction module saves final patient data separately at the project root:

- **Location**: `extracted_patient_data/` (not in `output/`)
- **Structure**: `<patient_id>/<table_name>.csv`
- **Content**: Complete OMOP CDM records for each patient
- **Purpose**: Direct consumption by ML pipelines without navigating deep folder structures

This separation ensures:

- Clean distinction between pipeline artifacts and final data products
- Easy access to patient data for downstream analysis

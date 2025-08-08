# CRISP Pipeline Guide

Overview of the CRISP pipeline modules.

## Pipeline Flow

```
Raw Data → [EDA] → [Cleaning] → [Mapping] → [Standardization] → [Extraction] → ML-Ready
```

## Module 1: Exploratory Data Analysis

**Purpose**: Analyze dataset characteristics

**Usage**:
```bash
python pipeline_modules/1_eda/run_eda_analysis.py
```

**Key Features**:
- Table statistics and data quality metrics
- Missing data analysis
- Cohort identification

## Module 2: Data Cleaning

**Purpose**: Ensure data quality and consistency

**Usage**:
```bash
python pipeline_modules/2_cleaning/run_data_cleaning.py
```

**Operations**:
- Duplicate removal
- Invalid concept ID filtering
- Temporal validation
- Column pruning (>95% missing)

## Module 3: Concept Mapping

**Purpose**: Map medical vocabularies to SNOMED CT

**Usage**:
```bash
python pipeline_modules/3_mapping/run_concept_mapping.py
```

**Supports**:
- LOINC, RxNorm, ICD codes → SNOMED
- 20+ vocabulary sources

## Module 4: Data Standardization

**Purpose**: Normalize values and formats

**Usage**:
```bash
python pipeline_modules/4_standardization/run_data_standardization.py
```

**Features**:
- DateTime to ISO 8601
- Unit conversions
- Outlier detection
- Visit merging

## Module 5: Feature Extraction

**Purpose**: Create ML-ready datasets

**Usage**:
```bash
python pipeline_modules/5_extraction/run_icu_extraction.py
```

**Features**:
- Cohort extraction
- Temporal windows
- Train/val/test splitting

## Configuration

Basic configuration in `config/pipeline_config.yaml`:

```yaml
input_dir: "data/"
output_dir: "output/"
chunk_size: 100000
```

## Troubleshooting

- **Memory errors**: Reduce chunk_size
- **Slow processing**: Enable parallel processing
- **Missing mappings**: Check vocabulary tables

For detailed documentation, check individual module files.
# Mapping Resources

This directory contains concept mapping resources used by the CRISP pipeline to standardize medical vocabularies across different OMOP CDM tables.

## Directory Structure

```
mapping_resources/
├── *_concept_mapping.csv           # Unified concept mapping files
├── original_mappings/              # Original frequency analysis from source data
│   └── *_concept_id_analysis.csv  # Concept frequency counts per table
└── processed_mappings/             # SNOMED-CT mapping references
    ├── *_mapping_reference.csv    # Mapping lookup tables
    ├── *_snomed_mapped.csv        # SNOMED-mapped concepts
    └── process_*_mappings.py      # Processing scripts
```

## File Descriptions

### Root Level: Unified Mapping Files
Files matching pattern `*_concept_mapping.csv` contain the final, unified concept mappings for each OMOP table. These are generated from the processed mappings and used directly by the pipeline.

**Examples:**
- `MEASUREMENT_concept_mapping.csv` - Lab tests and vital signs mappings
- `DRUG_EXPOSURE_drug_concept_mapping.csv` - Medication concept mappings
- `CONDITION_OCCURRENCE_condition_concept_mapping.csv` - Diagnosis mappings

### original_mappings/
Contains the original concept frequency analysis from the source data. These files show concept usage patterns and help identify mapping priorities.

**File Pattern:** `{TABLE_NAME}_{concept_field}_analysis.csv`

**Key Files:**
- `MEASUREMENT_measurement_concept_id_analysis_complete.csv` - Complete measurement concept frequencies
- `PROCEDURE_OCCURRENCE_procedure_concept_id_analysis_complete.csv` - Procedure concept frequencies
- `PERSON_*_concept_id_analysis.csv` - Demographics concept mappings (gender, race, ethnicity)

### processed_mappings/
Contains SNOMED-CT standardized mappings and the scripts to generate them.

**Mapping Reference Files:** `{TABLE_NAME}_mapping_reference.csv`
- Maps source concepts to standard SNOMED-CT codes
- Includes concept names and mapping confidence

**Processing Scripts:** `process_*_mappings.py`
- Scripts to generate unified mappings from reference files
- Can be run to update mappings when references change


## Usage in Pipeline

The mapping module (`pipeline_modules/3_mapping/run_concept_mapping.py`) uses these resources to:

1. Load appropriate mapping files based on table being processed
2. Apply concept standardization to source data
3. Output standardized data with SNOMED-CT codes

## Mapping Sources

Our concept mappings are derived from:
- **CRITICAL Data Codebook**: https://critical.fsm.northwestern.edu/data-codebook
- **OHDSI Athena**: https://athena.ohdsi.org/search-terms/start


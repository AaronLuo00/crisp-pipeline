# Processed OMOP Table Mappings

This directory contains processed versions of four OMOP table concept mappings with SNOMED CT conversions applied.

## Files

### 1. `DEVICE_EXPOSURE_snomed_mapped.csv`
A processed version of the original `DEVICE_EXPOSURE_device_concept_id_analysis.csv` file where:
- All original SNOMED concepts are preserved (190 concepts)
- Non-SNOMED concepts with available SNOMED mappings have been replaced (20 concepts)
- Unmapped non-SNOMED concepts remain unchanged (519 concepts)
- Additional columns track the mapping process:
  - `Original_Id`, `Original_Code`, `Original_Name`, `Original_Vocab`: Original concept information
  - `Mapping_Applied`: "Yes" if SNOMED mapping was applied, "No" otherwise
  - `Mapping_Relationship`: The type of mapping relationship (e.g., "Is a", "Subsumes")

### 2. `DEVICE_EXPOSURE_mapping_reference.csv`
A reference file containing only the successfully mapped concepts (20 entries) for use by `run_concept_mapping.py`:
- Columns: `original_concept_id`, `original_code`, `original_name`, `original_vocab`, `snomed_concept_id`, `snomed_concept_code`, `snomed_concept_name`, `snomed_vocab`, `mapping_relationship`, `frequency`
- This serves as a lookup table for the mapping pipeline

### 3. `process_device_exposure_mappings.py`
The Python script used to generate these files from:
- Source: `../original_mappings/DEVICE_EXPOSURE_device_concept_id_analysis.csv`
- Mappings: `../non_snomed_concepts/mapping_results/device_exposure_snomed_mappings.csv`

## Mapping Statistics

- **Total concepts**: 729
- **Already SNOMED**: 190 (26.1%)
- **Mapped to SNOMED**: 20 (2.7%)
- **Unmapped**: 519 (71.2%)

## Key Findings

1. Most mapped concepts are HCPCS codes (19 out of 20) and 1 CPT4 code
2. The SNOMED mappings are primarily to procedure concepts rather than device concepts:
   - "Application of dressing" (4 mappings)
   - "Replacement of device" (3 mappings)
   - "Provision of orthosis" (3 mappings)
   - "Amputation" procedures (3 mappings)
   - Other procedures (7 mappings)
3. This reflects that many HCPCS codes represent services/procedures related to devices rather than the devices themselves

## Summary Statistics

| Table | Total Concepts | Already SNOMED | Mapped to SNOMED | Unmapped | Mapped to Existing |
|-------|----------------|----------------|------------------|----------|-------------------|
| DEVICE_EXPOSURE | 729 | 190 (26.1%) | 20 (2.7%) | 519 (71.2%) | 0 |
| MEASUREMENT | 7,835 | 380 (4.9%) | 2,346 (29.9%) | 5,109 (65.2%) | 254 (3.2%) |
| OBSERVATION | 6,910 | 4,970 (71.9%) | 173 (2.5%) | 1,767 (25.6%) | 120 (1.7%) |
| PROCEDURE_OCCURRENCE | 33,161 | 5,893 (17.8%) | 20,378 (61.5%) | 6,890 (20.8%) | 1,128 (3.4%) |

## Table-Specific Details

### DEVICE_EXPOSURE
- Most mapped concepts are HCPCS codes (19 out of 20) and 1 CPT4 code
- All mappings are to NEW SNOMED concepts (procedures rather than devices)
- Reflects that HCPCS codes represent services/procedures related to devices

### MEASUREMENT
- 2,346 LOINC concepts mapped to SNOMED
- 254 mappings are to EXISTING SNOMED concepts (overlapping)
- Core vital signs and common lab tests included
- Deduplication needed in standardization phase

### OBSERVATION
- High proportion already in SNOMED (71.9%)
- Only 173 new mappings, mostly from LOINC and UK Biobank
- 120 mappings overlap with existing SNOMED concepts
- Many unmapped concepts are survey response codes

### PROCEDURE_OCCURRENCE
- Largest mapping set with 20,378 concepts mapped
- Primarily ICD10PCS and ICD9Proc codes (all mapped)
- 6,548 CPT4 codes remain unmapped (no mapping file available)
- 1,128 mappings overlap with existing concepts

## Processing Scripts

Each table has its own processing script:
- `process_device_exposure_mappings.py`
- `process_measurement_mappings.py`
- `process_observation_mappings.py`
- `process_procedure_occurrence_mappings.py`

## Important Notes

1. **Overlapping Concepts**: MEASUREMENT, OBSERVATION, and PROCEDURE_OCCURRENCE tables have mappings to existing SNOMED concepts. This creates potential duplicates that will be handled in the `4_standardization` module.

2. **Missing Mapping Files**: CPT4 vocabulary has no available SNOMED mapping file, affecting primarily PROCEDURE_OCCURRENCE table.

3. **Domain Preservation**: Unlike DEVICE_EXPOSURE (where mapped concepts become Procedure domain), other tables maintain their original domains.

## Usage

The `*_snomed_mapped.csv` files can be used as drop-in replacements for the original analysis files, with SNOMED concepts preferred where available. The `*_mapping_reference.csv` files provide clean mapping tables for integration into the broader CRISP pipeline.

## Future Work

- Additional mappings can be added to the source files in `non_snomed_concepts/mapping_results/`
- CPT4 to SNOMED mappings would significantly improve PROCEDURE_OCCURRENCE coverage
- Deduplication logic will be implemented in the `4_standardization` module
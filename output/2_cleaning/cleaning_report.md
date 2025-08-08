# Data Cleaning Report - subdataset_1000

**Date**: 2025-08-08 18:21:12
**Chunk Size**: 100,000 rows
**Tables Cleaned**: MEASUREMENT, OBSERVATION, DRUG_EXPOSURE, CONDITION_OCCURRENCE, VISIT_OCCURRENCE, PROCEDURE_OCCURRENCE, VISIT_DETAIL, CONDITION_ERA, SPECIMEN, DEATH, DEVICE_EXPOSURE, DRUG_ERA, OBSERVATION_PERIOD, PERSON
**Removed Records Directory**: /Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records

## Cleaning Summary

| Table | Original Records | Cleaned Records | Records Removed | Duplicates | Invalid Concept ID | Columns Removed |
|-------|-----------------|-----------------|-----------------|------------|-------------------|----------------|
| MEASUREMENT | 1,944,155 | 1,560,833 | 383,322 (19.7%) | 370,091 | 13,231 | 3 |
| OBSERVATION | 241,604 | 204,833 | 36,771 (15.2%) | 35,520 | 1,251 | 5 |
| DRUG_EXPOSURE | 204,252 | 182,525 | 21,727 (10.6%) | 12,373 | 9,016 | 6 |
| CONDITION_OCCURRENCE | 196,241 | 132,556 | 63,685 (32.5%) | 63,364 | 321 | 3 |
| VISIT_OCCURRENCE | 55,185 | 25,425 | 29,760 (53.9%) | 11,168 | 18,572 | 3 |
| PROCEDURE_OCCURRENCE | 47,395 | 46,067 | 1,328 (2.8%) | 1,225 | 103 | 2 |
| VISIT_DETAIL | 40,437 | 26,978 | 13,459 (33.3%) | 12,967 | 235 | 5 |
| CONDITION_ERA | 53,464 | 53,270 | 194 (0.4%) | 0 | 194 | 0 |
| SPECIMEN | 3,944 | 3,874 | 70 (1.8%) | 0 | 70 | 6 |
| DEATH | 113 | 112 | 1 (0.9%) | 0 | 1 | 1 |
| DEVICE_EXPOSURE | 6,338 | 5,914 | 424 (6.7%) | 424 | 0 | 3 |
| DRUG_ERA | 34,017 | 31,335 | 2,682 (7.9%) | 2,682 | 0 | 0 |
| OBSERVATION_PERIOD | 364 | 364 | 0 (0.0%) | 0 | 0 | 0 |
| PERSON | 538 | 538 | 0 (0.0%) | 0 | 0 | 4 |
| **TOTAL** | **2,828,047** | **2,274,624** | **553,423 (19.6%)** | **509,814** | **42,994** | - |

## Detailed Results

### MEASUREMENT

**Columns Removed** (>95% missing):
- measurement_source_value
- unit_source_value
- value_source_value

**Invalid Concept ID**: 13,231 records removed (concept_id = 0 or null)

**Outlier Detection**:
- value_as_number: 22,900 outliers (1.6% of 1,476,181 values)

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/MEASUREMENT.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/MEASUREMENT.csv`

### OBSERVATION

**Columns Removed** (>95% missing):
- value_as_string
- unit_concept_id
- observation_source_value
- unit_source_value
- qualifier_source_value

**Invalid Concept ID**: 1,251 records removed (concept_id = 0 or null)

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/OBSERVATION.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/OBSERVATION.csv`

### DRUG_EXPOSURE

**Columns Removed** (>95% missing):
- stop_reason
- sig
- lot_number
- drug_source_value
- route_source_value
- dose_unit_source_value

**Invalid Concept ID**: 9,016 records removed (concept_id = 0 or null)

**Temporal Issues**: 338 records with invalid date ranges

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/DRUG_EXPOSURE.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/DRUG_EXPOSURE.csv`
- Temporal Issues: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/temporal_issues/DRUG_EXPOSURE.csv`

### CONDITION_OCCURRENCE

**Columns Removed** (>95% missing):
- stop_reason
- condition_source_value
- condition_status_source_value

**Invalid Concept ID**: 321 records removed (concept_id = 0 or null)

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/CONDITION_OCCURRENCE.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/CONDITION_OCCURRENCE.csv`

### VISIT_OCCURRENCE

**Columns Removed** (>95% missing):
- visit_source_value
- admitting_source_value
- discharge_to_source_value

**Invalid Concept ID**: 18,572 records removed (concept_id = 0 or null)

**Temporal Issues**: 20 records with invalid date ranges

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/VISIT_OCCURRENCE.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/VISIT_OCCURRENCE.csv`
- Temporal Issues: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/temporal_issues/VISIT_OCCURRENCE.csv`

### PROCEDURE_OCCURRENCE

**Columns Removed** (>95% missing):
- procedure_source_value
- modifier_source_value

**Invalid Concept ID**: 103 records removed (concept_id = 0 or null)

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/PROCEDURE_OCCURRENCE.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/PROCEDURE_OCCURRENCE.csv`

### VISIT_DETAIL

**Columns Removed** (>95% missing):
- visit_detail_source_value
- admitting_source_value
- discharge_to_source_value
- discharge_to_concept_id
- visit_detail_parent_id

**Invalid Concept ID**: 235 records removed (concept_id = 0 or null)

**Temporal Issues**: 257 records with invalid date ranges

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/VISIT_DETAIL.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/VISIT_DETAIL.csv`
- Temporal Issues: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/temporal_issues/VISIT_DETAIL.csv`

### CONDITION_ERA

**Invalid Concept ID**: 194 records removed (concept_id = 0 or null)

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/CONDITION_ERA.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/CONDITION_ERA.csv`

### SPECIMEN

**Columns Removed** (>95% missing):
- disease_status_concept_id
- specimen_source_id
- specimen_source_value
- unit_source_value
- anatomic_site_source_value
- disease_status_source_value

**Invalid Concept ID**: 70 records removed (concept_id = 0 or null)

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/SPECIMEN.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/SPECIMEN.csv`

### DEATH

**Columns Removed** (>95% missing):
- cause_source_value

**Invalid Concept ID**: 1 records removed (concept_id = 0 or null)

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/DEATH.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/DEATH.csv`

### DEVICE_EXPOSURE

**Columns Removed** (>95% missing):
- device_exposure_end_date
- device_exposure_end_datetime
- device_source_value

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/DEVICE_EXPOSURE.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/DEVICE_EXPOSURE.csv`

### DRUG_ERA

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/DRUG_ERA.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/DRUG_ERA.csv`

### OBSERVATION_PERIOD

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/OBSERVATION_PERIOD.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/OBSERVATION_PERIOD.csv`

### PERSON

**Columns Removed** (>95% missing):
- person_source_value
- gender_source_value
- race_source_value
- ethnicity_source_value

**Removed Records Files**:
- Duplicates: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/duplicates/PERSON.csv`
- Invalid Concept ID: `/Users/aaron_luo/2_Harvard_Lifes/3_Research/5_Critical/0_crisp-pipeline/crisp_pipeline_public/output/2_cleaning/removed_records/invalid_concept_id/PERSON.csv`


# Patient Data Extraction Report

**Date**: 2025-08-08 18:24:58
**Dataset**: subdataset_1000
**ICU Concept IDs**: 581379, 32037

## Summary

- **Total ICU patients**: 413
- **Patients with pre-ICU analysis**: 413
- **Average records before ICU**: 40.91
- **Max records before ICU**: 840
- **Min records before ICU**: 0
- **Skipped patients**: 0

## Table Processing Summary

### Standardized Tables

| Table | Total Records | Unique Patients |
|-------|---------------|----------------|
| MEASUREMENT | 1,445,907 | 537 |
| OBSERVATION | 203,466 | 523 |
| DRUG_EXPOSURE | 173,985 | 532 |
| CONDITION_OCCURRENCE | 119,950 | 527 |
| VISIT_OCCURRENCE | 25,400 | 538 |
| VISIT_DETAIL | 26,971 | 537 |
| PROCEDURE_OCCURRENCE | 39,383 | 520 |
| DEVICE_EXPOSURE | 5,797 | 204 |
| SPECIMEN | 3,833 | 141 |
| CONDITION_ERA | 41,863 | 354 |
| DRUG_ERA | 29,499 | 364 |

### Basic Tables

| Table | Total Records | Unique Patients |
|-------|---------------|----------------|
| PERSON | 538 | 538 |
| DEATH | 112 | 112 |

## Directory Structure

```
output/5_extraction/
├── patient_data/
│   ├── {prefix}/              # Patient ID prefix (first 9 digits)
│   │   ├── {person_id}/       # Individual patient folder
│   │   │   ├── icu_visit_summary_{person_id}.csv
│   │   │   ├── PERSON.csv
│   │   │   ├── DEATH.csv (if applicable)
│   │   │   ├── MEASUREMENT.csv
│   │   │   ├── OBSERVATION.csv
│   │   │   └── ... (all other tables)
│   └── 600000071/             # Special prefix with grouping
│       ├── 000000-000999/
│       │   └── {person_id}/
│       └── ...
├── statistics/
│   ├── patient_before_icu_statistics.csv
│   └── skipped_patients.csv
└── extraction_report.md
```

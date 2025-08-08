# Comprehensive Data Processing Report

**Generated**: 2025-08-08 18:22:55
**Pipeline Module**: 3_mapping (comprehensive processing)

## Processing Configuration

- **Low frequency threshold**: ≤10
- **Outlier removal**: 0.5% lower, 0.5% upper
- **Deduplication**: Enabled
- **Visit episode window**: 2 hours

## Summary Statistics

**Total low frequency records removed**: 53,047
**Total outliers removed**: 12,217
**Total mappings applied**: 526,103
**Total duplicates removed**: 4,939

| Table | Input Rows | Low Freq Removed | Outliers Removed | Mappings Applied | Duplicates Removed | Output Rows |
|-------|------------|------------------|------------------|------------------|-------------------|-------------|
| MEASUREMENT | 1,560,833 | 7,645 | 12,217 | 524,195 | 4,869 | 1,536,102 |
| OBSERVATION | 204,833 | 1,351 | 0 | 77 | 16 | 203,466 |
| PROCEDURE_OCCURRENCE | 46,067 | 6,630 | 0 | 1,814 | 54 | 39,383 |
| DEVICE_EXPOSURE | 5,914 | 117 | 0 | 17 | 0 | 5,797 |
| CONDITION_OCCURRENCE | 132,556 | 13,606 | 0 | 0 | 0 | 118,950 |
| CONDITION_ERA | 53,270 | 12,187 | 0 | 0 | 0 | 41,083 |
| DRUG_EXPOSURE | 182,525 | 9,440 | 0 | 0 | 0 | 173,085 |
| DRUG_ERA | 31,335 | 2,046 | 0 | 0 | 0 | 29,289 |
| VISIT_OCCURRENCE | 25,425 | 25 | 0 | 0 | 0 | 25,400 |

## Detailed Column Statistics

### MEASUREMENT

| Column | Total Values | Mapped | Mapping Rate | Unique Concepts Mapped |
|--------|--------------|---------|--------------|------------------------|
| measurement_concept_id | 1,553,188 | 524,195 | 33.7% | 418 |

**Duplicates removed**: 4,869 (0.31%)

### OBSERVATION

| Column | Total Values | Mapped | Mapping Rate | Unique Concepts Mapped |
|--------|--------------|---------|--------------|------------------------|
| observation_concept_id | 203,482 | 77 | 0.0% | 3 |

**Duplicates removed**: 16 (0.01%)

### PROCEDURE_OCCURRENCE

| Column | Total Values | Mapped | Mapping Rate | Unique Concepts Mapped |
|--------|--------------|---------|--------------|------------------------|
| procedure_concept_id | 39,437 | 1,814 | 4.6% | 59 |

**Duplicates removed**: 54 (0.12%)

### DEVICE_EXPOSURE

| Column | Total Values | Mapped | Mapping Rate | Unique Concepts Mapped |
|--------|--------------|---------|--------------|------------------------|
| device_concept_id | 5,797 | 17 | 0.3% | 1 |

### CONDITION_OCCURRENCE

No concept columns processed.

### CONDITION_ERA

No concept columns processed.

### DRUG_EXPOSURE

No concept columns processed.

### DRUG_ERA

No concept columns processed.

### VISIT_OCCURRENCE

No concept columns processed.

## Notes

- **Low frequency filtering**: Removes records with concept frequency ≤ threshold
- **Date standardization**: Formats all dates consistently (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
- **SNOMED mapping**: Applied to MEASUREMENT, OBSERVATION, PROCEDURE_OCCURRENCE, DEVICE_EXPOSURE
- **Outlier removal**: Applied to MEASUREMENT table based on value_as_number percentiles
- **Deduplication**: Based on person_id + mapped_concept_id + datetime
- All removed records are saved for traceability in the removed_records subdirectories

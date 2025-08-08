# Data Standardization Report

**Generated**: 2025-08-08 18:24:40
**Dataset**: OMOP CDM
**Outlier Percentile**: 99.0%
**Minimum Concept Frequency**: 10
**Visit Merge Threshold**: 2.0 hours

## Standardization Summary

| Table | Input Records | Output Records | Low Freq Removed | Outliers Removed | Total Removed |
|-------|---------------|----------------|------------------|------------------|---------------|
| MEASUREMENT | 1,536,102 | 1,445,907 | 18 | 90,177 | 90,195 |
| OBSERVATION | 203,466 | 203,466 | 0 | 0 | 0 |
| PROCEDURE_OCCURRENCE | 39,383 | 39,383 | 0 | 0 | 0 |
| DEVICE_EXPOSURE | 5,797 | 5,797 | 0 | 0 | 0 |
| DRUG_EXPOSURE | 182,525 | 173,985 | 8,540 | 0 | 8,540 |
| VISIT_DETAIL | 26,978 | 26,971 | 7 | 0 | 7 |
| VISIT_OCCURRENCE | 25,425 | 25,400 | 25 | 0 | 25 |
| CONDITION_ERA | 53,270 | 41,863 | 11,407 | 0 | 11,407 |
| CONDITION_OCCURRENCE | 132,556 | 119,950 | 12,606 | 0 | 12,606 |
| DRUG_ERA | 31,335 | 29,499 | 1,836 | 0 | 1,836 |
| SPECIMEN | 3,874 | 3,833 | 41 | 0 | 41 |
| **Total** | **2,240,711** | **2,116,054** | - | - | **124,657** |

## Key Features

### 1. Low Frequency Filtering
- Removed concepts with frequency < 10
- Applied to all tables with concept IDs
- Preserves data quality by removing rare/erroneous concepts

### 2. Outlier Removal
- Percentile-based: Values beyond 99.0th percentile
- Range-based: Physiologically implausible values
- Applied per concept ID to preserve concept-specific distributions

### 3. Visit Merging
- Merged visits within 2.0 hours
- Created episode identifiers for continuous care
- Generated merged data files with traceable mappings

### 4. Standardizations Applied
- Datetime format: ISO 8601 (YYYY-MM-DD HH:MM:SS)
- Unit conversions: glucose, temperature, weight, height
- All changes are traceable through change logs

## Data Sources

- **Mapped tables** (from 3_mapping): MEASUREMENT, OBSERVATION, PROCEDURE_OCCURRENCE, DEVICE_EXPOSURE
- **Cleaned tables** (from 2_cleaning): DRUG_EXPOSURE, VISIT_DETAIL, VISIT_OCCURRENCE, CONDITION_ERA, CONDITION_OCCURRENCE, DRUG_ERA, SPECIMEN

## Output Structure

```
output/4_standardization/
├── [table]_standardized.csv      # Standardized data
├── removed_records/
│   ├── outliers_percentile/     # Percentile-based outliers
│   ├── outliers_range/          # Range-based outliers
│   ├── low_frequency/           # Low frequency concepts
│   └── removal_summary.csv      # Summary of all removals
├── merged_visits/               # Visit merge information
│   ├── [table]_merged.csv       # Merged visit data
│   └── [table]_merge_mapping.csv # Merge mappings
└── standardization_changes/     # All standardization changes
```

## Traceability

All removed records include:
- `removal_reason`: Specific reason for removal
- `original_row_number`: Row number in input file
- `additional_info`: JSON with detailed removal context

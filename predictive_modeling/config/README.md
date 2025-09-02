# Configuration Directory

This directory contains configuration files used by the predictive modeling pipeline.

## Files

### `concepts.json`

**Purpose**: Defines OMOP concept IDs for feature extraction

**Structure**:
```json
{
  "MEASUREMENT": [
    {
      "id": 3024171,
      "name": "Respiratory rate"
    }
  ],
  "CONDITION_OCCURRENCE": [...],
  "PROCEDURE_OCCURRENCE": [...],
  "DRUG_EXPOSURE": [...]
}
```

**Usage**: 
- Used by `SimpleTimeSeriesExtractor` for time series feature extraction
- Defines which clinical concepts to extract from OMOP CDM tables
- Organized by OMOP table type (MEASUREMENT, CONDITION_OCCURRENCE, etc.)

**Categories**:
- **MEASUREMENT**: Vital signs, lab values (~50 concepts)
- **CONDITION_OCCURRENCE**: Medical conditions for static features (~50 top conditions)  
- **PROCEDURE_OCCURRENCE**: Medical procedures for static features (~50 top procedures)
- **DRUG_EXPOSURE**: Medications (if applicable)

## Configuration Method

**Pipeline Configuration**: Command-line parameters

**Examples**:
```bash
# Configure time windows and observation periods
python run_feature_extraction.py --time-window 4 --min-observation 24

# Configure model training
python run_traditional_models.py --models LogisticRegression,RandomForest --use-smote

# Configure pipeline
python run_all_modules.py --time-window 8 --include-dl
```

## Modifying Concepts

To modify which clinical concepts are extracted:

1. **Edit `concepts.json`** directly
2. **Add/remove concept IDs** in the appropriate table category
3. **Ensure concept IDs are valid OMOP concept IDs**
4. **Re-run feature extraction** to apply changes

**Note**: Changes to concepts.json require re-running the entire feature extraction process.

# Data Directory

This directory is for storing your working datasets (sampled or full).

## Data Preparation Options

### Option 1: Sample from existing data location (Recommended)
```bash
# Sample patients from your OMOP data wherever it's stored
python utils/sample_patients.py \
  --input /path/to/your/OMOP_data/PERSON.csv \
  --output data/PERSON.csv \
  --sample-size 1000

# Then extract all related data for sampled patients
jupyter notebook notebooks/0_extract_data_subset.ipynb
```

### Option 2: Full dataset processing
If you need to process the complete dataset, you can place all OMOP CDM files directly in this `data/` directory. However, **we strongly recommend testing with a subset first** to ensure your pipeline configuration is correct.

## Expected Files

After data preparation, this directory should contain:
- PERSON.csv
- MEASUREMENT.csv
- OBSERVATION.csv
- CONDITION_OCCURRENCE.csv
- DRUG_EXPOSURE.csv
- VISIT_OCCURRENCE.csv
- And other OMOP CDM standard tables

## Important Notes

- **For testing**: Use small samples (100-1000 patients)
- **For production**: Can place full datasets here, but test first!
- **Git**: Large files are excluded from version control via .gitignore
# Data Directory

This directory is for storing your working datasets (sampled or full).

## Data Preparation Options

### Option 1: One-command sampling and extraction (Recommended)
```bash
# Sample patients and extract all related OMOP tables in one command
python utils/sample_patients.py \
  --input-dir /path/to/your/OMOP_data/ \
  --output-dir data/ \
  --sample-size 1000 \
  --extract-all \
  --prefixes 4 6 7 9  # Optional: filter by ID prefixes
```

### Option 2: Sample only (backward compatible)
```bash
# Just sample patients without extraction
python utils/sample_patients.py \
  --input /path/to/your/OMOP_data/PERSON.csv \
  --output data/PERSON.csv \
  --sample-size 1000
```

### Option 3: Full dataset processing
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
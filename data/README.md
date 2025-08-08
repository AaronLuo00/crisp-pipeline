# Data Directory

This directory is for storing sampled or test datasets. The pipeline processes data in the following workflow:

## Data Preparation Steps

1. **Place raw OMOP CDM data in `../raw_data/`**
   - Original OMOP CDM CSV files (PERSON.csv, MEASUREMENT.csv, etc.)
   - These files should not be committed to version control

2. **Generate a sample dataset**
   ```bash
   # Sample 1000 patients from your data
   python utils/sample_patients.py \
     --input ../raw_data/PERSON.csv \
     --output data/PERSON.csv \
     --sample-size 1000
   ```

3. **Extract all related data for sampled patients**
   - Use `notebooks/0_extract_data_subset.ipynb`
   - This will extract records from all OMOP tables for your sampled patients

## Expected Files

After data preparation, this directory should contain:
- PERSON.csv (sampled patients)
- MEASUREMENT.csv
- OBSERVATION.csv
- CONDITION_OCCURRENCE.csv
- DRUG_EXPOSURE.csv
- VISIT_OCCURRENCE.csv
- And other OMOP CDM standard tables

## Note

- Keep sample sizes small for testing (e.g., 100-1000 patients)
- Large datasets should be processed directly from `raw_data/`
- This directory is included in git, so avoid committing large files
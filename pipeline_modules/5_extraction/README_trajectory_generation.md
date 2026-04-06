# Trajectory Generation

## What this module does

`run_trajectory_generation.py` generates a `patient_trajectory.md` file for each patient under `extracted_patient_data/`.

Each markdown file contains:
- patient demographics (`PERSON`)
- visit-based timeline structure (`VISIT_OCCURRENCE`, `VISIT_DETAIL`)
- ICU episode annotations (`icu_episodes.csv`)
- clinical events from supported tables, organized chronologically

Output files are written to each patient folder as:
- `patient_trajectory.md`

A run summary is also saved to:
- `output/5_extraction/trajectory_generation_report.txt`

---

## Supported input tables

### Always included (not controlled by filters)
- `PERSON`
- `VISIT_OCCURRENCE`
- `VISIT_DETAIL`
- `ICU_EPISODES` (`icu_episodes.csv`)

### Filterable clinical content tables
- `CONDITION_OCCURRENCE`
- `CONDITION_ERA`
- `DRUG_EXPOSURE`
- `DRUG_ERA`
- `MEASUREMENT`
- `OBSERVATION`
- `PROCEDURE_OCCURRENCE`
- `DEVICE_EXPOSURE`
- `SPECIMEN`

---

## Important rendering rules

### 1. Zero-duration `VISIT_DETAIL`
If a `VISIT_DETAIL` row has the same start and end time, it is **not** rendered as a separate `#### Visit` heading.

Instead, it is converted into a normal timeline event, for example:

```text
2022-01-05 23:59: Inpatient Visit detail checkpoint [2022-01-05 23:59 -> 2022-01-05 23:59; zero-duration]
```

This keeps the timeline readable while preserving the original detail record.

### 2. Missing value handling
If an event has no explicit value, it is rendered as:

```text
Concept Name (No Value)
```

### 3. Era tables
- `DRUG_ERA` uses `drug_exposure_count` as its value
- `CONDITION_ERA` uses `condition_occurrence_count` as its value
- If an era end time exists, it is appended in the event text

Example:

```text
2010-02-05 00:00: aspirin, drug exposure count: 3; end time: 2010-03-06 00:00:00
```

### 4. Overflow sections
If an event cannot be confidently assigned to a specific visit detail section, it is placed into an overflow block such as:

- `Additional events before first visit`
- `Additional events between Visit X and Visit Y`
- `Additional events after Visit X`

This usually means one of the following:
- the event has no matching `visit_occurrence_id`
- the event has no matching `visit_detail_id`
- the event timestamp falls outside all available visit detail windows

Overflow is preserved intentionally instead of forcing uncertain reassignment.

---

## CLI usage

### Default: keep all supported clinical tables
```bash
python3 pipeline_modules/5_extraction/run_trajectory_generation.py
```

### Keep only selected clinical tables
```bash
python3 pipeline_modules/5_extraction/run_trajectory_generation.py \
  --keep-tables DRUG_EXPOSURE,DRUG_ERA
```

### Keep only events whose rendered text matches a keyword
```bash
python3 pipeline_modules/5_extraction/run_trajectory_generation.py \
  --keep-keywords glucose
```

### Combine both filters
```bash
python3 pipeline_modules/5_extraction/run_trajectory_generation.py \
  --keep-tables MEASUREMENT,OBSERVATION \
  --keep-keywords glucose
```

---

## Filter semantics

### `--keep-tables`
- values should match original table names (without `.csv`)
- case-insensitive in CLI parsing
- if omitted, all filterable clinical tables are used

### `--keep-keywords`
- comma-separated list
- case-insensitive substring match
- applies to rendered clinical event text
- if omitted, no keyword filtering is applied

### If both filters are provided
Both must pass:
- `keep-tables` = table filter
- `keep-keywords` = text filter

In other words:
- OR within `keep-tables`
- OR within `keep-keywords`
- AND between the two filters

---

## Notes

This module is designed to preserve timeline structure even under filtering.
That is why `PERSON`, `VISIT_OCCURRENCE`, `VISIT_DETAIL`, and `ICU_EPISODES` are always included.
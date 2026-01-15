#!/usr/bin/env python3
"""
Generate LOSO results tables for sites 4, 6, and 9
"""
import json
import os
from pathlib import Path

# Define the base path
BASE_PATH = Path("/n/holylabs/mili_lab/Lab/xluo/crisp-pipeline/predictive_modeling/modeling_results/models")

# Define site configurations
SITES = {
    "4": {
        "traditional": "traditional/loso_site4_20251010_025302",
        "deep_learning": "deep_learning/loso_site4_20251010_030302"
    },
    "6": {
        "traditional": "traditional/loso_site6_20251010_025351",
        "deep_learning": "deep_learning/loso_site6_20251010_030658"
    },
    "9": {
        "traditional": "traditional/loso_site9_20251010_025426",
        "deep_learning": "deep_learning/loso_site9_20251010_032649"
    }
}

# Define tasks in the order they should appear in the table
TASKS = [
    ("mortality", "mortality_7day", "7-day"),
    ("mortality", "mortality_30day", "30-day"),
    ("los", "los_greater_3days", "LOS > 3 days"),
    ("los", "los_greater_7days", "LOS > 7 days"),
    ("readmission", "readmission_7days", "7-day"),
    ("readmission", "readmission_30days", "30-day"),
    ("readmission", "readmission_90days", "90-day"),
    ("sepsis", "has_sepsis_after_icu", "After ICU"),
    ("sepsis", "sepsis_within_48h", "Within 48h"),
    ("sepsis", "sepsis_within_7days", "Within 7 days"),
]

# Task category display names
TASK_CATEGORIES = {
    "mortality": "Mortality",
    "los": "Length of Stay",
    "readmission": "Readmission",
    "sepsis": "In ICU Sepsis"
}

# Traditional models
TRADITIONAL_MODELS = ["logisticregression", "randomforest", "gradientboosting", "xgboost"]
TRADITIONAL_MODEL_ABBR = {"logisticregression": "LR", "randomforest": "RF", "gradientboosting": "GB", "xgboost": "XGB"}

# Deep learning models
DL_MODELS = ["MLP", "LSTM", "TCN", "Transformer"]
DL_MODEL_ABBR = {"MLP": "MLP", "LSTM": "LSTM", "TCN": "TCN", "Transformer": "TRF"}


def read_traditional_auroc(site_path, model, task_category, task_name):
    """Read AUROC from traditional model metrics file"""
    file_path = BASE_PATH / site_path / model / f"{task_category}_{task_name}_metrics.json"
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data.get("auroc", None)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def read_dl_summary(site_path):
    """Read deep learning summary file"""
    file_path = BASE_PATH / site_path / "deep_learning_summary_all_models.json"
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def extract_dl_auroc(dl_summary, model, task_category, task_name):
    """Extract AUROC from deep learning summary"""
    try:
        return dl_summary["models"][model][task_category][task_name]["auroc"]
    except (KeyError, TypeError):
        return None


def generate_table_for_site(site_id):
    """Generate results table for a specific site"""
    site_config = SITES[site_id]

    # Read deep learning summary
    dl_summary = read_dl_summary(site_config["deep_learning"])

    # Build table
    lines = []
    lines.append(f"**Table. Clinical Prediction Performance (LOSO Site {site_id}; AUROC)**")
    lines.append("")

    # Header
    header = "| **Task Category** | **Prediction Target** | **LR** | **RF** | **GB** | **XGB** | **MLP** | **LSTM** | **TCN** | **TRF** |"
    separator = "|--------------------|-----------------------|:------:|:------:|:------:|:-------:|:-------:|:--------:|:-------:|:-------:|"
    lines.append(header)
    lines.append(separator)

    # Data rows
    current_category = None
    for task_category, task_name, display_name in TASKS:
        row_data = {}

        # Read traditional models
        for trad_model in TRADITIONAL_MODELS:
            auroc = read_traditional_auroc(site_config["traditional"], trad_model, task_category, task_name)
            abbr = TRADITIONAL_MODEL_ABBR[trad_model]
            row_data[abbr] = auroc

        # Read deep learning models
        for dl_model in DL_MODELS:
            auroc = extract_dl_auroc(dl_summary, dl_model, task_category, task_name)
            abbr = DL_MODEL_ABBR[dl_model]
            row_data[abbr] = auroc

        # Format row
        if current_category != task_category:
            category_display = f"**{TASK_CATEGORIES[task_category]}**"
            current_category = task_category
        else:
            category_display = ""

        # Format AUROC values (4 decimal places)
        formatted_values = []
        for model_abbr in ["LR", "RF", "GB", "XGB", "MLP", "LSTM", "TCN", "TRF"]:
            auroc = row_data.get(model_abbr)
            if auroc is not None:
                formatted_values.append(f"{auroc:.4g}")
            else:
                formatted_values.append("-")

        row = f"| {category_display} | {display_name} | {' | '.join(formatted_values)} |"
        lines.append(row)

    lines.append("")
    return "\n".join(lines)


def main():
    """Generate all LOSO results tables"""
    for site_id in ["4", "6", "9"]:
        print(f"Generating table for LOSO site {site_id}...")
        table_content = generate_table_for_site(site_id)

        # Write to file
        output_file = BASE_PATH / f"loso_site{site_id}_results.md"
        with open(output_file, 'w') as f:
            f.write(table_content)

        print(f"Saved to {output_file}")


if __name__ == "__main__":
    main()

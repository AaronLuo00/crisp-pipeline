"""
Data type compatibility utilities for CRITICAL data format changes.

The CRITICAL database update (2026) changed CSV export format:
- Integer columns exported as floats: '8546' → '8546.0'
- Trailing whitespace in some fields: '1                   ' → '1.0'
- Nullable integer columns with NaN exported as float

This module provides utilities to normalize data types so the pipeline
works consistently with both old and new data formats.
"""

import pandas as pd
import numpy as np
import logging
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)

# All OMOP CDM ID columns that should be integer type
OMOP_ID_COLUMNS = [
    # Primary keys
    'person_id', 'visit_occurrence_id', 'visit_detail_id',
    'condition_occurrence_id', 'drug_exposure_id', 'procedure_occurrence_id',
    'device_exposure_id', 'measurement_id', 'observation_id',
    'specimen_id', 'condition_era_id', 'drug_era_id',
    'observation_period_id',
    # Foreign keys
    'provider_id', 'care_site_id', 'location_id',
    'preceding_visit_occurrence_id', 'preceding_visit_detail_id',
    # Concept IDs
    'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id',
    'gender_source_concept_id', 'race_source_concept_id',
    'ethnicity_source_concept_id',
    'visit_concept_id', 'visit_source_concept_id',
    'visit_detail_concept_id', 'visit_detail_source_concept_id',
    'condition_concept_id', 'condition_source_concept_id',
    'condition_status_concept_id', 'condition_type_concept_id',
    'drug_concept_id', 'drug_source_concept_id',
    'drug_type_concept_id', 'route_concept_id',
    'procedure_concept_id', 'procedure_source_concept_id',
    'procedure_type_concept_id', 'modifier_concept_id',
    'device_concept_id', 'device_source_concept_id',
    'device_type_concept_id',
    'measurement_concept_id', 'measurement_source_concept_id',
    'measurement_type_concept_id',
    'operator_concept_id', 'unit_concept_id',
    'value_as_concept_id',
    'observation_concept_id', 'observation_source_concept_id',
    'observation_type_concept_id', 'qualifier_concept_id',
    'specimen_concept_id', 'specimen_type_concept_id',
    'anatomic_site_concept_id', 'disease_status_concept_id',
    'admitting_source_concept_id', 'discharge_to_concept_id',
    'cause_concept_id', 'cause_source_concept_id',
    'visit_type_concept_id', 'visit_detail_type_concept_id',
]

# Numeric value columns that should remain float (not forced to int)
NUMERIC_VALUE_COLUMNS = [
    'value_as_number', 'range_low', 'range_high',
    'quantity', 'refills', 'days_supply', 'gap_days',
]


def normalize_id_columns(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Normalize ID columns from float to nullable Int64.
    
    Handles the '8546.0' → 8546 conversion for all ID/concept columns.
    Uses pandas nullable Int64 dtype to properly handle NaN values.
    
    Args:
        df: DataFrame to normalize
        columns: Specific columns to normalize. If None, auto-detects
                 all OMOP ID columns present in the DataFrame.
    
    Returns:
        DataFrame with normalized ID columns
    """
    if columns is None:
        columns = [c for c in df.columns if c in OMOP_ID_COLUMNS]
    
    for col in columns:
        if col not in df.columns:
            continue
        try:
            # pd.to_numeric handles '8546', '8546.0', 8546.0 all correctly
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        except Exception as e:
            logger.debug(f"Could not convert {col} to Int64: {e}")
    
    return df


def normalize_numeric_columns(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Normalize numeric value columns to float64.
    
    Handles '221' → 221.0, '221.0' → 221.0, strips whitespace.
    
    Args:
        df: DataFrame to normalize
        columns: Specific columns to normalize. If None, auto-detects.
    
    Returns:
        DataFrame with normalized numeric columns
    """
    if columns is None:
        columns = [c for c in df.columns if c in NUMERIC_VALUE_COLUMNS]
    
    for col in columns:
        if col not in df.columns:
            continue
        try:
            # Strip whitespace (fixes DRUG_EXPOSURE.refills padding)
            if df[col].dtype == object:
                df[col] = df[col].str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
        except Exception as e:
            logger.debug(f"Could not convert {col} to numeric: {e}")
    
    return df


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full normalization: ID columns → Int64, numeric values → float64.
    
    Call this after pd.read_csv() to ensure consistent types regardless
    of whether the source is old (int) or new (float) CRITICAL data.
    
    Args:
        df: DataFrame from pd.read_csv()
    
    Returns:
        Normalized DataFrame
    """
    df = normalize_id_columns(df)
    df = normalize_numeric_columns(df)
    return df


def safe_int(value, default=0) -> int:
    """
    Safely convert a value to int, handling '8546', '8546.0', 8546.0, NaN.
    
    Use this instead of int(value) or int(float(value)) throughout the pipeline.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
    
    Returns:
        Integer value
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return default
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default


def safe_concept_id_str(value) -> str:
    """
    Convert a concept ID to its canonical string form (no decimal).
    
    '8546.0' → '8546', '0.0' → '0', '' → '', NaN → ''
    
    Use this when concept IDs are used as dictionary keys or for string comparison.
    """
    if value is None or value == '' or (isinstance(value, float) and np.isnan(value)):
        return ''
    try:
        return str(int(float(str(value).strip())))
    except (ValueError, TypeError):
        return str(value).strip()


def is_zero_or_empty(value) -> bool:
    """
    Check if a concept_id value is zero or empty.
    
    Handles: '', '0', '0.0', 0, 0.0, NaN, None
    
    Use this instead of: str(concept_id).strip() == '' or str(concept_id).strip() == '0'
    """
    if value is None or value == '':
        return True
    if isinstance(value, float):
        return np.isnan(value) or value == 0.0
    try:
        s = str(value).strip()
        if s == '' or s == '0' or s == '0.0':
            return True
        # Handle '0.00', etc.
        return float(s) == 0.0
    except (ValueError, TypeError):
        return False

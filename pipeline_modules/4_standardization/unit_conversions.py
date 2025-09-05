"""
Unit conversion configurations for OMOP data standardization.
This module contains all unit conversion mappings and rules.
"""

# Unit concept ID mappings from Athena vocabulary
UNIT_CONCEPT_MAP = {
    8753: 'mmol/L',      # millimole per liter
    8840: 'mg/dL',       # milligram per deciliter  
    9289: 'Fahrenheit',  # degree Fahrenheit ([degF])
    8653: 'Celsius',     # degree Celsius ([degC] - non-standard, should convert to 586323)
    586323: 'Celsius',   # degree Celsius (Cel - standard UCUM, target unit)
    9484: 'Celsius',     # degree (deg - generic, should convert to 586323)
    8739: 'lb',          # pound (US)
    9529: 'kg',          # kilogram
    9330: 'inch',        # inch (US)
    8582: 'cm'           # centimeter
}

# Unit conversions based on concept IDs (source unit -> target unit)
UNIT_ID_CONVERSIONS = {
    # Glucose: mmol/L to mg/dL
    8753: {
        'target_id': 8840,
        'factor': 18.0182,
        'for_concepts': [4151414, 4018317]  # glucose measurement concepts
    },
    # Temperature: Fahrenheit to Celsius (standard UCUM Cel)
    9289: {
        'target_id': 586323,  # Convert to standard UCUM Celsius
        'formula': lambda f: (f - 32) * 5/9,
        'for_concepts': [3020891]  # temperature measurement concept
    },
    # Standardize non-standard Celsius: 8653 ([degC]) -> 586323 (Cel)
    8653: {
        'target_id': 586323,  # Convert to standard UCUM Celsius
        'factor': 1.0,  # No value conversion needed, just standardize unit ID
        'for_concepts': [3020891]  # temperature measurement concept
    },
    # Standardize generic degree: 9484 (deg) -> 586323 (Cel)
    9484: {
        'target_id': 586323,  # Convert to standard UCUM Celsius
        'factor': 1.0,  # Assuming these are Celsius based on value range
        'for_concepts': [3020891]  # temperature measurement concept
    },
    # Weight: pound to kilogram
    8739: {
        'target_id': 9529,
        'factor': 0.453592,
        'for_concepts': [37174455]  # weight measurement concept
    },
    # Height: inch to centimeter
    9330: {
        'target_id': 8582,
        'factor': 2.54,
        'for_concepts': [3036277]  # height measurement concept
    }
}

# Legacy unit conversions (kept for reference)
LEGACY_UNIT_CONVERSIONS = {
    # Glucose conversions (using SNOMED IDs)
    'mmol/L': {'target': 'mg/dL', 'factor': 18.0182, 'concepts': [4151414, 4018317]},
    
    # Temperature conversions (using original LOINC ID as not mapped to SNOMED)
    'Fahrenheit': {'target': 'Celsius', 'formula': lambda f: (f - 32) * 5/9, 'concepts': [3020891]},
    
    # Weight conversions (using SNOMED ID)
    'lb': {'target': 'kg', 'factor': 0.453592, 'concepts': [37174455]},
    'lbs': {'target': 'kg', 'factor': 0.453592, 'concepts': [37174455]},
    
    # Height conversions (using LOINC ID as common height concept)
    'in': {'target': 'cm', 'factor': 2.54, 'concepts': [3036277]},
    'inch': {'target': 'cm', 'factor': 2.54, 'concepts': [3036277]},
}
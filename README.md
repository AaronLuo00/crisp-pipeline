# CRISP Pipeline

<div align="center">

![CRISP Logo](https://img.shields.io/badge/CRISP-Pipeline-blue?style=for-the-badge)
![OMOP CDM](https://img.shields.io/badge/OMOP%20CDM-v5.3-orange?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)
![Status](https://img.shields.io/badge/Status-Active-success?style=flat-square)

**Clinical Records Integrated Standardization Pipeline**

*A comprehensive, open-source solution for transforming large-scale OMOP CDM datasets into ML-ready formats*

[Getting Started](#-getting-started) • [Documentation](#-documentation) • [Pipeline Modules](#-pipeline-architecture) • [Contributing](#-contributing)

</div>

---

## 🌟 Overview

CRISP is a **production-ready** data processing pipeline designed to handle **hundreds of millions of clinical records** efficiently. Originally developed to process the CRITICAL dataset (**571.7M records** from **~400K patients** across **4 major medical centers**), CRISP provides a robust framework for:

| Feature | Description |
|---------|-------------|
| ✅ **Data Quality Assurance** | Automated validation and cleaning of OMOP CDM data |
| ✅ **Concept Standardization** | Mapping diverse medical vocabularies to SNOMED CT |
| ✅ **Temporal Alignment** | Intelligent merging of fragmented clinical episodes |
| ✅ **ML-Ready Output** | Direct integration with machine learning workflows |

## 🚀 Getting Started

### Prerequisites

- **Python 3.8+** (with pip package manager)
- **Memory**: 16GB+ RAM recommended (8GB minimum)
- **Storage**: ~2x your data size in available disk space
- **Data Format**: OMOP CDM v5.3 compatible

### Quick Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/crisp-pipeline.git
cd crisp-pipeline

```

### Your First Run

**Step 1: Prepare your data**
```bash
# Create data directory and place your OMOP CDM files
mkdir raw_data
# Copy your PERSON.csv, MEASUREMENT.csv, etc. to raw_data/
```

**Step 2: Create a sample dataset** (recommended for testing)
```bash
# Sample 1000 patients for quick testing
python utils/sample_patients.py --sample-size 1000
```

**Step 3: Run the pipeline**
```bash
# Execute the complete pipeline
python main/run_pipeline.py
```

Your processed data will be available in the `output/` directory.

## 📊 Pipeline Architecture

CRISP implements a **5-stage progressive pipeline**, each module building upon the previous:

```
Raw Data → [EDA] → [Cleaning] → [Mapping] → [Standardization] → [Extraction] → ML-Ready
```

### Stage 1: Exploratory Data Analysis (EDA)
- **Purpose**: Understand your dataset's characteristics
- **Key Features**: 
  - Memory-efficient chunked processing for 100GB+ files
  - Automated data quality metrics
  - Cohort identification (e.g., ICU patients via concept IDs: 581379, 32037)

### Stage 2: Data Cleaning
- **Purpose**: Ensure data integrity and consistency
- **Key Features**:
  - Duplicate removal using table-specific composite keys
  - Invalid concept ID filtering (null, 0, or non-existent)
  - Temporal validation (ensuring start_date ≤ end_date)
  - Column pruning (removes features with >95% missing values)

### Stage 3: Concept Mapping
- **Purpose**: Standardize medical terminologies
- **Key Features**:
  - Maps LOINC, RxNorm, ICD codes to SNOMED CT
  - Leverages OMOP vocabulary relationships
  - Handles 20+ vocabulary sources

### Stage 4: Data Standardization
- **Purpose**: Normalize values and formats
- **Key Features**:
  - DateTime standardization to ISO 8601
  - Unit conversions (e.g., glucose: mmol/L → mg/dL)
  - Statistical outlier detection (IQR method)  
  - Visit episode merging (configurable window)

### Stage 5: Feature Extraction
- **Purpose**: Create ML-ready datasets
- **Key Features**:
  - Cohort-specific extraction
  - Temporal window generation
  - Feature aggregation

## 📚 Documentation

- [Getting Started Guide](docs/getting_started.md) - Detailed setup and first steps
- [Pipeline Guide](docs/pipeline_guide.md) - In-depth module documentation

## 🏗️ Project Structure

```
crisp-pipeline/
├── config/                 # Configuration files
├── data/                   # Sample datasets
├── docs/                   # Documentation
├── main/                   # Main pipeline runner
├── notebooks/              # Jupyter notebooks for exploration
├── pipeline_modules/       # Core processing modules
│   ├── 1_eda/             # Exploratory data analysis
│   ├── 2_cleaning/        # Data cleaning
│   ├── 3_mapping/         # Concept mapping
│   ├── 4_standardization/ # Data standardization
│   └── 5_extraction/      # Feature extraction
├── raw_data/              # Your OMOP data (not in git)
└── utils/                 # Utility scripts
```

## 🤝 Contributing

We welcome contributions! CRISP is designed to be extended and customized for different research needs.

### How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request


## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📖 Citation

If you use CRISP in your research, please cite:

TBA

## 🙏 Acknowledgments

- [OHDSI Community](https://www.ohdsi.org/) and [OMOP CDM](https://ohdsi.github.io/CommonDataModel/index.html) for the standardized data model
- [CRITICAL Consortium](https://critical.fsm.northwestern.edu/data-access) for dataset access
- All contributors who have helped improve CRISP

## 📬 Contact & Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/crisp-pipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/crisp-pipeline/discussions)
- **Email**: crisp-pipeline@gmail.com

---

<div align="center">
<br>

### Built with passion by the research community

<p>
<strong>CRISP Pipeline</strong> is maintained by researchers and developers committed to<br>
advancing healthcare analytics through open-source collaboration.
</p>
<sub>If you find CRISP helpful in your research, please consider giving us a ⭐ on GitHub!</sub>

</div>
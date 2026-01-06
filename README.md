# 🛡️ Data Quality Pipeline — Academic Records

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5+-orange.svg)](https://spark.apache.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.40+-red.svg)](https://streamlit.io)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A comprehensive **data quality assessment and cleaning pipeline** for academic student records, built with **Apache Spark** for scalable processing and **Streamlit** for interactive monitoring.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Project Structure](#-project-structure)
- [Installation](#-installation)
- [Usage](#-usage)
- [Data Quality Metrics](#-data-quality-metrics)
- [Pipeline Details](#-pipeline-details)
- [Dashboard](#-dashboard)
- [Contributing](#-contributing)

---

## 🎯 Overview

This project implements an end-to-end data quality solution for academic datasets containing:

| Field | Description | Example |
|-------|-------------|---------|
| `COD_ANU` | Academic year code | `2015` |
| `COD_ETU` | Student identifier | `12345678` |
| `COD_ELP` | Course/module code | `MATH1001` |
| `NOT_ELP` | Grade obtained | `14.5` |
| `COD_TRE` | Result status | `V`, `RAT`, `NV` |
| `COD_SES` | Exam session | `1` or `2` |

### Key Objectives

✅ **Detect** data quality issues (missing values, duplicates, format errors)
✅ **Clean** and normalize data using business rules
✅ **Measure** quality through standardized metrics
✅ **Monitor** quality evolution over time

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Data Quality Pipeline                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌─────────────────────────────────────┐    ┌────────┐ │
│  │  Raw     │    │           Spark Cluster             │    │Cleaned │ │
│  │  Data    │───▶│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐   │───▶│  Data  │ │
│  │  (.txt)  │    │  │Load │▶│Clean│▶│Valid│▶│Write│   │    │ (.csv) │ │
│  └──────────┘    │  └─────┘ └─────┘ └─────┘ └─────┘   │    └────────┘ │
│                  └─────────────────────────────────────┘               │
│                                    │                                    │
│                                    ▼                                    │
│                  ┌─────────────────────────────────────┐               │
│                  │       Streamlit Dashboard           │               │
│                  │  • Quality Metrics  • Comparisons   │               │
│                  │  • Trend Analysis   • Data Explorer │               │
│                  └─────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

### 🔧 Data Processing
- **Smart CSV Parsing** — Handles malformed rows with embedded commas
- **Hierarchical Cleaning** — Each step prepares data for the next
- **Traceability** — Flags track all corrections (`row_status`, `*_flag`)

### 📊 Quality Metrics
- **7 Dimensions** measured: Completeness, Uniqueness, Coherence, Validity, Exactitude, Distribution, Schema Integrity
- **Weighted Global Score** for overall quality assessment
- **Before/After Comparison** to measure cleaning effectiveness

### 📈 Monitoring Dashboard
- **Real-time Metrics** visualization
- **Historical Tracking** of quality evolution
- **Column-level Analysis** and error drill-down
- **Export Capabilities** for reporting

---

## 📁 Project Structure

```
projet-data-quality/
├── 📂 app/
│   └── streamlit_app.py          # Interactive dashboard
├── 📂 src/
│   ├── 📂 cleaners/              # Cleaning modules (PySpark)
│   │   ├── clean_cod_anu.py      # Year code normalization
│   │   ├── clean_cod_etud.py     # Student ID + deduplication
│   │   ├── clean_NOT_ELP.py      # Grade validation [0-20]
│   │   ├── cleaning_Code_ELP.py  # Module code format
│   │   ├── cleaning_COD_TRE.py   # Result status validation
│   │   └── cleaning_cod_ses.py   # Session validation (1/2)
│   └── 📂 jobs/
│       ├── load_data.py          # Data ingestion job
│       └── run_cleaning.py       # Cleaning pipeline job
├── 📂 data/
│   ├── 📂 raw/                   # Source data
│   │   └── dataset_metier.txt
│   ├── 📂 loaded_dataset/        # Ingested (Parquet)
│   ├── 📂 cleaned_data/          # Cleaned output (CSV)
│   └── metrics_history.csv       # Quality tracking
├── 📓 dq_metrics.ipynb           # Metrics exploration notebook
├── 📓 phase_exploration.ipynb    # Data exploration notebook
├── 🐳 docker-compose.yml         # Spark cluster setup
├── 📋 requirements.txt           # Python dependencies
└── 📖 README.md
```

---

## 🚀 Installation

### Prerequisites

- **Python 3.10+**
- **Docker & Docker Compose** (for Spark cluster)
- **Git**

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/projet-data-quality.git
cd projet-data-quality
```

### 2. Install Python Dependencies

```bash
python -m pip install -r requirements.txt
```

### 3. Start Spark Cluster

```bash
docker-compose up -d
```

Verify the cluster is running:
- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081

---

## 💻 Usage

### Option 1: Streamlit Dashboard (Recommended)

```bash
streamlit run app/streamlit_app.py
```

Then open http://localhost:8501 in your browser.

**Dashboard Workflow:**
1. **⚙️ Actions** tab → Click "📥 Lancer Ingestion" to load raw data
2. **⚙️ Actions** tab → Click "🧹 Lancer Nettoyage" to clean data
3. **📊 Vue d'ensemble** tab → View quality metrics and comparisons
4. **🔍 Analyse Détaillée** tab → Explore columns and errors

### Option 2: Command Line (Spark Jobs)

```bash
# Step 1: Ingest raw data
docker exec spark-master_1 bash -c \
  "PYTHONPATH=/opt/project/src /opt/spark/bin/spark-submit \
   --master local[*] /opt/project/src/jobs/load_data.py"

# Step 2: Run cleaning pipeline
docker exec spark-master_1 bash -c \
  "PYTHONPATH=/opt/project/src /opt/spark/bin/spark-submit \
   --master local[*] /opt/project/src/jobs/run_cleaning.py"
```

### Option 3: Jupyter Notebooks

```bash
jupyter notebook dq_metrics.ipynb
```

---

## 📏 Data Quality Metrics

| Metric | Weight | Description |
|--------|--------|-------------|
| **Complétude** | 20% | % of non-null, non-empty values |
| **Validité** | 20% | Values conform to business rules (sessions, result codes) |
| **Intégrité Schéma** | 20% | Rows with correct structure (not INVALID) |
| **Exactitude** | 15% | Grades within valid range [0, 20] |
| **Cohérence** | 15% | Business rule consistency (note ↔ status) |
| **Unicité** | 5% | No duplicates on key (ETU, ELP, SES) |
| **Distribution** | 5% | Balanced session distribution |

### Global Score Formula

```
Score = 0.20×Complétude + 0.20×Validité + 0.20×Schéma
      + 0.15×Exactitude + 0.15×Cohérence
      + 0.05×Unicité + 0.05×Distribution
```

---

## 🔄 Pipeline Details

### Cleaning Steps (Hierarchical)

```
1. COD_ANU  → Format YYYY normalization, flag corrections
      ↓
2. COD_ETU  → Null detection, duplicate removal (business key)
      ↓
3. COD_ELP  → Uppercase, trim, validate 8-char alphanumeric
      ↓
4. NOT_ELP  → Numeric conversion, range check [0-20], coherence
      ↓
5. COD_TRE  → Validate against allowed codes (V, RAT, NV, ADM...)
      ↓
6. COD_SES  → Must be 1 or 2, flag invalid sessions
```

### Generated Flags

| Flag | Meaning |
|------|---------|
| `row_status` | `OK` / `FIXED` / `INVALID` |
| `cod_anu_was_corrected` | Year format was normalized |
| `elp_invalid_format` | Module code doesn't match expected format |
| `note_out_of_range` | Grade outside [0, 20] |
| `note_incoherent` | Grade doesn't match result status |
| `dup_business_flag` | Duplicate on business key |

---

## 📸 Dashboard Preview

### Quality Radar Chart
![Radar Chart](docs/radar_chart.png)

### Before/After Comparison
![Comparison](docs/comparison.png)

---

## 🛠️ Development

### Running Tests

```bash
python -m pytest tests/ -v
```

### Code Style

```bash
# Format code
black src/ app/

# Lint
flake8 src/ app/
```

### Adding New Cleaners

1. Create a new file in `src/cleaners/`
2. Implement a function `clean_<field>_spark(df: DataFrame) -> DataFrame`
3. Add the import and call in `src/jobs/run_cleaning.py`

---

## 📦 Deployment

### Stop Spark Cluster

```bash
docker-compose down
```

### Clean Data Outputs

```bash
rm -rf data/loaded_dataset/output/*
rm -rf data/cleaned_data/output/*
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Hamza Charmaqe**
Master's Student — Data Quality Project 2025

---

## 🙏 Acknowledgments

- Apache Spark for distributed processing
- Streamlit for rapid dashboard development
- pandas & numpy for data manipulation
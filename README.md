# coreason_etl_pubmedcentral

**Sovereign, local mirror of the PubMed Central (PMC) Open Access subset.**

This ETL pipeline powers CoReason's Cognitive (Type B) applications by ingesting, cleaning, and structuring bulk XML datasets from NCBI/AWS.

[![CI](https://github.com/CoReason-AI/coreason_etl_pubmedcentral/actions/workflows/ci.yml/badge.svg)](https://github.com/CoReason-AI/coreason_etl_pubmedcentral/actions/workflows/ci.yml)

## 🚀 Features

*   **Dual-Source Architecture:**
    *   **Primary:** S3 (`s3://pmc-oa-opendata`) for high throughput.
    *   **Failover:** Automatic switch to FTP (`ftp.ncbi.nlm.nih.gov`) after 3 consecutive errors.
    *   **Resilience:** Persistent FTP sessions and error handling logic.
*   **Medallion Architecture:**
    *   **Bronze:** Raw XML storage, partitioned by `ingestion_date`.
    *   **Silver:** Parsed and cleaned data (JATS schema drift handling, entity resolution).
    *   **Gold:** "Wide Table" for analytics (`gold_pmc_analytics_rich`), optimized for complex filtering.
*   **Observability:**
    *   Structured logging (`loguru`) with metrics (`records_ingested_total`).
    *   Explicit alerts for schema violations, failover events, and retractions.
*   **Data Integrity:**
    *   **Stream & Clear:** Strict memory management using `lxml.etree.iterparse`.
    *   **Retraction Watch:** Retracted articles are flagged, not deleted, to preserve lineage.
    *   **Validations:** Strict typing and runtime checks for data quality.

## 🛠️ Architecture

### Layers

1.  **Bronze (Raw Lake):**
    *   Format: Parquet (partitioned by `ingestion_date`).
    *   Schema: `source_file_path`, `ingestion_ts`, `ingestion_source`, `raw_xml_payload` (Binary), `manifest_metadata` (JSON).

2.  **Silver (Refinery):**
    *   Normalizes JATS dates (handling seasons, missing days).
    *   Resolves Authors & Affiliations.
    *   Unifies Funding (Modern `funding-group` + Legacy `contract-num`).

3.  **Gold (Analytics):**
    *   Table: `gold_pmc_analytics_rich`.
    *   Flattened arrays: `grant_ids`, `agency_names`, `keywords`.
    *   Compliance flags: `is_commercial_safe`, `is_retracted`.

## 📦 Installation

### Prerequisites

*   Python 3.12+
*   Poetry

### Setup

1.  Clone the repository:
    ```sh
    git clone https://github.com/CoReason-AI/coreason_etl_pubmedcentral.git
    cd coreason_etl_pubmedcentral
    ```

2.  Install dependencies:
    ```sh
    poetry install
    ```

## 🧪 Testing

The project maintains **100% test coverage** with strict regression testing.

### Run Unit Tests

```sh
poetry run pytest
```

### Run Live Integration Test

Verifies connectivity to the public S3 bucket and end-to-end pipeline execution.

```sh
poetry run pytest tests/integration/test_live_pipeline.py
```

## 🧹 Code Quality

We enforce strict code quality standards using `ruff`, `mypy`, and `pre-commit`.

```sh
# Format code
poetry run ruff format .

# Check linting
poetry run ruff check --fix .

# Run all pre-commit hooks
poetry run pre-commit run --all-files
```

## 📄 License

**Prosperity Public License 3.0**
Free for non-commercial use and trial. Commercial use requires a license.
See `LICENSE` for details.

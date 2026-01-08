# coreason_etl_pubmedcentral

A sovereign, local mirror of the PubMed Central (PMC) Open Access subset to power CoReason's Cognitive (Type B) applications.

This project implements a robust ETL pipeline to ingest, process, and structure biomedical literature from PMC, adhering to a Medallion Architecture (Bronze -> Silver -> Gold).

## Architecture

### Dual-Source Architecture
The system ensures high availability by utilizing a primary cloud source with a legacy protocol failover:
- **Primary:** AWS Open Data (`s3://pmc-oa-opendata`) - High throughput.
- **Failover:** NCBI FTP (`ftp.ncbi.nlm.nih.gov`) - Legacy protocol triggered after persistent S3 failures.

### Medallion Pipeline
1.  **Bronze Layer (The Raw Lake):** Lossless, immutable storage of raw JATS XML blobs, partitioned by ingestion date.
2.  **Silver Layer (The Refinery):** Structures, cleans, and resolves entities. Handles JATS schema drift, temporal normalization, and author-affiliation resolution.
3.  **Gold Layer (The Product):** A "Wide Table" optimized for analytical queries (OLAP), featuring flattened metadata, search-ready text, and compliance flags.

## Features
-   **Incremental Ingestion:** Uses a high-water mark strategy based on the manifest's `Last Updated` timestamp.
-   **Resilience:** Implements retry logic with exponential backoff and circuit breaking for source failover.
-   **Observability:** Structured logging (using `loguru`) with metrics for ingestion counts and schema violations.
-   **Memory Efficiency:** strictly enforces "Stream & Clear" parsing for handling terabytes of XML.

## Getting Started

### Prerequisites

- Python 3.12+
- Poetry

### Installation

1.  Clone the repository:
    ```sh
    git clone https://github.com/CoReason-AI/coreason_etl_pubmedcentral.git
    cd coreason_etl_pubmedcentral
    ```
2.  Install dependencies:
    ```sh
    poetry install
    ```

### Usage

-   Run the pipeline (CLI):
    ```sh
    poetry run pmc-etl "oa_comm.filelist.csv"
    ```
-   Run the tests:
    ```sh
    poetry run pytest
    ```
-   Run code quality checks:
    ```sh
    poetry run pre-commit run --all-files
    ```

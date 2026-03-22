# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from dlt.common.configuration.specs import configspec


@configspec
class PubMedCentralConfig:
    """
    Configuration specification for the PubMed Central ETL Pipeline.
    Leverages dlt's @configspec for dependency injection and environment variable resolution.
    """

    __section__ = "pubmed_central"

    # Primary Source (Default)
    s3_bucket: str = "pmc-oa-opendata"

    # Secondary Source (Failover)
    ftp_host: str = "ftp.ncbi.nlm.nih.gov"
    ftp_path: str = "/pub/pmc/"

    # Local Storage Mandate
    local_archive_dir: str = "/var/lib/coreason/pmc/"

    # Failover trigger thresholds
    s3_max_retry_attempts: int = 3

    # Pipeline Metadata
    pipeline_name: str = "coreason_etl_pubmedcentral"
    dataset_name: str = "pmc_refined"
    destination_name: str = "postgres"

    # dlt Schema Explosion Prevention Protocol
    max_table_nesting: int = 0

# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason-etl-pubmedabstracts

from dlt.common.configuration.specs import configspec


@configspec
class PubMedAbstractsConfig:
    """
    Configuration specification for the PubMed Abstracts ETL Pipeline.
    Leverages dlt's @configspec for dependency injection and environment variable resolution.

    AGENT INSTRUCTION:
    This class defines the configuration boundaries specifically for the NLM FTP resource,
    including baseline and updates directories, adhering strictly to the FRD Layer 1 specification.
    """

    __section__ = "pubmed_abstracts"

    # Source Specification
    ftp_host: str = "ftp.ncbi.nlm.nih.gov"
    baseline_dir: str = "/pubmed/baseline/"
    updates_dir: str = "/pubmed/updatefiles/"

    # Pipeline Metadata
    pipeline_name: str = "coreason_etl_pubmedabstracts"
    destination_name: str = "postgres"

    # Medallion Schemas
    bronze_schema: str = "bronze"

    # Target Table as per schema evolution constraints
    bronze_table: str = "bronze_pubmed_raw"

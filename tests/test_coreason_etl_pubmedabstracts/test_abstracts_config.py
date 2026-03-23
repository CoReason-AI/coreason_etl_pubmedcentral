# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason-etl-pubmedabstracts

import os
from unittest.mock import patch

from dlt.common.configuration.resolve import resolve_configuration

from coreason_etl_pubmedabstracts.config import PubMedAbstractsConfig


def test_pubmed_abstracts_config_defaults() -> None:
    """Test that PubMedAbstractsConfig defaults match the BRD."""
    config = PubMedAbstractsConfig()

    # Source Specification defaults
    assert config.ftp_host == "ftp.ncbi.nlm.nih.gov"
    assert config.baseline_dir == "/pubmed/baseline/"
    assert config.updates_dir == "/pubmed/updatefiles/"

    # Pipeline Metadata defaults
    assert config.pipeline_name == "coreason_etl_pubmedabstracts"
    assert config.destination_name == "postgres"

    # Medallion Schemas defaults
    assert config.bronze_schema == "bronze"
    assert config.bronze_table == "bronze_pubmed_raw"


def test_pubmed_abstracts_config_env_override() -> None:
    """Test that PubMedAbstractsConfig overrides work with dlt's resolver via environment variables."""
    # Define custom environment variable values
    custom_env = {
        "PUBMED_ABSTRACTS__FTP_HOST": "ftp.custom.org",
        "PUBMED_ABSTRACTS__BASELINE_DIR": "/custom/baseline/",
        "PUBMED_ABSTRACTS__UPDATES_DIR": "/custom/updates/",
        "PUBMED_ABSTRACTS__PIPELINE_NAME": "custom_pipeline",
        "PUBMED_ABSTRACTS__DESTINATION_NAME": "custom_destination",
        "PUBMED_ABSTRACTS__BRONZE_SCHEMA": "custom_bronze",
        "PUBMED_ABSTRACTS__BRONZE_TABLE": "custom_table",
    }

    with patch.dict(os.environ, custom_env):
        # Resolve config with dlt's resolver
        resolved_config = resolve_configuration(PubMedAbstractsConfig(), sections=("pubmed_abstracts",))

        # Verify that the variables were successfully overridden
        assert resolved_config.ftp_host == "ftp.custom.org"
        assert resolved_config.baseline_dir == "/custom/baseline/"
        assert resolved_config.updates_dir == "/custom/updates/"
        assert resolved_config.pipeline_name == "custom_pipeline"
        assert resolved_config.destination_name == "custom_destination"
        assert resolved_config.bronze_schema == "custom_bronze"
        assert resolved_config.bronze_table == "custom_table"

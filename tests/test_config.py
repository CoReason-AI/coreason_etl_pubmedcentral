# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import os
from unittest.mock import patch

from coreason_etl_pubmedcentral.config import PubMedCentralConfig


def test_pubmed_central_config_defaults() -> None:
    """Test that PubMedCentralConfig initializes with the correct default values."""
    config = PubMedCentralConfig()

    assert config.s3_bucket == "pmc-oa-opendata"
    assert config.ftp_host == "ftp.ncbi.nlm.nih.gov"
    assert config.ftp_path == "/pub/pmc/"
    assert config.s3_max_retry_attempts == 3
    assert config.local_archive_dir == "/var/lib/coreason/pmc/"
    assert config.pipeline_name == "coreason_etl_pubmedcentral"
    assert config.dataset_name == "pmc_refined"
    assert config.destination_name == "postgres"
    assert config.max_table_nesting == 0


def test_pubmed_central_config_env_overrides() -> None:
    """Test that PubMedCentralConfig resolves configuration from environment variables."""

    env_vars = {
        "PUBMED_CENTRAL__S3_BUCKET": "custom-bucket",
        "PUBMED_CENTRAL__FTP_HOST": "ftp.custom.com",
        "PUBMED_CENTRAL__FTP_PATH": "/custom/path/",
        "PUBMED_CENTRAL__S3_MAX_RETRY_ATTEMPTS": "10",
        "PUBMED_CENTRAL__MAX_TABLE_NESTING": "2",
        "PUBMED_CENTRAL__LOCAL_ARCHIVE_DIR": "/custom_temp/custom_archive/",
    }

    with patch.dict(os.environ, env_vars, clear=True):
        from dlt.common.configuration.resolve import resolve_configuration

        # We resolve it directly since get_config within a test module caches the providers before mock runs
        config = resolve_configuration(PubMedCentralConfig(), sections=("pubmed_central",))  # type: ignore[type-var]

        assert config.s3_bucket == "custom-bucket"
        assert config.ftp_host == "ftp.custom.com"
        assert config.ftp_path == "/custom/path/"
        assert config.s3_max_retry_attempts == 10
        assert config.max_table_nesting == 2
        assert config.local_archive_dir == "/custom_temp/custom_archive/"

        # Unchanged defaults
        assert config.pipeline_name == "coreason_etl_pubmedcentral"
        assert config.dataset_name == "pmc_refined"
        assert config.destination_name == "postgres"

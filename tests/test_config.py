# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral


from coreason_etl_pubmedcentral.config import PubMedCentralConfiguration, get_pipeline_config


def test_pubmed_central_configuration_defaults() -> None:
    """Test that the configuration class has the expected default values."""
    config = PubMedCentralConfiguration()
    assert config.s3_bucket == "pmc-oa-opendata"
    assert config.ftp_host == "ftp.ncbi.nlm.nih.gov"
    assert config.ftp_path == "/pub/pmc/"
    assert config.manifest_commercial == "oa_comm/oa_comm.filelist.csv"
    assert config.manifest_noncommercial == "oa_noncomm/oa_noncomm.filelist.csv"


def test_get_pipeline_config() -> None:
    """Test retrieving pipeline config via dlt injection mechanism."""
    import os

    # We can inject configuration values into the environment via dlt's naming convention.
    # The configspec uses __section__ = "pubmed_central".
    os.environ["PUBMED_CENTRAL__S3_BUCKET"] = "mock-bucket"
    os.environ["PUBMED_CENTRAL__FTP_HOST"] = "mock.host.com"
    os.environ["PUBMED_CENTRAL__MANIFEST_COMMERCIAL"] = "mock_comm.csv"

    try:
        config = get_pipeline_config()
        assert config.s3_bucket == "mock-bucket"
        assert config.ftp_host == "mock.host.com"
        assert config.ftp_path == "/pub/pmc/"  # Fallback to default
        assert config.manifest_commercial == "mock_comm.csv"
        assert config.manifest_noncommercial == "oa_noncomm/oa_noncomm.filelist.csv"  # Fallback to default
    finally:
        os.environ.pop("PUBMED_CENTRAL__S3_BUCKET", None)
        os.environ.pop("PUBMED_CENTRAL__FTP_HOST", None)
        os.environ.pop("PUBMED_CENTRAL__MANIFEST_COMMERCIAL", None)

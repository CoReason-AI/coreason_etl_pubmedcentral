# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import datetime
from unittest import mock

import pytest

from coreason_etl_pubmedcentral.bronze_ingestion import _pmc_xml_files_generator
from coreason_etl_pubmedcentral.config import PubMedCentralConfiguration


@pytest.fixture
def mock_config() -> PubMedCentralConfiguration:
    return PubMedCentralConfiguration(
        s3_bucket="test-bucket",
        ftp_host="test-ftp",
        ftp_path="/test-path",
        manifest_commercial="test-comm.csv",
        manifest_noncommercial="test-noncomm.csv",
    )


@pytest.fixture
def mock_incremental() -> mock.MagicMock:
    incremental = mock.MagicMock()
    incremental.last_value = None
    return incremental


@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.parse_manifest")
@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.SourceManager")
def test_pmc_xml_files_generator_success(
    mock_source_manager_class: mock.MagicMock,
    mock_parse_manifest: mock.MagicMock,
    mock_config: PubMedCentralConfiguration,
    mock_incremental: mock.MagicMock,
) -> None:
    """Positive test verifying successful ingestion yields correctly formatted records."""
    mock_parse_manifest.return_value = [
        {
            "file_path": "test/path/PMC123.xml.tar.gz",
            "accession_id": "PMC123",
            "last_updated": "2024-01-01 12:00:00",
            "pmid": 123,
            "license": "CC-BY",
            "retracted": False,
        }
    ]

    mock_source_manager = mock_source_manager_class.return_value
    mock_source_manager.get_file.return_value = "/local/temp/path.tar.gz"
    mock_source_manager.is_failover_active = False

    generator = _pmc_xml_files_generator(mock_config, "manifest.csv", mock_incremental)
    records = list(generator)

    assert len(records) == 1
    record = records[0]

    assert record["source_file_path"] == "/local/temp/path.tar.gz"
    assert record["ingestion_source"] == "S3"
    assert "ingestion_ts" in record
    assert record["last_updated"] == datetime.datetime(2024, 1, 1, 12, 0)
    assert record["file_metadata"]["accession_id"] == "PMC123"
    assert record["file_metadata"]["original_file_path"] == "test/path/PMC123.xml.tar.gz"

    mock_source_manager.get_file.assert_called_once_with("test/path/PMC123.xml.tar.gz")


@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.parse_manifest")
@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.SourceManager")
def test_pmc_xml_files_generator_failover(
    mock_source_manager_class: mock.MagicMock,
    mock_parse_manifest: mock.MagicMock,
    mock_config: PubMedCentralConfiguration,
    mock_incremental: mock.MagicMock,
) -> None:
    """Test verifying FTP ingestion source when failover is active."""
    mock_parse_manifest.return_value = [
        {
            "file_path": "test/path/PMC123.xml.tar.gz",
            "accession_id": "PMC123",
            "last_updated": "2024-01-01 12:00:00",
            "pmid": 123,
            "license": "CC-BY",
            "retracted": False,
        }
    ]

    mock_source_manager = mock_source_manager_class.return_value
    mock_source_manager.get_file.return_value = "/local/temp/path.tar.gz"
    mock_source_manager.is_failover_active = True

    generator = _pmc_xml_files_generator(mock_config, "manifest.csv", mock_incremental)
    records = list(generator)

    assert len(records) == 1
    assert records[0]["ingestion_source"] == "FTP"


@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.parse_manifest")
@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.SourceManager")
def test_pmc_xml_files_generator_download_failure(
    mock_source_manager_class: mock.MagicMock,
    mock_parse_manifest: mock.MagicMock,
    mock_config: PubMedCentralConfiguration,
    mock_incremental: mock.MagicMock,
) -> None:
    """Negative test verifying the generator handles and skips individual file download errors."""
    mock_parse_manifest.return_value = [
        {
            "file_path": "test/path/PMC123.xml.tar.gz",
            "accession_id": "PMC123",
            "last_updated": "2024-01-01 12:00:00",
            "pmid": 123,
            "license": "CC-BY",
            "retracted": False,
        }
    ]

    mock_source_manager = mock_source_manager_class.return_value
    mock_source_manager.get_file.side_effect = Exception("Download failed")

    generator = _pmc_xml_files_generator(mock_config, "manifest.csv", mock_incremental)
    records = list(generator)

    assert len(records) == 0


@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.parse_manifest")
@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.SourceManager")
def test_pmc_xml_files_generator_invalid_date(
    mock_source_manager_class: mock.MagicMock,
    mock_parse_manifest: mock.MagicMock,
    mock_config: PubMedCentralConfiguration,
    mock_incremental: mock.MagicMock,
) -> None:
    """Boundary test verifying the generator handles and skips records with invalid dates."""
    mock_parse_manifest.return_value = [
        {
            "file_path": "test/path/PMC123.xml.tar.gz",
            "accession_id": "PMC123",
            "last_updated": "INVALID_DATE",
            "pmid": 123,
            "license": "CC-BY",
            "retracted": False,
        }
    ]

    mock_source_manager = mock_source_manager_class.return_value
    mock_source_manager.get_file.return_value = "/local/temp/path.tar.gz"

    generator = _pmc_xml_files_generator(mock_config, "manifest.csv", mock_incremental)
    records = list(generator)

    assert len(records) == 0


@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.parse_manifest")
@mock.patch("coreason_etl_pubmedcentral.bronze_ingestion.SourceManager")
def test_pmc_xml_files_generator_type_validation_local_path(
    mock_source_manager_class: mock.MagicMock,
    mock_parse_manifest: mock.MagicMock,
    mock_config: PubMedCentralConfiguration,
    mock_incremental: mock.MagicMock,
) -> None:
    """Negative test verifying type validation on source_file_path."""
    mock_parse_manifest.return_value = [
        {
            "file_path": "test/path/PMC123.xml.tar.gz",
            "accession_id": "PMC123",
            "last_updated": "2024-01-01 12:00:00",
            "pmid": 123,
            "license": "CC-BY",
            "retracted": False,
        }
    ]

    mock_source_manager = mock_source_manager_class.return_value
    mock_source_manager.get_file.return_value = 123  # Invalid type, should be string

    generator = _pmc_xml_files_generator(mock_config, "manifest.csv", mock_incremental)

    with pytest.raises(TypeError, match="Expected source_file_path to be a string"):
        list(generator)


def test_pmc_xml_files() -> None:
    """Test the dlt decorator wrapper."""
    from coreason_etl_pubmedcentral.bronze_ingestion import pmc_xml_files

    # Just call it and see that it returns a generator-like dlt object
    # The dlt wrapper evaluates it lazily
    resource = pmc_xml_files(manifest_path="test.csv")
    assert resource.name == "bronze_pmc_file"
    assert resource.write_disposition == "append"

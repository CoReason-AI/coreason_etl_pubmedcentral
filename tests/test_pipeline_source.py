# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from datetime import datetime, timezone
from typing import Generator
from unittest.mock import MagicMock, mock_open, patch

import dlt
import pytest
from dlt.extract.exceptions import ResourceExtractionError

from coreason_etl_pubmedcentral.manifest import ManifestRecord
from coreason_etl_pubmedcentral.pipeline_source import pmc_source, pmc_xml_files
from coreason_etl_pubmedcentral.source_manager import SourceManager, SourceType


@pytest.fixture  # type: ignore[misc]
def mock_source_manager() -> Generator[MagicMock, None, None]:
    sm = MagicMock(spec=SourceManager)
    # Mock the _current_source property/attribute
    sm._current_source = SourceType.S3
    yield sm


def test_pmc_source_entry_point(mock_source_manager: MagicMock) -> None:
    # Test the source entry point function (pmc_source)
    # It just returns the resource.

    # We mock pmc_xml_files to verify it's called
    with patch("coreason_etl_pubmedcentral.pipeline_source.pmc_xml_files") as mock_resource:
        mock_resource.return_value = "resource_obj"

        # Bypass dlt wrapper
        fn = getattr(pmc_source, "__wrapped__", pmc_source)

        res = fn("path.csv", source_manager=mock_source_manager)

        assert res == "resource_obj"
        mock_resource.assert_called_with("path.csv", mock_source_manager)


def test_pmc_xml_files_happy_path(mock_source_manager: MagicMock) -> None:
    # Setup
    manifest_path = "dummy_manifest.csv"
    record = ManifestRecord(
        file_path="oa_comm/xml/PMC1.xml",
        accession_id="PMC1",
        last_updated=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        pmid="123",
        license_type="CC-BY",
        is_retracted=False,
    )

    mock_source_manager.get_file.return_value = b"<article>Content</article>"

    # Mock parse_manifest to return our record
    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]) as mock_parse:
        with patch("builtins.open", mock_open(read_data="header\nline")):
            # Execute
            # Use real incremental object
            inc = dlt.sources.incremental("last_updated")

            generator = pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc)
            items = list(generator)

            # Verify
            assert len(items) == 1
            item = items[0]

            assert item["source_file_path"] == "oa_comm/xml/PMC1.xml"
            assert item["ingestion_source"] == "S3"
            assert item["raw_xml_payload"] == "<article>Content</article>"
            assert item["manifest_metadata"]["accession_id"] == "PMC1"
            assert item["manifest_metadata"]["last_updated"] == "2024-01-01T12:00:00+00:00"
            assert item["last_updated"] == record.last_updated

            # Verify parse_manifest called with None cutoff (since incremental had no initial value)
            mock_parse.assert_called_once()
            _, kwargs = mock_parse.call_args
            assert kwargs["last_ingested_cutoff"] is None

            # Verify get_file called
            mock_source_manager.get_file.assert_called_with("oa_comm/xml/PMC1.xml")


def test_pmc_xml_files_incremental(mock_source_manager: MagicMock) -> None:
    # Setup
    manifest_path = "dummy_manifest.csv"
    start_time_str = "2024-01-01T10:00:00+00:00"

    # Use real incremental object with initial value (string)
    inc = dlt.sources.incremental("last_updated", initial_value=start_time_str)

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]) as mock_parse:
        with patch("builtins.open", mock_open()):
            list(pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc))

            # Verify cutoff passed to parse_manifest
            expected_cutoff = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
            mock_parse.assert_called_once()
            _, kwargs = mock_parse.call_args
            assert kwargs["last_ingested_cutoff"] == expected_cutoff


def test_pmc_xml_files_incremental_datetime_object(mock_source_manager: MagicMock) -> None:
    # Setup: simulate dlt passing a datetime object directly.
    # We bypass the dlt decorator to test internal logic directly.

    manifest_path = "dummy_manifest.csv"
    start_time_dt = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    # Simple object with start_value
    mock_inc = MagicMock()
    mock_inc.start_value = start_time_dt

    # Bypass dlt wrapper
    fn = getattr(pmc_xml_files, "__wrapped__", pmc_xml_files)

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]) as mock_parse:
        with patch("builtins.open", mock_open()):
            list(fn(manifest_path, source_manager=mock_source_manager, last_updated=mock_inc))

            # Verify cutoff passed to parse_manifest
            mock_parse.assert_called_once()
            _, kwargs = mock_parse.call_args
            assert kwargs["last_ingested_cutoff"] == start_time_dt


def test_pmc_xml_files_incremental_naive_datetime(mock_source_manager: MagicMock) -> None:
    # Setup: simulate dlt passing a NAIVE datetime object.
    # This should trigger line 66 (replace tzinfo).

    manifest_path = "dummy_manifest.csv"
    start_time_naive = datetime(2024, 1, 1, 10, 0, 0) # No tzinfo

    mock_inc = MagicMock()
    mock_inc.start_value = start_time_naive

    # Bypass dlt wrapper
    fn = getattr(pmc_xml_files, "__wrapped__", pmc_xml_files)

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]) as mock_parse:
        with patch("builtins.open", mock_open()):
            list(fn(manifest_path, source_manager=mock_source_manager, last_updated=mock_inc))

            # Verify cutoff passed to parse_manifest has UTC
            mock_parse.assert_called_once()
            _, kwargs = mock_parse.call_args

            cutoff = kwargs["last_ingested_cutoff"]
            assert cutoff is not None
            assert cutoff.tzinfo == timezone.utc
            assert cutoff.replace(tzinfo=None) == start_time_naive


def test_pmc_xml_files_bad_incremental_value(mock_source_manager: MagicMock) -> None:
    # Setup
    # Pass a bad string as initial value
    inc = dlt.sources.incremental("last_updated", initial_value="invalid-date")

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]) as mock_parse:
        with patch("builtins.open", mock_open()):
            # Patch logger to verify warning
            with patch("coreason_etl_pubmedcentral.pipeline_source.logger") as mock_logger:
                list(pmc_xml_files("path", source_manager=mock_source_manager, last_updated=inc))

                # Verify fallback to None
                mock_parse.assert_called_once()
                _, kwargs = mock_parse.call_args
                assert kwargs["last_ingested_cutoff"] is None

                # Verify warning log
                mock_logger.warning.assert_called()
                args, _ = mock_logger.warning.call_args
                assert "Could not parse incremental start value" in args[0]


def test_pmc_xml_files_file_fetch_error(mock_source_manager: MagicMock) -> None:
    # Setup
    record = ManifestRecord(
        file_path="fail.xml",
        accession_id="PMC1",
        last_updated=datetime.now(timezone.utc),
        pmid=None,
        license_type="CC0",
        is_retracted=False,
    )

    mock_source_manager.get_file.side_effect = Exception("Download failed")

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
        with patch("builtins.open", mock_open()):
            inc = dlt.sources.incremental("last_updated")

            items = list(pmc_xml_files("path", source_manager=mock_source_manager, last_updated=inc))

            # Should skip the failed item
            assert len(items) == 0


def test_manifest_file_not_found(mock_source_manager: MagicMock) -> None:
    # dlt wraps exceptions in ResourceExtractionError
    with patch("builtins.open", side_effect=FileNotFoundError):
        with pytest.raises(ResourceExtractionError) as excinfo:
             list(pmc_xml_files("missing.csv", source_manager=mock_source_manager))

        # Verify the underlying cause
        assert isinstance(excinfo.value.__cause__, FileNotFoundError)


def test_source_manager_auto_creation() -> None:
    # Verify that if no source manager is passed, one is created
    with patch("coreason_etl_pubmedcentral.pipeline_source.SourceManager") as MockSM:
        mock_instance = MockSM.return_value
        mock_instance._current_source = SourceType.S3
        mock_instance.get_file.return_value = b""

        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]):
            with patch("builtins.open", mock_open()):
                list(pmc_xml_files("path"))

        MockSM.assert_called_once()

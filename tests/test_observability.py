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
from typing import Any
from unittest.mock import MagicMock, patch

import dlt
import pytest

from coreason_etl_pubmedcentral.manifest import ManifestRecord
from coreason_etl_pubmedcentral.pipeline_source import pmc_xml_files
from coreason_etl_pubmedcentral.source_manager import SourceManager, SourceType
from coreason_etl_pubmedcentral.utils.logger import logger as app_logger


@pytest.fixture  # type: ignore[misc]
def mock_source_manager() -> MagicMock:
    sm = MagicMock(spec=SourceManager)
    sm._current_source = SourceType.S3
    return sm


def test_pipeline_source_emits_success_metrics(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
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

    captured_logs: list[Any] = []

    def sink(message: Any) -> None:
        captured_logs.append(message.record)

    handler_id = app_logger.add(sink)

    try:
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
            with patch("builtins.open", side_effect=mock_manifest_open(read_data="header\nline")):
                inc = dlt.sources.incremental("last_updated")
                list(pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc))

        # Verify success log
        found = False
        for log_record in captured_logs:
            extra = log_record["extra"]
            if extra.get("metric") == "records_ingested_total":
                labels = extra.get("labels", {})
                if labels.get("status") == "success" and labels.get("source") == "s3":
                    found = True
                    break
        assert found, "Did not find success metric log"

    finally:
        app_logger.remove(handler_id)


def test_pipeline_source_emits_failure_metrics(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    # Setup
    manifest_path = "dummy_manifest.csv"
    record = ManifestRecord(
        file_path="fail.xml",
        accession_id="PMC1",
        last_updated=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        pmid=None,
        license_type="CC0",
        is_retracted=False,
    )
    mock_source_manager.get_file.side_effect = Exception("Download failed")

    captured_logs: list[Any] = []

    def sink(message: Any) -> None:
        captured_logs.append(message.record)

    handler_id = app_logger.add(sink)

    try:
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
            with patch("builtins.open", side_effect=mock_manifest_open(read_data="header\nline")):
                inc = dlt.sources.incremental("last_updated")
                list(pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc))

        # Verify failure log
        found = False
        for log_record in captured_logs:
            extra = log_record["extra"]
            if extra.get("metric") == "records_ingested_total":
                labels = extra.get("labels", {})
                if labels.get("status") == "fail" and labels.get("source") == "s3":
                    found = True
                    break
        assert found, "Did not find failure metric log"

    finally:
        app_logger.remove(handler_id)


def test_pipeline_source_metrics_failover_scenario(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Test Complex Scenario: 1 Failure (S3) -> Failover -> 1 Success (FTP).
    """
    manifest_path = "dummy_manifest.csv"
    records = [
        ManifestRecord(
            file_path="fail.xml",
            accession_id="PMC1",
            last_updated=datetime.now(timezone.utc),
            pmid=None,
            license_type="CC0",
            is_retracted=False,
        ),
        ManifestRecord(
            file_path="success.xml",
            accession_id="PMC2",
            last_updated=datetime.now(timezone.utc),
            pmid=None,
            license_type="CC0",
            is_retracted=False,
        ),
    ]

    def get_file_side_effect(path: str) -> bytes:
        if path == "fail.xml":
            mock_source_manager._current_source = SourceType.S3
            raise Exception("S3 Error")
        elif path == "success.xml":
            mock_source_manager._current_source = SourceType.FTP
            return b"<article>Content</article>"
        return b""

    mock_source_manager.get_file.side_effect = get_file_side_effect

    captured_logs: list[Any] = []
    handler_id = app_logger.add(lambda m: captured_logs.append(m.record))

    try:
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=records):
            with patch("builtins.open", side_effect=mock_manifest_open(read_data="header\nline")):
                inc = dlt.sources.incremental("last_updated")
                list(pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc))

        # Analyze logs
        metrics = [log["extra"] for log in captured_logs if log["extra"].get("metric") == "records_ingested_total"]

        assert len(metrics) == 2

        # 1st Metric: Fail, S3
        assert metrics[0]["labels"]["status"] == "fail"
        assert metrics[0]["labels"]["source"] == "s3"

        # 2nd Metric: Success, FTP
        assert metrics[1]["labels"]["status"] == "success"
        assert metrics[1]["labels"]["source"] == "ftp"

    finally:
        app_logger.remove(handler_id)


def test_pipeline_source_metrics_retraction_edge_case(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Edge Case: Retracted article.
    """
    manifest_path = "dummy_manifest.csv"
    record = ManifestRecord(
        file_path="retracted.xml",
        accession_id="PMC1",
        last_updated=datetime.now(timezone.utc),
        pmid="123",
        license_type="CC-BY",
        is_retracted=True,  # <--- Retracted
    )
    mock_source_manager.get_file.return_value = b"<article>Content</article>"

    captured_logs: list[Any] = []
    handler_id = app_logger.add(lambda m: captured_logs.append(m.record))

    try:
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
            with patch("builtins.open", side_effect=mock_manifest_open(read_data="header\nline")):
                inc = dlt.sources.incremental("last_updated")
                list(pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc))

        metrics = [log["extra"] for log in captured_logs if log["extra"].get("metric") == "records_ingested_total"]
        assert len(metrics) == 1
        assert metrics[0]["labels"]["status"] == "success"

    finally:
        app_logger.remove(handler_id)


def test_pipeline_source_metrics_zero_byte_file(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Edge Case: Zero-byte file.
    """
    manifest_path = "dummy_manifest.csv"
    record = ManifestRecord(
        file_path="empty.xml",
        accession_id="PMC1",
        last_updated=datetime.now(timezone.utc),
        pmid="123",
        license_type="CC-BY",
        is_retracted=False,
    )
    mock_source_manager.get_file.return_value = b""  # <--- Zero bytes

    captured_logs: list[Any] = []
    handler_id = app_logger.add(lambda m: captured_logs.append(m.record))

    try:
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
            with patch("builtins.open", side_effect=mock_manifest_open(read_data="header\nline")):
                inc = dlt.sources.incremental("last_updated")
                list(pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc))

        metrics = [log["extra"] for log in captured_logs if log["extra"].get("metric") == "records_ingested_total"]
        assert len(metrics) == 1
        assert metrics[0]["labels"]["status"] == "success"

    finally:
        app_logger.remove(handler_id)

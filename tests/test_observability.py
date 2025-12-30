# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from unittest.mock import MagicMock, mock_open, patch
from datetime import datetime, timezone
import dlt
import pytest
from coreason_etl_pubmedcentral.pipeline_source import pmc_xml_files
from coreason_etl_pubmedcentral.manifest import ManifestRecord
from coreason_etl_pubmedcentral.source_manager import SourceManager, SourceType
from coreason_etl_pubmedcentral.utils.logger import logger as app_logger

@pytest.fixture
def mock_source_manager():
    sm = MagicMock(spec=SourceManager)
    sm._current_source = SourceType.S3
    return sm

def test_pipeline_source_emits_success_metrics(mock_source_manager):
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

    # Capture logs
    captured_logs = []

    # We can't easily patch the 'logger' variable inside the module since it's imported.
    # But since we use loguru, we can add a sink!

    def sink(message):
        record = message.record
        captured_logs.append(record)

    handler_id = app_logger.add(sink)

    try:
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
            with patch("builtins.open", mock_open(read_data="header\nline")):
                inc = dlt.sources.incremental("last_updated")
                list(pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc))

        # Verify success log
        found = False
        for record in captured_logs:
            extra = record["extra"]
            if extra.get("metric") == "records_ingested_total":
                labels = extra.get("labels", {})
                if labels.get("status") == "success" and labels.get("source") == "s3":
                    found = True
                    break
        assert found, "Did not find success metric log"

    finally:
        app_logger.remove(handler_id)

def test_pipeline_source_emits_failure_metrics(mock_source_manager):
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

    captured_logs = []
    def sink(message):
        captured_logs.append(message.record)

    handler_id = app_logger.add(sink)

    try:
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
            with patch("builtins.open", mock_open(read_data="header\nline")):
                inc = dlt.sources.incremental("last_updated")
                list(pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc))

        # Verify failure log
        found = False
        for record in captured_logs:
            extra = record["extra"]
            if extra.get("metric") == "records_ingested_total":
                labels = extra.get("labels", {})
                if labels.get("status") == "fail" and labels.get("source") == "s3":
                    found = True
                    break
        assert found, "Did not find failure metric log"

    finally:
        app_logger.remove(handler_id)

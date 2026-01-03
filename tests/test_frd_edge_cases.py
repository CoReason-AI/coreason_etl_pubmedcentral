# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import builtins
from datetime import datetime, timezone
from typing import Any, Generator
from unittest.mock import MagicMock, mock_open, patch

import pytest
from dlt.common import pendulum

from coreason_etl_pubmedcentral.manifest import ManifestRecord
from coreason_etl_pubmedcentral.pipeline_silver import transform_silver_record
from coreason_etl_pubmedcentral.pipeline_source import pmc_xml_files
from coreason_etl_pubmedcentral.source_manager import SourceManager, SourceType
from coreason_etl_pubmedcentral.utils.logger import logger

# --- Author Schema Edge Cases ---


@pytest.fixture  # type: ignore[misc]
def sample_bronze_template() -> dict[str, Any]:
    return {
        "source_file_path": "oa_comm/xml/PMC12345.xml",
        "ingestion_ts": pendulum.now(),
        "ingestion_source": "S3",
        "raw_xml_payload": "",
        "manifest_metadata": {
            "accession_id": "PMC12345",
            "last_updated": "2024-01-01T12:00:00+00:00",
            "is_retracted": False,
        },
    }


def test_author_flattening_edge_cases(sample_bronze_template: dict[str, Any]) -> None:
    """
    Test robust handling of author name flattening.
    Cases:
    1. Surname Only
    2. Given Name Only
    3. Both Missing
    4. Whitespace padding in XML (handled by parser, but verifying result)
    5. Empty strings
    """
    xml = """
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC1</article-id>
                <title-group><article-title>T</article-title></title-group>
                <contrib-group>
                    <!-- Case 1: Surname Only -->
                    <contrib>
                        <name><surname>Doe</surname></name>
                    </contrib>
                    <!-- Case 2: Given Name Only -->
                    <contrib>
                        <name><given-names>John</given-names></name>
                    </contrib>
                    <!-- Case 3: Both Missing (Empty name tag) -->
                    <contrib>
                        <name></name>
                    </contrib>
                    <!-- Case 4: Whitespace (Parser strips this, but Silver joins) -->
                    <contrib>
                        <name>
                            <surname>  Smith  </surname>
                            <given-names>  Jane  </given-names>
                        </name>
                    </contrib>
                     <!-- Case 5: Empty Strings (Parser usually returns None for empty text) -->
                    <contrib>
                        <name>
                            <surname></surname>
                            <given-names></given-names>
                        </name>
                    </contrib>
                </contrib-group>
            </article-meta>
        </front>
    </article>
    """
    sample_bronze_template["raw_xml_payload"] = xml
    record = transform_silver_record(sample_bronze_template)

    assert record is not None
    authors = record["authors"]
    assert len(authors) == 5

    # Case 1: "Doe" + "" -> strip() -> "Doe"
    assert authors[0]["name"] == "Doe"

    # Case 2: "" + "John" -> strip() -> "John"
    assert authors[1]["name"] == "John"

    # Case 3: "" + "" -> ""
    assert authors[2]["name"] == ""

    # Case 4: "Smith" + "Jane" -> "Smith Jane"
    assert authors[3]["name"] == "Smith Jane"

    # Case 5: "" + "" -> ""
    assert authors[4]["name"] == ""


# --- Partitioning Boundary Cases ---


def test_ingestion_date_midnight_rollover() -> None:
    """
    Verify that ingestion_date reflects the current UTC date,
    checking boundaries around midnight.
    """
    # Define selective mock open
    real_open = builtins.open

    def conditional_open(file: Any, *args: Any, **kwargs: Any) -> Any:
        if file == "manifest.csv":
            return mock_open(read_data="header\nline")(file, *args, **kwargs)
        return real_open(file, *args, **kwargs)

    # 1. Before Midnight (23:59:59)
    ts_before = datetime(2024, 1, 1, 23, 59, 59, tzinfo=timezone.utc)

    # Mock datetime.now to return ts_before
    with patch("coreason_etl_pubmedcentral.pipeline_source.datetime") as mock_dt:
        mock_dt.now.return_value = ts_before
        # We need mock_dt.fromisoformat to work as real one for unrelated calls if any,
        # but pmc_source mostly uses now().

        sm = MagicMock(spec=SourceManager)
        sm._current_source = SourceType.S3
        sm.get_file.return_value = b"<xml/>"

        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest") as mock_parse:
            # Return one record
            mock_parse.return_value = [
                ManifestRecord(
                    file_path="f1",
                    last_updated=ts_before,
                    accession_id="1",
                    pmid=None,
                    license_type="CC0",
                    is_retracted=False,
                )
            ]

            # Run generator with selective open mock
            with patch("builtins.open", side_effect=conditional_open):
                gen = pmc_xml_files("manifest.csv", source_manager=sm)
                items = list(gen)

                assert len(items) == 1
                assert items[0]["ingestion_date"] == ts_before.date()  # 2024-01-01

    # 2. After Midnight (00:00:01 Next Day)
    ts_after = datetime(2024, 1, 2, 0, 0, 1, tzinfo=timezone.utc)

    with patch("coreason_etl_pubmedcentral.pipeline_source.datetime") as mock_dt:
        mock_dt.now.return_value = ts_after

        sm = MagicMock(spec=SourceManager)
        sm._current_source = SourceType.S3
        sm.get_file.return_value = b"<xml/>"

        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest") as mock_parse:
            mock_parse.return_value = [
                ManifestRecord(
                    file_path="f1",
                    last_updated=ts_after,
                    accession_id="1",
                    pmid=None,
                    license_type="CC0",
                    is_retracted=False,
                )
            ]
            with patch("builtins.open", side_effect=conditional_open):
                gen = pmc_xml_files("manifest.csv", source_manager=sm)
                items = list(gen)

                assert len(items) == 1
                assert items[0]["ingestion_date"] == ts_after.date()  # 2024-01-02


# --- Logging Robustness ---


@pytest.fixture  # type: ignore[misc]
def log_capture() -> Generator[list[str], None, None]:
    logs: list[str] = []
    handler_id = logger.add(lambda m: logs.append(m), format="{message}")
    yield logs
    logger.remove(handler_id)


def test_logging_special_chars_and_formatting(sample_bronze_template: dict[str, Any], log_capture: list[str]) -> None:
    """
    Verify SchemaViolation logs maintain the em-dash format
    even with special characters in PMCID.
    """
    # XML missing title, weird PMCID
    xml = """
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC_weird_@#$</article-id>
            </article-meta>
        </front>
    </article>
    """
    sample_bronze_template["raw_xml_payload"] = xml

    transform_silver_record(sample_bronze_template)

    # Expect: "SchemaViolation — Article _weird_@#$ missing mandatory 'title' field."
    # Note: Logic strips "PMC" prefix. "PMC_weird..." -> "_weird..."

    expected_msg = "SchemaViolation — Article _weird_@#$ missing mandatory 'title' field."
    assert any(expected_msg in m for m in log_capture)


def test_logging_missing_pmcid(sample_bronze_template: dict[str, Any], log_capture: list[str]) -> None:
    """
    Verify SchemaViolation log when PMCID is missing entirely.
    """
    xml = """
    <article article-type="research-article">
        <front>
            <article-meta>
                <!-- No IDs -->
            </article-meta>
        </front>
    </article>
    """
    sample_bronze_template["raw_xml_payload"] = xml

    transform_silver_record(sample_bronze_template)

    assert any("SchemaViolation — Article missing mandatory 'pmcid' field." in m for m in log_capture)

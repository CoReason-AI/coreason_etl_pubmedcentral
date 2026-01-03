# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from typing import Generator

import pytest
from dlt.common import pendulum as dlt_pendulum

from coreason_etl_pubmedcentral.manifest import parse_manifest
from coreason_etl_pubmedcentral.pipeline_silver import transform_silver_record
from coreason_etl_pubmedcentral.utils.logger import logger


@pytest.fixture  # type: ignore[misc]
def log_capture() -> Generator[list[str], None, None]:
    logs = []
    handler_id = logger.add(lambda m: logs.append(m), format="{message}", level="INFO")
    yield logs
    logger.remove(handler_id)


def test_perfect_storm_retracted_malformed_empty(log_capture: list[str]) -> None:
    """
    "The Perfect Storm":
    - Manifest says Retracted.
    - XML has malformed date (Year='abcd').
    - XML has broken funding (Agency but no ID, weird nesting).
    - XML has weird authors (Empty name).
    - XML has empty abstract.
    """
    manifest_metadata = {
        "accession_id": "PMC123",
        "last_updated": "2024-01-01T12:00:00+00:00",
        "is_retracted": True,  # RETRACTED
    }

    xml = """
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC123</article-id>
                <title-group><article-title>Stormy Article</article-title></title-group>
                <pub-date pub-type="epub">
                    <year>abcd</year> <!-- Malformed Date -->
                </pub-date>
                <contrib-group>
                    <contrib><name><surname></surname></name></contrib> <!-- Empty Author -->
                </contrib-group>
                <funding-group>
                    <award-group>
                        <funding-source>Broken Agency</funding-source>
                        <!-- No Award ID -->
                    </award-group>
                </funding-group>
                <abstract>   </abstract> <!-- Empty Abstract (Whitespace) -->
            </article-meta>
        </front>
    </article>
    """

    item = {
        "source_file_path": "path/storm.xml",
        "ingestion_ts": dlt_pendulum.now(),
        "ingestion_source": "S3",
        "raw_xml_payload": xml,
        "manifest_metadata": manifest_metadata,
    }

    record = transform_silver_record(item)
    assert record is not None

    # Verify Retraction
    assert record["is_retracted"] is True
    # Log check for retraction
    assert any("RetractionFound" in m for m in log_capture)

    # Verify Date (Should be 'abcd-01-01' based on robustness logic)
    assert record["date_published"] == "abcd-01-01"

    # Verify Authors (Empty name -> empty string or None handling)
    # Parser: surname="", given="" -> name=""
    assert record["authors"][0]["name"] == ""

    # Verify Funding (1 entry, None ID)
    assert len(record["funding"]) == 1
    assert record["funding"][0]["agency"] == "Broken Agency"
    assert record["funding"][0]["grant_id"] is None

    # Verify Abstract (Should be None due to whitespace only?)
    # _get_full_text strips whitespace. If empty -> None.
    assert record["abstract"] is None


def test_zombie_file_updated_but_empty(log_capture: list[str]) -> None:
    """
    "Zombie File": Manifest says updated, but XML content is empty string.
    Should return None (skip).
    """
    item = {
        "source_file_path": "path/zombie.xml",
        "ingestion_ts": dlt_pendulum.now(),
        "ingestion_source": "S3",
        "raw_xml_payload": "",  # Empty
        "manifest_metadata": {"accession_id": "PMC_Zombie"},
    }

    record = transform_silver_record(item)

    assert record is None
    assert any("Skipping record with empty raw_xml_payload" in m for m in log_capture)


def test_identity_crisis_id_mismatch(log_capture: list[str]) -> None:
    """
    "Identity Crisis": Manifest ID (PMC_Manifest) != XML ID (PMC_XML).
    Silver should use XML ID for the record 'pmcid' field.
    Manifest metadata should be preserved in 'manifest_metadata'.
    """
    manifest_metadata = {
        "accession_id": "PMC_Manifest",
        "last_updated": "2024-01-01",
        "is_retracted": False,
    }

    xml = """
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC_XML</article-id>
                <title-group><article-title>T</article-title></title-group>
            </article-meta>
        </front>
    </article>
    """

    item = {
        "source_file_path": "path/mismatch.xml",
        "ingestion_ts": dlt_pendulum.now(),
        "ingestion_source": "S3",
        "raw_xml_payload": xml,
        "manifest_metadata": manifest_metadata,
    }

    record = transform_silver_record(item)
    assert record is not None

    # Primary Identity from XML
    # Logic: `if raw_pmc.upper().startswith("PMC"): pmcid = raw_pmc[3:]`
    # So "PMC_XML" -> "_XML".
    assert record["pmcid"] == "_XML"

    # Manifest Metadata preserved
    assert record["manifest_metadata"]["accession_id"] == "PMC_Manifest"


def test_manifest_malformed_dates_skipping() -> None:
    """
    Verify that rows with malformed dates in the manifest are strictly skipped.
    """
    data = [
        "File Path,Accession ID,Last Updated (UTC),PMID,License,Retracted",
        "p1,acc1,INVALID-DATE,123,CC0,no",
        "p2,acc2,2024-01-01 12:00:00,123,CC0,no",
    ]
    records = list(parse_manifest(iter(data)))

    assert len(records) == 1
    assert records[0].accession_id == "acc2"

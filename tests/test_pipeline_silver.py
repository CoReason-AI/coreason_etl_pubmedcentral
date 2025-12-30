# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from typing import Any
from unittest.mock import patch

import pytest
from dlt.common import pendulum
from lxml import etree

from coreason_etl_pubmedcentral.pipeline_silver import _pmc_silver_generator, transform_silver_record


@pytest.fixture  # type: ignore[misc]
def sample_xml_content() -> str:
    """Returns a valid JATS XML string for testing."""
    return """
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC12345</article-id>
                <article-id pub-id-type="pmid">111111</article-id>
                <title-group>
                    <article-title>Test Article</article-title>
                </title-group>
                <contrib-group>
                    <contrib>
                        <name><surname>Doe</surname><given-names>John</given-names></name>
                        <xref ref-type="aff" rid="aff1"/>
                    </contrib>
                </contrib-group>
                <aff id="aff1">University of Test</aff>
                <pub-date pub-type="epub">
                    <year>2024</year><month>01</month><day>01</day>
                </pub-date>
            </article-meta>
        </front>
        <body></body>
    </article>
    """


@pytest.fixture  # type: ignore[misc]
def sample_bronze_record(sample_xml_content: str) -> dict[str, Any]:
    return {
        "source_file_path": "oa_comm/xml/PMC12345.xml",
        "ingestion_ts": pendulum.now(),
        "ingestion_source": "S3",
        "raw_xml_payload": sample_xml_content,
        "manifest_metadata": {
            "file_path": "oa_comm/xml/PMC12345.xml",
            "accession_id": "PMC12345",
            "last_updated": "2024-01-01T12:00:00+00:00",
            "is_retracted": False,
        },
        "last_updated": pendulum.datetime(2024, 1, 1, 12, 0, 0),
    }


def test_transform_silver_record_success(sample_bronze_record: dict[str, Any]) -> None:
    record = transform_silver_record(sample_bronze_record)

    assert record is not None

    # Check Identity
    assert record["pmcid"] == "12345"
    assert record["pmid"] == "111111"
    assert record["article_type"] == "RESEARCH"

    # Check Dates
    assert record["date_published"] == "2024-01-01"

    # Check Authors
    assert len(record["authors"]) == 1
    assert record["authors"][0]["surname"] == "Doe"
    assert record["authors"][0]["affiliations"] == ["University of Test"]

    # Check Retraction (False)
    assert record["is_retracted"] is False

    # Check Metadata pass-through
    assert record["ingestion_metadata"]["source_file_path"] == "oa_comm/xml/PMC12345.xml"
    assert record["manifest_metadata"]["accession_id"] == "PMC12345"


def test_transform_silver_record_retraction(sample_bronze_record: dict[str, Any]) -> None:
    # Set retracted = True in manifest
    sample_bronze_record["manifest_metadata"]["is_retracted"] = True

    record = transform_silver_record(sample_bronze_record)
    assert record is not None
    assert record["is_retracted"] is True


def test_transform_silver_record_malformed_xml(sample_bronze_record: dict[str, Any]) -> None:
    # Malformed XML (raises XMLSyntaxError usually, but iterparse might catch it or raise it)
    # We catch XMLSyntaxError explicitly.
    sample_bronze_record["raw_xml_payload"] = "<article>Unclosed tag"

    record = transform_silver_record(sample_bronze_record)
    assert record is None


def test_transform_silver_record_empty_xml(sample_bronze_record: dict[str, Any]) -> None:
    sample_bronze_record["raw_xml_payload"] = ""
    record = transform_silver_record(sample_bronze_record)
    assert record is None


def test_transform_silver_record_no_article_tag(sample_bronze_record: dict[str, Any]) -> None:
    # Valid XML but no article tag
    sample_bronze_record["raw_xml_payload"] = "<root><foo>bar</foo></root>"

    record = transform_silver_record(sample_bronze_record)
    assert record is None


def test_transform_silver_record_unicode(sample_bronze_record: dict[str, Any]) -> None:
    # Unicode in XML
    xml = """
    <article article-type="research-article">
        <front>
            <article-meta>
                <contrib-group>
                    <contrib>
                        <name><surname>Müller</surname></name>
                    </contrib>
                </contrib-group>
            </article-meta>
        </front>
    </article>
    """
    sample_bronze_record["raw_xml_payload"] = xml

    record = transform_silver_record(sample_bronze_record)
    assert record is not None
    assert record["authors"][0]["surname"] == "Müller"


def test_transform_silver_record_xml_syntax_error(sample_bronze_record: dict[str, Any]) -> None:
    # Mock etree.iterparse to raise XMLSyntaxError explicitly
    # Note: we need to mock iterparse from where it is imported/used
    with patch("coreason_etl_pubmedcentral.pipeline_silver.etree.iterparse") as mock_iterparse:
        mock_iterparse.side_effect = etree.XMLSyntaxError("Invalid XML", 0, 0, 0)

        record = transform_silver_record(sample_bronze_record)
        assert record is None


def test_transform_silver_record_unexpected_exception(sample_bronze_record: dict[str, Any]) -> None:
    # Mock parse_article_identity to raise generic Exception
    with patch("coreason_etl_pubmedcentral.pipeline_silver.parse_article_identity") as mock_parse:
        mock_parse.side_effect = Exception("Boom")

        record = transform_silver_record(sample_bronze_record)
        assert record is None


def test_pmc_silver_generator_iteration(sample_bronze_record: dict[str, Any]) -> None:
    # Test that the generator iterates and skips None

    # 1. Good record
    # 2. Bad record (returns None)
    # 3. Good record

    bad_record = sample_bronze_record.copy()
    bad_record["raw_xml_payload"] = "bad"

    items = [sample_bronze_record, bad_record, sample_bronze_record]

    results = list(_pmc_silver_generator(iter(items)))

    assert len(results) == 2
    assert results[0]["pmcid"] == "12345"
    assert results[1]["pmcid"] == "12345"


def test_memory_clearing_pattern(sample_bronze_record: dict[str, Any]) -> None:
    # Construct an XML with siblings to trigger `del elem.getparent()[0]`
    # We wrap the article in a root and add a previous sibling
    xml = """
    <root>
        <dummy>ignore me</dummy>
        <article article-type="research-article">
            <front>
                <article-meta>
                    <article-id pub-id-type="pmc">PMC999</article-id>
                </article-meta>
            </front>
        </article>
    </root>
    """
    sample_bronze_record["raw_xml_payload"] = xml

    # The transform should still work and find the article
    # And specifically, since there is a <dummy> sibling before <article>,
    # when <article> ends, it has a previous sibling.

    record = transform_silver_record(sample_bronze_record)

    assert record is not None
    assert record["pmcid"] == "999"
    # The coverage tool will confirm if the line was hit.

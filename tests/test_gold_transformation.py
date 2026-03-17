# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import json

from coreason_etl_pubmedcentral.gold_transformation import _gold_transformer_generator, build_gold_analytics


def test_gold_transformation_success() -> None:
    """Positive test verifying successful transformation of a silver record into a gold record."""
    silver_record = {
        "coreason_id": "uuid-1234",
        "pmcid": "PMC12345",
        "source_file_path": "oa_comm/xml/all/PMC12345.xml.tar.gz",
        "file_metadata": {"retracted": True, "license": "CC-BY"},
        "article": {
            "identity": {
                "pmcid": "12345",
                "title": "Test Title",
                "abstract": "Test Abstract",
                "journal_name": "Test Journal",
                "keywords": ["Keyword A", "Keyword B"],
            },
            "temporal": {"date_published": "2024-01-01"},
            "entity": {
                "contributors": [
                    {"name": "Doe J", "affs": ["Aff 1", "Aff 2"]},
                    {"name": "Smith A", "affs": ["Aff 1", "Aff 3"]},
                ]
            },
            "funding": {
                "funding": [
                    {"agency": "NIH", "grant_id": "G123"},
                    {"agency": "NSF", "grant_id": "N456"},
                ]
            },
        },
    }

    generator = _gold_transformer_generator(silver_record)
    records = list(generator)

    assert len(records) == 1
    record = records[0]

    assert record["coreason_id"] == "uuid-1234"
    assert record["pmcid"] == "PMC12345"

    # JSON arrays
    assert json.loads(record["grant_ids"]) == sorted(["G123", "N456"])
    assert json.loads(record["agency_names"]) == sorted(["NIH", "NSF"])
    assert json.loads(record["affiliations_text"]) == sorted(["Aff 1", "Aff 2", "Aff 3"])
    assert json.loads(record["keywords"]) == ["Keyword A", "Keyword B"]

    assert record["authors_display"] == "Doe J; Smith A"
    assert record["is_commercial_safe"] is True
    assert record["is_retracted"] is True
    assert record["license_type"] == "CC-BY"
    assert record["pub_year"] == 2024
    assert record["journal_name"] == "Test Journal"
    assert record["title"] == "Test Title"
    assert record["abstract"] == "Test Abstract"


def test_gold_transformation_missing_fields() -> None:
    """Test verifying handling of missing fields with fallback defaults."""
    silver_record = {
        "coreason_id": "uuid-5678",
        "pmcid": "PMC56789",
        "source_file_path": "oa_noncomm/xml/all/PMC56789.xml.tar.gz",
        "article": {
            # Minimal structure
            "entity": {},
            "funding": {},
            "temporal": {"date_published": "INVALID"},
        },
    }

    generator = _gold_transformer_generator(silver_record)
    records = list(generator)

    assert len(records) == 1
    record = records[0]

    assert record["coreason_id"] == "uuid-5678"
    assert json.loads(record["grant_ids"]) == []
    assert json.loads(record["agency_names"]) == []
    assert json.loads(record["affiliations_text"]) == []

    assert record["authors_display"] == ""
    assert record["is_commercial_safe"] is False
    assert record["is_retracted"] is False
    assert record["license_type"] == ""
    assert record["pub_year"] is None


def test_gold_transformation_missing_article() -> None:
    """Negative test verifying skipping of records when 'article' is missing."""
    silver_record = {"coreason_id": "uuid-9999"}

    generator = _gold_transformer_generator(silver_record)
    records = list(generator)

    assert len(records) == 0


def test_build_gold_analytics_decorator() -> None:
    """Test the dlt decorator wrapper initialization."""
    # Since dlt wrappers don't easily allow bypassing the engine to call _func,
    # we just assert it's a valid DltResource object with the right attributes.
    assert build_gold_analytics.name == "gold_pmc_analytics_rich"
    assert build_gold_analytics.write_disposition == "append"

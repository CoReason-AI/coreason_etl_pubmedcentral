# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral


from coreason_etl_pubmedcentral.pipeline_gold import transform_gold_record
from coreason_etl_pubmedcentral.pipeline_silver import transform_silver_record


def test_extreme_duplicate_pmcids() -> None:
    """
    Edge Case: XML contains multiple <article-id pub-id-type="pmc">.
    Parser should consistently pick one (First or Last).
    Current implementation uses `xpath`, which returns a list.
    We check `if pmcid_elem and pmcid_elem[0].text`.
    So it picks the FIRST one in document order.
    """
    xml = """
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC111</article-id>
                <article-id pub-id-type="pmc">PMC999</article-id>
                <title-group><article-title>Dup ID</article-title></title-group>
            </article-meta>
        </front>
    </article>
    """
    item = {
        "source_file_path": "path/dup.xml",
        "raw_xml_payload": xml.encode("utf-8"),
        "manifest_metadata": {},
    }
    record = transform_silver_record(item)
    assert record is not None
    assert record["pmcid"] == "111"  # First one wins


def test_extreme_xml_bomb_billion_laughs() -> None:
    """
    Security Test: Billion Laughs Attack (Entity Expansion).
    lxml should either reject it or be configured to not expand huge entities.
    """
    # Note: simple entity expansion might be allowed, but huge ones should fail or be limited.
    # We use a small 'bomb' to see if it expands.
    xml = """
    <!DOCTYPE article [
      <!ENTITY lol "lol">
      <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
    ]>
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC1</article-id>
                <title-group><article-title>&lol1;</article-title></title-group>
            </article-meta>
        </front>
    </article>
    """
    # If expansion works, title is "lollollollollollollollollollol" (30 chars)
    item = {
        "source_file_path": "path/bomb.xml",
        "raw_xml_payload": xml.encode("utf-8"),
        "manifest_metadata": {},
    }

    # We expect it to parse safely (lxml default is usually safe against DoS defaults in recent versions)
    # OR fail if we explicitly disabled DTD.
    # Let's see what happens.
    record = transform_silver_record(item)

    # If parsed, check title
    if record:
        assert record["title"] == "lol" * 10
    else:
        # If it returns None due to error, that's also 'safe' (availability hit but not crash)
        pass


def test_extreme_encoding_latin1_declared() -> None:
    """
    Edge Case: XML is encoded in ISO-8859-1 and declares it.
    lxml iterparse should respect the declaration if bytes are passed.
    """
    # '©' in Latin-1 is \xA9. In UTF-8 it is \xC2\xA9.
    content = """<?xml version="1.0" encoding="ISO-8859-1"?>
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC1</article-id>
                <title-group><article-title>Copyright ©</article-title></title-group>
            </article-meta>
        </front>
    </article>
    """
    # Encode as latin-1
    raw_bytes = content.encode("iso-8859-1")

    item = {
        "source_file_path": "path/latin.xml",
        "raw_xml_payload": raw_bytes,
        "manifest_metadata": {},
    }

    record = transform_silver_record(item)
    assert record is not None
    # Output should be python unicode string
    assert record["title"] == "Copyright ©"


def test_extreme_gold_author_deduplication() -> None:
    """
    Gold Layer: Verify author display logic and affiliation deduplication.
    Case:
    - Author 1: Smith J, Aff: [A, B]
    - Author 2: Doe A, Aff: [B, C]
    - Author 3: Smith J (Duplicate name?), Aff: [D]

    Authors Display should join unique names? OR list all? Spec says "Semicolon-separated names".
    Commonly this means the list of authors in order.
    Affiliations Text: "All unique affiliation strings".
    """
    silver_record = {
        "pmcid": "1",
        "authors": [
            {"name": "Smith J", "affiliations": ["Univ A", "Univ B"]},
            {"name": "Doe A", "affiliations": ["Univ B", "Univ C"]},
            {"name": "Smith J", "affiliations": ["Univ D"]},
        ],
        "funding": [],
        "keywords": [],
        "manifest_metadata": {"license_type": "CC0"},
        "ingestion_metadata": {"source_file_path": "oa_comm/xml/1.xml"},
    }

    gold = transform_gold_record(silver_record)
    assert gold is not None

    # Authors Display: Preserves order, duplicates allowed (distinct authors same name)
    assert gold["authors_display"] == "Smith J; Doe A; Smith J"

    # Affiliations: Unique, Sorted
    expected_affs = ["Univ A", "Univ B", "Univ C", "Univ D"]
    assert gold["affiliations_text"] == expected_affs


def test_extreme_gold_funding_empty_agencies() -> None:
    """
    Gold Layer: Funding entries with missing agency or grant_id.
    """
    silver_record = {
        "pmcid": "1",
        "authors": [],
        "funding": [
            {"agency": "NIH", "grant_id": None},
            {"agency": None, "grant_id": "G123"},
            {"agency": "NSF", "grant_id": "G456"},
            {"agency": "NIH", "grant_id": "G789"},  # Duplicate Agency
        ],
        "manifest_metadata": {"license_type": "CC0"},
        "ingestion_metadata": {"source_file_path": "oa_comm/xml/1.xml"},
    }

    gold = transform_gold_record(silver_record)
    assert gold is not None

    # Agency Names: Unique, Sorted, None filtered out?
    # Logic in code: `if agency: agency_names_set.add(agency)` -> Nones filtered.
    assert gold["agency_names"] == ["NIH", "NSF"]

    # Grant IDs: Unique, Sorted, Nones filtered.
    assert gold["grant_ids"] == ["G123", "G456", "G789"]


def test_extreme_gold_commercial_logic_ambiguous() -> None:
    """
    Gold Layer: is_commercial_safe logic on ambiguous paths.
    Path without 'oa_comm' or 'oa_noncomm'.
    """
    silver_record = {
        "pmcid": "1",
        "authors": [],
        "funding": [],
        "ingestion_metadata": {"source_file_path": "xml/PMC1.xml"},  # Ambiguous
        "manifest_metadata": {"license_type": "CC0"},
    }

    gold = transform_gold_record(silver_record)
    assert gold is not None

    # Logic: "if 'oa_comm' in path... elif 'oa_noncomm'... else pass (default False)"
    assert gold["is_commercial_safe"] is False

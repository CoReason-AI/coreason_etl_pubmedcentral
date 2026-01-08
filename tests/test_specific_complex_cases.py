# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import pytest
from dlt.common import pendulum as dlt_pendulum

from coreason_etl_pubmedcentral.pipeline_gold import transform_gold_record
from coreason_etl_pubmedcentral.pipeline_silver import transform_silver_record


@pytest.fixture  # type: ignore[misc]
def kitchen_sink_xml() -> str:
    """
    A "Kitchen Sink" XML containing:
    1. Identity: PMC, PMID, DOI
    2. Dates: Epub (Season=Spring), Received (YYYY-MM-DD), Accepted (YYYY)
    3. Authors:
       - Shared affiliations
       - Duplicate IDs definitions
       - Name flattening (Surname only, Given only)
    4. Funding:
       - Modern (Funding Group) with agencies and IDs
       - Legacy (Contract Num/Sponsor)
       - Overlap/Redundancy to check coalescing
    5. Content:
       - Title with markup (bold, italic)
       - Abstract with paragraphs
       - Keywords: compound and simple
    """
    return """
    <article article-type="research-article" xmlns:xlink="http://www.w3.org/1999/xlink">
        <front>
            <article-meta>
                <!-- Identity -->
                <article-id pub-id-type="pmc">PMC999999</article-id>
                <article-id pub-id-type="pmid">8888888</article-id>
                <article-id pub-id-type="doi">10.1000/kitchen.sink</article-id>

                <!-- Dates: Epub Spring 2024 -->
                <pub-date pub-type="epub">
                    <year>2024</year>
                    <season>Spring</season>
                </pub-date>
                <history>
                    <date date-type="received">
                        <year>2023</year><month>12</month><day>25</day>
                    </date>
                    <date date-type="accepted">
                        <year>2024</year>
                    </date>
                </history>

                <!-- Authors & Affiliations -->
                <contrib-group>
                    <contrib contrib-type="author">
                        <name><surname>Multi</surname><given-names>Tasker</given-names></name>
                        <xref ref-type="aff" rid="aff1"/>
                        <xref ref-type="aff" rid="aff2"/>
                    </contrib>
                    <contrib contrib-type="author">
                        <name><surname>SurnameOnly</surname></name>
                        <xref ref-type="aff" rid="aff1"/>
                    </contrib>
                </contrib-group>

                <aff id="aff1">University of Everything</aff>
                <aff id="aff2">Institute of Kitchen Sinks</aff>
                <!-- Duplicate ID definition (Last wins) -->
                <aff id="aff2">Institute of Advanced Kitchen Sinks</aff>

                <!-- Funding: Mixed Modern and Legacy -->
                <funding-group>
                    <award-group>
                        <funding-source>Modern Agency</funding-source>
                        <award-id>MOD-001</award-id>
                    </award-group>
                    <!-- Redundant signal check -->
                    <award-group>
                        <funding-source>Legacy Sponsor</funding-source> <!-- Overlaps with below -->
                    </award-group>
                </funding-group>

                <!-- Legacy Funding -->
                <contract-sponsor>Legacy Sponsor</contract-sponsor>
                <contract-num>LEG-002</contract-num>

                <!-- Content -->
                <title-group>
                    <article-title>The <bold>Complete</bold> &amp; <italic>Complex</italic> Guide</article-title>
                </title-group>

                <abstract>
                    <p>Paragraph One.</p>
                    <p>Paragraph Two.</p>
                </abstract>

                <kwd-group>
                    <kwd>SimpleKeyword</kwd>
                    <compound-kwd>
                        <part>Compound</part>
                        <part>Keyword</part>
                    </compound-kwd>
                </kwd-group>
            </article-meta>
        </front>
    </article>
    """


def test_complex_valid_kitchen_sink(kitchen_sink_xml: str) -> None:
    """
    End-to-end transformation test for a complex valid article.
    Verifies Silver and Gold layers handle all features simultaneously.
    """
    manifest_metadata = {
        "accession_id": "PMC999999",
        "last_updated": "2024-05-01T12:00:00+00:00",
        "is_retracted": False,
        "license_type": "CC-BY",
    }

    item = {
        "source_file_path": "oa_comm/xml/kitchen_sink.xml",
        "ingestion_ts": dlt_pendulum.now(),
        "ingestion_source": "S3",
        "raw_xml_payload": kitchen_sink_xml.encode("utf-8"),
        "manifest_metadata": manifest_metadata,
    }

    # --- Silver Layer Transformation ---
    silver = transform_silver_record(item)
    assert silver is not None

    # 1. Identity
    assert silver["pmcid"] == "999999"
    assert silver["pmid"] == "8888888"
    assert silver["doi"] == "10.1000/kitchen.sink"

    # 2. Dates
    # Spring -> 03 (March)
    assert silver["date_published"] == "2024-03-01"
    assert silver["date_received"] == "2023-12-25"
    # Missing Month/Day defaults to 01-01
    assert silver["date_accepted"] == "2024-01-01"

    # 3. Authors
    authors = silver["authors"]
    assert len(authors) == 2

    # Multi Tasker
    assert authors[0]["name"] == "Multi Tasker"
    # Aff2 should be "Institute of Advanced Kitchen Sinks" (Last wins)
    assert "Institute of Advanced Kitchen Sinks" in authors[0]["affiliations"]
    assert "University of Everything" in authors[0]["affiliations"]

    # SurnameOnly
    assert authors[1]["name"] == "SurnameOnly"
    assert authors[1]["affiliations"] == ["University of Everything"]

    # 4. Funding
    # We expect:
    # 1. Modern Agency + MOD-001
    # 2. Legacy Sponsor + None (from funding-group)
    # 3. Legacy Sponsor + None (from contract-sponsor) -> DUPLICATE SIGNAL in Silver?
    # 4. None + LEG-002 (from contract-num)

    # The parser extracts all it sees.
    # Silver list might contain duplicates. Gold should clean up.
    funding = silver["funding"]
    # Check for presence
    agencies = [f["agency"] for f in funding if f["agency"]]
    ids = [f["grant_id"] for f in funding if f["grant_id"]]

    assert "Modern Agency" in agencies
    assert "Legacy Sponsor" in agencies
    assert "MOD-001" in ids
    assert "LEG-002" in ids

    # 5. Content
    # Markup should be space-separated
    assert silver["title"] == "The Complete & Complex Guide"  # XML entities decoded by lxml
    # Abstract paragraphs space-separated
    assert "Paragraph One. Paragraph Two." in silver["abstract"]

    # Keywords
    assert "SimpleKeyword" in silver["keywords"]
    assert "Compound Keyword" in silver["keywords"]

    # --- Gold Layer Transformation ---
    gold = transform_gold_record(silver)
    assert gold is not None

    # 1. Commercial Safe Logic
    # path contains "oa_comm"
    assert gold["is_commercial_safe"] is True

    # 2. Funding Aggregation (Deduplication)
    # "Legacy Sponsor" appeared twice in Silver? Gold should set-ify it.
    assert sorted(gold["agency_names"]) == ["Legacy Sponsor", "Modern Agency"]
    assert sorted(gold["grant_ids"]) == ["LEG-002", "MOD-001"]

    # 3. Authors Display
    assert gold["authors_display"] == "Multi Tasker; SurnameOnly"

    # 4. Affiliations Text (Deduplicated and Sorted)
    expected_affs = ["Institute of Advanced Kitchen Sinks", "University of Everything"]
    assert sorted(gold["affiliations_text"]) == sorted(expected_affs)

    # 5. Pub Year
    assert gold["pub_year"] == 2024

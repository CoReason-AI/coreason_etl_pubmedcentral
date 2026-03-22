# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import pytest
from lxml import etree

from coreason_etl_pubmedcentral.models.contracts import CognitiveArticleTypeContract
from coreason_etl_pubmedcentral.models.xml_parser import CognitiveJATSParsingPolicy


def test_cognitive_jats_parsing_identity() -> None:
    """Test identity extraction (PMCID, PMID, DOI, Article Type)."""
    xml = b"""
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC12345</article-id>
                <article-id pub-id-type="pmid">9876</article-id>
                <article-id pub-id-type="doi">10.1000/xyz123</article-id>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.fromstring(xml).getroottree()
    payload = CognitiveJATSParsingPolicy.execute(tree)

    assert payload["pmcid"] == "12345"  # Prefix stripped
    assert payload["pmid"] == "9876"
    assert payload["doi"] == "10.1000/xyz123"
    assert payload["article_type"] == CognitiveArticleTypeContract.RESEARCH


def test_cognitive_jats_parsing_best_date_heuristic() -> None:
    """Test 'Best Date' heuristic prioritizing epub > ppub, and season parsing."""
    xml = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="ppub">
                    <year>2023</year>
                    <month>12</month>
                </pub-date>
                <pub-date pub-type="epub">
                    <year>2023</year>
                    <season>Spring</season>
                </pub-date>
                <pub-date pub-type="pmc-release">
                    <year>2024</year>
                    <season>Summer</season>
                </pub-date>
                <history>
                    <date date-type="received">
                        <year>2022</year><month>1</month><day>15</day>
                    </date>
                </history>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.fromstring(xml).getroottree()
    payload = CognitiveJATSParsingPolicy.execute(tree)

    # epub is prioritized. Spring -> 03. Missing day -> 01
    assert payload["date_published"] == "2023-03-01"
    assert payload["date_received"] == "2022-01-15"
    assert payload["date_accepted"] is None


@pytest.mark.parametrize(
    ("season_str", "expected_month"),
    [
        ("Summer", "06"),
        ("Fall", "09"),
        ("Winter", "12"),
    ],
)
def test_cognitive_jats_parsing_season_mapping(season_str: str, expected_month: str) -> None:
    """Test all season mappings."""
    xml = f"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="epub">
                    <year>2023</year>
                    <season>{season_str}</season>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """.encode()
    tree = etree.fromstring(xml).getroottree()
    payload = CognitiveJATSParsingPolicy.execute(tree)
    assert payload["date_published"] == f"2023-{expected_month}-01"


def test_cognitive_jats_parsing_fallback_date() -> None:
    """Test 'Best Date' fallback behavior when dates are corrupted or missing entirely."""
    xml = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="epub">
                    <year>INVALID</year>
                </pub-date>
                <pub-date pub-type="pmc-release">
                    <month>12</month>
                </pub-date>
                <pub-date pub-type="ppub" />
            </article-meta>
        </front>
    </article>
    """
    tree = etree.fromstring(xml).getroottree()
    payload = CognitiveJATSParsingPolicy.execute(tree)

    assert payload["date_published"] == "1970-01-01"


def test_cognitive_jats_parsing_missing_node() -> None:
    """Test skipping when nodes are not properly elements."""
    xml = b"""
    <article>
        <front>
            <article-meta>
                <history>
                    <date date-type="received" />
                    <date date-type="accepted">
                        <month>12</month>
                    </date>
                </history>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.fromstring(xml).getroottree()
    payload = CognitiveJATSParsingPolicy.execute(tree)
    assert payload["date_received"] is None
    assert payload["date_accepted"] is None


def test_cognitive_jats_parsing_text_extraction() -> None:
    """Test text extraction with nested HTML tags (title, abstract, keywords)."""
    xml = b"""
    <article>
        <front>
            <journal-meta>
                <journal-title-group>
                    <journal-title>Nature <i>Science</i></journal-title>
                </journal-title-group>
            </journal-meta>
            <article-meta>
                <title-group>
                    <article-title>A <b>bold</b> study on <i>cats</i>.</article-title>
                </title-group>
                <abstract>
                    <p>This is a <bold>very</bold> good abstract.</p>
                </abstract>
                <kwd-group>
                    <kwd>felines</kwd>
                    <kwd><i>purr</i></kwd>
                </kwd-group>
                <article-categories>
                    <subj-group>
                        <subject>Biology</subject>
                    </subj-group>
                </article-categories>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.fromstring(xml).getroottree()
    payload = CognitiveJATSParsingPolicy.execute(tree)

    assert payload["title"] == "A bold study on cats."
    assert payload["abstract"] == "This is a very good abstract."
    assert payload["journal_name"] == "Nature Science"
    assert payload["keywords"] == ["felines", "purr", "Biology"]


def test_cognitive_jats_parsing_entity_resolution() -> None:
    """Test resolution of authors to affiliations including non-element node skipping."""
    xml = b"""
    <article>
        <front>
            <article-meta>
                <contrib-group>
                    <contrib contrib-type="author">
                        <name><surname>Doe</surname><given-names>John</given-names></name>
                        <xref ref-type="aff" rid="aff1"/>
                    </contrib>
                    <contrib contrib-type="author">
                        <name><surname>Smith</surname><given-names>Jane</given-names></name>
                        <xref ref-type="aff" rid="aff1 aff2"/>
                    </contrib>
                    <contrib contrib-type="author">
                        <name><surname>NoAffil</surname></name>
                    </contrib>
                </contrib-group>
                <aff id="aff1">Pre Text <label>1</label>University of A.</aff>
                <aff id="aff2"><label>2</label>Institute B, Department C</aff>
                <aff><label>3</label>No ID Affil</aff>
                <aff id="aff4">Simple Text</aff>
                <aff id="aff5"><label>5</label>With <bold>inner</bold> tags</aff>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.fromstring(xml).getroottree()
    payload = CognitiveJATSParsingPolicy.execute(tree)

    assert payload["affiliations_text"] == [
        "Pre Text University of A",
        "Institute B, Department C",
        "Simple Text",
        "With inner tags",
    ]

    assert len(payload["authors"]) == 3
    assert payload["authors"][0]["name"] == "Doe John"
    assert payload["authors"][0]["affs"] == ["Pre Text University of A"]

    assert payload["authors"][1]["name"] == "Smith Jane"
    assert payload["authors"][1]["affs"] == ["Pre Text University of A", "Institute B, Department C"]

    assert payload["authors"][2]["name"] == "NoAffil"
    assert "affs" not in payload["authors"][2]


def test_cognitive_jats_parsing_funding_modern() -> None:
    """Test funding normalization using modern JATS (award-group)."""
    xml = b"""
    <article>
        <front>
            <article-meta>
                <funding-group>
                    <award-group>
                        <funding-source>NIH</funding-source>
                        <award-id>R01-123</award-id>
                    </award-group>
                    <award-group>
                        <funding-source>NSF</funding-source>
                    </award-group>
                </funding-group>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.fromstring(xml).getroottree()
    payload = CognitiveJATSParsingPolicy.execute(tree)

    assert payload["grant_ids"] == ["R01-123"]
    assert payload["agency_names"] == ["NIH", "NSF"]


def test_cognitive_jats_parsing_funding_legacy() -> None:
    """Test funding normalization using legacy JATS (contract-sponsor/num)."""
    xml = b"""
    <article>
        <front>
            <article-meta>
                <contract-sponsor>Legacy Agency 1</contract-sponsor>
                <contract-num>L123</contract-num>
                <contract-num>L456</contract-num>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.fromstring(xml).getroottree()
    payload = CognitiveJATSParsingPolicy.execute(tree)

    # Legacy generates independent objects
    assert payload["grant_ids"] == ["L123", "L456"]
    assert payload["agency_names"] == ["Legacy Agency 1"]

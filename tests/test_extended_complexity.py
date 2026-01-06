# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from lxml import etree

from coreason_etl_pubmedcentral.parsing.parser import (
    parse_article_authors,
    parse_article_dates,
    parse_article_funding,
    parse_article_identity,
    parse_article_keywords,
)


def test_funding_hybrid_duplication() -> None:
    """
    Verify behavior when both Modern (funding-group) and Legacy (contract-num) signals are present.
    The parser should extract BOTH, potentially resulting in duplicates in the raw list.
    (Deduplication happens in Gold layer or analytics).
    """
    xml = """
    <article>
        <front><article-meta>
            <!-- Modern -->
            <funding-group>
                <award-group>
                    <funding-source>NIH</funding-source>
                    <award-id>Grant1</award-id>
                </award-group>
            </funding-group>
            <!-- Legacy -->
            <contract-sponsor>NIH</contract-sponsor>
            <contract-num>Grant1</contract-num>
        </article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    funding = parse_article_funding(article)

    # Expect:
    # 1. (NIH, Grant1) from Modern
    # 2. (NIH, None) from contract-sponsor
    # 3. (None, Grant1) from contract-num
    # Total 3 entries.
    assert len(funding) == 3

    agencies = sorted([f.agency for f in funding if f.agency])
    grant_ids = sorted([f.grant_id for f in funding if f.grant_id])

    assert agencies == ["NIH", "NIH"]
    assert grant_ids == ["Grant1", "Grant1"]


def test_compound_keyword_recursion() -> None:
    """
    Verify <compound-kwd> is flattened correctly, joining children with spaces.
    """
    xml = """
    <article>
        <front><article-meta>
            <kwd-group>
                <compound-kwd>
                    <kwd>Artificial</kwd>
                    <related-kwd>Intelligence</related-kwd>
                </compound-kwd>
            </kwd-group>
        </article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    keywords = parse_article_keywords(article)

    assert len(keywords) == 1
    # Expect "Artificial Intelligence" (space joined)
    assert keywords[0] == "Artificial Intelligence"


def test_author_shared_affiliations() -> None:
    """
    Verify Many-to-Many relationship between Authors and Affiliations.
    Author 1 -> Aff 1, Aff 2
    Author 2 -> Aff 1, Aff 2
    """
    xml = """
    <article>
        <front><article-meta>
            <contrib-group>
                <contrib>
                    <name><surname>Smith</surname></name>
                    <xref ref-type="aff" rid="aff1"/>
                    <xref ref-type="aff" rid="aff2"/>
                </contrib>
                <contrib>
                    <name><surname>Doe</surname></name>
                    <xref ref-type="aff" rid="aff1 aff2"/> <!-- Space separated check -->
                </contrib>
            </contrib-group>
            <aff id="aff1">Univ A</aff>
            <aff id="aff2">Univ B</aff>
        </article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    authors = parse_article_authors(article)

    assert len(authors) == 2

    smith = authors[0]
    doe = authors[1]

    assert smith.surname == "Smith"
    assert smith.affiliations == ["Univ A", "Univ B"]

    assert doe.surname == "Doe"
    assert doe.affiliations == ["Univ A", "Univ B"]


def test_date_non_numeric_day() -> None:
    """
    Verify non-numeric day falls back to '01'.
    """
    xml = """
    <article>
        <front><article-meta><pub-date pub-type="epub">
            <year>2024</year>
            <month>01</month>
            <day>Unknown</day>
        </pub-date></article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    dates = parse_article_dates(article)

    assert dates.date_published == "2024-01-01"


def test_pmcid_complex_prefix() -> None:
    """
    Verify PMCID extraction logic for various prefix formats.
    """
    # Case 1: "pmc123" (lowercase) -> "123"
    xml1 = (
        '<article><front><article-meta><article-id pub-id-type="pmc">'
        "pmc123</article-id></article-meta></front></article>"
    )
    id1 = parse_article_identity(etree.fromstring(xml1))
    assert id1.pmcid == "123"

    # Case 2: "PMC 123" (space) -> " 123" (The logic removes first 3 chars "PMC")
    # This might leave a leading space. Let's verify expected behavior.
    # Current logic: `if raw_pmc.upper().startswith("PMC"): pmcid = raw_pmc[3:]`
    xml2 = (
        '<article><front><article-meta><article-id pub-id-type="pmc">'
        "PMC 123</article-id></article-meta></front></article>"
    )
    id2 = parse_article_identity(etree.fromstring(xml2))
    assert id2.pmcid == " 123"

    # Case 3: "PMC-123" -> "-123"
    xml3 = (
        '<article><front><article-meta><article-id pub-id-type="pmc">'
        "PMC-123</article-id></article-meta></front></article>"
    )
    id3 = parse_article_identity(etree.fromstring(xml3))
    assert id3.pmcid == "-123"

    # Case 4: "123" (No prefix) -> "123"
    xml4 = (
        '<article><front><article-meta><article-id pub-id-type="pmc">123</article-id></article-meta></front></article>'
    )
    id4 = parse_article_identity(etree.fromstring(xml4))
    assert id4.pmcid == "123"

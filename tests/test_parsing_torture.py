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
    _get_full_text,
    parse_article_dates,
    parse_article_funding,
    parse_article_identity,
)


def test_parsing_torture_mixed_content() -> None:
    """
    Verify text extraction on deeply nested mixed content.
    Confirms 'space insertion' strategy: 'Wo' + 'rd' -> 'Wo rd'.
    """
    xml = """
    <root>
        <p>
            Start
            <bold>Bold</bold>
            <italic>Italic <bold>BoldItalic</bold></italic>
            <sup >Sup</sup>
            <sub>Sub</sub>
            End.
        </p>
    </root>
    """
    root = etree.fromstring(xml)
    text = _get_full_text(root)

    # We expect normalization to join everything with single spaces
    # "Start Bold Italic BoldItalic Sup Sub End."
    expected = "Start Bold Italic BoldItalic Sup Sub End."
    assert text == expected


def test_parsing_torture_date_precedence() -> None:
    """
    Verify Date Logic: If both Month and Season exist, Month wins.
    """
    xml = """
    <article>
        <front><article-meta><pub-date pub-type="epub">
            <year>2024</year>
            <month>05</month>
            <season>Winter</season> <!-- Winter is 12, Month is 05 -->
        </pub-date></article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    dates = parse_article_dates(article)

    # Month (05) should win over Season (12)
    assert dates.date_published == "2024-05-01"


def test_parsing_torture_funding_cross_product() -> None:
    """
    Verify N x M cross product for funding.
    3 Agencies, 2 Grants -> 6 entries.
    """
    xml = """
    <article>
        <front><article-meta>
            <funding-group>
                <award-group>
                    <funding-source>Agency A</funding-source>
                    <funding-source>Agency B</funding-source>
                    <funding-source>Agency C</funding-source>
                    <award-id>Grant 1</award-id>
                    <award-id>Grant 2</award-id>
                </award-group>
            </funding-group>
        </article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    funding = parse_article_funding(article)

    assert len(funding) == 6

    # Verify strict combinations
    combinations = set((f.agency, f.grant_id) for f in funding)
    expected = {
        ("Agency A", "Grant 1"), ("Agency A", "Grant 2"),
        ("Agency B", "Grant 1"), ("Agency B", "Grant 2"),
        ("Agency C", "Grant 1"), ("Agency C", "Grant 2"),
    }
    assert combinations == expected


def test_parsing_torture_namespaces() -> None:
    """
    Verify handling of alternating default namespaces.
    """
    # Root defines ns1. Child redefines default to ns2. Grandchild redefines to ns1.
    # local-name() should handle this seamlessly.
    xml = """
    <article xmlns="http://ns1.com">
        <front>
            <article-meta xmlns="http://ns2.com">
                <article-id pub-id-type="pmc" xmlns="http://ns1.com">PMC111</article-id>
            </article-meta>
        </front>
    </article>
    """
    article = etree.fromstring(xml)
    identity = parse_article_identity(article)

    assert identity.pmcid == "111"


def test_parsing_torture_inline_splitting_behavior() -> None:
    """
    Explicitly document/test the splitting of words in inline tags.
    <p>Wo<b>rd</b></p> -> 'Wo rd'
    """
    xml = "<root><p>Wo<bold>rd</bold></p></root>"
    root = etree.fromstring(xml)
    text = _get_full_text(root)

    assert text == "Wo rd"

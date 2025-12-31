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
    _get_text,
    parse_article_dates,
    parse_article_identity,
)


def test_get_text_whitespace_handling() -> None:
    """
    Verify that _get_text returns None for whitespace-only text,
    not empty strings.
    Memory: "XML text extraction logic must handle whitespace-only text content by converting it to None"
    """
    xml = "<root><child>   </child><empty></empty></root>"
    root = etree.fromstring(xml)

    # 1. Whitespace only
    # Current implementation likely returns "" (empty string)
    val_ws = _get_text(root, "child")
    assert val_ws is None, f"Expected None for whitespace-only text, got '{val_ws}'"

    # 2. Empty tag
    # Current implementation returns None
    val_empty = _get_text(root, "empty")
    assert val_empty is None


def test_season_case_insensitivity() -> None:
    """
    Verify strict case insensitivity for Season mapping.
    Spring -> 03
    """
    # MIXED CASE
    xml = """
    <article>
        <front><article-meta><pub-date pub-type="epub">
            <year>2024</year>
            <season>SpRiNg</season>
        </pub-date></article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    dates = parse_article_dates(article)
    assert dates.date_published == "2024-03-01", f"Expected 2024-03-01, got {dates.date_published}"


def test_pmc_prefix_strictness() -> None:
    """
    Verify PMC prefix stripping is robust.
    """
    # "PMC" prefix (standard)
    xml_std = (
        "<article><front><article-meta>"
        "<article-id pub-id-type='pmc'>PMC123</article-id>"
        "</article-meta></front></article>"
    )
    identity_std = parse_article_identity(etree.fromstring(xml_std))
    assert identity_std.pmcid == "123"

    # "pmc" prefix (lowercase) -> Should still strip?
    # Logic: raw_pmc.upper().startswith("PMC")
    # "pmc123".upper() -> "PMC123" -> starts with PMC -> strip 3 chars -> "123". Correct.
    xml_lower = (
        "<article><front><article-meta>"
        "<article-id pub-id-type='pmc'>pmc456</article-id>"
        "</article-meta></front></article>"
    )
    identity_lower = parse_article_identity(etree.fromstring(xml_lower))
    assert identity_lower.pmcid == "456"

    # No prefix
    xml_none = (
        "<article><front><article-meta><article-id pub-id-type='pmc'>789</article-id></article-meta></front></article>"
    )
    identity_none = parse_article_identity(etree.fromstring(xml_none))
    assert identity_none.pmcid == "789"

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
)


def test_date_robustness_invalid_year() -> None:
    """
    Verify behavior when Year is present but non-numeric or malformed.
    Current logic: `_normalize_date_element` checks `if not year: return None`.
    It does NOT strictly check if year is numeric in the current implementation?
    Let's check `parser.py`:
      `year = _get_text(...)`
      `if not year: return None`
      It constructs f"{year}-{month}-{day}".
    If year is "abcd", it returns "abcd-01-01".
    This might be valid ISO for 'year' conceptually? No.
    But let's verify what it DOES do, to ensure no crash.
    """
    xml = """
    <article>
        <front><article-meta><pub-date pub-type="epub">
            <year>abcd</year>
            <month>05</month>
            <day>10</day>
        </pub-date></article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    dates = parse_article_dates(article)
    # The current implementation blindly concatenates.
    # While "abcd-05-10" is invalid date, the parser's job (currently) is extraction.
    # We verify it doesn't crash.
    assert dates.date_published == "abcd-05-10"


def test_date_robustness_weird_seasons() -> None:
    """
    Verify unknown seasons default to '01'.
    """
    xml = """
    <article>
        <front><article-meta><pub-date pub-type="epub">
            <year>2024</year>
            <season>Mid-Summer</season>
        </pub-date></article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    dates = parse_article_dates(article)
    # Unknown season -> "01"
    assert dates.date_published == "2024-01-01"


def test_date_robustness_out_of_range_day() -> None:
    """
    Verify out-of-range days are just passed through if numeric-ish?
    Logic: `if raw_day.isdigit(): day = raw_day.zfill(2)`
    So "32" -> "32". "99" -> "99".
    "2024-02-30" is invalid calendar date but valid ISO string structure.
    We verify it preserves the data (GIGO - Garbage In Garbage Out) rather than crashing.
    """
    xml = """
    <article>
        <front><article-meta><pub-date pub-type="epub">
            <year>2024</year>
            <month>02</month>
            <day>30</day>
        </pub-date></article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    dates = parse_article_dates(article)
    assert dates.date_published == "2024-02-30"


def test_funding_robustness_large_group() -> None:
    """
    Verify cross-product logic with a larger group.
    10 agencies * 10 grants = 100 entries.
    """
    # Construct XML programmatically to avoid huge string
    sources = "".join([f"<funding-source>Agency {i}</funding-source>" for i in range(10)])
    ids = "".join([f"<award-id>Grant {i}</award-id>" for i in range(10)])

    xml = f"""
    <article>
        <front><article-meta>
            <funding-group>
                <award-group>
                    {sources}
                    {ids}
                </award-group>
            </funding-group>
        </article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    funding = parse_article_funding(article)

    assert len(funding) == 100
    assert funding[0].agency is not None
    assert funding[0].grant_id is not None


def test_funding_robustness_empty_elements() -> None:
    """
    Verify empty funding-source or award-id tags are ignored.
    """
    xml = """
    <article>
        <front><article-meta>
            <funding-group>
                <award-group>
                    <funding-source></funding-source> <!-- Empty -->
                    <funding-source>  </funding-source> <!-- Whitespace -->
                    <funding-source>Valid Agency</funding-source>
                    <award-id>Valid Grant</award-id>
                    <award-id></award-id>
                </award-group>
            </funding-group>
        </article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    funding = parse_article_funding(article)

    # Should result in 1 agency * 1 grant = 1 entry
    assert len(funding) == 1
    assert funding[0].agency == "Valid Agency"
    assert funding[0].grant_id == "Valid Grant"


def test_author_robustness_broken_refs() -> None:
    """
    Verify authors with rids that don't exist in aff list.
    """
    xml = """
    <article>
        <front><article-meta>
            <contrib-group>
                <contrib>
                    <name><surname>Doe</surname></name>
                    <xref ref-type="aff" rid="missing_id"/>
                    <xref ref-type="aff" rid="aff1"/>
                </contrib>
            </contrib-group>
            <aff id="aff1">University A</aff>
        </article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    authors = parse_article_authors(article)

    assert len(authors) == 1
    # Should have 1 affiliation (aff1), missing_id is ignored
    assert authors[0].affiliations == ["University A"]


def test_author_robustness_duplicate_ids() -> None:
    """
    Verify behavior when two <aff> tags have the same ID.
    The spec implies ID uniqueness, but real data is messy.
    Logic: `aff_map[aff_id] = text` -> Last one wins.
    """
    xml = """
    <article>
        <front><article-meta>
            <contrib-group>
                <contrib>
                    <name><surname>Doe</surname></name>
                    <xref ref-type="aff" rid="aff1"/>
                </contrib>
            </contrib-group>
            <aff id="aff1">First Definition</aff>
            <aff id="aff1">Second Definition</aff>
        </article-meta></front>
    </article>
    """
    article = etree.fromstring(xml)
    authors = parse_article_authors(article)

    assert len(authors) == 1
    # Last one wins
    assert authors[0].affiliations == ["Second Definition"]

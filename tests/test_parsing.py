# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import os
from typing import Generator

import pytest
from lxml import etree

from coreason_etl_pubmedcentral.parsing.parser import (
    ArticleType,
    parse_article_authors,
    parse_article_dates,
    parse_article_identity,
)


@pytest.fixture  # type: ignore
def sample_data_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "jats_identity_sample.xml")


@pytest.fixture  # type: ignore
def dates_data_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "jats_dates_sample.xml")


@pytest.fixture  # type: ignore
def authors_data_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "jats_authors_sample.xml")


@pytest.fixture  # type: ignore
def authors_edge_cases_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "jats_authors_edge_cases.xml")


@pytest.fixture  # type: ignore
def articles(sample_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(sample_data_path)
    root = tree.getroot()
    # Return all article elements
    yield list(root.findall("article"))


@pytest.fixture  # type: ignore
def complex_articles() -> Generator[list[etree._Element], None, None]:
    path = os.path.join(os.path.dirname(__file__), "data", "jats_complex_sample.xml")
    tree = etree.parse(path)
    root = tree.getroot()
    # Find all 'article' elements regardless of namespace
    # Using XPath with local-name() is safer here
    yield root.xpath("//*[local-name()='article']")


@pytest.fixture  # type: ignore
def date_articles(dates_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(dates_data_path)
    root = tree.getroot()
    # Find all 'article' elements regardless of namespace
    yield root.xpath("//*[local-name()='article']")


@pytest.fixture  # type: ignore
def author_articles(authors_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(authors_data_path)
    root = tree.getroot()
    # Find all 'article' elements regardless of namespace
    yield root.xpath("//*[local-name()='article']")


@pytest.fixture  # type: ignore
def author_edge_case_articles(authors_edge_cases_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(authors_edge_cases_path)
    root = tree.getroot()
    # Find all 'article' elements regardless of namespace
    yield root.xpath("//*[local-name()='article']")


def test_parse_identity_research(articles: list[etree._Element]) -> None:
    # First article is research-article
    article = articles[0]
    identity = parse_article_identity(article)

    assert identity.pmcid == "12345"  # PMC prefix stripped
    assert identity.pmid == "12345678"
    assert identity.doi == "10.1000/123"
    assert identity.article_type == ArticleType.RESEARCH


def test_parse_identity_review(articles: list[etree._Element]) -> None:
    # Second article is review-article
    article = articles[1]
    identity = parse_article_identity(article)

    assert identity.pmcid == "67890"
    assert identity.pmid is None  # Missing
    assert identity.doi == "10.1000/456"
    assert identity.article_type == ArticleType.REVIEW


def test_parse_identity_case_report(articles: list[etree._Element]) -> None:
    # Third article is case-report
    article = articles[2]
    identity = parse_article_identity(article)

    assert identity.pmcid == "11111"
    assert identity.article_type == ArticleType.CASE_REPORT


def test_parse_identity_other(articles: list[etree._Element]) -> None:
    # Fourth article is editorial -> OTHER
    article = articles[3]
    identity = parse_article_identity(article)

    assert identity.pmcid == "22222"
    assert identity.article_type == ArticleType.OTHER


def test_parse_identity_minimal(articles: list[etree._Element]) -> None:
    # Fifth article is minimal -> no IDs, no type
    article = articles[4]
    identity = parse_article_identity(article)

    assert identity.pmcid is None
    assert identity.pmid is None
    assert identity.doi is None
    assert identity.article_type == ArticleType.OTHER


def test_pmc_strip_variations() -> None:
    # Test variation where PMC prefix might be missing or different case (though typically uppercase)
    xml = """
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">12345</article-id>
            </article-meta>
        </front>
    </article>
    """
    article = etree.fromstring(xml)
    identity = parse_article_identity(article)
    assert identity.pmcid == "12345"

    xml_lower = """
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">pmc12345</article-id>
            </article-meta>
        </front>
    </article>
    """
    article = etree.fromstring(xml_lower)
    identity = parse_article_identity(article)
    assert identity.pmcid == "12345"


def test_parse_identity_namespaces(complex_articles: list[etree._Element]) -> None:
    # 1. Namespaced Article
    article = complex_articles[0]
    identity = parse_article_identity(article)

    assert identity.pmcid == "99999"
    assert identity.pmid == "99999999"
    assert identity.article_type == ArticleType.RESEARCH


def test_parse_identity_multiple_ids(complex_articles: list[etree._Element]) -> None:
    # 2. Multiple IDs
    article = complex_articles[1]
    identity = parse_article_identity(article)

    # Should pick the first one found
    assert identity.pmcid == "11111"
    assert identity.doi == "10.1000/first"
    assert identity.article_type == ArticleType.REVIEW


def test_parse_identity_whitespace_and_empty(complex_articles: list[etree._Element]) -> None:
    # 3. Empty Tags / Weird Whitespace
    article = complex_articles[2]
    identity = parse_article_identity(article)

    # Whitespace around ID should be stripped
    assert identity.pmcid == "33333"
    # Empty tags or whitespace-only tags should return None (not empty string)
    assert identity.pmid is None  # Was empty tag
    assert identity.doi is None  # Was whitespace only
    assert identity.article_type == ArticleType.CASE_REPORT


def test_parse_identity_other_types(complex_articles: list[etree._Element]) -> None:
    # 4. Other types (letter)
    article_letter = complex_articles[3]
    identity_letter = parse_article_identity(article_letter)
    assert identity_letter.pmcid == "44444"
    assert identity_letter.article_type == ArticleType.OTHER

    # 5. Other types (correction)
    article_correction = complex_articles[4]
    identity_correction = parse_article_identity(article_correction)
    assert identity_correction.pmcid == "55555"
    assert identity_correction.article_type == ArticleType.OTHER


def test_parse_identity_real_namespace(complex_articles: list[etree._Element]) -> None:
    # 6. Real Default Namespace
    article = complex_articles[5]
    identity = parse_article_identity(article)

    # This often fails if XPath doesn't handle namespaces or local-name()
    assert identity.pmcid == "66666"
    assert identity.article_type == ArticleType.RESEARCH


def test_parse_dates_priority_and_history(date_articles: list[etree._Element]) -> None:
    # 1. Full Dates & Priority (epub > ppub)
    article = date_articles[0]
    dates = parse_article_dates(article)

    # epub is 2023-05-15
    assert dates.date_published == "2023-05-15"
    # received: 2023-01-10
    assert dates.date_received == "2023-01-10"
    # accepted: 2023-04-20
    assert dates.date_accepted == "2023-04-20"


def test_parse_dates_defaults_year_only(date_articles: list[etree._Element]) -> None:
    # 2. Year only -> 2023-01-01
    article = date_articles[1]
    dates = parse_article_dates(article)
    assert dates.date_published == "2023-01-01"


def test_parse_dates_defaults_year_month_only(date_articles: list[etree._Element]) -> None:
    # 3. Year and Month only -> 2023-07-01
    article = date_articles[2]
    dates = parse_article_dates(article)
    assert dates.date_published == "2023-07-01"


def test_parse_dates_season_spring(date_articles: list[etree._Element]) -> None:
    # 4. Season: Spring -> 03
    article = date_articles[3]
    dates = parse_article_dates(article)
    assert dates.date_published == "2024-03-01"


def test_parse_dates_season_summer(date_articles: list[etree._Element]) -> None:
    # 5. Season: Summer -> 06
    article = date_articles[4]
    dates = parse_article_dates(article)
    assert dates.date_published == "2024-06-01"


def test_parse_dates_season_fall(date_articles: list[etree._Element]) -> None:
    # 6. Season: Fall -> 09
    article = date_articles[5]
    dates = parse_article_dates(article)
    assert dates.date_published == "2024-09-01"


def test_parse_dates_season_winter(date_articles: list[etree._Element]) -> None:
    # 7. Season: Winter -> 12
    article = date_articles[6]
    dates = parse_article_dates(article)
    assert dates.date_published == "2024-12-01"


def test_parse_dates_fallback_ppub(date_articles: list[etree._Element]) -> None:
    # 8. Fallback to ppub (no epub)
    article = date_articles[7]
    dates = parse_article_dates(article)
    assert dates.date_published == "2022-12-25"


def test_parse_dates_fallback_pmc(date_articles: list[etree._Element]) -> None:
    # 9. Fallback to pmc-release (no epub, no ppub)
    article = date_articles[8]
    dates = parse_article_dates(article)
    assert dates.date_published == "2021-01-01"


def test_parse_dates_missing_year(date_articles: list[etree._Element]) -> None:
    # 10. Missing Year -> None
    article = date_articles[9]
    dates = parse_article_dates(article)
    assert dates.date_published is None


def test_parse_dates_namespace(date_articles: list[etree._Element]) -> None:
    # 11. Namespaced elements
    article = date_articles[10]
    dates = parse_article_dates(article)
    assert dates.date_published == "2025-05-05"


def test_parse_dates_unknown_season(date_articles: list[etree._Element]) -> None:
    # 12. Unknown Season -> 01
    article = date_articles[11]
    dates = parse_article_dates(article)
    assert dates.date_published == "2026-01-01"


def test_parse_dates_textual_month(date_articles: list[etree._Element]) -> None:
    # 13. Textual Month "May" -> "01" (Strict ISO enforcement)
    article = date_articles[12]
    dates = parse_article_dates(article)
    assert dates.date_published == "2027-01-01"


def test_parse_dates_whitespace(date_articles: list[etree._Element]) -> None:
    # 14. Whitespace in fields -> Stripped
    article = date_articles[13]
    dates = parse_article_dates(article)
    assert dates.date_published == "2028-08-15"


def test_parse_dates_multiple_same_type(date_articles: list[etree._Element]) -> None:
    # 15. Multiple epub -> Pick first
    article = date_articles[14]
    dates = parse_article_dates(article)
    assert dates.date_published == "2029-01-01"


def test_parse_dates_case_insensitive_type(date_articles: list[etree._Element]) -> None:
    # 16. pub-type="EPUB" -> Should match
    article = date_articles[15]
    dates = parse_article_dates(article)
    assert dates.date_published == "2030-01-01"


def test_parse_dates_non_numeric_day(date_articles: list[etree._Element]) -> None:
    # 17. Non-numeric day "15th" -> "01" (Strict ISO enforcement)
    article = date_articles[16]
    dates = parse_article_dates(article)
    assert dates.date_published == "2031-05-01"


def test_parse_authors_simple(author_articles: list[etree._Element]) -> None:
    # Case 1: Simple case, one author, one affiliation
    article = author_articles[0]
    authors = parse_article_authors(article)

    assert len(authors) == 1
    assert authors[0].surname == "Doe"
    assert authors[0].given_names == "John"
    assert authors[0].affiliations == ["University of Testing"]


def test_parse_authors_multiple_shared(author_articles: list[etree._Element]) -> None:
    # Case 2: Multiple authors, shared and unique affiliations
    article = author_articles[1]
    authors = parse_article_authors(article)

    assert len(authors) == 2
    # Smith: aff1, aff2
    assert authors[0].surname == "Smith"
    assert authors[0].given_names == "Alice"
    assert authors[0].affiliations == ["Institute of Science", "Department of Logic"]
    # Jones: aff2
    assert authors[1].surname == "Jones"
    assert authors[1].given_names == "Bob"
    assert authors[1].affiliations == ["Department of Logic"]


def test_parse_authors_none(author_articles: list[etree._Element]) -> None:
    # Case 3: Author with no affiliation, partial name
    article = author_articles[2]
    authors = parse_article_authors(article)

    assert len(authors) == 1
    assert authors[0].surname == "Lonely"
    assert authors[0].given_names is None
    assert authors[0].affiliations == []


def test_parse_authors_broken_link(author_articles: list[etree._Element]) -> None:
    # Case 4: Broken link (rid does not exist)
    article = author_articles[3]
    authors = parse_article_authors(article)

    assert len(authors) == 1
    assert authors[0].surname == "Broken"
    assert authors[0].affiliations == []  # Should handle missing ID gracefully


def test_parse_authors_complex_text(author_articles: list[etree._Element]) -> None:
    # Case 5: Complex affiliation text
    article = author_articles[4]
    authors = parse_article_authors(article)

    assert len(authors) == 1
    assert authors[0].surname == "Complex"
    assert "University of Complex Data" in authors[0].affiliations[0]
    assert "Techland" in authors[0].affiliations[0]


def test_parse_authors_multiple_groups(author_edge_case_articles: list[etree._Element]) -> None:
    # Case 6: Multiple contrib-groups (authors and editors)
    article = author_edge_case_articles[0]
    authors = parse_article_authors(article)

    # Logic iterates all contrib-group/contrib, so should get both
    assert len(authors) == 2
    names = {a.surname for a in authors}
    assert "Author" in names
    assert "Editor" in names
    assert authors[0].affiliations[0] == "Shared Institute"


def test_parse_authors_duplicate_ids(author_edge_case_articles: list[etree._Element]) -> None:
    # Case 7: Duplicate aff IDs (Last one wins)
    article = author_edge_case_articles[1]
    authors = parse_article_authors(article)

    assert len(authors) == 1
    assert authors[0].surname == "Dupe"
    # The map logic `aff_map[aff_id] = text` overwrites, so second definition wins
    assert authors[0].affiliations == ["Second Definition"]


def test_parse_authors_unicode(author_edge_case_articles: list[etree._Element]) -> None:
    # Case 8: Unicode Characters
    article = author_edge_case_articles[2]
    authors = parse_article_authors(article)

    assert len(authors) == 1
    assert authors[0].surname == "Müller"
    assert authors[0].given_names == "Jürgen"
    # "Universität Tübingen"
    # In XML it is encoded as HTML entities or UTF-8. lxml handles decoding.
    # We check if it contains the correct unicode string.
    assert "Universität Tübingen" in authors[0].affiliations[0]


def test_parse_authors_mixed_rids(author_edge_case_articles: list[etree._Element]) -> None:
    # Case 9: Mixed Valid/Invalid RIDs
    article = author_edge_case_articles[3]
    authors = parse_article_authors(article)

    assert len(authors) == 1
    # "aff1 invalid_id aff2" -> Should get aff1 and aff2, ignore invalid_id
    affs = authors[0].affiliations
    assert len(affs) == 2
    assert "Affiliation One" in affs
    assert "Affiliation Two" in affs


def test_parse_authors_xref_types(author_edge_case_articles: list[etree._Element]) -> None:
    # Case 10: Xref missing ref-type or wrong type
    article = author_edge_case_articles[4]
    authors = parse_article_authors(article)

    assert len(authors) == 1
    # Should only pick up the one with ref-type="aff"
    assert len(authors[0].affiliations) == 1
    assert authors[0].affiliations[0] == "Correct Affiliation"

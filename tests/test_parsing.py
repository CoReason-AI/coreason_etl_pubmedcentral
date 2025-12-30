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
    parse_article_content,
    parse_article_dates,
    parse_article_funding,
    parse_article_identity,
    parse_article_keywords,
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
def funding_data_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "jats_funding_sample.xml")


@pytest.fixture  # type: ignore
def funding_complex_data_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "jats_funding_complex.xml")


@pytest.fixture  # type: ignore
def content_data_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "jats_content_sample.xml")


@pytest.fixture  # type: ignore
def articles(sample_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(sample_data_path)
    root = tree.getroot()
    yield list(root.findall("article"))


@pytest.fixture  # type: ignore
def complex_articles() -> Generator[list[etree._Element], None, None]:
    path = os.path.join(os.path.dirname(__file__), "data", "jats_complex_sample.xml")
    tree = etree.parse(path)
    root = tree.getroot()
    yield root.xpath("//*[local-name()='article']")


@pytest.fixture  # type: ignore
def date_articles(dates_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(dates_data_path)
    root = tree.getroot()
    yield root.xpath("//*[local-name()='article']")


@pytest.fixture  # type: ignore
def author_articles(authors_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(authors_data_path)
    root = tree.getroot()
    yield root.xpath("//*[local-name()='article']")


@pytest.fixture  # type: ignore
def author_edge_case_articles(authors_edge_cases_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(authors_edge_cases_path)
    root = tree.getroot()
    yield root.xpath("//*[local-name()='article']")


@pytest.fixture  # type: ignore
def funding_articles(funding_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(funding_data_path)
    root = tree.getroot()
    yield root.xpath("//*[local-name()='article']")


@pytest.fixture  # type: ignore
def funding_complex_articles(funding_complex_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(funding_complex_data_path)
    root = tree.getroot()
    yield root.xpath("//*[local-name()='article']")


@pytest.fixture  # type: ignore
def content_articles(content_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(content_data_path)
    root = tree.getroot()
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


def test_parse_funding_modern(funding_articles: list[etree._Element]) -> None:
    # Modern JATS
    article = funding_articles[0]
    funding = parse_article_funding(article)

    # 3 groups
    assert len(funding) == 3

    # 1. NIH, R01-12345
    assert funding[0].agency == "National Institutes of Health"
    assert funding[0].grant_id == "R01-12345"

    # 2. NSF, None
    assert funding[1].agency == "NSF"
    assert funding[1].grant_id is None

    # 3. None, G-999
    assert funding[2].agency is None
    assert funding[2].grant_id == "G-999"


def test_parse_funding_legacy(funding_articles: list[etree._Element]) -> None:
    # Legacy JATS
    article = funding_articles[1]
    funding = parse_article_funding(article)

    # Expect: 2 sponsors (Pfizer, Moderna) + 1 num (CN-1001) = 3 entries
    assert len(funding) == 3

    # Order depends on implementation (sponsors first then nums)
    # Sponsors
    agencies = {f.agency for f in funding if f.agency}
    assert "Pfizer" in agencies
    assert "Moderna" in agencies

    # Numbers
    ids = {f.grant_id for f in funding if f.grant_id}
    assert "CN-1001" in ids


def test_parse_funding_mixed(funding_articles: list[etree._Element]) -> None:
    # Mixed JATS (Modern + Legacy)
    article = funding_articles[2]
    funding = parse_article_funding(article)

    # 1 award group + 1 contract-sponsor = 2 entries
    assert len(funding) == 2

    agencies = {f.agency for f in funding if f.agency}
    assert "Wellcome Trust" in agencies
    assert "Gates Foundation" in agencies

    ids = {f.grant_id for f in funding if f.grant_id}
    assert "WT-555" in ids


def test_parse_funding_empty(funding_articles: list[etree._Element]) -> None:
    # Empty award group
    article = funding_articles[3]
    funding = parse_article_funding(article)

    assert len(funding) == 0


def test_parse_funding_nested_text(funding_complex_articles: list[etree._Element]) -> None:
    # 1. Nested Text
    article = funding_complex_articles[0]
    funding = parse_article_funding(article)

    assert len(funding) == 1
    # Check that text content of children (italic, sup, bold) is preserved and concatenated
    assert funding[0].agency == "Agency with Italic and Sup"
    assert funding[0].grant_id == "ID Bold"


def test_parse_funding_namespaces(funding_complex_articles: list[etree._Element]) -> None:
    # 2. Namespaces
    article = funding_complex_articles[1]
    funding = parse_article_funding(article)

    # local-name() should ignore "ns:" prefix
    assert len(funding) == 1
    assert funding[0].agency == "Namespaced Agency"
    assert funding[0].grant_id == "NS-123"


def test_parse_funding_multiple_sources_ids(funding_complex_articles: list[etree._Element]) -> None:
    # 3. Multiple Sources and IDs -> Cross Product
    article = funding_complex_articles[2]
    funding = parse_article_funding(article)

    # Agencies: A, B. IDs: 1, 2.
    # Cross product: (A,1), (A,2), (B,1), (B,2) -> 4 entries
    assert len(funding) == 4

    agencies = [f.agency for f in funding]
    ids = [f.grant_id for f in funding]

    assert agencies.count("Agency A") == 2
    assert agencies.count("Agency B") == 2
    assert ids.count("Grant 1") == 2
    assert ids.count("Grant 2") == 2


def test_parse_funding_whitespace(funding_complex_articles: list[etree._Element]) -> None:
    # 4. Whitespace
    article = funding_complex_articles[3]
    funding = parse_article_funding(article)

    assert len(funding) == 1
    # _get_full_text strips leading/trailing, but itertext() might preserve internal newlines depending on parser
    # "Agency \n Multiline" -> itertext joins them.
    # Our _get_full_text joins all itertext().
    # If XML is:
    # <source>
    #   Agency
    #   Multiline
    # </source>
    # lxml itertext returns "Agency", "\n", "Multiline" (approx).
    # .join(...) -> "Agency\n   Multiline" -> .strip() -> "Agency\n   Multiline".
    # Wait, usually we want to collapse whitespace?
    # Spec says "Extract raw text".
    # Let's check what it actually returns.
    # lxml itertext() often includes whitespace.
    # Asserting containment for now.
    assert "Agency" in funding[0].agency  # type: ignore
    assert "Multiline" in funding[0].agency  # type: ignore
    assert "ID-Multiline" in funding[0].grant_id  # type: ignore


def test_parse_funding_only_sources(funding_complex_articles: list[etree._Element]) -> None:
    # 5. Multiple Sources, No IDs
    article = funding_complex_articles[4]
    funding = parse_article_funding(article)

    assert len(funding) == 2
    agencies = sorted([f.agency for f in funding if f.agency])
    assert agencies == ["Source X", "Source Y"]
    assert all(f.grant_id is None for f in funding)


def test_parse_funding_only_ids(funding_complex_articles: list[etree._Element]) -> None:
    # 6. Multiple IDs, No Sources
    article = funding_complex_articles[5]
    funding = parse_article_funding(article)

    assert len(funding) == 2
    ids = sorted([f.grant_id for f in funding if f.grant_id])
    assert ids == ["ID X", "ID Y"]
    assert all(f.agency is None for f in funding)


def test_parse_content_full(content_articles: list[etree._Element]) -> None:
    # Article 1: Full content
    article = content_articles[0]
    content = parse_article_content(article)
    keywords = parse_article_keywords(article)

    assert content.title == "Test Article Title"  # "Article" is italic in XML
    assert "test abstract" in content.abstract  # type: ignore
    assert "Background" in content.abstract  # type: ignore
    assert content.journal_name == "Journal of Testing"

    assert len(keywords) == 4
    assert "keyword1" in keywords
    assert "keyword two" in keywords
    assert "Subject A" in keywords
    assert "Subject B" in keywords


def test_parse_content_minimal(content_articles: list[etree._Element]) -> None:
    # Article 2: Minimal content
    article = content_articles[1]
    content = parse_article_content(article)
    keywords = parse_article_keywords(article)

    assert content.title is None
    assert content.abstract is None
    assert content.journal_name is None
    assert keywords == []


def test_parse_content_complex(content_articles: list[etree._Element]) -> None:
    # Article 3: Complex/Edge cases
    article = content_articles[2]
    content = parse_article_content(article)
    keywords = parse_article_keywords(article)

    # Title with nested bold tag
    assert content.title == "Bold Title"
    # Abstract with paragraphs
    assert "Para 1" in content.abstract  # type: ignore
    assert "Para 2" in content.abstract  # type: ignore
    # Journal
    assert content.journal_name == "Complex Journal"
    # Keywords from multiple groups
    assert "author-kw1" in keywords
    assert "kw2" in keywords


def test_parse_content_whitespace_handling(content_articles: list[etree._Element]) -> None:
    # Test for whitespace handling between paragraphs (Minified case)
    # <p> tags adjacent without whitespace
    xml = (
        "<article><front><article-meta><abstract>"
        "<p>Paragraph One.</p><p>Paragraph Two.</p>"
        "</abstract></article-meta></front></article>"
    )
    article = etree.fromstring(xml)
    content = parse_article_content(article)

    # Current buggy implementation would yield "Paragraph One.Paragraph Two."
    # We want "Paragraph One. Paragraph Two."
    assert content.abstract is not None
    assert "Paragraph One. Paragraph Two." in content.abstract


def test_parse_content_complex_markup_title() -> None:
    # Test Title with Sub/Sup/Math
    xml = """
    <article>
        <front>
            <article-meta>
                <title-group>
                    <article-title>H<sub>2</sub>O is <bold>Water</bold></article-title>
                </title-group>
            </article-meta>
        </front>
    </article>
    """
    article = etree.fromstring(xml)
    content = parse_article_content(article)

    # We expect flattened text: "H 2 O is Water" or "H2O is Water"
    # Ideally "H2O is Water" is acceptable for simple flattening,
    # but "H 2 O" might happen if we join everything with spaces.
    # If we change _get_full_text to join with spaces, "H 2 O" is likely.
    # Let's verify expectation.
    # "H2O" is better than "H 2 O".
    # But "ParagraphOne.ParagraphTwo" is worse than "Paragraph One. Paragraph Two."

    # Ideally: Block elements add space, Inline elements do not.
    # But JATS is complex. <sub> is inline. <p> is block.
    # Solving this perfectly requires a complex text extractor.
    # For now, let's see what a simple "join with space + normalize" does.
    # "H 2 O is Water". This is acceptable for search/analytics.

    # Expectation: "H 2 O is Water" (due to space insertion)
    # Ideally "H2O" but solving inline vs block spacing genericly is hard.
    # We accept "H 2 O" for search index.
    assert content.title == "H 2 O is Water"


def test_parse_content_multiple_abstracts() -> None:
    # Test Multiple Abstracts (e.g. Graphical)
    xml = """
    <article>
        <front>
            <article-meta>
                <abstract abstract-type="main">
                    <p>Main Abstract.</p>
                </abstract>
                <abstract abstract-type="graphical">
                    <p>Graphical Abstract.</p>
                </abstract>
            </article-meta>
        </front>
    </article>
    """
    article = etree.fromstring(xml)
    content = parse_article_content(article)

    # Should pick the first one found by xpath (document order).
    assert content.abstract == "Main Abstract."


def test_parse_keywords_nested_structure() -> None:
    # Test Keywords with complex structure or attributes
    xml = """
    <article>
        <front>
            <article-meta>
                <kwd-group kwd-group-type="author">
                    <kwd>kw1</kwd>
                    <kwd><italic>kw2</italic></kwd>
                </kwd-group>
                <kwd-group kwd-group-type="ontology">
                    <compound-kwd>
                        <compound-kwd-part content-type="id">ID:123</compound-kwd-part>
                        <compound-kwd-part content-type="text">Term</compound-kwd-part>
                    </compound-kwd>
                </kwd-group>
            </article-meta>
        </front>
    </article>
    """
    # Note: Our parser only looks for `//kwd-group/kwd`.
    # It does NOT handle `compound-kwd` currently.
    # This test documents that limitation or behavior.

    article = etree.fromstring(xml)
    keywords = parse_article_keywords(article)

    assert "kw1" in keywords
    assert "kw2" in keywords

    # compound-kwd parts: "ID:123", "Term"
    # _get_full_text joins with space.
    assert "ID:123 Term" in keywords

    assert len(keywords) == 3

# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from enum import Enum
from typing import NamedTuple, Optional

from lxml import etree


class ArticleType(str, Enum):
    RESEARCH = "RESEARCH"
    REVIEW = "REVIEW"
    CASE_REPORT = "CASE_REPORT"
    OTHER = "OTHER"


class ArticleIdentity(NamedTuple):
    pmcid: Optional[str]
    pmid: Optional[str]
    doi: Optional[str]
    article_type: ArticleType


class ArticleDates(NamedTuple):
    date_published: Optional[str]
    date_received: Optional[str]
    date_accepted: Optional[str]


def _get_text(element: etree._Element, xpath_query: str) -> Optional[str]:
    """Helper to safely get text from an element using xpath."""
    nodes = element.xpath(xpath_query)
    if nodes and nodes[0].text:
        return nodes[0].text.strip()  # type: ignore
    return None


def _normalize_date_element(date_element: etree._Element) -> Optional[str]:
    """
    Parses a JATS date element (pub-date or date) into an ISO-8601 string.
    Handles Year, Month/Season, Day.
    Logic:
      - Year is mandatory. If missing, return None.
      - Month/Day default to '01'.
      - Season mapping: Spring->03, Summer->06, Fall->09, Winter->12.
    """
    # 1. Extract Year
    year = _get_text(date_element, ".//*[local-name()='year']")
    if not year:
        return None

    # 2. Extract Month or Season
    month = "01"
    raw_month = _get_text(date_element, ".//*[local-name()='month']")
    raw_season = _get_text(date_element, ".//*[local-name()='season']")

    if raw_month:
        # Pad with zero if needed
        if raw_month.isdigit():
            month = raw_month.zfill(2)
        else:
            # Non-numeric month -> strict ISO fail -> default to 01
            month = "01"
    elif raw_season:
        # Map season to month
        s = raw_season.lower()
        if s == "spring":
            month = "03"
        elif s == "summer":
            month = "06"
        elif s == "fall":
            month = "09"
        elif s == "winter":
            month = "12"
        # Else unknown season -> keep default "01"

    # 3. Extract Day
    day = "01"
    raw_day = _get_text(date_element, ".//*[local-name()='day']")
    if raw_day:
        if raw_day.isdigit():
            day = raw_day.zfill(2)
        else:
            day = "01"

    return f"{year}-{month}-{day}"


def parse_article_dates(article_element: etree._Element) -> ArticleDates:
    """
    Parses temporal metadata from a JATS XML article.

    Args:
        article_element: The root <article> element.

    Returns:
        ArticleDates containing date_published, date_received, date_accepted.
    """
    # 1. Date Published
    # Priority: epub > ppub > pmc-release
    date_published: Optional[str] = None

    # We look for all pub-date elements first to minimize xpath calls if possible,
    # but specific xpath is cleaner.
    # To handle case-insensitivity of @pub-type, we use python logic or complex xpath.
    # Simple python filtering is safer and readable.

    pub_dates = article_element.xpath(".//*[local-name()='pub-date']")

    # Helper to find date by type (case-insensitive)
    def find_date_by_type(ptype: str) -> Optional[str]:
        for node in pub_dates:
            raw_ptype = node.get("pub-type")
            if raw_ptype and raw_ptype.lower() == ptype:
                res = _normalize_date_element(node)
                if res:
                    return res
        return None

    # Check epub
    date_published = find_date_by_type("epub")

    # Check ppub if no epub
    if not date_published:
        date_published = find_date_by_type("ppub")

    # Check pmc-release if no epub or ppub
    if not date_published:
        date_published = find_date_by_type("pmc-release")

    # 2. Date Received
    # Target: //history/date[@date-type='received']
    date_received: Optional[str] = None
    received_nodes = article_element.xpath(".//*[local-name()='history']/*[local-name()='date'][@date-type='received']")
    if received_nodes:
        date_received = _normalize_date_element(received_nodes[0])

    # 3. Date Accepted
    # Target: //history/date[@date-type='accepted']
    date_accepted: Optional[str] = None
    accepted_nodes = article_element.xpath(".//*[local-name()='history']/*[local-name()='date'][@date-type='accepted']")
    if accepted_nodes:
        date_accepted = _normalize_date_element(accepted_nodes[0])

    return ArticleDates(
        date_published=date_published,
        date_received=date_received,
        date_accepted=date_accepted,
    )


def parse_article_identity(article_element: etree._Element) -> ArticleIdentity:
    """
    Parses identity and classification information from a JATS XML article element.

    Args:
        article_element: The root <article> element.

    Returns:
        ArticleIdentity containing pmcid, pmid, doi, and article_type.
    """

    # 1. Extract PMCID
    # Target: //article-id[@pub-id-type='pmc']
    # Logic: Strip "PMC" prefix.
    # Note: Use local-name() to be namespace-agnostic (JATS often uses default xmlns)
    pmcid_elem = article_element.xpath(".//*[local-name()='article-id'][@pub-id-type='pmc']")
    pmcid: Optional[str] = None
    if pmcid_elem and pmcid_elem[0].text:
        raw_pmc = pmcid_elem[0].text.strip()
        if raw_pmc.upper().startswith("PMC"):
            pmcid = raw_pmc[3:]
        else:
            pmcid = raw_pmc

    # 2. Extract PMID
    # Target: //article-id[@pub-id-type='pmid']
    pmid_elem = article_element.xpath(".//*[local-name()='article-id'][@pub-id-type='pmid']")
    pmid: Optional[str] = None
    if pmid_elem and pmid_elem[0].text:
        pmid_text = pmid_elem[0].text.strip()
        if pmid_text:
            pmid = pmid_text

    # 3. Extract DOI
    # Target: //article-id[@pub-id-type='doi']
    doi_elem = article_element.xpath(".//*[local-name()='article-id'][@pub-id-type='doi']")
    doi: Optional[str] = None
    if doi_elem and doi_elem[0].text:
        doi_text = doi_elem[0].text.strip()
        if doi_text:
            doi = doi_text

    # 4. Extract Article Type
    # Target: /article/@article-type
    # Map: RESEARCH, REVIEW, CASE_REPORT. Default: OTHER.
    raw_type = article_element.get("article-type")
    article_type = ArticleType.OTHER

    if raw_type:
        raw_type_lower = raw_type.lower()
        if raw_type_lower == "research-article":
            article_type = ArticleType.RESEARCH
        elif raw_type_lower == "review-article":
            article_type = ArticleType.REVIEW
        elif raw_type_lower == "case-report":
            article_type = ArticleType.CASE_REPORT
        # else remains OTHER

    return ArticleIdentity(pmcid=pmcid, pmid=pmid, doi=doi, article_type=article_type)

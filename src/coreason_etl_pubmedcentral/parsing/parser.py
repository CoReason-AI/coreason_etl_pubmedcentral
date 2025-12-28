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
    pmcid_elem = article_element.xpath(".//article-id[@pub-id-type='pmc']")
    pmcid: Optional[str] = None
    if pmcid_elem and pmcid_elem[0].text:
        raw_pmc = pmcid_elem[0].text.strip()
        if raw_pmc.upper().startswith("PMC"):
            pmcid = raw_pmc[3:]
        else:
            pmcid = raw_pmc

    # 2. Extract PMID
    # Target: //article-id[@pub-id-type='pmid']
    pmid_elem = article_element.xpath(".//article-id[@pub-id-type='pmid']")
    pmid: Optional[str] = None
    if pmid_elem and pmid_elem[0].text:
        pmid = pmid_elem[0].text.strip()

    # 3. Extract DOI
    # Target: //article-id[@pub-id-type='doi']
    doi_elem = article_element.xpath(".//article-id[@pub-id-type='doi']")
    doi: Optional[str] = None
    if doi_elem and doi_elem[0].text:
        doi = doi_elem[0].text.strip()

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

# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import enum
from typing import Any

from lxml import etree
from pydantic import BaseModel, Field


class ArticleTypeEnum(enum.StrEnum):
    """Strictly typed classification for PubMed Central articles."""

    RESEARCH = "RESEARCH"
    REVIEW = "REVIEW"
    CASE_REPORT = "CASE_REPORT"
    OTHER = "OTHER"


class ArticleIdentityState(BaseModel):
    """
    Epistemic snapshot containing the canonical identities and classification
    of a parsed PubMed Central document.
    """

    pmcid: str = Field(description="Canonical PMCID with prefix stripped")
    pmid: str | None = Field(default=None, description="PubMed ID")
    doi: str | None = Field(default=None, description="Document Object Identifier")
    article_type: ArticleTypeEnum = Field(
        default=ArticleTypeEnum.OTHER,
        description="Categorical designation mapped from JATS article-type",
    )


class ArticleTemporalState(BaseModel):
    """
    Epistemic snapshot containing the temporal facts of a parsed PubMed Central document.
    """

    date_published: str | None = Field(
        default=None, description="Best available publication date in ISO-8601 format (YYYY-MM-DD)"
    )
    date_received: str | None = Field(
        default=None, description="Date the manuscript was received in ISO-8601 format (YYYY-MM-DD)"
    )
    date_accepted: str | None = Field(
        default=None, description="Date the manuscript was accepted in ISO-8601 format (YYYY-MM-DD)"
    )


def extract_identity_state(root: etree._Element) -> ArticleIdentityState:
    """
    AGENT INSTRUCTION: Parse the JATS XML tree to extract core identifiers
    and classification using defined XPath strategies.

    Returns:
        ArticleIdentityState: The populated epistemic state.
    """

    # Extract PMCID
    pmcid_nodes: Any = root.xpath("//article-id[@pub-id-type='pmc']")
    pmcid = ""
    if pmcid_nodes and hasattr(pmcid_nodes, "__getitem__"):
        node = pmcid_nodes[0]
        if hasattr(node, "itertext"):
            raw_pmcid = "".join(str(t) for t in node.itertext()).strip()
            pmcid = raw_pmcid.lower().replace("pmc", "").upper()

    # Extract PMID
    pmid_nodes: Any = root.xpath("//article-id[@pub-id-type='pmid']")
    pmid = None
    if pmid_nodes and hasattr(pmid_nodes, "__getitem__"):
        node = pmid_nodes[0]
        if hasattr(node, "itertext"):
            pmid = "".join(str(t) for t in node.itertext()).strip()

    # Extract DOI
    doi_nodes: Any = root.xpath("//article-id[@pub-id-type='doi']")
    doi = None
    if doi_nodes and hasattr(doi_nodes, "__getitem__"):
        node = doi_nodes[0]
        if hasattr(node, "itertext"):
            doi = "".join(str(t) for t in node.itertext()).strip()

    # Extract Article Type
    article_type_attr: Any = root.xpath("/article/@article-type")
    article_type = ArticleTypeEnum.OTHER

    if article_type_attr and hasattr(article_type_attr, "__getitem__"):
        attr = article_type_attr[0]
        if hasattr(attr, "lower"):
            raw_type = attr.lower()
            if raw_type in ("research-article", "research", "original-research"):
                article_type = ArticleTypeEnum.RESEARCH
            elif raw_type in ("review-article", "review"):
                article_type = ArticleTypeEnum.REVIEW
            elif raw_type == "case-report":
                article_type = ArticleTypeEnum.CASE_REPORT

    return ArticleIdentityState(pmcid=pmcid, pmid=pmid, doi=doi, article_type=article_type)


def _parse_date_element(date_node: Any) -> str | None:
    """AGENT INSTRUCTION: Helper function to parse a date element into YYYY-MM-DD format."""
    if not hasattr(date_node, "xpath"):
        return None

    year_nodes = date_node.xpath("./year/text()")
    if not year_nodes:
        return None
    year = str(year_nodes[0]).strip()

    month_nodes = date_node.xpath("./month/text()")
    season_nodes = date_node.xpath("./season/text()")

    if month_nodes:
        month_val = str(month_nodes[0]).strip().lower()
    elif season_nodes:
        month_val = str(season_nodes[0]).strip().lower()
    else:
        month_val = "01"

    # Season Map: Spring->03, Summer->06, Fall->09, Winter->12
    season_map = {"spring": "03", "summer": "06", "fall": "09", "winter": "12"}
    month = season_map.get(month_val, month_val)

    # Pad month if numeric and length 1
    if month.isdigit() and len(month) == 1:
        month = f"0{month}"
    # If month is not digits and not in season map, default to 01
    if not month.isdigit():
        month = "01"

    day_nodes = date_node.xpath("./day/text()")
    day_val = str(day_nodes[0]).strip() if day_nodes else "01"
    if day_val.isdigit() and len(day_val) == 1:
        day_val = f"0{day_val}"
    if not day_val.isdigit():
        day_val = "01"

    return f"{year}-{month}-{day_val}"


def extract_temporal_state(root: etree._Element) -> ArticleTemporalState:
    """
    AGENT INSTRUCTION: Parse the JATS XML tree to extract core temporal facts
    using defined XPath strategies and 'Best Date' heuristic.

    Returns:
        ArticleTemporalState: The populated epistemic state.
    """
    # Extract date_published
    # Priority: epub > ppub > pmc-release
    pub_dates: Any = root.xpath("//pub-date")
    date_published = None
    if pub_dates and hasattr(pub_dates, "__iter__"):
        dates_by_type = {}
        fallback_date = None
        for pd in pub_dates:
            pub_type_attrs = pd.xpath("./@pub-type")
            parsed_date = _parse_date_element(pd)
            if not parsed_date:
                continue

            if pub_type_attrs:
                ptype = str(pub_type_attrs[0]).strip().lower()
                dates_by_type[ptype] = parsed_date
            elif fallback_date is None:
                # Capture the first date without a pub-type as a last resort
                fallback_date = parsed_date

        if "epub" in dates_by_type:
            date_published = dates_by_type["epub"]
        elif "ppub" in dates_by_type:
            date_published = dates_by_type["ppub"]
        elif "pmc-release" in dates_by_type:
            date_published = dates_by_type["pmc-release"]
        elif dates_by_type:
            # Fallback to the first available date if none match the priority
            for parsed_date in dates_by_type.values():
                date_published = parsed_date
                break
        elif fallback_date:
            date_published = fallback_date

    # Extract date_received
    recv_dates: Any = root.xpath("//history/date[@date-type='received']")
    date_received = None
    if recv_dates and hasattr(recv_dates, "__getitem__"):
        date_received = _parse_date_element(recv_dates[0])

    # Extract date_accepted
    acc_dates: Any = root.xpath("//history/date[@date-type='accepted']")
    date_accepted = None
    if acc_dates and hasattr(acc_dates, "__getitem__"):
        date_accepted = _parse_date_element(acc_dates[0])

    return ArticleTemporalState(date_published=date_published, date_received=date_received, date_accepted=date_accepted)

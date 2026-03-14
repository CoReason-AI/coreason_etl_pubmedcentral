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

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

from coreason_etl_pubmedcentral.parsing.parser import ArticleType, parse_article_identity


@pytest.fixture  # type: ignore
def sample_data_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "jats_identity_sample.xml")


@pytest.fixture  # type: ignore
def articles(sample_data_path: str) -> Generator[list[etree._Element], None, None]:
    tree = etree.parse(sample_data_path)
    root = tree.getroot()
    # Return all article elements
    yield list(root.findall("article"))


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

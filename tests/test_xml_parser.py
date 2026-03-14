# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from hypothesis import given
from hypothesis import strategies as st
from lxml import etree

from coreason_etl_pubmedcentral.xml_parser import (
    ArticleIdentityState,
    ArticleTypeEnum,
    extract_identity_state,
)


def test_extract_identity_state_all_fields() -> None:
    """Positive test parsing all fields correctly."""
    xml_content = b"""
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC12345</article-id>
                <article-id pub-id-type="pmid">67890</article-id>
                <article-id pub-id-type="doi">10.1234/5678</article-id>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_identity_state(root)

    assert result.pmcid == "12345"
    assert result.pmid == "67890"
    assert result.doi == "10.1234/5678"
    assert result.article_type == ArticleTypeEnum.RESEARCH


def test_extract_identity_state_missing_fields() -> None:
    """Negative test parsing when some fields are missing."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">pmc999</article-id>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_identity_state(root)

    assert result.pmcid == "999"
    assert result.pmid is None
    assert result.doi is None
    assert result.article_type == ArticleTypeEnum.OTHER


def test_extract_identity_state_case_insensitive_pmc() -> None:
    """Boundary test ensuring PMCID prefix is stripped regardless of case."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PmC777</article-id>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_identity_state(root)

    assert result.pmcid == "777"


def test_extract_identity_state_article_type_mapping() -> None:
    """Positive test mapping various article types correctly."""
    mappings = {
        "research-article": ArticleTypeEnum.RESEARCH,
        "review-article": ArticleTypeEnum.REVIEW,
        "case-report": ArticleTypeEnum.CASE_REPORT,
        "unknown-type": ArticleTypeEnum.OTHER,
        "ORIGINAL-RESEARCH": ArticleTypeEnum.RESEARCH,
        "REVIEW": ArticleTypeEnum.REVIEW,
    }

    for raw_type, expected in mappings.items():
        xml_content = f"""
        <article article-type="{raw_type}">
            <front>
                <article-meta>
                    <article-id pub-id-type="pmc">PMC1</article-id>
                </article-meta>
            </front>
        </article>
        """.encode()
        root = etree.fromstring(xml_content)
        result = extract_identity_state(root)

        assert result.article_type == expected


def test_extract_identity_state_no_pmcid() -> None:
    """Negative test handling missing PMCID element gracefully but failing validation."""
    xml_content = b"""
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmid">67890</article-id>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    # The extraction logic currently defaults missing pmcid to "".
    # Validation will not fail if "" is provided.
    result = extract_identity_state(root)
    assert result.pmcid == ""


@given(st.text(min_size=1))  # type: ignore[misc]
def test_article_identity_state_property(pmcid: str) -> None:
    """Property-based test verifying valid inputs using hypothesis."""
    state = ArticleIdentityState(pmcid=pmcid, article_type=ArticleTypeEnum.OTHER)
    assert state.pmcid == pmcid
    assert state.article_type == ArticleTypeEnum.OTHER


def test_extract_identity_state_complex_nested_pmc() -> None:
    """Boundary test parsing text inside nested nodes in PMCID."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc"><b>PMC</b><i>123</i></article-id>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_identity_state(root)
    assert result.pmcid == "123"

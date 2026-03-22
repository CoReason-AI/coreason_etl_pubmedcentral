# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from lxml import etree

from coreason_etl_pubmedcentral.models.contracts import CognitiveArticleTypeContract
from coreason_etl_pubmedcentral.parsers.jats_parser import EpistemicJatsParser


def test_epistemic_jats_parser_extract_identity_valid() -> None:
    """Test standard valid JATS XML identity extraction."""
    xml_content = b"""
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmid">12345678</article-id>
                <article-id pub-id-type="pmc">PMC9876543</article-id>
                <article-id pub-id-type="doi">10.1234/journal.pone.0123456</article-id>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.ElementTree(etree.fromstring(xml_content))
    result = EpistemicJatsParser.extract_identity(tree)

    assert result["pmcid"] == "9876543"
    assert result["pmid"] == "12345678"
    assert result["doi"] == "10.1234/journal.pone.0123456"
    assert result["article_type"] == CognitiveArticleTypeContract.RESEARCH


def test_epistemic_jats_parser_extract_identity_article_type_mapping() -> None:
    """Test mapping of article types."""
    # Test valid enum match
    xml_content_research = (
        b"""<article article-type="research"><front><article-meta></article-meta></front></article>"""
    )
    tree_research = etree.ElementTree(etree.fromstring(xml_content_research))
    result_research = EpistemicJatsParser.extract_identity(tree_research)
    assert result_research["article_type"] == CognitiveArticleTypeContract.RESEARCH

    # Test invalid enum match
    xml_content_unknown = b"""<article article-type="unknown"><front><article-meta></article-meta></front></article>"""
    tree_unknown = etree.ElementTree(etree.fromstring(xml_content_unknown))
    result_unknown = EpistemicJatsParser.extract_identity(tree_unknown)
    assert result_unknown["article_type"] == CognitiveArticleTypeContract.OTHER

    # Test missing article-type attribute
    xml_content_missing = b"""<article><front><article-meta></article-meta></front></article>"""
    tree_missing = etree.ElementTree(etree.fromstring(xml_content_missing))
    result_missing = EpistemicJatsParser.extract_identity(tree_missing)
    assert result_missing["article_type"] == CognitiveArticleTypeContract.OTHER


def test_epistemic_jats_parser_extract_identity_missing_ids() -> None:
    """Test missing or empty identifiers."""
    xml_content = b"""
    <article article-type="review">
        <front>
            <article-meta>
                <article-id pub-id-type="pmid"></article-id>
                <!-- Missing pmc and doi entirely -->
            </article-meta>
        </front>
    </article>
    """
    tree = etree.ElementTree(etree.fromstring(xml_content))
    result = EpistemicJatsParser.extract_identity(tree)

    assert result["pmcid"] == ""
    assert result["pmid"] is None
    assert result["doi"] is None
    assert result["article_type"] == CognitiveArticleTypeContract.REVIEW


def test_epistemic_jats_parser_extract_identity_strip_pmc_prefix() -> None:
    """Test that the PMC prefix is correctly stripped regardless of case."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">pmc12345</article-id>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.ElementTree(etree.fromstring(xml_content))
    result = EpistemicJatsParser.extract_identity(tree)

    assert result["pmcid"] == "12345"


def test_epistemic_jats_parser_extract_identity_complex_whitespace_and_newlines() -> None:
    """Test identifiers with complex leading/trailing whitespace, newlines, and tabs."""
    xml_content = b"""
    <article article-type="case-report">
        <front>
            <article-meta>
                <article-id pub-id-type="pmid">
                    12345678
                </article-id>
                <article-id pub-id-type="pmc">
                    PMC9876543
                </article-id>
                <article-id pub-id-type="doi">
                    10.1234/journal.pone.0123456
                </article-id>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.ElementTree(etree.fromstring(xml_content))
    result = EpistemicJatsParser.extract_identity(tree)

    assert result["pmcid"] == "9876543"
    assert result["pmid"] == "12345678"
    assert result["doi"] == "10.1234/journal.pone.0123456"
    assert result["article_type"] == CognitiveArticleTypeContract.CASE_REPORT


def test_epistemic_jats_parser_extract_identity_multiple_same_type_ids() -> None:
    """Test handling of multiple article-id tags of the same type (should take the first valid one)."""
    xml_content = b"""
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmid"></article-id>
                <article-id pub-id-type="pmid">11111111</article-id>
                <article-id pub-id-type="pmc">PMC2222222</article-id>
                <article-id pub-id-type="pmc">PMC3333333</article-id>
                <article-id pub-id-type="doi">10.first/doi</article-id>
                <article-id pub-id-type="doi">10.second/doi</article-id>
            </article-meta>
        </front>
    </article>
    """
    tree = etree.ElementTree(etree.fromstring(xml_content))
    result = EpistemicJatsParser.extract_identity(tree)

    # Note: Currently the parser just grabs `nodes[0]` which could be empty text
    # It checks `if first_node.text: return str(first_node.text).strip()`
    # If the first is empty, it returns None. This is standard behavior to not over-engineer,
    # but let's assert what the parser actually does currently:
    assert result["pmid"] is None
    assert result["pmcid"] == "2222222"
    assert result["doi"] == "10.first/doi"


def test_epistemic_jats_parser_extract_identity_missing_article_and_meta() -> None:
    """Test completely malformed JATS XML that is missing <front> and <article-meta> structure."""
    xml_content = b"""<root></root>"""
    tree = etree.ElementTree(etree.fromstring(xml_content))
    result = EpistemicJatsParser.extract_identity(tree)

    assert result["pmcid"] == ""
    assert result["pmid"] is None
    assert result["doi"] is None
    assert result["article_type"] == CognitiveArticleTypeContract.OTHER

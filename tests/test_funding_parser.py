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
    ArticleFundingState,
    FundingEntityState,
    extract_funding_state,
)


def test_extract_funding_state_modern_jats() -> None:
    """Positive test for modern JATS funding extraction."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <funding-group>
                    <award-group>
                        <funding-source>National Institutes of Health</funding-source>
                        <award-id>R01-CA123456</award-id>
                    </award-group>
                    <award-group>
                        <funding-source>National Science Foundation</funding-source>
                        <award-id>NSF-987654</award-id>
                    </award-group>
                </funding-group>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_funding_state(root)

    # Sorting ensures order
    assert len(result.funding) == 2
    assert result.funding[0].agency == "National Institutes of Health"
    assert result.funding[0].grant_id == "R01-CA123456"
    assert result.funding[1].agency == "National Science Foundation"
    assert result.funding[1].grant_id == "NSF-987654"


def test_extract_funding_state_legacy_jats() -> None:
    """Positive test for legacy JATS funding extraction, independent sibling nodes."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <contract-num>ABC-123</contract-num>
                <contract-num>XYZ-789</contract-num>
                <contract-sponsor>Department of Defense</contract-sponsor>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_funding_state(root)

    assert len(result.funding) == 3
    # Sorting order: agency then grant_id
    # ("", "ABC-123"), ("", "XYZ-789"), ("Department of Defense", "")
    assert result.funding[0].agency == ""
    assert result.funding[0].grant_id == "ABC-123"
    assert result.funding[1].agency == ""
    assert result.funding[1].grant_id == "XYZ-789"
    assert result.funding[2].agency == "Department of Defense"
    assert result.funding[2].grant_id == ""


def test_extract_funding_state_empty_nodes() -> None:
    """Boundary test for empty or missing text in nodes."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <funding-group>
                    <award-group>
                        <funding-source></funding-source>
                        <award-id>  </award-id>
                    </award-group>
                </funding-group>
                <contract-num></contract-num>
                <contract-sponsor>   </contract-sponsor>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_funding_state(root)

    assert len(result.funding) == 0


def test_extract_funding_state_nested_text() -> None:
    """Test extracting text from nested formatting tags."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <funding-group>
                    <award-group>
                        <funding-source>National <b>Institutes</b> of <i>Health</i></funding-source>
                        <award-id>R01-<span>CA123456</span></award-id>
                    </award-group>
                </funding-group>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_funding_state(root)

    assert len(result.funding) == 1
    assert result.funding[0].agency == "National Institutes of Health"
    assert result.funding[0].grant_id == "R01-CA123456"


def test_extract_funding_state_mixed_missing_modern() -> None:
    """Test modern JATS with missing agency or missing grant id."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <funding-group>
                    <award-group>
                        <funding-source>Agency A</funding-source>
                    </award-group>
                    <award-group>
                        <award-id>Grant B</award-id>
                    </award-group>
                </funding-group>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_funding_state(root)

    assert len(result.funding) == 2
    assert result.funding[0].agency == ""
    assert result.funding[0].grant_id == "Grant B"
    assert result.funding[1].agency == "Agency A"
    assert result.funding[1].grant_id == ""


@given(
    st.lists(
        st.builds(
            FundingEntityState,
            agency=st.text(min_size=1, max_size=50),
            grant_id=st.text(min_size=1, max_size=50),
        ),
        min_size=1,
        max_size=10,
    )
)
def test_article_funding_state_sorting(funding_list: list[FundingEntityState]) -> None:
    """Property-based test verifying deterministic sorting of ArticleFundingState."""
    state = ArticleFundingState(funding=funding_list)

    # Verify it is sorted
    is_sorted = all(
        (state.funding[i].agency, state.funding[i].grant_id)
        <= (state.funding[i + 1].agency, state.funding[i + 1].grant_id)
        for i in range(len(state.funding) - 1)
    )
    assert is_sorted

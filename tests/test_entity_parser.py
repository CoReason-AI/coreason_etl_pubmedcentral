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
    ContributorEntityState,
    extract_entity_state,
)


def test_extract_entity_state_success() -> None:
    """Positive test for successful entity parsing."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <contrib-group>
                    <contrib contrib-type="author">
                        <name>
                            <surname>Doe</surname>
                            <given-names>John A</given-names>
                        </name>
                        <xref ref-type="aff" rid="aff1"/>
                    </contrib>
                    <contrib contrib-type="author">
                        <name>
                            <surname>Smith</surname>
                            <given-names>Jane</given-names>
                        </name>
                        <xref ref-type="aff" rid="aff2"/>
                    </contrib>
                </contrib-group>
                <aff id="aff1"><label>1</label>University of X</aff>
                <aff id="aff2"><label>2</label>Institute Y</aff>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_entity_state(root)

    assert len(result.contributors) == 2
    assert result.contributors[0].name == "Doe JA"
    assert result.contributors[0].affs == ["University of X"]
    assert result.contributors[1].name == "Smith J"
    assert result.contributors[1].affs == ["Institute Y"]


def test_extract_entity_state_missing_affiliations() -> None:
    """Negative test where authors have no affiliations or unresolvable affiliations."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <contrib-group>
                    <contrib contrib-type="author">
                        <name>
                            <surname>NoAff</surname>
                        </name>
                    </contrib>
                    <contrib contrib-type="author">
                        <name>
                            <surname>BadRef</surname>
                        </name>
                        <xref ref-type="aff" rid="aff99"/>
                    </contrib>
                </contrib-group>
                <aff id="aff1">Valid Aff</aff>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_entity_state(root)

    assert len(result.contributors) == 2
    assert result.contributors[0].name == "NoAff"
    assert result.contributors[0].affs == []
    assert result.contributors[1].name == "BadRef"
    assert result.contributors[1].affs == []


def test_extract_entity_state_multiple_affiliations() -> None:
    """Positive test handling authors with multiple affiliations, separated or combined."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <contrib-group>
                    <contrib contrib-type="author">
                        <name>
                            <surname>Multi</surname>
                            <given-names>M M</given-names>
                        </name>
                        <xref ref-type="aff" rid="aff1 aff2"/>
                        <xref ref-type="aff" rid="aff3"/>
                    </contrib>
                </contrib-group>
                <aff id="aff1">Aff 1</aff>
                <aff id="aff2"><label>2</label>Aff 2</aff>
                <aff id="aff3">Aff 3</aff>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_entity_state(root)

    assert len(result.contributors) == 1
    assert result.contributors[0].name == "Multi MM"
    # Testing deterministic sorting: Aff 1, Aff 2, Aff 3
    assert result.contributors[0].affs == ["Aff 1", "Aff 2", "Aff 3"]


def test_extract_entity_state_missing_name_fields() -> None:
    """Boundary test handling authors with missing structured names."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <contrib-group>
                    <contrib contrib-type="author">
                        <name>
                            <given-names>Madonna</given-names>
                        </name>
                    </contrib>
                    <contrib contrib-type="author">
                        <collab>The Consortium</collab>
                    </contrib>
                    <contrib contrib-type="author">
                    </contrib>
                </contrib-group>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_entity_state(root)

    assert len(result.contributors) == 3
    assert result.contributors[0].name == "Madonna"
    assert result.contributors[1].name == "The Consortium"
    assert result.contributors[2].name == "Unknown"


def test_extract_entity_state_no_contribs() -> None:
    """Negative test handling no contributors gracefully."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <aff id="aff1">Isolated Aff</aff>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_entity_state(root)

    assert len(result.contributors) == 0


def test_extract_entity_state_empty_aff() -> None:
    """Boundary test handling empty affiliation text."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <contrib-group>
                    <contrib contrib-type="author">
                        <name><surname>Lone</surname></name>
                        <xref ref-type="aff" rid="aff1"/>
                    </contrib>
                </contrib-group>
                <aff id="aff1"><label>1</label></aff>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_entity_state(root)
    assert len(result.contributors) == 1
    assert result.contributors[0].affs == []


def test_extract_entity_state_aff_no_id() -> None:
    """Boundary test handling affiliation without id attribute."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <contrib-group>
                    <contrib contrib-type="author">
                        <name><surname>Lone</surname></name>
                        <xref ref-type="aff" rid="aff1"/>
                    </contrib>
                </contrib-group>
                <aff>Invalid Aff</aff>
                <aff id="aff1">Valid Aff</aff>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_entity_state(root)

    assert len(result.contributors) == 1
    assert result.contributors[0].affs == ["Valid Aff"]


@given(st.lists(st.text(min_size=1), min_size=1, max_size=5))  # type: ignore[misc]
def test_contributor_entity_state_property(affs: list[str]) -> None:
    """Property-based test verifying deterministic sorting and assignments."""
    state = ContributorEntityState(name="Prop Author", affs=affs)
    assert state.name == "Prop Author"
    assert state.affs == sorted(affs)

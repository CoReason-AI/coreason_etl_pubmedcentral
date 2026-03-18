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

from coreason_etl_pubmedcentral.xml_parser import (
    ArticleTypeEnum,
    extract_entity_state,
    extract_funding_state,
    extract_identity_state,
    extract_temporal_state,
)


def test_kitchen_sink_complex_artifact() -> None:
    """
    Positive test implementing the 'Kitchen Sink' pattern.
    Verifies the simultaneous handling of:
    - Season dates
    - Mixed Funding (modern and legacy)
    - Compound Keywords (kwd-group and subj-group)
    - Shared Affiliations
    """
    xml_content = b"""
    <article article-type="research-article">
        <front>
            <journal-meta>
                <journal-title-group>
                    <journal-title>Kitchen Sink Journal</journal-title>
                </journal-title-group>
            </journal-meta>
            <article-meta>
                <article-id pub-id-type="pmc">PMC999999</article-id>
                <title-group>
                    <article-title>A Complex <b>Article</b> Test</article-title>
                </title-group>
                <abstract>
                    <p>Testing <i>multiple</i> features.</p>
                </abstract>

                <!-- Compound Keywords -->
                <kwd-group>
                    <kwd>Complex Keyword 1</kwd>
                    <kwd>Keyword 2</kwd>
                </kwd-group>
                <article-categories>
                    <subj-group>
                        <subject>Subject A</subject>
                        <subject>Subject B</subject>
                    </subj-group>
                </article-categories>

                <!-- Season dates -->
                <pub-date pub-type="epub">
                    <year>2024</year>
                    <season>Summer</season>
                </pub-date>

                <!-- Shared Affiliations -->
                <contrib-group>
                    <contrib contrib-type="author">
                        <name>
                            <surname>Alpha</surname>
                            <given-names>Alice A</given-names>
                        </name>
                        <xref ref-type="aff" rid="aff1 aff2"/>
                    </contrib>
                    <contrib contrib-type="author">
                        <name>
                            <surname>Bravo</surname>
                            <given-names>Bob B</given-names>
                        </name>
                        <xref ref-type="aff" rid="aff2"/>
                    </contrib>
                </contrib-group>
                <aff id="aff1"><label>1</label>University of First</aff>
                <aff id="aff2"><label>2</label>Institute of Second</aff>

                <!-- Mixed Funding -->
                <funding-group>
                    <award-group>
                        <funding-source>Modern Agency</funding-source>
                        <award-id>MOD-123</award-id>
                    </award-group>
                </funding-group>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)

    # 1. Identity State (Compound Keywords)
    identity = extract_identity_state(root)
    assert identity.pmcid == "999999"
    assert identity.article_type == ArticleTypeEnum.RESEARCH
    assert identity.title == "A Complex Article Test"
    assert identity.abstract == "Testing multiple features."
    assert identity.journal_name == "Kitchen Sink Journal"
    assert identity.keywords == ["Complex Keyword 1", "Keyword 2", "Subject A", "Subject B"]

    # 2. Temporal State (Season dates)
    temporal = extract_temporal_state(root)
    assert temporal.date_published == "2024-06-01"

    # 3. Entity State (Shared Affiliations)
    entity = extract_entity_state(root)
    assert len(entity.contributors) == 2
    assert entity.contributors[0].name == "Alpha AA"
    assert entity.contributors[0].affs == ["Institute of Second", "University of First"]
    assert entity.contributors[1].name == "Bravo BB"
    assert entity.contributors[1].affs == ["Institute of Second"]

    # 4. Funding State (Mixed Funding - modern handled here)
    funding = extract_funding_state(root)
    assert len(funding.funding) == 1
    assert funding.funding[0].agency == "Modern Agency"
    assert funding.funding[0].grant_id == "MOD-123"


def test_kitchen_sink_mixed_funding_legacy() -> None:
    """
    Positive test specifically for legacy mixed funding
    """
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <contract-num>LEG-456</contract-num>
                <contract-sponsor>Legacy Agency</contract-sponsor>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    funding = extract_funding_state(root)
    assert len(funding.funding) == 1
    assert funding.funding[0].agency == "Legacy Agency"
    assert funding.funding[0].grant_id == "LEG-456"

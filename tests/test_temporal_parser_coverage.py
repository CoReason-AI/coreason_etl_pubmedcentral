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

from coreason_etl_pubmedcentral.xml_parser import _parse_date_element, extract_temporal_state


def test_parse_date_element_no_xpath() -> None:
    """Negative test for _parse_date_element with invalid node."""
    assert _parse_date_element("not an element") is None


def test_parse_date_element_single_digit_day() -> None:
    """Boundary test for padding a single digit day."""
    xml_content = b"""
    <pub-date pub-type="epub">
        <year>2023</year>
        <month>05</month>
        <day>5</day>
    </pub-date>
    """
    node = etree.fromstring(xml_content)
    result = _parse_date_element(node)
    assert result == "2023-05-05"


def test_extract_temporal_state_ppub_priority() -> None:
    """Positive test for ppub priority when epub is missing."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="ppub">
                    <year>2023</year>
                    <month>06</month>
                    <day>01</day>
                </pub-date>
                <pub-date pub-type="pmc-release">
                    <year>2023</year>
                    <month>07</month>
                    <day>01</day>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published == "2023-06-01"


def test_extract_temporal_state_pmc_release_priority() -> None:
    """Positive test for pmc-release priority when others are missing."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="pmc-release">
                    <year>2023</year>
                    <month>07</month>
                    <day>01</day>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published == "2023-07-01"

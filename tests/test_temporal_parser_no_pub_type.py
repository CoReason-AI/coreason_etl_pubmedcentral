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

from coreason_etl_pubmedcentral.xml_parser import extract_temporal_state


def test_extract_temporal_state_no_pub_type_fallback() -> None:
    """Boundary test to ensure fallback works when pub-type is completely missing."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <pub-date>
                    <year>2018</year>
                    <month>11</month>
                    <day>15</day>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published == "2018-11-15"

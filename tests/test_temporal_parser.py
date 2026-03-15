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


def test_extract_temporal_state_best_date() -> None:
    """Positive test for Best Date heuristic (epub > ppub > pmc-release)."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="epub">
                    <year>2023</year>
                    <month>05</month>
                    <day>15</day>
                </pub-date>
                <pub-date pub-type="ppub">
                    <year>2023</year>
                    <month>06</month>
                    <day>01</day>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published == "2023-05-15"


def test_extract_temporal_state_missing_day_month() -> None:
    """Negative/Boundary test for missing day/month padding and default."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="epub">
                    <year>2022</year>
                    <month>4</month>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published == "2022-04-01"


def test_extract_temporal_state_season_mapping() -> None:
    """Positive test for season to numeric month mapping."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="epub">
                    <year>2021</year>
                    <season>Spring</season>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published == "2021-03-01"


def test_extract_temporal_state_history_dates() -> None:
    """Positive test for received and accepted history dates."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <history>
                    <date date-type="received">
                        <year>2020</year>
                        <month>01</month>
                        <day>10</day>
                    </date>
                    <date date-type="accepted">
                        <year>2020</year>
                        <month>02</month>
                        <day>20</day>
                    </date>
                </history>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_received == "2020-01-10"
    assert result.date_accepted == "2020-02-20"


def test_extract_temporal_state_no_dates() -> None:
    """Negative test handling absence of dates gracefully."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published is None
    assert result.date_received is None
    assert result.date_accepted is None


def test_extract_temporal_state_fallback_date() -> None:
    """Boundary test to ensure fallback works when pub-type isn't standard."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="other">
                    <year>2019</year>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published == "2019-01-01"


def test_extract_temporal_state_invalid_month_day() -> None:
    """Boundary test to ensure invalid month/day are defaulted."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="epub">
                    <year>2019</year>
                    <month>invalid</month>
                    <day>invalid</day>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published == "2019-01-01"


def test_extract_temporal_state_no_year() -> None:
    """Negative test to ensure no year results in None."""
    xml_content = b"""
    <article>
        <front>
            <article-meta>
                <pub-date pub-type="epub">
                    <month>05</month>
                    <day>15</day>
                </pub-date>
            </article-meta>
        </front>
    </article>
    """
    root = etree.fromstring(xml_content)
    result = extract_temporal_state(root)
    assert result.date_published is None

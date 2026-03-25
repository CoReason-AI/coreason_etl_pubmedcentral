# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason-etl-pubmedabstracts

import io

from coreason_etl_pubmedabstracts.parsers.xml_parser import EpistemicMedlineParser


def test_epistemic_medline_parser_medlinecitation() -> None:
    """Test extracting a basic MedlineCitation."""
    xml_data = b"""<?xml version="1.0" ?>
    <PubmedArticleSet>
        <PubmedArticle>
            <MedlineCitation Status="MEDLINE" Owner="NLM">
                <PMID Version="1">12345</PMID>
                <DateCompleted>
                    <Year>2026</Year>
                    <Month>03</Month>
                    <Day>14</Day>
                </DateCompleted>
                <Article PubModel="Print-Electronic">
                    <Journal>
                        <ISSN IssnType="Electronic">1234-5678</ISSN>
                        <Title>Journal of CoReason</Title>
                    </Journal>
                    <ArticleTitle>A test article.</ArticleTitle>
                </Article>
            </MedlineCitation>
        </PubmedArticle>
    </PubmedArticleSet>
    """
    stream = io.BytesIO(xml_data)
    results = list(EpistemicMedlineParser.execute(stream))

    assert len(results) == 1
    record = results[0]

    assert record["event_type"] == "MedlineCitation"
    assert record["pmid"] == "12345"
    assert "content_hash" in record
    assert isinstance(record["content_hash"], str)
    assert len(record["content_hash"]) == 32

    raw = record["raw_data"]
    assert raw["@Status"] == "MEDLINE"
    assert raw["@Owner"] == "NLM"
    assert raw["PMID"]["#text"] == "12345"
    assert raw["PMID"]["@Version"] == "1"
    assert raw["Article"]["ArticleTitle"] == "A test article."
    assert raw["Article"]["Journal"]["Title"] == "Journal of CoReason"


def test_epistemic_medline_parser_deletecitation() -> None:
    """Test extracting a DeleteCitation event."""
    xml_data = b"""<?xml version="1.0" ?>
    <DeleteCitation>
        <PMID Version="1">67890</PMID>
        <PMID Version="1">98765</PMID>
    </DeleteCitation>
    """
    stream = io.BytesIO(xml_data)
    results = list(EpistemicMedlineParser.execute(stream))

    assert len(results) == 1
    record = results[0]

    assert record["event_type"] == "DeleteCitation"
    assert record["pmid"] == "67890"  # Extracts the first one

    raw = record["raw_data"]
    assert isinstance(raw["PMID"], list)
    assert raw["PMID"][0]["#text"] == "67890"
    assert raw["PMID"][1]["#text"] == "98765"


def test_epistemic_medline_parser_xml_to_dict_empty() -> None:
    """Test xml_to_dict handles empty tags correctly."""
    xml_data = b"""<?xml version="1.0" ?>
    <PubmedArticleSet>
        <PubmedArticle>
            <MedlineCitation>
                <EmptyTag></EmptyTag>
                <EmptySelfClosingTag/>
            </MedlineCitation>
        </PubmedArticle>
    </PubmedArticleSet>
    """
    stream = io.BytesIO(xml_data)
    results = list(EpistemicMedlineParser.execute(stream))

    assert len(results) == 1
    raw = results[0]["raw_data"]
    assert raw["EmptyTag"] is None
    assert raw["EmptySelfClosingTag"] is None


def test_epistemic_medline_parser_multiple_records() -> None:
    """Test streaming multiple records from a single file."""
    xml_data = b"""<?xml version="1.0" ?>
    <PubmedArticleSet>
        <PubmedArticle>
            <MedlineCitation>
                <PMID>1</PMID>
            </MedlineCitation>
        </PubmedArticle>
        <PubmedArticle>
            <MedlineCitation>
                <PMID>2</PMID>
            </MedlineCitation>
        </PubmedArticle>
        <DeleteCitation>
            <PMID>3</PMID>
        </DeleteCitation>
    </PubmedArticleSet>
    """
    stream = io.BytesIO(xml_data)
    results = list(EpistemicMedlineParser.execute(stream))

    assert len(results) == 3
    assert results[0]["pmid"] == "1"
    assert results[1]["pmid"] == "2"
    assert results[2]["pmid"] == "3"
    assert results[2]["event_type"] == "DeleteCitation"


def test_epistemic_medline_parser_mixed_content() -> None:
    """Test parser handles mixed text and children."""
    xml_data = b"""<?xml version="1.0" ?>
    <PubmedArticleSet>
        <PubmedArticle>
            <MedlineCitation>
                <Abstract>
                    Some text before
                    <AbstractText>Text inside</AbstractText>
                    Some text after
                </Abstract>
            </MedlineCitation>
        </PubmedArticle>
    </PubmedArticleSet>
    """
    stream = io.BytesIO(xml_data)
    results = list(EpistemicMedlineParser.execute(stream))

    assert len(results) == 1
    raw = results[0]["raw_data"]

    # Text before the child node should be preserved in '#text' key
    assert "Some text before" in raw["Abstract"]["#text"]


def test_epistemic_medline_parser_lists_of_lists() -> None:
    """Test xml_to_dict correctly creates list of lists if elements repeat."""
    xml_data = b"""<?xml version="1.0" ?>
    <PubmedArticleSet>
        <PubmedArticle>
            <MedlineCitation>
                <KeywordList>
                    <Keyword>A</Keyword>
                    <Keyword>B</Keyword>
                    <Keyword>C</Keyword>
                </KeywordList>
            </MedlineCitation>
        </PubmedArticle>
    </PubmedArticleSet>
    """
    stream = io.BytesIO(xml_data)
    results = list(EpistemicMedlineParser.execute(stream))
    assert len(results) == 1
    raw = results[0]["raw_data"]
    assert raw["KeywordList"]["Keyword"] == ["A", "B", "C"]

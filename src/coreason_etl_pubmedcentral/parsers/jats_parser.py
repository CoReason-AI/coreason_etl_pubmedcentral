# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from typing import Any, ClassVar

from lxml import etree

from coreason_etl_pubmedcentral.models.contracts import CognitiveArticleTypeContract


class EpistemicJatsParser:
    """
    Parser for extracting structured data from PMC JATS XML.

    AGENT INSTRUCTION:
    This class strictly implements the Medallion Pipeline's Layer 2 parsing logic
    for Identity & Classification. It operates on parsed lxml AST payloads and
    returns dictionaries mapping canonical metadata.
    """

    _ARTICLE_TYPE_MAPPING: ClassVar[dict[str, CognitiveArticleTypeContract]] = {
        "research-article": CognitiveArticleTypeContract.RESEARCH,
        "review-article": CognitiveArticleTypeContract.REVIEW,
        "case-report": CognitiveArticleTypeContract.CASE_REPORT,
        "research": CognitiveArticleTypeContract.RESEARCH,
        "review": CognitiveArticleTypeContract.REVIEW,
    }

    @classmethod
    def _extract_article_id(cls, tree: etree._ElementTree, pub_id_type: str) -> str | None:
        """
        Helper method to extract the stripped text of the first matching article-id element.
        """
        nodes = tree.xpath(f"//article-id[@pub-id-type='{pub_id_type}']")
        if isinstance(nodes, list):
            first_node = next(iter(nodes), None)
            if isinstance(first_node, etree._Element) and first_node.text:
                return str(first_node.text).strip()
        return None

    _SEASON_MAP: ClassVar[dict[str, str]] = {
        "spring": "03",
        "summer": "06",
        "fall": "09",
        "winter": "12",
    }

    @classmethod
    def _parse_jats_date(cls, node: etree._Element | None) -> str | None:
        """
        Parses a JATS <date> or <pub-date> node into an ISO-8601 string (YYYY-MM-DD).
        - Year is required.
        - Missing Month/Day default to '01'.
        - Maps seasons (Spring, Summer, Fall, Winter) to start months.
        - Strips whitespace.
        """
        if node is None:
            return None  # pragma: no cover

        def _get_text(tag: str) -> str | None:
            child = node.find(tag)
            if child is not None and child.text:
                return str(child.text).strip()
            return None  # pragma: no cover

        year = _get_text("year")
        if not year:
            return None

        month_text = _get_text("month") or "01"
        month_text_lower = month_text.lower()

        # Handle Seasons
        if month_text_lower in cls._SEASON_MAP:
            month = cls._SEASON_MAP[month_text_lower]
        elif month_text.isdigit():
            month = month_text.zfill(2)
        else:
            # Fallback if unparsable text is in the month tag
            month = "01"

        day_text = _get_text("day") or "01"
        day = day_text.zfill(2) if day_text.isdigit() else "01"

        return f"{year}-{month}-{day}"

    @classmethod
    def _extract_first_matching_date(cls, tree: etree._ElementTree, xpath: str) -> str | None:
        nodes = tree.xpath(xpath)
        if isinstance(nodes, list) and nodes:
            first_node = next(iter(nodes), None)
            if isinstance(first_node, etree._Element):
                return cls._parse_jats_date(first_node)
        return None

    @classmethod
    def extract_temporal(cls, tree: etree._ElementTree) -> dict[str, str | None]:
        """
        Extracts temporal metadata (published, received, accepted dates) using the "Best Date" heuristic.

        Transformation Logic:
        - date_published: Priority: epub > ppub > pmc-release.
        - date_received: //history/date[@date-type='received']
        - date_accepted: //history/date[@date-type='accepted']
        - Missing month/day defaults to '01'.
        - Seasons map to integers (Spring->03).
        """
        date_published = (
            cls._extract_first_matching_date(tree, "//pub-date[@pub-type='epub']")
            or cls._extract_first_matching_date(tree, "//pub-date[@pub-type='ppub']")
            or cls._extract_first_matching_date(tree, "//pub-date[@pub-type='pmc-release']")
        )

        date_received = cls._extract_first_matching_date(tree, "//history/date[@date-type='received']")
        date_accepted = cls._extract_first_matching_date(tree, "//history/date[@date-type='accepted']")

        return {
            "date_published": date_published,
            "date_received": date_received,
            "date_accepted": date_accepted,
        }

    @classmethod
    def extract_identity(cls, tree: etree._ElementTree) -> dict[str, Any]:
        """
        Extracts canonical identifiers (pmcid, pmid, doi) and article type from the JATS XML AST.

        Transformation Logic:
        - pmcid: //article-id[@pub-id-type='pmc']. Strip 'PMC' prefix.
        - pmid: //article-id[@pub-id-type='pmid']. Keep as nullable string.
        - doi: //article-id[@pub-id-type='doi']. Keep as nullable string.
        - article_type: /article/@article-type. Map to CognitiveArticleTypeContract Enum. Default OTHER.
        """
        root = tree.getroot()

        # Handle Article Type
        article_type_str = root.get("article-type", "") if root is not None else ""

        article_type = cls._ARTICLE_TYPE_MAPPING.get(
            article_type_str.strip().lower(), CognitiveArticleTypeContract.OTHER
        )

        pmcid = cls._extract_article_id(tree, "pmc")
        if pmcid and pmcid.upper().startswith("PMC"):
            pmcid = pmcid[3:]

        # Handle edge case where PMCID is missing
        if not pmcid:
            pmcid = ""

        pmid = cls._extract_article_id(tree, "pmid")
        doi = cls._extract_article_id(tree, "doi")

        return {
            "pmcid": pmcid,
            "pmid": pmid,
            "doi": doi,
            "article_type": article_type,
        }

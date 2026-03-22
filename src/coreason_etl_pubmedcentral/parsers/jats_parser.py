# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from typing import Any

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

        # Explicit mapping from common JATS article types to the structured Enum
        article_type_mapping = {
            "research-article": CognitiveArticleTypeContract.RESEARCH,
            "review-article": CognitiveArticleTypeContract.REVIEW,
            "case-report": CognitiveArticleTypeContract.CASE_REPORT,
            "research": CognitiveArticleTypeContract.RESEARCH,
            "review": CognitiveArticleTypeContract.REVIEW,
        }

        article_type = article_type_mapping.get(
            article_type_str.strip().lower(),
            CognitiveArticleTypeContract.OTHER
        )

        # Helper to extract text from an element matching an XPath
        def get_id(pub_id_type: str) -> str | None:
            # Note: lxml uses .xpath() which returns a list
            nodes = tree.xpath(f"//article-id[@pub-id-type='{pub_id_type}']")
            if isinstance(nodes, list) and len(nodes) > 0:
                first_node = nodes[0]
                # In lxml, Element supports .text, but xpath can return strings/tuples.
                if isinstance(first_node, etree._Element) and first_node.text:
                    return str(first_node.text).strip()
            return None

        pmcid = get_id("pmc")
        if pmcid and pmcid.upper().startswith("PMC"):
            pmcid = pmcid[3:]

        # Handle edge case where PMCID is missing (should not happen in valid PMC data, but we must be defensive)
        if not pmcid:
            # A valid pmcid is required for Silver manifest, but we will return what we find
            pmcid = ""

        pmid = get_id("pmid")
        doi = get_id("doi")

        return {
            "pmcid": pmcid,
            "pmid": pmid,
            "doi": doi,
            "article_type": article_type,
        }

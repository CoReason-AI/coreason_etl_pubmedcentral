# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import contextlib
from typing import Any

from lxml import etree

from coreason_etl_pubmedcentral.models.contracts import CognitiveArticleTypeContract


class CognitiveJATSParsingPolicy:
    """
    CognitiveJATSParsingPolicy is responsible for navigating structural variations
    and drift within JATS XML formats (Legacy 1.0 vs Modern 1.3).

    AGENT INSTRUCTION:
    This class enforces strict temporal normalization using the "Best Date" heuristic,
    resolves author affiliations via internal XML references (`rid` -> `id`),
    and standardizes funding structures across schema versions.
    """

    @classmethod
    def execute(cls, tree: etree._ElementTree) -> dict[str, Any]:
        """
        Extracts and normalizes metadata from a JATS XML ElementTree.
        """
        payload: dict[str, Any] = {}

        # 1. Identity & Classification
        payload["pmcid"] = cls._extract_id(tree, "pmc", strip_prefix=True)
        payload["pmid"] = cls._extract_id(tree, "pmid")
        payload["doi"] = cls._extract_id(tree, "doi")
        payload["article_type"] = cls._extract_article_type(tree)

        # 2. Temporal Normalization
        payload["date_published"] = cls._extract_best_date(tree)
        payload["date_received"] = cls._extract_history_date(tree, "received")
        payload["date_accepted"] = cls._extract_history_date(tree, "accepted")

        # 3. Text Extraction
        payload["title"] = cls._extract_text(tree, "//article-meta//title-group/article-title")
        payload["abstract"] = cls._extract_text(tree, "//article-meta//abstract")
        payload["journal_name"] = cls._extract_text(tree, "//journal-meta//journal-title") or cls._extract_text(
            tree, "//journal-meta//journal-title-group/journal-title"
        )

        # 4. Filters (Keywords)
        payload["keywords"] = cls._extract_keywords(tree)

        # 5. Entity Resolution (Authors & Affiliations)
        payload["authors"], payload["affiliations_text"] = cls._resolve_contributors(tree)

        # 6. Funding Normalization (Drift Handling)
        funding = cls._resolve_funding(tree)
        payload["grant_ids"] = [f.get("grant_id") for f in funding if f.get("grant_id")]
        payload["agency_names"] = [f.get("agency") for f in funding if f.get("agency")]

        return payload

    @classmethod
    def _extract_id(cls, tree: etree._ElementTree, pub_id_type: str, strip_prefix: bool = False) -> str | None:
        """Extracts article IDs based on pub-id-type."""
        nodes = tree.xpath(f"//article-id[@pub-id-type='{pub_id_type}']")
        if isinstance(nodes, list) and nodes:
            first_node = nodes[0]
            if isinstance(first_node, etree._Element) and first_node.text:
                text = str(first_node.text).strip()
                if strip_prefix and text.startswith("PMC"):
                    return text[3:]
                return text
        return None

    @classmethod
    def _extract_article_type(cls, tree: etree._ElementTree) -> CognitiveArticleTypeContract:
        """Maps /article/@article-type to a CognitiveArticleTypeContract."""
        root = tree.getroot()
        article_type = root.get("article-type", "").lower()

        mapping = {
            "research-article": CognitiveArticleTypeContract.RESEARCH,
            "review-article": CognitiveArticleTypeContract.REVIEW,
            "case-report": CognitiveArticleTypeContract.CASE_REPORT,
        }
        return mapping.get(article_type, CognitiveArticleTypeContract.OTHER)

    @classmethod
    def _extract_best_date(cls, tree: etree._ElementTree) -> str:
        """
        Implements the "Best Date" Heuristic.
        Priority: epub > ppub > pmc-release.
        Defaults missing Day/Month to '01'.
        Maps seasons to standard months.
        """
        for date_type in ["epub", "ppub", "pmc-release"]:
            date_nodes = tree.xpath(f"//pub-date[@pub-type='{date_type}']")
            if not isinstance(date_nodes, list) or not date_nodes:
                continue

            # In JATS 1.3, date-type is often used instead of pub-type
            node = date_nodes[0]
            if not isinstance(node, etree._Element):
                continue  # pragma: no cover

            year = cls._extract_element_text(node, "year")
            if not year:
                continue

            month = cls._extract_element_text(node, "month") or "01"
            day = cls._extract_element_text(node, "day") or "01"

            season = cls._extract_element_text(node, "season")
            if season:
                season = season.lower()
                if "spring" in season:
                    month = "03"
                elif "summer" in season:
                    month = "06"
                elif "fall" in season:
                    month = "09"
                elif "winter" in season:
                    month = "12"

            with contextlib.suppress(ValueError):
                return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

        # Fallback if no valid date found at all.
        return "1970-01-01"

    @classmethod
    def _extract_history_date(cls, tree: etree._ElementTree, date_type: str) -> str | None:
        """Constructs ISO-8601 string from history dates."""
        nodes = tree.xpath(f"//history/date[@date-type='{date_type}']")
        if not isinstance(nodes, list) or not nodes:
            return None

        node = nodes[0]
        if not isinstance(node, etree._Element):
            return None  # pragma: no cover

        year = cls._extract_element_text(node, "year")
        month = cls._extract_element_text(node, "month") or "01"
        day = cls._extract_element_text(node, "day") or "01"

        if year:
            with contextlib.suppress(ValueError):
                return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        return None

    @classmethod
    def _extract_element_text(cls, node: etree._Element, tag: str) -> str | None:
        """Helper to safely extract text from a child element."""
        child = node.find(tag)
        if child is not None and child.text:
            return str(child.text).strip()
        return None

    @classmethod
    def _extract_text(cls, tree: etree._ElementTree, xpath_expr: str) -> str:
        """
        Safely extracts plain text from XML nodes containing nested HTML tags
        (like <abstract> or <article-title>).
        """
        nodes = tree.xpath(xpath_expr)
        if not isinstance(nodes, list) or not nodes:
            return ""

        node = nodes[0]
        if not isinstance(node, etree._Element):
            return ""  # pragma: no cover

        return "".join(str(t) for t in node.itertext()).strip()

    @classmethod
    def _extract_keywords(cls, tree: etree._ElementTree) -> list[str]:
        """Extracts and flattens keywords from <kwd-group> and <subject> elements."""
        keywords: list[str] = []
        kwd_nodes = tree.xpath("//kwd-group/kwd")
        if isinstance(kwd_nodes, list):
            for kwd in kwd_nodes:
                if isinstance(kwd, etree._Element):
                    text = "".join(str(t) for t in kwd.itertext()).strip()
                    if text:
                        keywords.append(text)

        subj_nodes = tree.xpath("//subject")
        if isinstance(subj_nodes, list):
            for subj in subj_nodes:
                if isinstance(subj, etree._Element):
                    text = "".join(str(t) for t in subj.itertext()).strip()
                    if text:
                        keywords.append(text)

        return keywords

    @classmethod
    def _resolve_contributors(cls, tree: etree._ElementTree) -> tuple[list[dict[str, Any]], list[str]]:
        """
        Entity Resolution logic linking //contrib-group/contrib to //aff via rid.
        Returns:
            - A list of structured author dictionaries: `[{"name": "Doe J", "affs": ["Univ X"]}]`
            - A list of all unique affiliation text strings found for the article.
        """
        # Step 1: Map Affiliations
        aff_map: dict[str, str] = {}
        all_affiliations: list[str] = []

        aff_nodes = tree.xpath("//aff")
        if isinstance(aff_nodes, list):
            for aff_node in aff_nodes:
                if not isinstance(aff_node, etree._Element):
                    continue  # pragma: no cover
                aff_id = aff_node.get("id")
                if not aff_id:
                    continue

                # Exclude <label> text when extracting affiliation content
                # We must be careful to grab all text EXCEPT from children named 'label'
                parts = []
                if aff_node.text:
                    parts.append(aff_node.text)
                for child in aff_node:
                    if child.tag != "label":
                        parts.append("".join(str(t) for t in child.itertext()))
                    if child.tail:
                        parts.append(child.tail)

                text = "".join(parts).strip()

                # Remove trailing commas or periods common in JATS
                text = text.rstrip(".,").strip()

                if text:
                    aff_map[aff_id] = text
                    all_affiliations.append(text)

        # Step 2: Resolve Contributors
        authors: list[dict[str, Any]] = []
        contrib_nodes = tree.xpath("//contrib-group/contrib[@contrib-type='author']")
        if isinstance(contrib_nodes, list):
            for contrib in contrib_nodes:
                if not isinstance(contrib, etree._Element):
                    continue  # pragma: no cover
                author_dict: dict[str, Any] = {}

                # Construct display name
                surname = cls._extract_element_text(contrib, "name/surname") or ""
                given = cls._extract_element_text(contrib, "name/given-names") or ""

                name = f"{surname} {given}".strip()
                if name:
                    author_dict["name"] = name

                # Resolve Affiliations via internal XML references
                affs: list[str] = []
                xref_nodes = contrib.xpath("xref[@ref-type='aff']")
                if isinstance(xref_nodes, list):
                    for xref in xref_nodes:
                        if not isinstance(xref, etree._Element):
                            continue  # pragma: no cover
                        rid = xref.get("rid")
                        # Sometimes rid is a space-separated list of IDs
                        if rid:
                            affs.extend(
                                aff_map[individual_rid] for individual_rid in rid.split() if individual_rid in aff_map
                            )

                if affs:
                    author_dict["affs"] = affs

                if author_dict:
                    authors.append(author_dict)

        return authors, all_affiliations

    @classmethod
    def _resolve_funding(cls, tree: etree._ElementTree) -> list[dict[str, str]]:
        """
        Normalizes Funding to standardize schema drift across JATS versions.
        Coalesces //funding-group/award-group (Modern)
        and //article-meta/contract-num & contract-sponsor (Legacy).
        """
        funding: list[dict[str, str]] = []

        # 1. Modern JATS
        award_groups = tree.xpath("//funding-group/award-group")
        if isinstance(award_groups, list):
            for award_group in award_groups:
                if not isinstance(award_group, etree._Element):
                    continue  # pragma: no cover
                agency = ""
                agency_nodes = award_group.xpath(".//funding-source")
                if isinstance(agency_nodes, list) and agency_nodes:
                    first_agency = agency_nodes[0]
                    if isinstance(first_agency, etree._Element):
                        agency = "".join(str(t) for t in first_agency.itertext()).strip()

                grant_id = ""
                grant_nodes = award_group.xpath(".//award-id")
                if isinstance(grant_nodes, list) and grant_nodes:
                    first_grant = grant_nodes[0]
                    if isinstance(first_grant, etree._Element):
                        grant_id = "".join(str(t) for t in first_grant.itertext()).strip()

                if agency or grant_id:
                    funding.append({"agency": agency, "grant_id": grant_id})

        # 2. Legacy JATS
        # To prioritize data preservation over unsafe heuristic merging, independent
        # objects are generated for un-grouped elements when funding-group is absent.
        if not funding:
            sponsor_nodes = tree.xpath("//article-meta/contract-sponsor")
            if isinstance(sponsor_nodes, list):
                for sponsor_node in sponsor_nodes:
                    if not isinstance(sponsor_node, etree._Element):
                        continue  # pragma: no cover
                    agency = "".join(str(t) for t in sponsor_node.itertext()).strip()
                    if agency:
                        funding.append({"agency": agency, "grant_id": ""})

            grant_nodes = tree.xpath("//article-meta/contract-num")
            if isinstance(grant_nodes, list):
                for grant_node in grant_nodes:
                    if not isinstance(grant_node, etree._Element):
                        continue  # pragma: no cover
                    grant_id = "".join(str(t) for t in grant_node.itertext()).strip()
                    if grant_id:
                        funding.append({"agency": "", "grant_id": grant_id})

        return funding

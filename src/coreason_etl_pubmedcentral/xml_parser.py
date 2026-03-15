# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import enum
from typing import Any

from lxml import etree
from pydantic import BaseModel, Field, field_validator


class ArticleTypeEnum(enum.StrEnum):
    """Strictly typed classification for PubMed Central articles."""

    RESEARCH = "RESEARCH"
    REVIEW = "REVIEW"
    CASE_REPORT = "CASE_REPORT"
    OTHER = "OTHER"


class ArticleIdentityState(BaseModel):
    """
    Epistemic snapshot containing the canonical identities and classification
    of a parsed PubMed Central document.
    """

    pmcid: str = Field(description="Canonical PMCID with prefix stripped")
    pmid: str | None = Field(default=None, description="PubMed ID")
    doi: str | None = Field(default=None, description="Document Object Identifier")
    article_type: ArticleTypeEnum = Field(
        default=ArticleTypeEnum.OTHER,
        description="Categorical designation mapped from JATS article-type",
    )


class ContributorEntityState(BaseModel):
    """
    Epistemic snapshot of a single contributor and their resolved affiliations.
    """

    name: str = Field(description="Resolved display name, e.g., 'Doe J'")
    affs: list[str] = Field(default_factory=list, description="List of resolved affiliation strings")

    def __init__(self, **data: Any) -> None:
        """AGENT INSTRUCTION: Ensure deterministic sorting of affiliations."""
        super().__init__(**data)
        self.affs = sorted(self.affs)


class ArticleEntityState(BaseModel):
    """
    Epistemic snapshot containing the resolved contributors and their affiliations.
    """

    contributors: list[ContributorEntityState] = Field(
        default_factory=list, description="List of resolved contributors"
    )


class FundingEntityState(BaseModel):
    """
    Epistemic snapshot of a single funding entity.
    """

    agency: str = Field(description="Normalized agency name")
    grant_id: str = Field(description="Normalized grant ID")


class ArticleFundingState(BaseModel):
    """
    Epistemic snapshot containing the resolved funding entities.
    """

    funding: list[FundingEntityState] = Field(default_factory=list, description="List of funding entities")

    @field_validator("funding")
    @classmethod
    def sort_funding(cls, v: list[FundingEntityState]) -> list[FundingEntityState]:
        """AGENT INSTRUCTION: Ensure deterministic sorting of funding."""
        return sorted(v, key=lambda x: (x.agency, x.grant_id))


class ArticleTemporalState(BaseModel):
    """
    Epistemic snapshot containing the temporal facts of a parsed PubMed Central document.
    """

    date_published: str | None = Field(
        default=None, description="Best available publication date in ISO-8601 format (YYYY-MM-DD)"
    )
    date_received: str | None = Field(
        default=None, description="Date the manuscript was received in ISO-8601 format (YYYY-MM-DD)"
    )
    date_accepted: str | None = Field(
        default=None, description="Date the manuscript was accepted in ISO-8601 format (YYYY-MM-DD)"
    )


def extract_identity_state(root: etree._Element) -> ArticleIdentityState:
    """
    AGENT INSTRUCTION: Parse the JATS XML tree to extract core identifiers
    and classification using defined XPath strategies.

    Returns:
        ArticleIdentityState: The populated epistemic state.
    """

    # Extract PMCID
    pmcid_nodes: Any = root.xpath("//article-id[@pub-id-type='pmc']")
    pmcid = ""
    if pmcid_nodes and hasattr(pmcid_nodes, "__getitem__"):
        node = pmcid_nodes[0]
        if hasattr(node, "itertext"):
            raw_pmcid = "".join(str(t) for t in node.itertext()).strip()
            pmcid = raw_pmcid.lower().replace("pmc", "").upper()

    # Extract PMID
    pmid_nodes: Any = root.xpath("//article-id[@pub-id-type='pmid']")
    pmid = None
    if pmid_nodes and hasattr(pmid_nodes, "__getitem__"):
        node = pmid_nodes[0]
        if hasattr(node, "itertext"):
            pmid = "".join(str(t) for t in node.itertext()).strip()

    # Extract DOI
    doi_nodes: Any = root.xpath("//article-id[@pub-id-type='doi']")
    doi = None
    if doi_nodes and hasattr(doi_nodes, "__getitem__"):
        node = doi_nodes[0]
        if hasattr(node, "itertext"):
            doi = "".join(str(t) for t in node.itertext()).strip()

    # Extract Article Type
    article_type_attr: Any = root.xpath("/article/@article-type")
    article_type = ArticleTypeEnum.OTHER

    if article_type_attr and hasattr(article_type_attr, "__getitem__"):
        attr = article_type_attr[0]
        if hasattr(attr, "lower"):
            raw_type = attr.lower()
            if raw_type in ("research-article", "research", "original-research"):
                article_type = ArticleTypeEnum.RESEARCH
            elif raw_type in ("review-article", "review"):
                article_type = ArticleTypeEnum.REVIEW
            elif raw_type == "case-report":
                article_type = ArticleTypeEnum.CASE_REPORT

    return ArticleIdentityState(pmcid=pmcid, pmid=pmid, doi=doi, article_type=article_type)


def _parse_date_element(date_node: Any) -> str | None:
    """AGENT INSTRUCTION: Helper function to parse a date element into YYYY-MM-DD format."""
    if not hasattr(date_node, "xpath"):
        return None

    year_nodes = date_node.xpath("./year/text()")
    if not year_nodes:
        return None
    year = str(year_nodes[0]).strip()

    month_nodes = date_node.xpath("./month/text()")
    season_nodes = date_node.xpath("./season/text()")

    if month_nodes:
        month_val = str(month_nodes[0]).strip().lower()
    elif season_nodes:
        month_val = str(season_nodes[0]).strip().lower()
    else:
        month_val = "01"

    # Season Map: Spring->03, Summer->06, Fall->09, Winter->12
    season_map = {"spring": "03", "summer": "06", "fall": "09", "winter": "12"}
    month = season_map.get(month_val, month_val)

    # Pad month if numeric and length 1
    if month.isdigit() and len(month) == 1:
        month = f"0{month}"
    # If month is not digits and not in season map, default to 01
    if not month.isdigit():
        month = "01"

    day_nodes = date_node.xpath("./day/text()")
    day_val = str(day_nodes[0]).strip() if day_nodes else "01"
    if day_val.isdigit() and len(day_val) == 1:
        day_val = f"0{day_val}"
    if not day_val.isdigit():
        day_val = "01"

    return f"{year}-{month}-{day_val}"


def extract_temporal_state(root: etree._Element) -> ArticleTemporalState:
    """
    AGENT INSTRUCTION: Parse the JATS XML tree to extract core temporal facts
    using defined XPath strategies and 'Best Date' heuristic.

    Returns:
        ArticleTemporalState: The populated epistemic state.
    """
    # Extract date_published
    # Priority: epub > ppub > pmc-release
    pub_dates: Any = root.xpath("//pub-date")
    date_published = None
    if pub_dates and hasattr(pub_dates, "__iter__"):
        dates_by_type = {}
        fallback_date = None
        for pd in pub_dates:
            pub_type_attrs = pd.xpath("./@pub-type")
            parsed_date = _parse_date_element(pd)
            if not parsed_date:
                continue

            if pub_type_attrs:
                ptype = str(pub_type_attrs[0]).strip().lower()
                dates_by_type[ptype] = parsed_date
            elif fallback_date is None:
                # Capture the first date without a pub-type as a last resort
                fallback_date = parsed_date

        if "epub" in dates_by_type:
            date_published = dates_by_type["epub"]
        elif "ppub" in dates_by_type:
            date_published = dates_by_type["ppub"]
        elif "pmc-release" in dates_by_type:
            date_published = dates_by_type["pmc-release"]
        elif dates_by_type:
            # Fallback to the first available date if none match the priority
            for parsed_date in dates_by_type.values():
                date_published = parsed_date
                break
        elif fallback_date:
            date_published = fallback_date

    # Extract date_received
    recv_dates: Any = root.xpath("//history/date[@date-type='received']")
    date_received = None
    if recv_dates and hasattr(recv_dates, "__getitem__"):
        date_received = _parse_date_element(recv_dates[0])

    # Extract date_accepted
    acc_dates: Any = root.xpath("//history/date[@date-type='accepted']")
    date_accepted = None
    if acc_dates and hasattr(acc_dates, "__getitem__"):
        date_accepted = _parse_date_element(acc_dates[0])

    return ArticleTemporalState(date_published=date_published, date_received=date_received, date_accepted=date_accepted)


def extract_entity_state(root: etree._Element) -> ArticleEntityState:
    """
    AGENT INSTRUCTION: Parse the JATS XML tree to resolve entities (authors and affiliations)
    using a mapping strategy for internal references.

    Returns:
        ArticleEntityState: The populated epistemic state.
    """
    # Step 1: Map Affiliations
    aff_nodes: Any = root.xpath("//aff")
    aff_map: dict[str, str] = {}
    if aff_nodes and hasattr(aff_nodes, "__iter__"):
        for aff in aff_nodes:
            # Get ID attribute
            id_attrs = aff.xpath("./@id")
            if not id_attrs:
                continue
            aff_id = str(id_attrs[0]).strip()

            # We need to extract the text of the affiliation, excluding the label.
            # E.g., <aff id="a1"><label>1</label>University of X</aff>

            # Simple approach: get all text nodes and join them, skipping <label> text
            text_parts: list[str] = []
            for child in aff.iter():
                # Avoid label tags, only extract text
                if (not isinstance(child.tag, str) or child.tag.lower() != "label") and child.text:
                    text_parts.append(str(child.text).strip())
                if child.tail:
                    text_parts.append(str(child.tail).strip())

            # For the root `aff` element, ensure we don't duplicate tail text
            # if it was iterated over differently, but `iter()` is a depth-first
            # traversal yielding elements. Actually, `aff.text` may not be covered
            # if we just loop `child in aff.iter()` because `aff` is the first element yielded.
            # But the logic above correctly skips `<label>` text.

            # We should be careful about extracting text properly.
            # Let's use a simpler method: just remove <label> nodes, then get text.
            # But we shouldn't modify the original tree.
            # Instead, let's use XPath to get text nodes excluding those under <label>
            valid_text_nodes = aff.xpath(".//text()[not(ancestor-or-self::label)]")
            if valid_text_nodes:
                text_parts = [str(t).strip() for t in valid_text_nodes if str(t).strip()]
                aff_text = " ".join(text_parts).strip()
            else:
                aff_text = ""

            if aff_text:
                aff_map[aff_id] = aff_text

    # Step 2: Resolve Contributors
    contrib_nodes: Any = root.xpath("//contrib-group/contrib")
    contributors: list[ContributorEntityState] = []

    if contrib_nodes and hasattr(contrib_nodes, "__iter__"):
        for contrib in contrib_nodes:
            # Extract name
            surname_nodes = contrib.xpath(".//surname/text()")
            given_nodes = contrib.xpath(".//given-names/text()")

            surname = str(surname_nodes[0]).strip() if surname_nodes else ""
            given = str(given_nodes[0]).strip() if given_nodes else ""

            if surname and given:
                # E.g. Doe J -> split given names and take first letters
                given_initials = "".join([n[0] for n in given.split() if n])
                name = f"{surname} {given_initials}".strip()
            elif surname:
                name = surname
            elif given:
                name = given
            else:
                # Try getting the raw text if no structured name elements exist
                # Or just skip if name cannot be found
                raw_name = "".join(str(t) for t in contrib.itertext()).strip()
                name = raw_name or "Unknown"

            # Extract affiliations
            affs: list[str] = []
            xref_nodes = contrib.xpath(".//xref[@ref-type='aff']/@rid")
            if xref_nodes and hasattr(xref_nodes, "__iter__"):
                for rid in xref_nodes:
                    rid_str = str(rid).strip()
                    # Some JATS XMLs put multiple space-separated IDs in a single rid attribute
                    affs.extend(
                        [aff_map[individual_rid] for individual_rid in rid_str.split() if individual_rid in aff_map]
                    )

            # Deduplicate affiliations
            affs = list(set(affs))

            contributors.append(ContributorEntityState(name=name, affs=affs))

    return ArticleEntityState(contributors=contributors)


def extract_funding_state(root: etree._Element) -> ArticleFundingState:
    """
    AGENT INSTRUCTION: Parse the JATS XML tree to resolve funding entities.
    Handles both modern JATS (<funding-group>) and legacy JATS (<contract-num>, <contract-sponsor>).

    Returns:
        ArticleFundingState: The populated epistemic state.
    """
    funding: list[FundingEntityState] = []

    # Strategy 1: Modern JATS (funding-group)
    award_nodes: Any = root.xpath("//funding-group//award-group")
    if award_nodes and hasattr(award_nodes, "__iter__"):
        for award in award_nodes:
            agency_nodes = award.xpath(".//funding-source")
            grant_nodes = award.xpath(".//award-id")

            # In modern JATS, an award-group can have multiple funding-sources or award-ids,
            # but usually it's one of each or related. We'll join them or take the first.
            # To be robust, we combine text of all matching nodes.
            agency_text = ""
            if agency_nodes:
                agency_text = " ".join("".join(str(t) for t in n.itertext()).strip() for n in agency_nodes).strip()

            grant_text = ""
            if grant_nodes:
                grant_text = " ".join("".join(str(t) for t in n.itertext()).strip() for n in grant_nodes).strip()

            if agency_text or grant_text:
                funding.append(FundingEntityState(agency=agency_text, grant_id=grant_text))

        return ArticleFundingState(funding=funding)

    # Strategy 2: Legacy JATS (article-meta)
    # AGENT INSTRUCTION: Legacy funding extraction generates independent ArticleFunding objects
    # for contract-num and contract-sponsor elements when they appear as siblings without grouping,
    # prioritizing data preservation over unsafe heuristic merging.
    contract_nums: Any = root.xpath("//article-meta//contract-num")
    if contract_nums and hasattr(contract_nums, "__iter__"):
        for num in contract_nums:
            num_text = "".join(str(t) for t in num.itertext()).strip()
            if num_text:
                funding.append(FundingEntityState(agency="", grant_id=num_text))

    contract_sponsors: Any = root.xpath("//article-meta//contract-sponsor")
    if contract_sponsors and hasattr(contract_sponsors, "__iter__"):
        for sponsor in contract_sponsors:
            sponsor_text = "".join(str(t) for t in sponsor.itertext()).strip()
            if sponsor_text:
                funding.append(FundingEntityState(agency=sponsor_text, grant_id=""))

    return ArticleFundingState(funding=funding)

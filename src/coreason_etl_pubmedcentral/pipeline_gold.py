# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from collections.abc import Iterator
from typing import Any, Optional

import dlt

from coreason_etl_pubmedcentral.utils.logger import logger


def transform_gold_record(item: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Pure function to transform a Silver record into a Gold record.
    Returns None if the record should be skipped (though typically Gold consumes all Silver).
    """
    # 1. Extract context
    ingestion_metadata = item.get("ingestion_metadata", {})
    manifest_metadata = item.get("manifest_metadata", {})
    source_file_path = ingestion_metadata.get("source_file_path", "")

    # 2. Logic: is_commercial_safe
    # "False if from oa_noncomm; True if oa_comm."
    # We check if the path starts with oa_comm or contains it?
    # Spec says: "oa_comm/oa_comm.filelist.csv" etc.
    # Usually file paths are "oa_comm/xml/..." or "oa_noncomm/xml/..."
    # We will look for "oa_comm" in the path vs "oa_noncomm".
    # Default to False for safety if unknown.
    is_commercial_safe = False
    if "oa_comm" in source_file_path and "oa_noncomm" not in source_file_path:
        is_commercial_safe = True
    elif "oa_noncomm" in source_file_path:
        is_commercial_safe = False
    else:
        # Fallback or strict? "oa_comm" check covers it.
        # If path is just "PMC123.xml", we might not know.
        # Assuming folder structure is preserved in source_file_path as per Bronze.
        pass

    # 3. Logic: pub_year
    date_published = item.get("date_published")
    pub_year: Optional[int] = None
    if date_published and isinstance(date_published, str):
        # Expected format YYYY-MM-DD
        parts = date_published.split("-")
        if parts and parts[0].isdigit():
            pub_year = int(parts[0])

    # 4. Logic: Authors Display & Affiliations
    authors = item.get("authors", [])
    authors_display_list: list[str] = []
    affiliations_set: set[str] = set()

    for auth in authors:
        name = auth.get("name")

        if name:
            authors_display_list.append(name)

        # Collect affiliations
        affs = auth.get("affiliations", [])
        if affs:
            for aff in affs:
                if aff:
                    affiliations_set.add(aff)

    authors_display = "; ".join(authors_display_list)
    affiliations_text = sorted(list(affiliations_set))

    # 5. Logic: Funding (Grant IDs & Agencies)
    funding = item.get("funding", [])
    grant_ids_set: set[str] = set()
    agency_names_set: set[str] = set()

    for fund in funding:
        agency = fund.get("agency")
        gid = fund.get("grant_id")

        if agency:
            agency_names_set.add(agency)
        if gid:
            grant_ids_set.add(gid)

    grant_ids = sorted(list(grant_ids_set))
    agency_names = sorted(list(agency_names_set))

    # 6. Construct Gold Record
    gold_record = {
        # Filters
        "grant_ids": grant_ids,
        "agency_names": agency_names,
        "keywords": item.get("keywords", []),
        "affiliations_text": affiliations_text,
        # Search
        "title": item.get("title"),
        "abstract": item.get("abstract"),
        "authors_display": authors_display,
        # Compliance
        "is_commercial_safe": is_commercial_safe,
        "is_retracted": item.get("is_retracted", False),
        "license_type": manifest_metadata.get("license_type"),
        # Context
        "journal_name": item.get("journal_name"),
        "pub_year": pub_year,
        # Keys for merging/tracking (not explicitly in wide table spec but essential for DLT/DB)
        "pmcid": item.get("pmcid"),  # Primary Key usually
        "pmid": item.get("pmid"),
        "doi": item.get("doi"),
    }

    return gold_record


def _pmc_gold_generator(items: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """
    Generator that transforms Silver records to Gold.
    """
    # DLT sometimes passes a single dict instead of an iterator/list.
    # We must normalize it to a list.
    if isinstance(items, dict):
        items = [items]  # type: ignore

    for item in items:
        try:
            res = transform_gold_record(item)
            if res:
                yield res
        except Exception as e:
            # We log and skip to avoid breaking the whole pipeline on one bad record
            pmcid = item.get("pmcid", "unknown")
            logger.error(f"Error transforming Silver record {pmcid} to Gold: {e}")
            pass


@dlt.transformer(name="gold_pmc_analytics_rich", write_disposition="merge", primary_key="pmcid")  # type: ignore[misc]
def pmc_gold(items: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:  # pragma: no cover
    """
    Gold Layer Transformer.
    Transforms structured Silver records into a "Wide Table" optimized for analytics.
    Aggregates metadata for search and filtering.
    """
    return _pmc_gold_generator(items)

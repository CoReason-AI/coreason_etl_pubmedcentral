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
import json
from collections.abc import Generator
from typing import Any

import dlt

from coreason_etl_pubmedcentral.utils.logger import logger


def _gold_transformer_generator(silver_item: dict[str, Any]) -> Generator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: Internal generator logic for the gold layer transformer.
    Takes a record from the silver layer, denormalizes the `ParsedArticleState`
    into the `gold_pmc_analytics_rich` wide table schema, and applies JSON serialization
    to array fields to prevent dlt schema explosion.
    """
    article = silver_item.get("article", {})
    if not article:
        logger.warning(f"Silver item missing 'article' payload. coreason_id: {silver_item.get('coreason_id')}")
        return

    # Extract sections
    entity_data = article.get("entity", {})
    funding_data = article.get("funding", {})
    temporal_data = article.get("temporal", {})

    # Extract Filters arrays
    grant_ids = [f.get("grant_id") for f in funding_data.get("funding", []) if f.get("grant_id")]
    agency_names = [f.get("agency") for f in funding_data.get("funding", []) if f.get("agency")]

    # We do not have keywords in the parsed state right now, but schema requires an array
    keywords: list[str] = []

    affiliations_text: list[str] = []
    authors_display_list: list[str] = []

    for contributor in entity_data.get("contributors", []):
        if contributor.get("name"):
            authors_display_list.append(contributor["name"])
        for aff in contributor.get("affs", []):
            if aff not in affiliations_text:
                affiliations_text.append(aff)

    authors_display = "; ".join(authors_display_list)

    # Extract Context
    journal_name = ""

    pub_year = None
    date_published = temporal_data.get("date_published")
    if date_published:
        with contextlib.suppress(ValueError):
            pub_year = int(date_published.split("-")[0])

    source_file_path = silver_item.get("source_file_path", "")
    is_commercial_safe = "oa_comm" in source_file_path

    file_metadata = silver_item.get("file_metadata", {})
    is_retracted = file_metadata.get("retracted", False)
    license_type = file_metadata.get("license", "")

    yield {
        "coreason_id": silver_item.get("coreason_id"),
        "pmcid": silver_item.get("pmcid"),
        # Filters (JSON serialized to prevent dlt schema explosion)
        "grant_ids": json.dumps(sorted(set(grant_ids))),
        "agency_names": json.dumps(sorted(set(agency_names))),
        "keywords": json.dumps(keywords),
        "affiliations_text": json.dumps(sorted(affiliations_text)),
        # Search
        "title": "",
        "abstract": "",
        "authors_display": authors_display,
        # Compliance
        "is_commercial_safe": is_commercial_safe,
        "is_retracted": is_retracted,
        "license_type": license_type,
        # Context
        "journal_name": journal_name,
        "pub_year": pub_year,
    }


@dlt.transformer(name="gold_pmc_analytics_rich", write_disposition="append")
def build_gold_analytics(silver_item: dict[str, Any]) -> Generator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: Gold layer dlt transformer.
    Takes a record from the silver layer and denormalizes it into the wide table schema.
    Applies JSON serialization to lists to prevent dlt schema explosion.
    """
    yield from _gold_transformer_generator(silver_item)  # pragma: no cover

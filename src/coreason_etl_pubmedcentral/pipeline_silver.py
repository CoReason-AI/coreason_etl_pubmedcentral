# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import io
from collections.abc import Iterator
from typing import Any, Optional

import dlt
from lxml import etree

from coreason_etl_pubmedcentral.parsing.parser import (
    parse_article_authors,
    parse_article_content,
    parse_article_dates,
    parse_article_funding,
    parse_article_identity,
    parse_article_keywords,
)
from coreason_etl_pubmedcentral.utils.logger import logger


def transform_silver_record(item: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Pure function to transform a Bronze record into a Silver record.
    Returns None if the record should be skipped.
    """
    file_path = item.get("source_file_path", "unknown")
    raw_xml = item.get("raw_xml_payload")
    manifest_metadata = item.get("manifest_metadata", {})

    # Manifest Metadata extraction
    is_retracted_manifest = manifest_metadata.get("is_retracted", False)

    context_logger = logger.bind(file_path=file_path, layer="silver")

    if not raw_xml:
        context_logger.warning("Skipping record with empty raw_xml_payload")
        return None

    try:
        # Enforce "Stream & Clear" memory management pattern
        # Wrap string/bytes in BytesIO for iterparse
        # Handle both bytes (preferred from Bronze) and str (legacy/fallback)
        if isinstance(raw_xml, bytes):
            f = io.BytesIO(raw_xml)
        elif isinstance(raw_xml, str):
            f = io.BytesIO(raw_xml.encode("utf-8"))
        else:
            # Fallback for unexpected types
            f = io.BytesIO(str(raw_xml).encode("utf-8"))

        # Using iterparse to parse the single article in the blob
        # Use tag=None to catch namespaced articles, then filter by localname
        context = etree.iterparse(f, events=("end",), tag=None)

        parsed_record: dict[str, Any] = {}
        processed = False

        for _, elem in context:
            # Check if tag is 'article' (ignoring namespace)
            if etree.QName(elem).localname == "article":
                try:
                    # Identity
                    identity = parse_article_identity(elem)

                    # Dates
                    dates = parse_article_dates(elem)

                    # Authors
                    authors = parse_article_authors(elem)

                    # Funding
                    funding = parse_article_funding(elem)

                    # Content (Title, Abstract, Journal)
                    content = parse_article_content(elem)

                    # Keywords
                    keywords = parse_article_keywords(elem)

                    # Validation & Observability
                    if not identity.pmcid:
                        context_logger.warning("SchemaViolation — Article missing mandatory 'pmcid' field.")

                    if not content.title:
                        pmcid_log = identity.pmcid or "unknown"
                        context_logger.warning(
                            f"SchemaViolation — Article {pmcid_log} missing mandatory 'title' field."
                        )

                    if is_retracted_manifest:
                        pmcid_log = identity.pmcid or "unknown"
                        context_logger.info(f"RetractionFound — Marking {pmcid_log} as retracted based on manifest.")

                    # Construct Silver Record
                    parsed_record = {
                        "pmcid": identity.pmcid,
                        "pmid": identity.pmid,
                        "doi": identity.doi,
                        "article_type": identity.article_type.value,
                        "date_published": dates.date_published,
                        "date_received": dates.date_received,
                        "date_accepted": dates.date_accepted,
                        # Content Fields
                        "title": content.title,
                        "abstract": content.abstract,
                        "journal_name": content.journal_name,
                        "keywords": keywords,
                        # Serialize authors
                        "authors": [
                            {
                                "name": f"{a.surname or ''} {a.given_names or ''}".strip(),
                                "affiliations": a.affiliations,
                            }
                            for a in authors
                        ],
                        # Serialize funding
                        "funding": [{"agency": f.agency, "grant_id": f.grant_id} for f in funding],
                        # Retraction Logic:
                        # Flag if listed as retracted in manifest.
                        # Note: We do NOT delete, we flag.
                        "is_retracted": is_retracted_manifest,
                        # Pass through metadata
                        "manifest_metadata": manifest_metadata,
                        "ingestion_metadata": {
                            "source_file_path": item.get("source_file_path"),
                            "ingestion_ts": item.get("ingestion_ts"),
                            "ingestion_source": item.get("ingestion_source"),
                        },
                    }
                    processed = True

                finally:
                    # CRITICAL: Clear memory immediately for article and its siblings
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

        if processed:
            return parsed_record
        else:
            context_logger.warning("No <article> tag found in XML payload")
            return None

    except etree.XMLSyntaxError as e:
        context_logger.error(f"XML Syntax Error: {e}")
        return None
    except Exception as e:
        context_logger.exception(f"Unexpected error in Silver transformation: {e}")
        return None


def _pmc_silver_generator(items: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    """
    Generator function that iterates over items and applies transformation.
    """
    for item in items:
        result = transform_silver_record(item)
        if result:
            yield result


@dlt.transformer(name="pmc_silver", write_disposition="append")  # type: ignore[misc]
def pmc_silver(items: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:  # pragma: no cover
    """
    Silver Layer Transformer.
    Parses raw XML from Bronze layer into structured Silver records.
    Handles JATS schema drift, identity extraction, temporal normalization,
    entity resolution, funding normalization, and content extraction.
    """
    return _pmc_silver_generator(items)

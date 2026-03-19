# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import datetime
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from coreason_etl_pubmedcentral.models.contracts import CognitiveArticleTypeContract


class EpistemicBronzeManifest(BaseModel):
    """
    Schema for the Bronze Layer table: `bronze_pmc_file`.
    Represents the metadata of a raw ingested source file without payload contents.
    """

    model_config = ConfigDict(strict=True)

    source_file_path: str = Field(
        description="Local path/URI to the downloaded archive (e.g., /mnt/data/oa_comm/PMC12345.tar.gz)."
    )
    ingestion_ts: datetime.datetime = Field(description="UTC time of download.")
    ingestion_source: str = Field(description="'S3' or 'FTP'")
    file_metadata: dict[str, Any] = Field(
        description="Metadata extracted from the CSV (License, Last Updated). Saved as JSONB."
    )


class CognitiveGoldManifest(BaseModel):
    """
    Schema for the Gold Layer table: `gold_pmc_analytics_rich`.
    Represents the refined "Wide Table" optimized for OLAP.
    Lists are serialized to JSON strings to prevent schema explosion.
    """

    model_config = ConfigDict(strict=True)

    pmcid: str = Field(description="Canonical PMC ID (stripped of 'PMC' prefix).")
    coreason_id: str = Field(description="Deterministic surrogate key (UUIDv5).")
    pmid: str | None = Field(default=None, description="Nullable PMID string.")
    doi: str | None = Field(default=None, description="Nullable DOI string.")

    article_type: CognitiveArticleTypeContract = Field(
        default=CognitiveArticleTypeContract.OTHER, description="Normalized article classification type."
    )

    date_published: str = Field(description="ISO-8601 published date using the Best Date Heuristic.")
    date_received: str | None = Field(default=None, description="ISO-8601 date received.")
    date_accepted: str | None = Field(default=None, description="ISO-8601 date accepted.")

    # Filters
    grant_ids: list[str] = Field(default_factory=list, description="List of all Grant IDs.")
    agency_names: list[str] = Field(default_factory=list, description="Normalized Agency names.")
    keywords: list[str] = Field(default_factory=list, description="Flattened list of keywords.")
    affiliations_text: list[str] = Field(default_factory=list, description="All unique affiliation strings.")

    # Entity Resolution (Serialized structured data)
    authors: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Structured list of contributors with affiliations: [{name: 'Doe J', affs: ['Univ X']}].",
    )

    # Search
    title: str = Field(description="Cleaned title text.")
    abstract: str = Field(description="HTML-stripped abstract.")
    authors_display: str = Field(description="Semicolon-separated names (e.g., 'Smith J; Doe A').")

    # Compliance
    is_commercial_safe: bool = Field(description="False if from oa_noncomm; True if oa_comm.")
    is_retracted: bool = Field(default=False, description="True if listed as retracted in file.")
    license_type: str = Field(description="License type, e.g., 'CC-BY', 'CC0', 'NO-CC'.")

    # Context
    journal_name: str = Field(description="Canonical NLM Journal Title.")
    pub_year: int = Field(description="Extracted integer year from date_published.")

    @field_validator("grant_ids", "agency_names", "keywords", "affiliations_text", mode="after")
    @classmethod
    def deterministically_sort_lists(cls, v: list[str]) -> list[str]:
        """
        Sort list fields deterministically to ensure consistent serialization.
        """
        return sorted(set(v))

    @field_validator("authors", mode="after")
    @classmethod
    def deterministically_sort_authors(cls, v: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Sort authors by name to ensure consistent serialization.
        Ensure affiliations within authors are also sorted.
        """
        for author in v:
            if "affs" in author and isinstance(author["affs"], list):
                author["affs"] = sorted(set(author["affs"]))
        return sorted(v, key=lambda x: str(x.get("name", "")))

    @field_serializer("grant_ids", "agency_names", "keywords", "affiliations_text", "authors")
    def serialize_lists_to_json(self, v: list[Any]) -> str:
        """
        AGENT INSTRUCTION: Serialize Python lists to JSON strings.
        This strictly satisfies the dlt schema explosion prevention protocol
        by ensuring nested data is stored as a single string column rather
        than generating relationally mapped child tables.
        """
        return json.dumps(v, separators=(",", ":"))

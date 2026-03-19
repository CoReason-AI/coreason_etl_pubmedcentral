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

import polars as pl
from hypothesis import given
from hypothesis import strategies as st

from coreason_etl_pubmedcentral.models.contracts import CognitiveArticleTypeContract
from coreason_etl_pubmedcentral.models.manifests import CognitiveGoldManifest, EpistemicBronzeManifest
from coreason_etl_pubmedcentral.models.topology import CognitiveIdentityTopology


def test_cognitive_article_type_contract() -> None:
    """Test standard categorical values for Article Type Contract."""
    assert CognitiveArticleTypeContract.RESEARCH == "RESEARCH"
    assert CognitiveArticleTypeContract.REVIEW == "REVIEW"
    assert CognitiveArticleTypeContract.CASE_REPORT == "CASE_REPORT"
    assert CognitiveArticleTypeContract.OTHER == "OTHER"


def test_epistemic_bronze_manifest() -> None:
    """Test the instantiation and validation of EpistemicBronzeManifest."""
    manifest = EpistemicBronzeManifest(
        source_file_path="/mnt/data/oa_comm/PMC12345.tar.gz",
        ingestion_ts=datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=datetime.UTC),
        ingestion_source="S3",
        file_metadata={"License": "CC-BY", "Last Updated": "2023-01-01"},
    )
    assert manifest.source_file_path == "/mnt/data/oa_comm/PMC12345.tar.gz"
    assert manifest.ingestion_source == "S3"
    assert manifest.file_metadata == {"License": "CC-BY", "Last Updated": "2023-01-01"}


def test_cognitive_gold_manifest_list_sorting() -> None:
    """Test that list fields are deterministically sorted and JSON serialized."""
    manifest = CognitiveGoldManifest(
        pmcid="12345",
        coreason_id="dummy-uuid",
        article_type=CognitiveArticleTypeContract.RESEARCH,
        date_published="2023-01-01",
        title="Test Title",
        abstract="Test Abstract",
        authors_display="Doe J",
        is_commercial_safe=True,
        license_type="CC-BY",
        journal_name="Test Journal",
        pub_year=2023,
        grant_ids=["G3", "G1", "G2"],
        agency_names=["NIH", "NSF", "NIH"],
        keywords=["Cancer", "Biology", "Cancer"],
        affiliations_text=["Univ B", "Univ A"],
        authors=[
            {"name": "Smith J", "affs": ["Univ B", "Univ A"]},
            {"name": "Doe J", "affs": ["Univ C"]},
        ],
    )

    # Assert deterministic sorting inside the Python object
    assert manifest.grant_ids == ["G1", "G2", "G3"]
    assert manifest.agency_names == ["NIH", "NSF"]
    assert manifest.keywords == ["Biology", "Cancer"]
    assert manifest.affiliations_text == ["Univ A", "Univ B"]

    assert manifest.authors[0]["name"] == "Doe J"
    assert manifest.authors[1]["name"] == "Smith J"
    assert manifest.authors[1]["affs"] == ["Univ A", "Univ B"]

    # Assert JSON serialization for lists via model_dump
    dumped = manifest.model_dump()
    assert dumped["grant_ids"] == json.dumps(["G1", "G2", "G3"], separators=(",", ":"))
    assert dumped["agency_names"] == json.dumps(["NIH", "NSF"], separators=(",", ":"))
    assert dumped["keywords"] == json.dumps(["Biology", "Cancer"], separators=(",", ":"))
    assert dumped["affiliations_text"] == json.dumps(["Univ A", "Univ B"], separators=(",", ":"))


def test_cognitive_identity_topology_generation() -> None:
    """Test deterministic UUIDv5 surrogate key generation."""
    pmcid_1 = "PMC12345"
    pmcid_2 = "PMC67890"

    key_1 = CognitiveIdentityTopology.generate_surrogate_key(pmcid_1)
    key_2 = CognitiveIdentityTopology.generate_surrogate_key(pmcid_2)
    key_1_dup = CognitiveIdentityTopology.generate_surrogate_key(pmcid_1)

    assert key_1 != key_2
    assert key_1 == key_1_dup


def test_cognitive_identity_topology_vectorized() -> None:
    """Test vectorized generation of surrogate keys using Polars."""
    series = pl.Series("pmcid", ["PMC12345", "PMC67890"])

    keys = CognitiveIdentityTopology.generate_surrogate_keys_vectorized(series)

    assert len(keys) == 2
    assert keys[0] == CognitiveIdentityTopology.generate_surrogate_key("PMC12345")
    assert keys[1] == CognitiveIdentityTopology.generate_surrogate_key("PMC67890")


@given(st.lists(st.text(), min_size=0, max_size=5))
def test_cognitive_gold_manifest_hypothesis(grant_list: list[str]) -> None:
    """Property-based test for deterministic sorting of grant_ids."""
    manifest = CognitiveGoldManifest(
        pmcid="12345",
        coreason_id="dummy-uuid",
        article_type=CognitiveArticleTypeContract.RESEARCH,
        date_published="2023-01-01",
        title="Test Title",
        abstract="Test Abstract",
        authors_display="Doe J",
        is_commercial_safe=True,
        license_type="CC-BY",
        journal_name="Test Journal",
        pub_year=2023,
        grant_ids=grant_list,
    )

    expected_sorted = sorted(set(grant_list))
    assert manifest.grant_ids == expected_sorted


def test_cognitive_gold_manifest_empty_and_null_fields() -> None:
    """Test CognitiveGoldManifest handles empty lists, nulls, and missing optional fields."""
    manifest = CognitiveGoldManifest(
        pmcid="12345",
        coreason_id="dummy-uuid",
        article_type=CognitiveArticleTypeContract.OTHER,
        date_published="2023-01-01",
        title="Test Title",
        abstract="Test Abstract",
        authors_display="",
        is_commercial_safe=False,
        license_type="CC0",
        journal_name="Test Journal",
        pub_year=2023,
    )

    assert manifest.pmid is None
    assert manifest.doi is None
    assert manifest.date_received is None
    assert manifest.date_accepted is None
    assert manifest.grant_ids == []
    assert manifest.agency_names == []
    assert manifest.keywords == []
    assert manifest.affiliations_text == []
    assert manifest.authors == []

    dumped = manifest.model_dump()
    assert dumped["grant_ids"] == "[]"
    assert dumped["authors"] == "[]"


def test_cognitive_gold_manifest_complex_authors() -> None:
    """Test CognitiveGoldManifest handles complex authors (missing affs, duplicate names/affs)."""
    manifest = CognitiveGoldManifest(
        pmcid="12345",
        coreason_id="dummy-uuid",
        article_type=CognitiveArticleTypeContract.RESEARCH,
        date_published="2023-01-01",
        title="Test Title",
        abstract="Test Abstract",
        authors_display="Doe J",
        is_commercial_safe=True,
        license_type="CC-BY",
        journal_name="Test Journal",
        pub_year=2023,
        authors=[
            {"name": "Zebra A", "affs": ["Univ Z", "Univ Z", "Univ Y"]},  # Dupe affs
            {"name": "Aardvark B"},  # Missing affs
            {"name": "Zebra A", "affs": ["Univ X"]},  # Dupe name, different aff
            {"name": "", "affs": ["Univ W"]},  # Empty name
            {"affs": ["Univ V"]},  # Missing name key entirely
        ],
    )

    assert manifest.authors[0].get("name") == ""
    assert manifest.authors[0].get("affs") == ["Univ W"]

    assert manifest.authors[1].get("name", "") == ""
    assert manifest.authors[1].get("affs") == ["Univ V"]

    assert manifest.authors[2].get("name") == "Aardvark B"
    assert "affs" not in manifest.authors[2]

    # Zebra A sorting depends on stability, but we sort by name. Both are Zebra A.
    zebra_authors = [a for a in manifest.authors if a.get("name") == "Zebra A"]
    assert len(zebra_authors) == 2

    # Check that duplicated affs are deduplicated and sorted
    affs_set = {tuple(a.get("affs", [])) for a in zebra_authors}
    assert ("Univ Y", "Univ Z") in affs_set
    assert ("Univ X",) in affs_set


def test_cognitive_identity_topology_empty_and_duplicate() -> None:
    """Test CognitiveIdentityTopology handles empty string, empty series, and duplicates."""

    # Empty string should generate a valid but specific UUID
    empty_key = CognitiveIdentityTopology.generate_surrogate_key("")
    assert len(empty_key) == 36
    assert isinstance(empty_key, str)

    # Empty Polars Series
    empty_series = pl.Series("pmcid", [], dtype=pl.String)
    keys_empty = CognitiveIdentityTopology.generate_surrogate_keys_vectorized(empty_series)
    assert len(keys_empty) == 0
    assert keys_empty.dtype == pl.String

    # Duplicates in Polars Series
    duplicate_series = pl.Series("pmcid", ["A", "B", "A", "C", "B"])
    keys_dupe = CognitiveIdentityTopology.generate_surrogate_keys_vectorized(duplicate_series)

    assert len(keys_dupe) == 5
    assert keys_dupe[0] == keys_dupe[2]
    assert keys_dupe[1] == keys_dupe[4]
    assert keys_dupe[0] != keys_dupe[1]
    assert keys_dupe[3] != keys_dupe[0]

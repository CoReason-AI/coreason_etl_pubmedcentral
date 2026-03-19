import datetime

import polars as pl

from coreason_etl_pubmedcentral.models.contracts import CognitiveArticleTypeContract
from coreason_etl_pubmedcentral.models.manifests import CognitiveGoldManifest, EpistemicBronzeManifest
from coreason_etl_pubmedcentral.models.topology import CognitiveIdentityTopology


def test_kitchen_sink_cognitive_gold_manifest() -> None:
    """Kitchen Sink: Testing maximum complexity simultaneously."""
    manifest = CognitiveGoldManifest(
        pmcid="PMC999999",
        coreason_id="dummy-uuid",
        pmid="12345678",
        doi="10.1000/xyz123",
        article_type=CognitiveArticleTypeContract.RESEARCH,
        date_published="2024-02-29",  # Leap year
        date_received="2023-11-15T12:00:00Z",
        date_accepted="2024-01-05",
        title="A Comprehensive Study of \u03b2-amyloid and café",  # Unicode
        abstract="<p>Abstract with <b>HTML</b></p>",
        authors_display="René Descartes; Søren Kierkegaard",
        is_commercial_safe=True,
        is_retracted=True,
        license_type="CC BY-NC-ND 4.0",
        journal_name="The Journal of Obscure \u2014 Sciences",
        pub_year=2024,
        grant_ids=["NIH-123", "NSF-456", "NIH-123", ""],  # Duplicates and empty
        agency_names=["National Institutes of Health", "National Science Foundation", "National Institutes of Health"],
        keywords=["\u03b2-amyloid", "alzheimer's", "café", "neuroscience", "\u03b2-amyloid"],
        affiliations_text=["Univ Paris", "Univ Copenhagen", "Univ Paris"],
        authors=[
            {"name": "René Descartes", "affs": ["Univ Paris", "Univ Leiden"]},
            {"name": "Søren Kierkegaard", "affs": ["Univ Copenhagen", "Univ Berlin"]},
            {"name": "Unknown Entity", "affs": []},  # Empty affs
            {"name": "Duplicate Author", "affs": ["Univ X"]},
            {"name": "Duplicate Author", "affs": ["Univ Y"]},  # Same name, different aff
        ],
    )

    # Asserts
    assert manifest.title == "A Comprehensive Study of \u03b2-amyloid and café"
    assert manifest.is_retracted is True

    # Sorting and Deduplication
    assert manifest.grant_ids == ["", "NIH-123", "NSF-456"]
    assert manifest.agency_names == ["National Institutes of Health", "National Science Foundation"]
    assert manifest.keywords == ["alzheimer's", "café", "neuroscience", "\u03b2-amyloid"]
    assert manifest.affiliations_text == ["Univ Copenhagen", "Univ Paris"]

    # Author Sorting (by name)
    assert manifest.authors[0]["name"] == "Duplicate Author"
    assert manifest.authors[1]["name"] == "Duplicate Author"
    assert manifest.authors[2]["name"] == "René Descartes"
    assert manifest.authors[3]["name"] == "Søren Kierkegaard"
    assert manifest.authors[4]["name"] == "Unknown Entity"

    # Affiliations sorting within authors
    rene = next(a for a in manifest.authors if a["name"] == "René Descartes")
    assert rene["affs"] == ["Univ Leiden", "Univ Paris"]

    # Serialization
    dumped = manifest.model_dump()
    assert '"\\u03b2-amyloid"' in dumped["keywords"]
    assert '"caf\\u00e9"' in dumped["keywords"]  # codespell:ignore caf


def test_epistemic_bronze_manifest_deep_nesting() -> None:
    """Test EpistemicBronzeManifest handles complex and deeply nested metadata dictionaries."""
    deeply_nested_metadata = {
        "source": "ftp",
        "nested": {
            "level2": {
                "level3": [
                    {"id": 1, "value": None},
                    {"id": 2, "value": "\u2603"},  # Snowman unicode
                ]
            }
        },
        "flags": [True, False, None],
        "metrics": {"size": 1024.5, "count": -42},
    }
    manifest = EpistemicBronzeManifest(
        source_file_path="/var/lib/pmc/data/bulk.tar.gz",
        ingestion_ts=datetime.datetime(2025, 12, 31, 23, 59, 59, tzinfo=datetime.UTC),
        ingestion_source="FTP",
        file_metadata=deeply_nested_metadata,
    )

    assert manifest.file_metadata["nested"]["level2"]["level3"][1]["value"] == "\u2603"
    assert manifest.file_metadata["flags"] == [True, False, None]
    assert manifest.file_metadata["metrics"]["size"] == 1024.5


def test_cognitive_identity_topology_unicode_and_extreme() -> None:
    """Test CognitiveIdentityTopology handles Unicode, massive strings, and symbols."""
    pmcid_unicode = "PMC-çüö"
    pmcid_symbols = "PMC@#$%^&*()"
    pmcid_massive = "PMC" * 1000

    key_unicode = CognitiveIdentityTopology.generate_surrogate_key(pmcid_unicode)
    key_symbols = CognitiveIdentityTopology.generate_surrogate_key(pmcid_symbols)
    key_massive = CognitiveIdentityTopology.generate_surrogate_key(pmcid_massive)

    assert len(key_unicode) == 36
    assert len(key_symbols) == 36
    assert len(key_massive) == 36

    assert key_unicode != key_symbols
    assert key_unicode != key_massive
    assert key_symbols != key_massive

    series = pl.Series("pmcid", [pmcid_unicode, pmcid_symbols, pmcid_massive])
    keys_vectorized = CognitiveIdentityTopology.generate_surrogate_keys_vectorized(series)

    assert len(keys_vectorized) == 3
    assert keys_vectorized[0] == key_unicode
    assert keys_vectorized[1] == key_symbols
    assert keys_vectorized[2] == key_massive

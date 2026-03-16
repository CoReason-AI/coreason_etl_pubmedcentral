# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import uuid

import polars as pl
from hypothesis import given
from hypothesis import strategies as st

from coreason_etl_pubmedcentral.utils.surrogate_key import PMC_NAMESPACE, generate_surrogate_keys


def test_generate_surrogate_keys_positive() -> None:
    """Positive test validating valid inputs produce deterministic UUIDv5 strings."""
    pmcids = pl.Series("pmcid", ["12345", "67890"])

    # Expected uuids calculated manually
    expected_uuid_1 = str(uuid.uuid5(PMC_NAMESPACE, "12345"))
    expected_uuid_2 = str(uuid.uuid5(PMC_NAMESPACE, "67890"))

    result = generate_surrogate_keys(pmcids)

    assert len(result) == 2
    assert result[0] == expected_uuid_1
    assert result[1] == expected_uuid_2
    assert result.dtype == pl.String


def test_generate_surrogate_keys_empty() -> None:
    """Boundary test handling empty Series."""
    pmcids = pl.Series("pmcid", [], dtype=pl.String)
    result = generate_surrogate_keys(pmcids)

    assert len(result) == 0
    assert result.dtype == pl.String


def test_generate_surrogate_keys_with_nulls() -> None:
    """Negative/Boundary test handling null values inside the Series."""
    pmcids = pl.Series("pmcid", ["12345", None, "67890"], dtype=pl.String)

    expected_uuid_1 = str(uuid.uuid5(PMC_NAMESPACE, "12345"))
    expected_uuid_3 = str(uuid.uuid5(PMC_NAMESPACE, "67890"))

    result = generate_surrogate_keys(pmcids)

    assert len(result) == 3
    assert result[0] == expected_uuid_1
    assert result[1] is None
    assert result[2] == expected_uuid_3


def test_generate_surrogate_keys_all_nulls() -> None:
    """Boundary test handling Series consisting entirely of null values."""
    pmcids = pl.Series("pmcid", [None, None], dtype=pl.String)
    result = generate_surrogate_keys(pmcids)

    assert len(result) == 2
    assert result[0] is None
    assert result[1] is None


def test_generate_surrogate_keys_identical() -> None:
    """Boundary test checking identical inputs produce the same UUID."""
    pmcids = pl.Series("pmcid", ["12345", "12345"])
    result = generate_surrogate_keys(pmcids)

    expected_uuid = str(uuid.uuid5(PMC_NAMESPACE, "12345"))

    assert len(result) == 2
    assert result[0] == expected_uuid
    assert result[1] == expected_uuid


@given(st.lists(st.text(min_size=1), max_size=100))
def test_generate_surrogate_keys_hypothesis(pmcid_list: list[str]) -> None:
    """Property-based test verifying different list sizes and strings."""
    pmcids = pl.Series("pmcid", pmcid_list, dtype=pl.String)
    result = generate_surrogate_keys(pmcids)

    assert len(result) == len(pmcid_list)
    assert result.dtype == pl.String
    for i, original in enumerate(pmcid_list):
        assert result[i] == str(uuid.uuid5(PMC_NAMESPACE, original))

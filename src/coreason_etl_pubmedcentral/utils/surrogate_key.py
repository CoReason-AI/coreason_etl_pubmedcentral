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

# Fixed namespace for CoReason PMC articles
PMC_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


def generate_surrogate_keys(pmcids: pl.Series) -> pl.Series:
    """
    Generate deterministic surrogate keys (coreason_id) using UUIDv5 by hashing the canonical pmcid
    via vectorized Polars operations.

    Args:
        pmcids (pl.Series): A Polars Series containing canonical PMCIDs.

    Returns:
        pl.Series: A Polars Series containing the generated deterministic UUIDv5 strings.
    """
    # map_batches is an Expr method, so we must use select() or to_frame().select()
    # to apply it to a Series, then extract the Series back out.
    return (
        pmcids.to_frame()
        .select(
            pl.col(pmcids.name).map_batches(
                lambda s: pl.Series(
                    [str(uuid.uuid5(PMC_NAMESPACE, str(x))) if x is not None else None for x in s],
                    dtype=pl.String,
                ),
                return_dtype=pl.String,
            )
        )
        .to_series()
    )

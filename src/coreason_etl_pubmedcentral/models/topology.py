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
from typing import ClassVar

import polars as pl


class CognitiveIdentityTopology:
    """
    Topology class for generating deterministic surrogate keys using UUIDv5.
    """

    # A fixed UUID namespace for CoReason PMC articles.
    # Generated via uuid.uuid5(uuid.NAMESPACE_DNS, "coreason.ai/pmc")
    NAMESPACE: ClassVar[uuid.UUID] = uuid.UUID("3685e1b2-1111-5369-ba0b-3316dbbc4e6e")

    @classmethod
    def generate_surrogate_key(cls, pmcid: str) -> str:
        """
        Generate a deterministic surrogate key for a given PMCID using UUIDv5.
        """
        return str(uuid.uuid5(cls.NAMESPACE, pmcid))

    @classmethod
    def generate_surrogate_keys_vectorized(cls, series: pl.Series) -> pl.Series:
        """
        Vectorized generation of surrogate keys using Polars map_batches.
        Converts a Series of PMCIDs into a Series of UUIDv5 strings.
        """

        def map_uuid(s: pl.Series) -> pl.Series:
            return pl.Series(s.name, [cls.generate_surrogate_key(val) for val in s.to_list()])

        df = series.to_frame()
        return df.select(pl.col(series.name).map_batches(map_uuid)).to_series()

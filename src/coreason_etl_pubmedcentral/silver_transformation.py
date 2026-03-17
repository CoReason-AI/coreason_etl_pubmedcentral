# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from collections.abc import Generator
from typing import Any

import dlt
import polars as pl

from coreason_etl_pubmedcentral.tarball_processor import stream_tarball_xmls
from coreason_etl_pubmedcentral.utils.logger import logger
from coreason_etl_pubmedcentral.utils.surrogate_key import generate_surrogate_keys


def _silver_transformer_generator(bronze_item: dict[str, Any]) -> Generator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: Internal generator logic for the silver layer transformer.
    Reads a bronze record containing `source_file_path`, streams the tarball,
    generates deterministic surrogate keys using Polars, and yields Silver records.
    """
    source_file_path = bronze_item.get("source_file_path")
    if not source_file_path:
        logger.error("Bronze item missing 'source_file_path'. Skipping.")
        return

    try:
        with open(source_file_path, "rb") as f:
            for parsed_article_state in stream_tarball_xmls(f):
                # We need to extract the canonical pmcid and hash it via Polars
                # to strictly satisfy the surrogate key generation requirement.
                pmcid = parsed_article_state.identity.pmcid

                # generate_surrogate_keys expects a polars Series
                if not pmcid:
                    logger.warning(f"Parsed article missing pmcid in {source_file_path}. Skipping.")
                    continue

                pmcid_series = pl.Series("pmcid", [pmcid], dtype=pl.String)
                coreason_id_series = generate_surrogate_keys(pmcid_series)
                coreason_id = coreason_id_series[0]

                if not coreason_id:
                    logger.warning(f"Failed to generate coreason_id for pmcid: {pmcid}")
                    continue

                yield {
                    "coreason_id": coreason_id,
                    "pmcid": pmcid,
                    "article": parsed_article_state.model_dump(),
                    "source_file_path": source_file_path,
                }
    except FileNotFoundError:
        logger.error(f"Silver layer failed to open tarball: {source_file_path} - File not found.")
    except Exception as e:
        logger.error(f"Silver layer unexpected error processing {source_file_path}: {e}")


@dlt.transformer(name="silver_pmc_article", write_disposition="append")
def parse_pmc_xml(bronze_item: dict[str, Any]) -> Generator[dict[str, Any]]:
    """
    AGENT INSTRUCTION: Silver layer dlt transformer.
    Takes a record from the bronze layer, reads the tarball from local filesystem,
    extracts XML, constructs epistemic states, generates deterministic keys,
    and yields them.
    """
    yield from _silver_transformer_generator(bronze_item)  # pragma: no cover

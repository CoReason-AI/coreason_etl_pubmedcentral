# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import argparse

import dlt

from coreason_etl_pubmedcentral.bronze_ingestion import pmc_xml_files
from coreason_etl_pubmedcentral.gold_transformation import build_gold_analytics
from coreason_etl_pubmedcentral.silver_transformation import parse_pmc_xml
from coreason_etl_pubmedcentral.utils.logger import logger


def run_pipeline(manifest_path: str) -> None:
    """
    AGENT INSTRUCTION: Orchestrates the Medallion pipeline.
    Connects Bronze -> Silver -> Gold layers and executes the dlt pipeline.
    Configures max_table_nesting=0 to prevent schema explosion.
    """
    logger.info(f"Running pipeline with manifest: {manifest_path}")

    pipeline = dlt.pipeline(pipeline_name="pmc_pipeline", destination="duckdb", dataset_name="pmc_data")

    # Medallion Architecture Data Flow:
    # 1. pmc_xml_files reads manifest and yields Bronze records.
    # 2. parse_pmc_xml consumes Bronze, extracts XML, yields Silver records.
    # 3. build_gold_analytics consumes Silver records, yields Gold wide table.

    bronze_resource = pmc_xml_files(manifest_path=manifest_path)

    # Enforce max_table_nesting=0 to satisfy schema explosion prevention rule.
    bronze_resource.max_table_nesting = 0

    # In dlt, transformers are applied to resources using the bind/call
    # Actually, we pipe them. The `dlt.transformer` wrapper expects the data source as positional argument.
    # Since dlt 0.4+, piping via `|` is also supported, e.g., `bronze_resource | parse_pmc_xml`
    # without parenthesis for the transformer. Let's use `parse_pmc_xml(bronze_resource)`
    # wait, the exception says "too many positional arguments".
    # In `parse_pmc_xml`, the signature is `def parse_pmc_xml(bronze_item: dict[str, Any])`.
    # When dlt binds it to a resource via `parse_pmc_xml(bronze_resource)`, it works if `data_from` is supported,
    # but `bronze_resource | parse_pmc_xml` is safer. Let's do `parse_pmc_xml` without invoking it.
    silver_transformer = bronze_resource | parse_pmc_xml
    silver_transformer.max_table_nesting = 0

    gold_transformer = silver_transformer | build_gold_analytics
    gold_transformer.max_table_nesting = 0

    info = pipeline.run(
        [bronze_resource, silver_transformer, gold_transformer],
        dataset_name="pmc_data",
        schema_contract="evolve",
    )

    logger.info(f"Pipeline executed successfully:\n{info}")


def cli() -> None:
    """
    AGENT INSTRUCTION: Entry point for the CLI.
    """
    parser = argparse.ArgumentParser(description="ETL process for extracting medical literature from PubMed Central")
    parser.add_argument("manifest_path", type=str, help="Path to the manifest file")
    args = parser.parse_args()

    run_pipeline(args.manifest_path)


if __name__ == "__main__":
    cli()  # pragma: no cover

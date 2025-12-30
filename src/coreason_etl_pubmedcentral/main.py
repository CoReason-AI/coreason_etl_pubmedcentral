# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import dlt
from dlt.pipeline.pipeline import Pipeline
from dlt.common.pipeline import LoadInfo

from coreason_etl_pubmedcentral.pipeline_gold import pmc_gold
from coreason_etl_pubmedcentral.pipeline_silver import pmc_silver
from coreason_etl_pubmedcentral.pipeline_source import pmc_source
from coreason_etl_pubmedcentral.utils.logger import logger


def run_pipeline(
    manifest_path: str,
    destination: str = "duckdb",
    dataset_name: str = "pmc_data",
) -> LoadInfo:
    """
    Orchestrates the PMC ETL pipeline: Bronze -> Silver -> Gold.

    Args:
        manifest_path: Path to the CSV manifest file.
        destination: DLT destination (default: "duckdb").
        dataset_name: Target dataset name (default: "pmc_data").

    Returns:
        LoadInfo object containing execution metrics.
    """
    logger.info(f"Initializing pipeline with manifest: {manifest_path}")

    # 1. Initialize Pipeline
    pipeline: Pipeline = dlt.pipeline(
        pipeline_name="coreason_pmc_etl",
        destination=destination,
        dataset_name=dataset_name,
    )

    # 2. Bronze Layer (Source)
    # The source function returns a DltSource object containing the 'pmc_xml_files' resource
    bronze_source = pmc_source(manifest_file_path=manifest_path)

    # Extract the resource for wiring.
    # Note: We must refer to the resource by name as defined in pipeline_source.py
    bronze_resource = bronze_source.resources["pmc_xml_files"]

    # 3. Silver Layer (Transformation)
    # Pipe Bronze -> Silver
    # Note: pmc_silver is a transformer, so we pipe the resource into it.
    silver_resource = bronze_resource | pmc_silver

    # 4. Gold Layer (Transformation)
    # Pipe Silver -> Gold
    gold_resource = silver_resource | pmc_gold

    # 5. Execution
    # We pass the list of resources to persist all layers:
    # - bronze_source: contains the bronze resource
    # - silver_resource: the transformed stream
    # - gold_resource: the final analytical stream
    #
    # Note: When passing 'bronze_source' and 'silver_resource' (which depends on bronze),
    # dlt handles the forking of the stream so data flows to both raw storage and silver transformation.
    logger.info("Running pipeline...")
    info = pipeline.run([bronze_source, silver_resource, gold_resource])

    logger.info(f"Pipeline finished. Load Info: {info}")
    return info

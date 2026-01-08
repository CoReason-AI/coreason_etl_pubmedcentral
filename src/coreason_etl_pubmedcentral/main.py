# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import argparse
import sys
from typing import Optional

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.pipeline.pipeline import Pipeline

from coreason_etl_pubmedcentral.pipeline_gold import pmc_gold
from coreason_etl_pubmedcentral.pipeline_silver import pmc_silver
from coreason_etl_pubmedcentral.pipeline_source import pmc_source
from coreason_etl_pubmedcentral.utils.logger import logger


def run_pipeline(
    manifest_path: str,
    destination: str = "duckdb",
    dataset_name: str = "pmc_data",
    remote_manifest_path: Optional[str] = None,
) -> LoadInfo:
    """
    Orchestrates the PMC ETL pipeline: Bronze -> Silver -> Gold.

    Args:
        manifest_path: Path to the local CSV manifest file.
        destination: DLT destination (default: "duckdb").
        dataset_name: Target dataset name (default: "pmc_data").
        remote_manifest_path: Optional S3/FTP path to download manifest from.

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
    bronze_source = pmc_source(manifest_file_path=manifest_path, remote_manifest_path=remote_manifest_path)

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


def cli() -> None:
    """
    CLI entry point for the PMC ETL Pipeline.
    Invoked by the `pmc-etl` command installed via Poetry.
    """
    parser = argparse.ArgumentParser(description="Run the PMC ETL Pipeline.")
    parser.add_argument("manifest_path", help="Path to the local CSV manifest file.")
    parser.add_argument("--destination", default="duckdb", help="DLT destination (default: duckdb).")
    parser.add_argument("--dataset-name", default="pmc_data", help="Target dataset name (default: pmc_data).")
    parser.add_argument("--remote-manifest-path", help="Optional S3/FTP path to download manifest from.")

    args = parser.parse_args()

    try:
        run_pipeline(
            manifest_path=args.manifest_path,
            destination=args.destination,
            dataset_name=args.dataset_name,
            remote_manifest_path=args.remote_manifest_path,
        )
    except Exception:
        logger.exception("Pipeline execution failed.")
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    cli()

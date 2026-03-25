# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason-etl-pubmedabstracts

import argparse
import sys

import dlt

from coreason_etl_pubmedabstracts.config import PubMedAbstractsConfig
from coreason_etl_pubmedabstracts.resources.baseline import get_pubmed_baseline
from coreason_etl_pubmedabstracts.resources.updates import get_pubmed_updates


def run_pipeline(run_baseline: bool = True, run_updates: bool = True) -> None:
    """
    Executes the PubMed Abstracts ETL pipeline.

    AGENT INSTRUCTION:
    This function initializes a dlt pipeline directed to PostgreSQL, injecting
    configuration attributes such as pipeline_name, destination_name, and dataset_name
    from PubMedAbstractsConfig. It explicitly enforces the schema drift strategy by dynamically
    building the data payload into a generic Variant/JSONB target table defined via `bronze_table`.
    """
    config = PubMedAbstractsConfig()

    pipeline = dlt.pipeline(
        pipeline_name=config.pipeline_name,
        destination=config.destination_name,
        dataset_name=config.bronze_schema,
    )

    resources_to_run = []

    if run_baseline:
        # We explicitly configure the table name to land everything in bronze_pubmed_raw
        baseline_res = get_pubmed_baseline(config=config).with_name(config.bronze_table)
        resources_to_run.append(baseline_res)

    if run_updates:
        # Same target table for the append resource
        updates_res = get_pubmed_updates(config=config).with_name(config.bronze_table)
        resources_to_run.append(updates_res)

    if not resources_to_run:
        print("No resources selected to run.")
        return

    # Run the extraction and load
    load_info = pipeline.run(resources_to_run)
    print(load_info)


def cli() -> None:
    """
    Command Line Interface entrypoint.
    """
    parser = argparse.ArgumentParser(description="PubMed Abstracts ETL Pipeline")
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Run the baseline (replace) extraction from NLM FTP.",
    )
    parser.add_argument(
        "--updates",
        action="store_true",
        help="Run the updates (append) extraction from NLM FTP.",
    )

    args = parser.parse_args()

    # If neither is specified, we default to running both for completeness,
    # though typically they might be scheduled separately in production.
    run_b = args.baseline
    run_u = args.updates

    if not run_b and not run_u:
        run_b = True
        run_u = True

    try:
        run_pipeline(run_baseline=run_b, run_updates=run_u)
    except Exception as e:
        print(f"Pipeline Failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    cli()  # pragma: no cover

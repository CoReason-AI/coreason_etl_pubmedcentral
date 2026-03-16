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

from coreason_etl_pubmedcentral.utils.logger import logger


def run_pipeline(manifest_path: str) -> None:
    """
    AGENT INSTRUCTION: Stub for the actual pipeline execution.
    """
    logger.info(f"Running pipeline with manifest: {manifest_path}")


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

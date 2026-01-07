# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import os
from pathlib import Path

import dlt
import pytest

from coreason_etl_pubmedcentral.main import run_pipeline


@pytest.mark.integration  # type: ignore[misc]
def test_live_s3_ingestion(tmp_path: Path) -> None:
    """
    Live integration test verifying connectivity to the public S3 bucket
    and end-to-end pipeline execution for a single file.

    This test fetches a known existing small file from the public S3 bucket:
    oa_comm/xml/all/PMC10000000.xml
    """
    # 1. Setup Manifest
    # Create a temporary manifest file pointing to a real file in S3.
    manifest_path = tmp_path / "live_manifest.csv"
    manifest_content = (
        "File Path,Accession ID,Last Updated (UTC),PMID,License,Retracted\n"
        "oa_comm/xml/all/PMC10000000.xml,PMC10000000,2023-08-16 06:51:23,37586523,CC0,no\n"
    )
    manifest_path.write_text(manifest_content, encoding="utf-8")

    # 2. Configure Pipeline Destination
    # We use a unique dataset name to avoid collisions
    dataset_name = f"live_test_{os.urandom(4).hex()}"

    # 3. Run Pipeline
    # We use DuckDB as it's the default and requires no external setup.
    # dlt will create a local duckdb file.
    info = run_pipeline(
        manifest_path=str(manifest_path),
        destination="duckdb",
        dataset_name=dataset_name,
    )

    # 4. Verify Execution Metrics
    assert not info.has_failed_jobs, f"Pipeline failed with errors: {info.load_packages}"
    # Ensure something was loaded
    assert len(info.loads_ids) > 0, "No load IDs returned, nothing loaded?"

    # 5. Verify Data in DuckDB
    # Connect to the created DuckDB database to inspect results
    pipeline = dlt.pipeline(pipeline_name="coreason_pmc_etl", destination="duckdb", dataset_name=dataset_name)
    with pipeline.sql_client() as client:
        # Check Bronze Table
        # Note: dlt normalizes table names. 'pmc_xml_files' -> 'pmc_xml_files'
        res_bronze = client.execute_sql("SELECT count(*) FROM pmc_xml_files")
        count_bronze = res_bronze[0][0]
        assert count_bronze == 1, f"Expected 1 record in Bronze, found {count_bronze}"

        # Check Gold Table
        # 'gold_pmc_analytics_rich'
        res_gold = client.execute_sql("SELECT count(*) FROM gold_pmc_analytics_rich")
        count_gold = res_gold[0][0]
        assert count_gold == 1, f"Expected 1 record in Gold, found {count_gold}"

        # Verify Content (Spot check)
        res_title = client.execute_sql("SELECT title FROM gold_pmc_analytics_rich LIMIT 1")
        title = res_title[0][0]
        # PMC10000000 title should be present and non-empty.
        # We don't hardcode the exact title to avoid brittleness if metadata updates,
        # but we check it's a string.
        assert title and isinstance(title, str)
        print(f"Successfully ingested PMC10000000. Title: {title}")

# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import sys
from unittest import mock

import pytest

from coreason_etl_pubmedcentral.main import cli, run_pipeline


@mock.patch("coreason_etl_pubmedcentral.main.dlt.pipeline")
@mock.patch("coreason_etl_pubmedcentral.main.pmc_xml_files")
@mock.patch("coreason_etl_pubmedcentral.main.parse_pmc_xml")
@mock.patch("coreason_etl_pubmedcentral.main.build_gold_analytics")
def test_run_pipeline(
    mock_build_gold: mock.MagicMock,
    mock_parse_silver: mock.MagicMock,
    mock_pmc_bronze: mock.MagicMock,
    mock_dlt_pipeline: mock.MagicMock,
) -> None:
    """Positive test verifying run_pipeline constructs and runs the DLT pipeline."""
    mock_pipeline_instance = mock.MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline_instance

    mock_bronze_res = mock.MagicMock()
    mock_pmc_bronze.return_value = mock_bronze_res

    mock_silver_res = mock.MagicMock()
    # The pipeline code uses `|` operator on the mock
    mock_bronze_res.__or__.return_value = mock_silver_res

    mock_gold_res = mock.MagicMock()
    mock_silver_res.__or__.return_value = mock_gold_res

    # Provide a return value for the pipeline execution
    mock_pipeline_instance.run.return_value = "Mock Pipeline Output"

    run_pipeline("test_manifest.csv")

    mock_dlt_pipeline.assert_called_once_with(
        pipeline_name="pmc_pipeline", destination="postgres", dataset_name="pmc_data"
    )

    mock_pmc_bronze.assert_called_once_with(manifest_path="test_manifest.csv")

    # We used `|` so we check __or__ calls
    mock_bronze_res.__or__.assert_called_once_with(mock_parse_silver)
    mock_silver_res.__or__.assert_called_once_with(mock_build_gold)

    mock_pipeline_instance.run.assert_called_once_with(
        [mock_bronze_res, mock_silver_res, mock_gold_res],
        dataset_name="pmc_data",
        schema_contract="evolve",
    )


@mock.patch("coreason_etl_pubmedcentral.main.run_pipeline")
def test_cli(mock_run_pipeline: mock.MagicMock) -> None:
    """Positive test verifying CLI correctly parses arguments and calls run_pipeline."""
    test_args = ["pmc-etl", "my_manifest.csv"]
    with mock.patch.object(sys, "argv", test_args):
        cli()

    mock_run_pipeline.assert_called_once_with("my_manifest.csv")


def test_cli_missing_args() -> None:
    """Negative test verifying CLI raises SystemExit when missing required arguments."""
    test_args = ["pmc-etl"]
    with mock.patch.object(sys, "argv", test_args), pytest.raises(SystemExit):
        cli()

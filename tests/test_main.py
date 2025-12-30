# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from typing import Any, Generator
from unittest.mock import MagicMock, patch

import pytest
from dlt.common.pipeline import LoadInfo

from coreason_etl_pubmedcentral.main import run_pipeline


@pytest.fixture  # type: ignore[misc]
def mock_dependencies() -> Generator[dict[str, Any], None, None]:
    """Fixture to mock all external dependencies of run_pipeline."""
    with (
        patch("coreason_etl_pubmedcentral.main.dlt.pipeline") as mock_pipeline,
        patch("coreason_etl_pubmedcentral.main.pmc_source") as mock_source,
        patch("coreason_etl_pubmedcentral.main.pmc_silver") as mock_silver,
        patch("coreason_etl_pubmedcentral.main.pmc_gold") as mock_gold,
    ):
        # Setup common mock structure
        mock_pipeline_instance = MagicMock()
        mock_pipeline.return_value = mock_pipeline_instance
        mock_load_info = MagicMock(spec=LoadInfo)
        mock_pipeline_instance.run.return_value = mock_load_info

        # Setup Resources
        mock_bronze_source = MagicMock()
        mock_bronze_resource = MagicMock()
        # Default behavior: source has the expected resource
        mock_bronze_source.resources = {"pmc_xml_files": mock_bronze_resource}
        mock_source.return_value = mock_bronze_source

        # Wiring
        mock_silver_resource = MagicMock()
        mock_bronze_resource.__or__.return_value = mock_silver_resource

        mock_gold_resource = MagicMock()
        mock_silver_resource.__or__.return_value = mock_gold_resource

        yield {
            "pipeline": mock_pipeline,
            "pipeline_instance": mock_pipeline_instance,
            "source": mock_source,
            "silver": mock_silver,
            "gold": mock_gold,
            "bronze_resource": mock_bronze_resource,
            "silver_resource": mock_silver_resource,
            "gold_resource": mock_gold_resource,
            "load_info": mock_load_info,
            "bronze_source_instance": mock_bronze_source,
        }


def test_run_pipeline_success(mock_dependencies: dict[str, Any]) -> None:
    deps = mock_dependencies
    manifest_path = "test_manifest.csv"

    # Execute
    info = run_pipeline(manifest_path)

    # Verify return
    assert info == deps["load_info"]

    # Verify defaults
    deps["pipeline"].assert_called_once_with(
        pipeline_name="coreason_pmc_etl",
        destination="duckdb",
        dataset_name="pmc_data",
    )

    # Verify Wiring
    deps["bronze_resource"].__or__.assert_called_once_with(deps["silver"])
    deps["silver_resource"].__or__.assert_called_once_with(deps["gold"])

    # Verify Run
    deps["pipeline_instance"].run.assert_called_once()
    args, _ = deps["pipeline_instance"].run.call_args
    assert args[0] == [deps["bronze_source_instance"], deps["silver_resource"], deps["gold_resource"]]


def test_run_pipeline_custom_config(mock_dependencies: dict[str, Any]) -> None:
    deps = mock_dependencies
    manifest_path = "custom.csv"
    destination = "postgres"
    dataset = "my_pmc"

    run_pipeline(manifest_path, destination=destination, dataset_name=dataset)

    deps["pipeline"].assert_called_once_with(
        pipeline_name="coreason_pmc_etl",
        destination=destination,
        dataset_name=dataset,
    )
    deps["source"].assert_called_once_with(manifest_file_path=manifest_path)


def test_run_pipeline_execution_error(mock_dependencies: dict[str, Any]) -> None:
    deps = mock_dependencies
    # Simulate run failure
    error = RuntimeError("Pipeline crashed")
    deps["pipeline_instance"].run.side_effect = error

    with pytest.raises(RuntimeError, match="Pipeline crashed"):
        run_pipeline("path.csv")


def test_run_pipeline_missing_resource_key(mock_dependencies: dict[str, Any]) -> None:
    deps = mock_dependencies
    # Simulate source returning resources dict WITHOUT the expected key
    deps["bronze_source_instance"].resources = {"wrong_key": MagicMock()}

    with pytest.raises(KeyError, match="'pmc_xml_files'"):
        run_pipeline("path.csv")


def test_run_pipeline_wiring_error(mock_dependencies: dict[str, Any]) -> None:
    deps = mock_dependencies
    # Simulate failure during piping (e.g. incompatible types)
    deps["bronze_resource"].__or__.side_effect = TypeError("Cannot pipe")

    with pytest.raises(TypeError, match="Cannot pipe"):
        run_pipeline("path.csv")

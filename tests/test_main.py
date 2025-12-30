# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from unittest.mock import MagicMock, patch

import dlt
from dlt.common.pipeline import LoadInfo

from coreason_etl_pubmedcentral.main import run_pipeline


@patch("coreason_etl_pubmedcentral.main.dlt.pipeline")
@patch("coreason_etl_pubmedcentral.main.pmc_source")
@patch("coreason_etl_pubmedcentral.main.pmc_silver")
@patch("coreason_etl_pubmedcentral.main.pmc_gold")
def test_run_pipeline(
    mock_gold: MagicMock,
    mock_silver: MagicMock,
    mock_source: MagicMock,
    mock_pipeline: MagicMock,
) -> None:
    # Setup Mocks
    mock_pipeline_instance = MagicMock()
    mock_pipeline.return_value = mock_pipeline_instance
    mock_load_info = MagicMock(spec=LoadInfo)
    mock_pipeline_instance.run.return_value = mock_load_info

    # Setup Source/Resources
    mock_bronze_source = MagicMock()
    # Mock resources dict access
    mock_bronze_resource = MagicMock()
    mock_bronze_source.resources = {"pmc_xml_files": mock_bronze_resource}
    mock_source.return_value = mock_bronze_source

    # Wiring mocks
    # silver = bronze | pmc_silver
    mock_silver_resource = MagicMock()
    # When bronze_resource | pmc_silver is called.
    # Actually syntax is `resource | transformer`.
    # So `mock_bronze_resource.__or__` is called with `pmc_silver` (which is the transformer func).
    # But `pmc_silver` here is the *mocked function*.
    # Wait, `pmc_silver` in code is the `@dlt.transformer` decorated object.
    # When imported, it acts as a callable but also can be used in pipe.
    # The pipe operator `|` on a dlt resource expects a callable or another resource.

    # Let's mock the `__or__` method of the resources to simulate piping.
    mock_bronze_resource.__or__.return_value = mock_silver_resource

    mock_gold_resource = MagicMock()
    mock_silver_resource.__or__.return_value = mock_gold_resource

    # Execute
    manifest_path = "test_manifest.csv"
    info = run_pipeline(manifest_path, destination="duckdb", dataset_name="test_ds")

    # Verify
    assert info == mock_load_info

    # Verify Pipeline Init
    mock_pipeline.assert_called_once_with(
        pipeline_name="coreason_pmc_etl",
        destination="duckdb",
        dataset_name="test_ds",
    )

    # Verify Source Init
    mock_source.assert_called_once_with(manifest_file_path=manifest_path)

    # Verify Wiring
    # 1. bronze | silver
    mock_bronze_resource.__or__.assert_called_once_with(mock_silver)
    # 2. silver | gold
    mock_silver_resource.__or__.assert_called_once_with(mock_gold)

    # Verify Run
    # pipeline.run([bronze_source, silver_resource, gold_resource])
    mock_pipeline_instance.run.assert_called_once()
    args, _ = mock_pipeline_instance.run.call_args
    resources_list = args[0]
    assert len(resources_list) == 3
    assert resources_list[0] == mock_bronze_source
    assert resources_list[1] == mock_silver_resource
    assert resources_list[2] == mock_gold_resource

# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason-etl-pubmedabstracts

from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_pubmedabstracts.main import cli, run_pipeline


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
def test_run_pipeline_all(mock_pipeline: MagicMock) -> None:
    """Test that run_pipeline initializes the dlt pipeline and runs both resources."""
    mock_run = MagicMock(return_value="LoadInfo Mock")
    mock_pipeline_instance = MagicMock()
    mock_pipeline_instance.run = mock_run
    mock_pipeline.return_value = mock_pipeline_instance

    with patch("coreason_etl_pubmedabstracts.main.get_pubmed_baseline") as mock_base:
        mock_base_res = MagicMock()
        mock_base.return_value.with_name.return_value = mock_base_res

        with patch("coreason_etl_pubmedabstracts.main.get_pubmed_updates") as mock_up:
            mock_up_res = MagicMock()
            mock_up.return_value.with_name.return_value = mock_up_res

            run_pipeline(run_baseline=True, run_updates=True)

            mock_pipeline.assert_called_once()
            _args, kwargs = mock_pipeline.call_args
            assert kwargs["pipeline_name"] == "coreason_etl_pubmedabstracts"
            assert kwargs["destination"] == "postgres"

            mock_base.assert_called_once()
            mock_up.assert_called_once()
            mock_run.assert_called_once_with([mock_base_res, mock_up_res])


@patch("coreason_etl_pubmedabstracts.main.dlt.pipeline")
def test_run_pipeline_none(mock_pipeline: MagicMock) -> None:
    """Test that run_pipeline gracefully exits if neither is selected."""
    mock_run = MagicMock()
    mock_pipeline_instance = MagicMock()
    mock_pipeline_instance.run = mock_run
    mock_pipeline.return_value = mock_pipeline_instance

    run_pipeline(run_baseline=False, run_updates=False)

    # The pipeline run shouldn't happen
    mock_run.assert_not_called()


@patch("coreason_etl_pubmedabstracts.main.run_pipeline")
def test_cli_default(mock_run_pipeline: MagicMock) -> None:
    """Test CLI runs both when no args are provided."""
    with patch("sys.argv", ["pmc-abstracts-etl"]):
        cli()
        mock_run_pipeline.assert_called_once_with(run_baseline=True, run_updates=True)


@patch("coreason_etl_pubmedabstracts.main.run_pipeline")
def test_cli_baseline_only(mock_run_pipeline: MagicMock) -> None:
    """Test CLI respects baseline flag."""
    with patch("sys.argv", ["pmc-abstracts-etl", "--baseline"]):
        cli()
        mock_run_pipeline.assert_called_once_with(run_baseline=True, run_updates=False)


@patch("coreason_etl_pubmedabstracts.main.run_pipeline")
def test_cli_updates_only(mock_run_pipeline: MagicMock) -> None:
    """Test CLI respects updates flag."""
    with patch("sys.argv", ["pmc-abstracts-etl", "--updates"]):
        cli()
        mock_run_pipeline.assert_called_once_with(run_baseline=False, run_updates=True)


@patch("coreason_etl_pubmedabstracts.main.run_pipeline")
def test_cli_handles_exceptions(mock_run_pipeline: MagicMock) -> None:
    """Test CLI gracefully handles crashes by calling sys.exit(1)."""
    mock_run_pipeline.side_effect = Exception("System Crash")
    with patch("sys.argv", ["pmc-abstracts-etl"]):
        with pytest.raises(SystemExit) as exc:
            cli()
        assert exc.value.code == 1

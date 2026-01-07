# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

from datetime import datetime, timezone
from typing import Any, Generator
from unittest.mock import MagicMock, mock_open, patch

import dlt
import pytest
from dlt.extract.exceptions import ResourceExtractionError

from coreason_etl_pubmedcentral.manifest import ManifestRecord
from coreason_etl_pubmedcentral.pipeline_source import pmc_source, pmc_xml_files
from coreason_etl_pubmedcentral.source_manager import SourceManager, SourceType


@pytest.fixture  # type: ignore[misc]
def mock_source_manager() -> Generator[MagicMock, None, None]:
    sm = MagicMock(spec=SourceManager)
    # Mock the _current_source property/attribute
    sm._current_source = SourceType.S3
    yield sm


def test_pmc_source_entry_point(mock_source_manager: MagicMock) -> None:
    # Test the source entry point function (pmc_source)
    # It just returns the resource.

    # We mock pmc_xml_files to verify it's called
    with patch("coreason_etl_pubmedcentral.pipeline_source.pmc_xml_files") as mock_resource:
        mock_resource.return_value = "resource_obj"

        # Bypass dlt wrapper
        fn = getattr(pmc_source, "__wrapped__", pmc_source)

        res = fn("path.csv", source_manager=mock_source_manager)

        assert res == "resource_obj"
        mock_resource.assert_called_with("path.csv", None, mock_source_manager)


def test_pmc_xml_files_happy_path(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    # Setup
    manifest_path = "dummy_manifest.csv"
    record = ManifestRecord(
        file_path="oa_comm/xml/PMC1.xml",
        accession_id="PMC1",
        last_updated=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        pmid="123",
        license_type="CC-BY",
        is_retracted=False,
    )

    mock_source_manager.get_file.return_value = b"<article>Content</article>"

    # Mock parse_manifest to return our record
    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]) as mock_parse:
        with patch("builtins.open", side_effect=mock_manifest_open(read_data="header\nline")):
            # Execute
            # Mock incremental object to ensure clean state
            inc = MagicMock()
            inc.start_value = None

            # Bypass dlt wrapper to avoid state leakage
            fn = getattr(pmc_xml_files, "__wrapped__", pmc_xml_files)
            generator = fn(manifest_path, source_manager=mock_source_manager, last_updated=inc)
            items = list(generator)

            # Verify
            assert len(items) == 1
            item = items[0]

            assert item["source_file_path"] == "oa_comm/xml/PMC1.xml"
            assert item["ingestion_source"] == "S3"
            assert "ingestion_date" in item
            assert item["ingestion_date"] == item["ingestion_ts"].date()
            # UPDATED: Expect bytes now, not string
            assert item["raw_xml_payload"] == b"<article>Content</article>"
            assert item["manifest_metadata"]["accession_id"] == "PMC1"
            assert item["manifest_metadata"]["last_updated"] == "2024-01-01T12:00:00+00:00"
            assert item["last_updated"] == record.last_updated

            # Verify parse_manifest called with None cutoff (since incremental had no initial value)
            mock_parse.assert_called_once()
            _, kwargs = mock_parse.call_args
            assert kwargs["last_ingested_cutoff"] is None

            # Verify get_file called for the XML file
            mock_source_manager.get_file.assert_called_with("oa_comm/xml/PMC1.xml")


def test_pmc_xml_files_remote_manifest_download(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """Verify that providing remote_manifest_path triggers download."""
    manifest_path = "local_manifest.csv"
    remote_path = "s3/path/to/manifest.csv"
    manifest_content = b"header\nfile,PMC1,2024-01-01 12:00:00,123,CC0,no"

    mock_source_manager.get_file.return_value = manifest_content

    # We need to mock open so we can verify write AND read
    # We use a mock that handles both read and write

    m_open = mock_open(read_data=manifest_content.decode("utf-8"))

    # We define a local side effect to pass through non-csvs
    import builtins

    orig_open = builtins.open

    def side_effect(file: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if isinstance(file, str) and file.endswith(".csv"):
            return m_open(file, mode, *args, **kwargs)
        return orig_open(file, mode, *args, **kwargs)

    with patch("builtins.open", side_effect=side_effect):
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]) as mock_parse:
            list(
                pmc_xml_files(
                    manifest_path,
                    remote_manifest_path=remote_path,
                    source_manager=mock_source_manager,
                )
            )

    # Verify download called
    mock_source_manager.get_file.assert_any_call(remote_path)

    # Verify file written (wb mode)
    m_open.assert_any_call(manifest_path, "wb")
    handle = m_open()
    handle.write.assert_called_with(manifest_content)

    # Verify file read (r mode) happens after (implied by parse_manifest being called)
    mock_parse.assert_called()


def test_pmc_xml_files_remote_manifest_download_failure(
    mock_source_manager: MagicMock, mock_manifest_open: Any
) -> None:
    """Verify exception propagation if manifest download fails."""
    manifest_path = "local_manifest.csv"
    remote_path = "s3/fail.csv"

    mock_source_manager.get_file.side_effect = Exception("Download Fail")

    with patch("builtins.open", side_effect=mock_manifest_open()):
        with pytest.raises(Exception, match="Download Fail"):
            list(
                pmc_xml_files(
                    manifest_path,
                    remote_manifest_path=remote_path,
                    source_manager=mock_source_manager,
                )
            )


def test_pmc_xml_files_incremental(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    # Setup
    manifest_path = "dummy_manifest.csv"
    start_time_str = "2024-01-01T10:00:00+00:00"

    # Mock incremental object with initial value (string)
    inc = MagicMock()
    inc.start_value = start_time_str

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]) as mock_parse:
        with patch("builtins.open", side_effect=mock_manifest_open()):
            # Bypass dlt wrapper
            fn = getattr(pmc_xml_files, "__wrapped__", pmc_xml_files)
            list(fn(manifest_path, source_manager=mock_source_manager, last_updated=inc))

            # Verify cutoff passed to parse_manifest
            expected_cutoff = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
            mock_parse.assert_called_once()
            _, kwargs = mock_parse.call_args
            assert kwargs["last_ingested_cutoff"] == expected_cutoff


def test_pmc_xml_files_incremental_datetime_object(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    # Setup: simulate dlt passing a datetime object directly.
    # We bypass the dlt decorator to test internal logic directly.

    manifest_path = "dummy_manifest.csv"
    start_time_dt = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)

    # Simple object with start_value
    mock_inc = MagicMock()
    mock_inc.start_value = start_time_dt

    # Bypass dlt wrapper
    fn = getattr(pmc_xml_files, "__wrapped__", pmc_xml_files)

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]) as mock_parse:
        with patch("builtins.open", side_effect=mock_manifest_open()):
            list(fn(manifest_path, source_manager=mock_source_manager, last_updated=mock_inc))

            # Verify cutoff passed to parse_manifest
            mock_parse.assert_called_once()
            _, kwargs = mock_parse.call_args
            assert kwargs["last_ingested_cutoff"] == start_time_dt


def test_pmc_xml_files_incremental_naive_datetime(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    # Setup: simulate dlt passing a NAIVE datetime object.
    # This should trigger line 66 (replace tzinfo).

    manifest_path = "dummy_manifest.csv"
    start_time_naive = datetime(2024, 1, 1, 10, 0, 0)  # No tzinfo

    mock_inc = MagicMock()
    mock_inc.start_value = start_time_naive

    # Bypass dlt wrapper
    fn = getattr(pmc_xml_files, "__wrapped__", pmc_xml_files)

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]) as mock_parse:
        with patch("builtins.open", side_effect=mock_manifest_open()):
            list(fn(manifest_path, source_manager=mock_source_manager, last_updated=mock_inc))

            # Verify cutoff passed to parse_manifest has UTC
            mock_parse.assert_called_once()
            _, kwargs = mock_parse.call_args

            cutoff = kwargs["last_ingested_cutoff"]
            assert cutoff is not None
            assert cutoff.tzinfo == timezone.utc
            assert cutoff.replace(tzinfo=None) == start_time_naive


def test_pmc_xml_files_bad_incremental_value(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    # Setup
    # Pass a bad string as initial value
    inc = MagicMock()
    inc.start_value = "invalid-date"

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]) as mock_parse:
        with patch("builtins.open", side_effect=mock_manifest_open()):
            # Patch logger to verify warning
            with patch("coreason_etl_pubmedcentral.pipeline_source.logger") as mock_logger:
                # Bypass dlt wrapper
                fn = getattr(pmc_xml_files, "__wrapped__", pmc_xml_files)
                list(fn("path.csv", source_manager=mock_source_manager, last_updated=inc))

                # Verify fallback to None
                mock_parse.assert_called_once()
                _, kwargs = mock_parse.call_args
                assert kwargs["last_ingested_cutoff"] is None

                # Verify warning log
                mock_logger.warning.assert_called()
                args, _ = mock_logger.warning.call_args
                assert "Could not parse incremental start value" in args[0]


def test_pmc_xml_files_file_fetch_error(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    # Setup
    record = ManifestRecord(
        file_path="fail.xml",
        accession_id="PMC1",
        last_updated=datetime.now(timezone.utc),
        pmid=None,
        license_type="CC0",
        is_retracted=False,
    )

    mock_source_manager.get_file.side_effect = Exception("Download failed")

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
        with patch("builtins.open", side_effect=mock_manifest_open()):
            inc = dlt.sources.incremental("last_updated")

            items = list(pmc_xml_files("path.csv", source_manager=mock_source_manager, last_updated=inc))

            # Should skip the failed item
            assert len(items) == 0


def test_manifest_file_not_found(mock_source_manager: MagicMock) -> None:
    # dlt wraps exceptions in ResourceExtractionError
    # We need to simulate FileNotFoundError ONLY for the manifest, not dlt state

    import builtins

    orig_open = builtins.open

    def side_effect(file: Any, *args: Any, **kwargs: Any) -> Any:
        if isinstance(file, str) and file.endswith(".csv"):
            raise FileNotFoundError
        return orig_open(file, *args, **kwargs)

    with patch("builtins.open", side_effect=side_effect):
        with pytest.raises(ResourceExtractionError) as excinfo:
            list(pmc_xml_files("missing.csv", source_manager=mock_source_manager))

        # Verify the underlying cause
        assert isinstance(excinfo.value.__cause__, FileNotFoundError)


def test_source_manager_auto_creation(mock_manifest_open: Any) -> None:
    # Verify that if no source manager is passed, one is created
    with patch("coreason_etl_pubmedcentral.pipeline_source.SourceManager") as MockSM:
        mock_instance = MockSM.return_value
        mock_instance._current_source = SourceType.S3
        mock_instance.get_file.return_value = b""

        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]):
            with patch("builtins.open", side_effect=mock_manifest_open()):
                list(pmc_xml_files("path.csv"))

        MockSM.assert_called_once()


# --- Complex / Edge Cases ---


def test_pmc_xml_files_mixed_batch(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Test processing a mixed batch of files:
    1. Success
    2. Download Failure
    3. Decode Failure (UTF-8) -- UPDATED: Should NOT fail anymore if we just pass bytes!
    4. Success
    """
    records = [
        ManifestRecord("file1.xml", "PMC1", datetime.now(timezone.utc), None, "CC0", False),
        ManifestRecord("file2.xml", "PMC2", datetime.now(timezone.utc), None, "CC0", False),
        ManifestRecord("file3.xml", "PMC3", datetime.now(timezone.utc), None, "CC0", False),
        ManifestRecord("file4.xml", "PMC4", datetime.now(timezone.utc), None, "CC0", False),
    ]

    mock_source_manager.get_file.side_effect = [
        b"<doc>1</doc>",
        Exception("Download Error"),
        b"\xff\xfe",  # Invalid UTF-8 but valid bytes
        b"<doc>4</doc>",
    ]

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=records):
        with patch("builtins.open", side_effect=mock_manifest_open()):
            with patch("coreason_etl_pubmedcentral.pipeline_source.logger") as mock_logger:
                # Mock the bind method to return the mock_logger itself (or another mock)
                mock_context_logger = MagicMock()
                mock_logger.bind.return_value = mock_context_logger

                inc = dlt.sources.incremental("last_updated")
                items = list(pmc_xml_files("path.csv", source_manager=mock_source_manager, last_updated=inc))

                assert len(items) == 3
                assert items[0]["manifest_metadata"]["accession_id"] == "PMC1"
                assert items[1]["manifest_metadata"]["accession_id"] == "PMC3"
                assert items[2]["manifest_metadata"]["accession_id"] == "PMC4"

                assert items[1]["raw_xml_payload"] == b"\xff\xfe"

                # Verify logs
                assert mock_context_logger.exception.call_count >= 1

                error_calls = mock_context_logger.exception.call_args_list
                assert any("file2.xml" in str(call.args) for call in error_calls)


def test_pmc_xml_files_utf8_decode_error(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """Explicitly test handling of invalid UTF-8. Should now succeed in Bronze."""
    record = ManifestRecord("bad_enc.xml", "PMC1", datetime.now(timezone.utc), None, "CC0", False)

    mock_source_manager.get_file.return_value = b"\xff\xfe\x00\x00"

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
        with patch("builtins.open", side_effect=mock_manifest_open()):
            with patch("coreason_etl_pubmedcentral.pipeline_source.logger") as mock_logger:
                mock_context_logger = MagicMock()
                mock_logger.bind.return_value = mock_context_logger

                inc = dlt.sources.incremental("last_updated")
                items = list(pmc_xml_files("path.csv", source_manager=mock_source_manager, last_updated=inc))

                assert len(items) == 1
                assert items[0]["raw_xml_payload"] == b"\xff\xfe\x00\x00"
                mock_context_logger.exception.assert_not_called()


def test_pmc_xml_files_source_change_tracking(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Verify that ingestion_source is captured correctly if it changes during execution.
    """
    records = [
        ManifestRecord("file1.xml", "PMC1", datetime.now(timezone.utc), None, "CC0", False),
        ManifestRecord("file2.xml", "PMC2", datetime.now(timezone.utc), None, "CC0", False),
    ]

    def get_file_side_effect(path: str) -> bytes:
        if path == "file1.xml":
            mock_source_manager._current_source = SourceType.S3
            return b"content1"
        else:
            mock_source_manager._current_source = SourceType.FTP
            return b"content2"

    mock_source_manager.get_file.side_effect = get_file_side_effect

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=records):
        with patch("builtins.open", side_effect=mock_manifest_open()):
            inc = dlt.sources.incremental("last_updated")
            items = list(pmc_xml_files("path.csv", source_manager=mock_source_manager, last_updated=inc))

            assert len(items) == 2
            assert items[0]["ingestion_source"] == "S3"
            assert items[1]["ingestion_source"] == "FTP"


def test_pmc_xml_files_empty_manifest(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """Verify behavior with empty manifest (no records)."""
    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]):
        with patch("builtins.open", side_effect=mock_manifest_open()):
            inc = dlt.sources.incremental("last_updated")
            items = list(pmc_xml_files("path.csv", source_manager=mock_source_manager, last_updated=inc))

            assert len(items) == 0


def test_pmc_xml_files_manifest_write_permission_error(mock_source_manager: MagicMock) -> None:
    """
    Verify behavior when the local file system denies write access for the downloaded manifest.
    """
    manifest_path = "protected.csv"
    remote_path = "s3/remote.csv"
    mock_source_manager.get_file.return_value = b"content"

    # We need a custom side effect here that raises PermissionError for the manifest file
    import builtins

    orig_open = builtins.open

    def side_effect(file: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if isinstance(file, str) and file.endswith(".csv"):
            if "w" in mode:
                raise PermissionError("Access Denied")
            # Return a mock for read if needed, though we don't get there
            return mock_open()(file, mode, *args, **kwargs)
        return orig_open(file, mode, *args, **kwargs)

    with patch("builtins.open", side_effect=side_effect):
        with pytest.raises(ResourceExtractionError) as excinfo:
            list(
                pmc_xml_files(
                    manifest_path,
                    remote_manifest_path=remote_path,
                    source_manager=mock_source_manager,
                )
            )
        assert isinstance(excinfo.value.__cause__, PermissionError)


def test_pmc_xml_files_manifest_corrupted_content(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Verify that the pipeline handles corrupted/garbage manifest content correctly.
    """
    manifest_path = "corrupt.csv"
    remote_path = "s3/corrupt.csv"
    corrupt_content = b"\x80\x81\xff"

    mock_source_manager.get_file.return_value = corrupt_content

    with patch("builtins.open", side_effect=mock_manifest_open()):
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest") as mock_parse:
            mock_parse.side_effect = UnicodeDecodeError("utf-8", b"", 0, 1, "invalid")

            with pytest.raises(ResourceExtractionError) as excinfo:
                list(
                    pmc_xml_files(
                        manifest_path,
                        remote_manifest_path=remote_path,
                        source_manager=mock_source_manager,
                    )
                )
            assert isinstance(excinfo.value.__cause__, UnicodeDecodeError)


def test_pmc_xml_files_remote_manifest_empty(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Verify behavior when the downloaded manifest is empty.
    """
    manifest_path = "empty.csv"
    remote_path = "s3/empty.csv"
    mock_source_manager.get_file.return_value = b""

    with patch("builtins.open", side_effect=mock_manifest_open(read_data="")):
        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[]):
            items = list(
                pmc_xml_files(
                    manifest_path,
                    remote_manifest_path=remote_path,
                    source_manager=mock_source_manager,
                )
            )
            assert len(items) == 0


def test_pmc_xml_files_schema_definition() -> None:
    """Verify that ingestion_date is configured as a partition column in the resource schema."""
    columns = pmc_xml_files.columns

    assert "ingestion_date" in columns
    assert columns["ingestion_date"]["data_type"] == "date"
    assert columns["ingestion_date"]["partition"] is True

    assert columns["source_file_path"]["data_type"] == "text"
    assert columns["ingestion_ts"]["data_type"] == "timestamp"
    assert columns["ingestion_source"]["data_type"] == "text"
    assert columns["raw_xml_payload"]["data_type"] == "binary"
    assert columns["manifest_metadata"]["data_type"] == "json"


def test_bronze_partitioning_compliance(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Strictly verify Bronze Layer Partitioning requirements.
    """
    columns = pmc_xml_files.columns
    assert "ingestion_date" in columns
    assert columns["ingestion_date"]["data_type"] == "date"
    assert columns["ingestion_date"]["partition"] is True

    fixed_ts = datetime(2025, 1, 1, 15, 30, 0, tzinfo=timezone.utc)

    with patch("coreason_etl_pubmedcentral.pipeline_source.datetime") as mock_dt:
        mock_dt.now.return_value = fixed_ts
        mock_dt.fromisoformat.side_effect = datetime.fromisoformat

        manifest_path = "dummy.csv"
        record = ManifestRecord(
            file_path="f.xml",
            accession_id="ID",
            last_updated=fixed_ts,
            pmid=None,
            license_type="Lic",
            is_retracted=False,
        )

        mock_source_manager.get_file.return_value = b"<root/>"

        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
            with patch("builtins.open", side_effect=mock_manifest_open()):
                inc = dlt.sources.incremental("last_updated")
                items = list(pmc_xml_files(manifest_path, source_manager=mock_source_manager, last_updated=inc))

                assert len(items) == 1
                item = items[0]

                assert item["ingestion_ts"] == fixed_ts
                assert item["ingestion_date"] == fixed_ts.date()
                assert str(item["ingestion_date"]) == "2025-01-01"


def test_bronze_partitioning_midnight_boundary(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Verify that if processing runs across a UTC midnight boundary,
    records are assigned to their respective partitions correctly.
    """
    ts_before = datetime(2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    ts_after = datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc)

    records = [
        ManifestRecord("file1.xml", "PMC1", ts_before, None, "CC0", False),
        ManifestRecord("file2.xml", "PMC2", ts_after, None, "CC0", False),
    ]

    mock_source_manager.get_file.return_value = b"<root/>"

    with patch("coreason_etl_pubmedcentral.pipeline_source.datetime") as mock_dt:
        mock_dt.now.side_effect = [ts_before, ts_after]
        mock_dt.fromisoformat.side_effect = datetime.fromisoformat

        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=records):
            with patch("builtins.open", side_effect=mock_manifest_open()):
                inc = dlt.sources.incremental("last_updated")
                items = list(pmc_xml_files("dummy.csv", source_manager=mock_source_manager, last_updated=inc))

                assert len(items) == 2
                assert items[0]["ingestion_ts"] == ts_before
                assert str(items[0]["ingestion_date"]) == "2023-12-31"

                assert items[1]["ingestion_ts"] == ts_after
                assert str(items[1]["ingestion_date"]) == "2024-01-01"


def test_bronze_partitioning_leap_year(mock_source_manager: MagicMock, mock_manifest_open: Any) -> None:
    """
    Verify correct date extraction for Leap Day (Feb 29).
    """
    leap_ts = datetime(2024, 2, 29, 12, 0, 0, tzinfo=timezone.utc)
    record = ManifestRecord("file1.xml", "PMC1", leap_ts, None, "CC0", False)
    mock_source_manager.get_file.return_value = b"<root/>"

    with patch("coreason_etl_pubmedcentral.pipeline_source.datetime") as mock_dt:
        mock_dt.now.return_value = leap_ts
        mock_dt.fromisoformat.side_effect = datetime.fromisoformat

        with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=[record]):
            with patch("builtins.open", side_effect=mock_manifest_open()):
                inc = dlt.sources.incremental("last_updated")
                items = list(pmc_xml_files("dummy.csv", source_manager=mock_source_manager, last_updated=inc))

                assert len(items) == 1
                assert str(items[0]["ingestion_date"]) == "2024-02-29"

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from coreason_etl_pubmedcentral.manifest import ManifestRecord

# Import the internal generator for direct testing
from coreason_etl_pubmedcentral.pipeline_source import _pmc_xml_files_generator


def test_pmc_xml_files_validation_error() -> None:
    """
    Test that pmc_xml_files raises error when source_file_path is not a string.
    Testing the internal generator logic directly.
    """
    mock_source_manager = MagicMock()
    mock_source_manager._current_source.name = "S3"
    mock_source_manager.get_file.return_value = b"<xml></xml>"

    bad_record = ManifestRecord(
        file_path=12345,  # type: ignore
        accession_id="PMC123",
        last_updated=datetime(2024, 1, 1, tzinfo=timezone.utc),
        pmid="1",
        license_type="CC0",
        is_retracted=False,
    )

    mock_last_updated = MagicMock()
    mock_last_updated.start_value = None

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=iter([bad_record])):
        with patch("builtins.open", new_callable=MagicMock):
            generator = _pmc_xml_files_generator(
                manifest_file_path="dummy.csv",
                remote_manifest_path=None,
                source_manager=mock_source_manager,
                last_updated=mock_last_updated,
            )

            with pytest.raises(TypeError, match="Validation Error"):
                list(generator)


def test_pmc_xml_files_validation_source_error() -> None:
    """
    Test validation for ingestion_source.
    """
    mock_source_manager = MagicMock()
    mock_source_manager._current_source.name = 123
    mock_source_manager.get_file.return_value = b"<xml></xml>"

    record = ManifestRecord(
        file_path="path/to/xml",
        accession_id="PMC123",
        last_updated=datetime(2024, 1, 1, tzinfo=timezone.utc),
        pmid="1",
        license_type="CC0",
        is_retracted=False,
    )

    mock_last_updated = MagicMock()
    mock_last_updated.start_value = None

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest", return_value=iter([record])):
        with patch("builtins.open", new_callable=MagicMock):
            generator = _pmc_xml_files_generator(
                manifest_file_path="dummy.csv",
                remote_manifest_path=None,
                source_manager=mock_source_manager,
                last_updated=mock_last_updated,
            )

            with pytest.raises(TypeError, match="Validation Error"):
                list(generator)

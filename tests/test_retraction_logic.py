from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from coreason_etl_pubmedcentral.manifest import ManifestRecord
from coreason_etl_pubmedcentral.pipeline_source import pmc_xml_files
from coreason_etl_pubmedcentral.source_manager import SourceManager


def test_retraction_bypass_high_water_mark() -> None:
    """
    Verify that if a record is marked as Retracted in the manifest,
    it is ingested even if its last_updated timestamp is NOT newer than the cutoff.
    This ensures we catch retractions even if the file timestamp wasn't bumped.
    """
    cutoff = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Record has OLD timestamp (equal to cutoff), but is_retracted=True
    # Normal logic would skip (cutoff >= last_updated).
    # We want it to be YIELDED because is_retracted=True.
    record = ManifestRecord(
        file_path="oa_comm/xml/PMC_Retracted.xml",
        accession_id="PMC_Retracted",
        last_updated=cutoff,  # Same as cutoff
        pmid="123",
        license_type="CC0",
        is_retracted=True,
    )

    sm = MagicMock(spec=SourceManager)
    sm.get_file.return_value = b"<xml>Retracted</xml>"
    # Setup the nested property correctly
    type(sm)._current_source = MagicMock(name="_current_source")
    sm._current_source.name = "S3"

    with patch("coreason_etl_pubmedcentral.pipeline_source.parse_manifest") as mock_parse:
        mock_parse.return_value = [record]

        # Mock dlt incremental state
        last_updated_mock = MagicMock()
        last_updated_mock.start_value = cutoff

        # Mock open
        with patch("builtins.open", new_callable=MagicMock):
            gen = pmc_xml_files(manifest_file_path="dummy.csv", source_manager=sm, last_updated=last_updated_mock)
            items = list(gen)

    # If the logic is strictly following "Delta", this will be empty (current behavior).
    # We want it to be 1.
    assert len(items) == 1
    assert items[0]["manifest_metadata"]["is_retracted"] is True

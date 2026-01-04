import builtins
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, mock_open, patch

import dlt

from coreason_etl_pubmedcentral.pipeline_source import pmc_xml_files
from coreason_etl_pubmedcentral.source_manager import SourceManager


def test_retraction_bypass_high_water_mark() -> None:
    """
    Verify that if a record is marked as Retracted in the manifest,
    it is ingested even if its last_updated timestamp is NOT newer than the cutoff.
    This ensures we catch retractions even if the file timestamp wasn't bumped.
    """
    cutoff = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # CSV content representing the record
    # File Path | Accession ID | Last Updated (UTC) | PMID | License | Retracted
    csv_content = (
        "File Path,Accession ID,Last Updated (UTC),PMID,License,Retracted\n"
        "oa_comm/xml/PMC_Retracted.xml,PMC_Retracted,2024-01-01 12:00:00,123,CC0,yes"
    )

    sm = MagicMock(spec=SourceManager)
    sm.get_file.return_value = b"<xml>Retracted</xml>"
    type(sm)._current_source = MagicMock(name="_current_source")
    sm._current_source.name = "S3"

    # Use real dlt incremental to avoid mock comparison errors
    incremental = dlt.sources.incremental("last_updated", initial_value=cutoff)

    # Mock open with side_effect
    real_open = builtins.open

    def conditional_open(file: str | bytes | int, *args: Any, **kwargs: Any) -> Any:
        if file == "dummy.csv":
            return mock_open(read_data=csv_content)(file, *args, **kwargs)
        return real_open(file, *args, **kwargs)

    with patch("builtins.open", side_effect=conditional_open):
        # We do NOT patch parse_manifest, we want to test its logic
        gen = pmc_xml_files(manifest_file_path="dummy.csv", source_manager=sm, last_updated=incremental)
        items = list(gen)

    assert len(items) == 1
    assert items[0]["manifest_metadata"]["is_retracted"] is True

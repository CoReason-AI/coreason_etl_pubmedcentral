# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import io
import tarfile
from unittest.mock import patch

import pytest

from coreason_etl_pubmedcentral.tarball_processor import stream_tarball_xmls


def create_tarball(files: dict[str, bytes]) -> io.BytesIO:
    """Helper to create an in-memory tarball for testing."""
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    stream.seek(0)
    return stream


def test_stream_tarball_xmls_valid() -> None:
    """Positive test handling valid XMLs."""
    valid_xml = b"""
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC12345</article-id>
            </article-meta>
        </front>
    </article>
    """
    stream = create_tarball({"valid.nxml": valid_xml})

    results = list(stream_tarball_xmls(stream))
    assert len(results) == 1
    state = results[0]
    assert state.identity.pmcid == "12345"


def test_stream_tarball_xmls_empty() -> None:
    """Boundary test handling an empty tarball."""
    stream = create_tarball({})
    results = list(stream_tarball_xmls(stream))
    assert len(results) == 0


def test_stream_tarball_xmls_invalid_xml() -> None:
    """Negative test handling invalid XML within a tarball."""
    invalid_xml = b"""
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC12345</article-id>
            <!-- missing closing tags -->
    """
    valid_xml = b"""
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC67890</article-id>
            </article-meta>
        </front>
    </article>
    """
    stream = create_tarball({"invalid.nxml": invalid_xml, "valid.nxml": valid_xml})

    # The generator should skip the invalid one and yield the valid one
    results = list(stream_tarball_xmls(stream))
    assert len(results) == 1
    assert results[0].identity.pmcid == "67890"


def test_stream_tarball_xmls_not_nxml() -> None:
    """Negative test ensuring non-nxml files are ignored."""
    valid_xml = b"""
    <article article-type="research-article">
        <front>
            <article-meta>
                <article-id pub-id-type="pmc">PMC12345</article-id>
            </article-meta>
        </front>
    </article>
    """
    stream = create_tarball({"ignored.txt": b"some text", "valid.nxml": valid_xml})

    results = list(stream_tarball_xmls(stream))
    assert len(results) == 1
    assert results[0].identity.pmcid == "12345"


def test_stream_tarball_xmls_extraction_error() -> None:
    """Negative test handling an unexpected error during XML processing."""
    valid_xml = b"<article></article>"
    stream = create_tarball({"valid.nxml": valid_xml})

    # Mock extract_identity_state to raise an exception
    with patch("coreason_etl_pubmedcentral.tarball_processor.extract_identity_state", side_effect=ValueError("Test")):
        results = list(stream_tarball_xmls(stream))
        assert len(results) == 0


def test_stream_tarball_xmls_read_error() -> None:
    """Negative test handling a corrupted tarball stream."""
    stream = io.BytesIO(b"not a tarball")
    with pytest.raises(tarfile.ReadError):
        list(stream_tarball_xmls(stream))


def test_stream_tarball_xmls_unexpected_error() -> None:
    """Negative test handling an unexpected tarfile open error."""
    stream = io.BytesIO(b"")
    with patch("tarfile.open", side_effect=Exception("Unexpected")), pytest.raises(Exception, match="Unexpected"):
        list(stream_tarball_xmls(stream))


def test_stream_tarball_xmls_file_none() -> None:
    """Test when tar.extractfile returns None."""
    stream = create_tarball({"valid.nxml": b"<article></article>"})

    with patch("tarfile.TarFile.extractfile", return_value=None):
        results = list(stream_tarball_xmls(stream))
        assert len(results) == 0

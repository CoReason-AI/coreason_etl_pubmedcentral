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

from lxml import etree

from coreason_etl_pubmedcentral.models.tarball_streamer import EpistemicTarballStreamingTask


def _create_tarball(files: dict[str, bytes]) -> io.BytesIO:
    """Helper function to create an in-memory tar.gz file."""
    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode="w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(tarinfo=info, fileobj=io.BytesIO(data))
    tar_stream.seek(0)
    return tar_stream


def test_epistemic_tarball_streaming_task_valid_files() -> None:
    """Test streaming and parsing valid .nxml files from a tarball."""
    files = {
        "article1.nxml": b"<?xml version='1.0'?><article><title>Article 1</title></article>",
        "article2.nxml": b"<?xml version='1.0'?><article><title>Article 2</title></article>",
        "some_image.jpg": b"fake image data",  # Should be ignored
        "README.txt": b"Just some text.",  # Should be ignored
    }
    tar_stream = _create_tarball(files)

    trees = list(EpistemicTarballStreamingTask.execute(tar_stream))

    assert len(trees) == 2
    for tree in trees:
        assert isinstance(tree, etree._ElementTree)
        root = tree.getroot()
        assert root.tag == "article"
        title = root.find("title")
        assert title is not None
        assert title.text in ["Article 1", "Article 2"]


def test_epistemic_tarball_streaming_task_malformed_xml() -> None:
    """Test that malformed XML files are gracefully skipped without crashing the stream."""
    files = {
        "good.nxml": b"<?xml version='1.0'?><article><title>Good</title></article>",
        "bad.nxml": b"<?xml version='1.0'?><article><title>Bad",  # Missing closing tags
    }
    tar_stream = _create_tarball(files)

    trees = list(EpistemicTarballStreamingTask.execute(tar_stream))

    # Only the good file should be yielded
    assert len(trees) == 1

    title = trees[0].getroot().find("title")
    assert title is not None
    assert title.text == "Good"


def test_epistemic_tarball_streaming_task_empty_tarball() -> None:
    """Test handling of a completely empty tarball."""
    tar_stream = _create_tarball({})
    trees = list(EpistemicTarballStreamingTask.execute(tar_stream))
    assert len(trees) == 0


def test_epistemic_tarball_streaming_task_no_nxml() -> None:
    """Test handling of a tarball containing files, but no .nxml files."""
    files = {
        "image1.png": b"fake",
        "doc.pdf": b"fake",
    }
    tar_stream = _create_tarball(files)
    trees = list(EpistemicTarballStreamingTask.execute(tar_stream))
    assert len(trees) == 0

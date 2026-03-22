# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import tarfile
from collections.abc import Iterator
from io import BytesIO

from lxml import etree


class EpistemicTarballStreamingTask:
    """
    EpistemicTarballStreamingTask implements memory-safe iteration over bulk PMC Open Access archives.

    AGENT INSTRUCTION:
    This class enforces strict memory discipline. It iterates through the archive sequentially
    and parses each file individually, yielding the lxml payload to allow Python's garbage
    collector to clear memory, avoiding direct `iterparse` on the archive stream.
    """

    @classmethod
    def execute(cls, stream: BytesIO) -> Iterator[etree._ElementTree]:
        """
        Stream a `.tar.gz` payload sequentially and extract `.nxml` AST payloads.

        Args:
            stream: A BytesIO object containing the raw tarball binary payload.

        Yields:
            An lxml ElementTree object for every valid .nxml file within the archive.
        """
        # We start by seeking the stream to 0 to ensure reading begins from the start.
        stream.seek(0)

        with tarfile.open(fileobj=stream, mode="r|gz") as tar:
            for member in tar:
                # We specifically only care about .nxml article files
                if member.name.endswith(".nxml"):
                    f = tar.extractfile(member)
                    if f:
                        # Parsing into a complete tree, then yielding.
                        # This avoids massive XML strings resting in memory.
                        try:
                            tree = etree.parse(f)
                            yield tree
                        except etree.XMLSyntaxError:
                            # If a specific file is malformed, we simply skip it rather than halting the whole tarball.
                            continue

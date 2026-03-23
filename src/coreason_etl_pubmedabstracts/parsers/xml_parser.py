# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason-etl-pubmedabstracts

import hashlib
from collections.abc import Iterator
from typing import Any, BinaryIO

from lxml import etree


class EpistemicMedlineParser:
    """
    Parser for extracting structured data from NLM PubMed/MEDLINE XML baseline/update files.

    AGENT INSTRUCTION:
    This class strictly utilizes an event-driven `iterparse` strategy to ensure constant memory
    pressure (~500MB max) when parsing massive XML files. It extracts `MedlineCitation` and
    `DeleteCitation/PMID` elements. Since `xmltodict` is prohibited unless pre-installed,
    this class converts the lxml elements to Python dictionaries dynamically.
    """

    @classmethod
    def execute(cls, stream: BinaryIO) -> Iterator[dict[str, Any]]:
        """
        Streams a PubMed XML file, yielding a structured dictionary for each
        MedlineCitation or DeleteCitation element encountered.

        Yields:
            dict: {
                "event_type": "MedlineCitation" | "DeleteCitation",
                "pmid": "12345",
                "content_hash": "md5_hash_of_raw_xml",
                "raw_data": dict (parsed XML structure)
            }
        """
        context = etree.iterparse(stream, events=("end",))

        for event, elem in context:
            if event == "end" and elem.tag in ("MedlineCitation", "DeleteCitation"):
                # We serialize to string to generate an MD5 content hash
                raw_xml_bytes = etree.tostring(elem, encoding="utf-8")
                content_hash = hashlib.md5(raw_xml_bytes).hexdigest()  # noqa: S324

                # Fast XML to Dict Conversion
                raw_data = cls._xml_to_dict(elem)

                # Extract canonical PMID
                pmid = None
                if elem.tag == "MedlineCitation":
                    pmid_node = elem.find("PMID")
                    if pmid_node is not None and pmid_node.text:
                        pmid = str(pmid_node.text).strip()
                elif elem.tag == "DeleteCitation":
                    # For DeleteCitation, we might have multiple PMIDs.
                    # We will treat the raw_data as the authoritative source
                    # but surface the first one as the `pmid` metric.
                    pmid_node = elem.find("PMID")
                    if pmid_node is not None and pmid_node.text:
                        pmid = str(pmid_node.text).strip()

                yield {
                    "event_type": elem.tag,
                    "pmid": pmid,
                    "content_hash": content_hash,
                    "raw_data": raw_data,
                }

                # Clear the element from memory to prevent memory leaks
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

        # Free the context reference
        del context

    @classmethod
    def _xml_to_dict(cls, elem: etree._Element) -> dict[str, Any] | str | None:
        """
        Recursively converts an lxml ElementTree to a Python dictionary,
        approximating the behavior of xmltodict.
        """
        # Base case: text only element
        if len(elem) == 0:
            if elem.attrib:
                # If it has attributes, we must return a dict to hold @attr and #text
                res: dict[str, Any] = {f"@{k}": str(v) for k, v in elem.attrib.items()}
                if elem.text and elem.text.strip():
                    res["#text"] = elem.text.strip()
                return res
            if elem.text and elem.text.strip():
                return str(elem.text.strip())
            return None

        # Recursive case
        result: dict[str, Any] = {}
        # Merge attributes
        if elem.attrib:
            for k, v in elem.attrib.items():
                result[f"@{k!s}"] = str(v)

        for child in elem:
            child_result = cls._xml_to_dict(child)
            tag = child.tag
            if tag in result:
                # If we've seen this tag before, convert to a list
                if isinstance(result[tag], list):
                    result[tag].append(child_result)
                else:
                    result[tag] = [result[tag], child_result]
            else:
                result[tag] = child_result

        # Sometimes a parent node has both text and children.
        # xmltodict usually puts the text in '#text' key.
        if elem.text and elem.text.strip():
            result["#text"] = str(elem.text.strip())

        return result

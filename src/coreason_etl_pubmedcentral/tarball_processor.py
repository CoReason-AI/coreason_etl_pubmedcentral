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
from collections.abc import Generator
from typing import IO

from lxml import etree
from pydantic import BaseModel, Field

from coreason_etl_pubmedcentral.utils.logger import logger
from coreason_etl_pubmedcentral.xml_parser import (
    ArticleEntityState,
    ArticleFundingState,
    ArticleIdentityState,
    ArticleTemporalState,
    extract_entity_state,
    extract_funding_state,
    extract_identity_state,
    extract_temporal_state,
)


class ParsedArticleState(BaseModel):
    """
    Epistemic snapshot containing the fully aggregated state of a parsed PubMed Central document.
    """

    identity: ArticleIdentityState = Field(description="Canonical identifiers and classification")
    temporal: ArticleTemporalState = Field(description="Temporal facts")
    entity: ArticleEntityState = Field(description="Resolved contributors and affiliations")
    funding: ArticleFundingState = Field(description="Resolved funding entities")


def stream_tarball_xmls(stream: IO[bytes]) -> Generator[ParsedArticleState]:
    """
    AGENT INSTRUCTION: Iterate sequentially through a .tar.gz archive,
    extracting and parsing .nxml files into fully populated epistemic states.
    Memory Management: Do not use iterparse on the archive stream.
    Parse file-by-file so Python's GC can clear the small tree.
    """
    try:
        with tarfile.open(fileobj=stream, mode="r:gz") as tar:
            for member in tar:
                if member.name.endswith(".nxml"):
                    f = tar.extractfile(member)
                    if f:
                        try:
                            # Parse the file
                            tree = etree.parse(f)
                            root = tree.getroot()

                            # Extract the various states
                            identity_state = extract_identity_state(root)
                            temporal_state = extract_temporal_state(root)
                            entity_state = extract_entity_state(root)
                            funding_state = extract_funding_state(root)

                            # Yield the fully populated state
                            yield ParsedArticleState(
                                identity=identity_state,
                                temporal=temporal_state,
                                entity=entity_state,
                                funding=funding_state,
                            )
                        except etree.XMLSyntaxError as e:
                            logger.error(f"SchemaViolation - Invalid XML in {member.name}: {e}")
                            # Skip invalid files to continue processing
                            continue
                        except Exception as e:
                            logger.error(f"SchemaViolation - Error processing {member.name}: {e}")
                            continue
                        finally:
                            f.close()
    except tarfile.ReadError as e:
        logger.error(f"ReadError - Failed to read tarball stream: {e}")
        raise
    except Exception as e:
        logger.error(f"StreamError - Unexpected error during tarball streaming: {e}")
        raise

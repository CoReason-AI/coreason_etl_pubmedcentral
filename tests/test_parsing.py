# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_pubmedcentral

import unittest
from pathlib import Path

from coreason_etl_pubmedcentral.parsing.parser import (
    ArticleType,
    parse_jats_xml,
)

DATA_DIR = Path(__file__).parent / "data"


class TestJatsParsing(unittest.TestCase):
    def setUp(self) -> None:
        self.legacy_xml = (DATA_DIR / "jats_legacy.xml").read_text(encoding="utf-8")
        self.modern_xml = (DATA_DIR / "jats_modern.xml").read_text(encoding="utf-8")
        self.edge_xml = (DATA_DIR / "jats_edge_cases.xml").read_text(encoding="utf-8")
        self.coverage_xml = (DATA_DIR / "jats_coverage.xml").read_text(encoding="utf-8")
        self.wrapper_xml = (DATA_DIR / "jats_wrapper.xml").read_text(encoding="utf-8")
        self.no_type_xml = (DATA_DIR / "jats_no_type.xml").read_text(encoding="utf-8")
        self.winter_xml = (DATA_DIR / "jats_winter.xml").read_text(encoding="utf-8")

    def test_legacy_parsing(self) -> None:
        result = parse_jats_xml(self.legacy_xml)

        # Identity
        identity = result["identity"]
        self.assertEqual(identity["pmcid"], "12345")  # Strip PMC
        self.assertEqual(identity["pmid"], "98765432")
        self.assertEqual(identity["doi"], "10.1234/test.v1i1.1")
        self.assertEqual(identity["article_type"], ArticleType.RESEARCH)

        # Dates
        dates = result["dates"]
        # Legacy xml only has year 2020. Default month/day 01.
        self.assertEqual(dates["date_published"], "2020-01-01")
        self.assertIsNone(dates["date_received"])

        # Authors
        authors = result["authors"]
        self.assertEqual(len(authors), 1)
        self.assertEqual(authors[0]["surname"], "Doe")
        self.assertEqual(authors[0]["given_names"], "John")
        self.assertEqual(authors[0]["affiliations"], ["Legacy University"])

        # Funding (Legacy: Independent)
        funding = result["funding"]
        # Expected: 2 entries (one for grant, one for sponsor)
        # order depends on xml order logic. We scan sponsors then nums.
        agencies = [f["agency"] for f in funding if f["agency"]]
        grant_ids = [f["grant_id"] for f in funding if f["grant_id"]]

        self.assertIn("Legacy Foundation", agencies)
        self.assertIn("GRANT123", grant_ids)
        self.assertEqual(len(funding), 2)
        # Verify independence (one has agency/no-id, other has id/no-agency)
        for f in funding:
            if f["agency"]:
                self.assertIsNone(f["grant_id"])
            if f["grant_id"]:
                self.assertIsNone(f["agency"])

    def test_modern_parsing(self) -> None:
        result = parse_jats_xml(self.modern_xml)

        # Identity
        identity = result["identity"]
        self.assertEqual(identity["pmcid"], "67890")
        self.assertEqual(identity["article_type"], ArticleType.REVIEW)

        # Dates
        dates = result["dates"]
        self.assertEqual(dates["date_published"], "2024-02-15")
        # Season Fall -> 09. Missing day -> 01.
        self.assertEqual(dates["date_received"], "2023-09-01")
        self.assertEqual(dates["date_accepted"], "2024-01-10")

        # Authors
        authors = result["authors"]
        self.assertEqual(len(authors), 1)
        jane = authors[0]
        self.assertEqual(jane["surname"], "Smith")
        self.assertEqual(jane["affiliations"], ["Modern Institute", "Global Research Center"])

        # Funding (Modern: Cross Product)
        funding = result["funding"]
        # 2 Sources * 2 IDs = 4 Entries
        self.assertEqual(len(funding), 4)

        # Verify cross product
        pairs = {(f["agency"], f["grant_id"]) for f in funding}
        expected = {
            ("Modern Agency", "MA-2024"),
            ("Modern Agency", "CS-999"),
            ("Co-Sponsor Inc.", "MA-2024"),
            ("Co-Sponsor Inc.", "CS-999"),
        }
        self.assertEqual(pairs, expected)

    def test_edge_cases(self) -> None:
        result = parse_jats_xml(self.edge_xml)

        dates = result["dates"]
        # pmc-release, month=invalid -> 01
        self.assertEqual(dates["date_published"], "2022-01-01")

        # received, day=XX -> 01
        self.assertEqual(dates["date_received"], "2022-05-01")

        identity = result["identity"]
        self.assertEqual(identity["article_type"], ArticleType.CASE_REPORT)

    def test_coverage_cases(self) -> None:
        result = parse_jats_xml(self.coverage_xml)

        # Identity: Editorial -> OTHER. PMCID missing text -> None
        identity = result["identity"]
        self.assertEqual(identity["article_type"], ArticleType.OTHER)
        # We expect 99999 because it's valid text but no PMC prefix
        self.assertEqual(identity["pmcid"], "99999")

        # Dates: Seasons
        dates = result["dates"]
        # Spring -> 03
        self.assertEqual(dates["date_published"], "2021-03-01")
        # Received Summer -> 06
        self.assertEqual(dates["date_received"], "2021-06-01")
        # Accepted missing year -> None
        self.assertIsNone(dates["date_accepted"])

        # Funding: Partial
        funding = result["funding"]
        # Lone Agency (Grant None), Lone ID (Agency None)
        self.assertEqual(len(funding), 2)
        agencies = [f["agency"] for f in funding if f["agency"]]
        ids = [f["grant_id"] for f in funding if f["grant_id"]]

        self.assertIn("Lone Agency", agencies)
        self.assertIn("LONE-ID-123", ids)
        for f in funding:
            if f["agency"] == "Lone Agency":
                self.assertIsNone(f["grant_id"])
            if f["grant_id"] == "LONE-ID-123":
                self.assertIsNone(f["agency"])

    def test_winter_case(self) -> None:
        result = parse_jats_xml(self.winter_xml)
        dates = result["dates"]
        # Winter -> 12
        self.assertEqual(dates["date_published"], "2022-12-01")

    def test_wrapper_xml(self) -> None:
        # Tests that logic handles wrapped article (standard bulk or concat)
        # And specifically hits the 'del elem.getparent()[0]' line.
        # Also tests the <month></month> empty text case.
        result = parse_jats_xml(self.wrapper_xml)

        identity = result["identity"]
        self.assertEqual(identity["pmcid"], "999")

        dates = result["dates"]
        # Empty month -> default 01
        self.assertEqual(dates["date_published"], "2023-01-01")

    def test_no_type_xml(self) -> None:
        # Tests logic when article-type attribute is missing
        result = parse_jats_xml(self.no_type_xml)

        identity = result["identity"]
        self.assertEqual(identity["article_type"], ArticleType.OTHER)
        self.assertEqual(identity["pmcid"], "888")

    def test_bytes_input(self) -> None:
        """Verify parser accepts bytes."""
        xml_bytes = self.legacy_xml.encode("utf-8")
        result = parse_jats_xml(xml_bytes)
        self.assertEqual(result["identity"]["pmcid"], "12345")

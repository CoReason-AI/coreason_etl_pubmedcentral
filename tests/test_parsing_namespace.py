from coreason_etl_pubmedcentral.pipeline_silver import transform_silver_record


def test_silver_parsing_namespace() -> None:
    """
    Verify that Silver layer correctly processes XML with namespaces.
    The current implementation uses iterparse(..., tag="article").
    If the XML has a default namespace (e.g. xmlns="http://jats.nlm.nih.gov"),
    the tag will be '{http://jats.nlm.nih.gov}article', causing iterparse to skip it
    if it strictly looks for "article".
    """
    with open("tests/data/jats_namespace.xml", "rb") as f:
        raw_xml = f.read()

    item = {
        "source_file_path": "test/path.xml",
        "raw_xml_payload": raw_xml,
        "manifest_metadata": {},
        "ingestion_ts": "2024-01-01T00:00:00Z",
        "ingestion_source": "S3",
    }

    # Transform
    result = transform_silver_record(item)

    # If parsing fails due to namespace mismatch, result will be None (or empty record if logic differs)
    assert result is not None, "Silver transformation returned None for namespaced XML"
    assert result["pmcid"] == "12345"
    assert result["title"] == "Namespaced Article"

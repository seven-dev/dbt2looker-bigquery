from dbt2looker_bigquery.parsers.type import TypeParser


def test_type_extract():
    type_parser = TypeParser()

    assert type_parser._find_inner_content("STRING") == "STRING"
    assert type_parser._find_inner_content("ARRAY<A STRING>") == "A STRING"
    assert (
        type_parser._find_inner_content("STRUCT<name STRING, age INT64>")
        == "name STRING, age INT64"
    )

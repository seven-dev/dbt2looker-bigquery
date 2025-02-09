from dbt2looker_bigquery.parsers.type import TypeParser


def test_type_extract():
    type_parser = TypeParser()

    assert type_parser._find_inner_content("STRING") == "STRING"
    assert type_parser._find_inner_content("ARRAY<A STRING>") == "A STRING"
    assert (
        type_parser._find_inner_content("STRUCT<name STRING, age INT64>")
        == "name STRING, age INT64"
    )


def test_map_type():
    parser = TypeParser()

    assert parser._map_type("INT64") == "INT64"
    assert parser._map_type("INTEGER") == "INT64"
    assert parser._map_type("FLOAT") == "FLOAT64"
    assert parser._map_type("BOOL") == "BOOLEAN"
    assert parser._map_type("STRING") == "STRING"
    assert parser._map_type("TIMESTAMP") == "TIMESTAMP"
    assert parser._map_type("DATE") == "DATE"
    assert parser._map_type("TIME") == "TIME"
    assert parser._map_type("BOOLEAN") == "BOOLEAN"
    assert parser._map_type("GEOGRAPHY") == "GEOGRAPHY"
    assert parser._map_type("BYTES") == "BYTES"

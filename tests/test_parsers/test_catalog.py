"""Tests for the catalog parser module."""

import pytest

from dbt2looker_bigquery.models.dbt import (
    DbtCatalog,
    DbtModel,
    DbtModelColumn,
    DbtModelColumnMeta,
    DbtModelMeta,
)
from dbt2looker_bigquery.models.looker import (
    DbtMetaColumnLooker,
    DbtMetaLookerDimension,
)
from dbt2looker_bigquery.parsers.catalog import CatalogParser


class TestCatalogParser:
    @pytest.fixture
    def sample_catalog(self):
        raw_catalog = {
            "nodes": {
                "model.test.model1": {
                    "unique_id": "model.test.model1",
                    "metadata": {
                        "type": "table",
                        "schema": "test_schema",
                        "name": "model1",
                    },
                    "columns": {
                        "id": {
                            "name": "id",
                            "type": "INT64",
                        },
                        "alt_int": {
                            "name": "alt_int",
                            "type": "INTEGER",
                        },
                        "float_col": {
                            "name": "float_col",
                            "type": "FLOAT",
                        },
                        "alt_float": {
                            "name": "alt_float",
                            "type": "FLOAT64",
                        },
                        "numeric_col": {
                            "name": "numeric_col",
                            "type": "NUMERIC(10, 2)",
                        },
                        "struct_col": {
                            "name": "struct_col",
                            "type": "STRUCT<id INT64, name STRING>",
                        },
                        "array_struct_col": {
                            "name": "array_col",
                            "type": "ARRAY<STRUCT<id INT64, name STRING>>",
                        },
                        "array_col": {
                            "name": "array_col",
                            "type": "ARRAY<STRING>",
                        },
                    },
                }
            }
        }
        return DbtCatalog.model_validate(raw_catalog)

    @pytest.fixture
    def parser(self, sample_catalog):
        return CatalogParser(sample_catalog)

    def test_get_catalog_column_info(self, parser):
        """Test retrieving column type from catalog."""
        parser._get_catalog_node("model.test.model1")
        data_type, inner_types = parser._get_typing_information("id")
        assert data_type == "INT64"
        assert inner_types == ["INT64"]

        data_type, inner_types = parser._get_typing_information("struct_col")
        assert data_type == "STRUCT"
        assert inner_types == ["id INT64", "name STRING"]

        data_type, inner_types = parser._get_typing_information("array_struct_col")
        assert data_type == "ARRAY"
        assert inner_types == ["id INT64", "name STRING"]

        data_type, inner_types = parser._get_typing_information("array_col")
        assert data_type == "ARRAY"
        assert inner_types == ["STRING"]

        data_type, inner_types = parser._get_typing_information("numeric_col")
        assert data_type == "NUMERIC"
        assert inner_types == ["NUMERIC"]

        data_type, inner_types = parser._get_typing_information("float_col")
        assert data_type == "FLOAT64"
        assert inner_types == ["FLOAT64"]

        data_type, inner_types = parser._get_typing_information("alt_int")
        assert data_type == "INT64"
        assert inner_types == ["INT64"]

        data_type, inner_types = parser._get_typing_information("alt_float")
        assert data_type == "FLOAT64"
        assert inner_types == ["FLOAT64"]

        # Test non-existent model/column
        data_type, inner_types = parser._get_typing_information("non_existent")
        assert data_type is None
        assert inner_types == []

    def test_create_missing_array_column(self, parser):
        """Test creating a missing array column."""
        column = parser._create_missing_column(
            column_name="test_array", data_type="ARRAY<STRING>", inner_types=["STRING"]
        )

        assert column.name == "test_array"
        assert column.data_type == "ARRAY<STRING>"
        assert column.inner_types == ["STRING"]
        assert (
            column.description
            == "missing column from manifest.json, generated from catalog.json"
        )
        assert isinstance(column.meta, DbtModelColumnMeta)

    def test_process_model(self, parser):
        """Test processing a model with catalog information."""

        model = DbtModel(
            resource_type="model",
            name="model1",
            unique_id="model.test.model1",
            relation_name="model1",
            schema="test_schema",
            description="Test model",
            columns={
                "id": DbtModelColumn(
                    name="id",
                    description="Primary key",
                    meta=DbtModelColumnMeta(
                        looker=DbtMetaColumnLooker(
                            dimension=DbtMetaLookerDimension(
                                hidden=False,
                            )
                        )
                    ),
                ),
            },
            meta=DbtModelMeta(),
            path="models/test.sql",
            tags=[],  # Add empty tags list
        )

        processed_model = parser.process_model(model)
        assert processed_model is not None
        assert processed_model.columns["id"].data_type == "INT64"
        assert processed_model.columns["id"].inner_types == ["INT64"]

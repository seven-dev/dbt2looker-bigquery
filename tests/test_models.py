import pytest

from dbt2looker_bigquery.models.dbt import (
    DbtCatalogNode,
    DbtCatalogNodeColumn,
    DbtModel,
    DbtModelColumn,
    DbtModelColumnMeta,
    DbtModelMeta,
    DbtExposure,
)
from dbt2looker_bigquery.models.looker import (
    DbtMetaLooker,
    DbtMetaColumnLooker,
    DbtMetaLookerBase,
    DbtMetaLookerDimension,
)


class TestDbtModels:
    relation_name = "base.test_model"
    resource_type = "model"
    db_schema = "my_schema"
    fallback_db_schema = "undefined schema"
    model_name = "test_model"
    unique_id = "test_model.id"
    path = "models/test_model.sql"
    description = ""

    sparse_model = {
        "resource_type": resource_type,
        "relation_name": relation_name,
        "schema": db_schema,
        "name": model_name,
        "unique_id": unique_id,
        "tags": [],
        "path": path,
        "description": description,
    }

    well_defined_model_params = {
        "description": "A test model",
        "columns": {
            "id": {
                "name": "id",
                "description": "Primary key",
                "data_type": "INT64",
                "meta": {"looker": {"dimension": {"hidden": False, "label": "ID"}}},
            }
        },
        "tags": ["test"],
        "meta": {
            "looker": {
                "hidden": False,
                "view": {
                    "hidden": False,
                    "label": "Test View",
                },
            }
        },
        "path": path,
    }
    well_defined_model = sparse_model.copy()
    well_defined_model.update(well_defined_model_params)

    malformed_looker_dimension_params = {
        "columns": {
            "id": {
                "name": "id",
                "description": "Primary key",
                "data_type": "INT64",
                "meta": {"looker": {"dimension": {"hidden": "yes", "label": 1}}},
            }
        }
    }
    malformed_looker_dimension = sparse_model.copy()
    malformed_looker_dimension.update(malformed_looker_dimension_params)

    malformed_looker_model_measure_params = {
        "columns": {
            "id": {
                "name": "id",
                "description": "Primary key",
                "data_type": "INT64",
                "meta": {"looker": {"measure": {"hidden": "yes", "label": 1}}},
            }
        }
    }
    malformed_looker_model_measure = sparse_model.copy()
    malformed_looker_model_measure.update(malformed_looker_model_measure)

    malformed_looker_view_definition_params = {
        "meta": {
            "looker": {
                "view": {
                    "hidden": "yes",
                    "label": 1,
                },
            },
        },
    }
    malformed_looker_view_definition = sparse_model.copy()
    malformed_looker_view_definition.update(malformed_looker_view_definition_params)

    malformed_looker_view_definition_measures_params = {
        "meta": {
            "looker": {
                "measures": {
                    "hidden": "yes",
                    "label": 1,
                },
            },
        },
    }
    malformed_looker_view_definition_measures = sparse_model.copy()
    malformed_looker_view_definition_measures.update(
        malformed_looker_view_definition_measures_params
    )

    malformed_looker_view_definition_explore_params = {
        "meta": {
            "looker": {
                "explore": {
                    "hidden": "yes",
                    "label": 1,
                },
            },
        },
    }
    malformed_looker_view_definition_explore = sparse_model.copy()
    malformed_looker_view_definition_explore.update(
        malformed_looker_view_definition_explore_params
    )

    malformed_looker_view_definition_dimension_params = {
        "meta": {
            "looker": {
                "dimension": {
                    "hidden": "yes",
                    "label": 1,
                },
            },
        },
    }
    malformed_looker_view_definition_dimension = sparse_model.copy()
    malformed_looker_view_definition_dimension.update(
        malformed_looker_view_definition_dimension_params
    )

    @pytest.mark.parametrize(
        "node",
        [
            well_defined_model,
            sparse_model,
            malformed_looker_dimension,
            malformed_looker_model_measure,
            malformed_looker_view_definition,
            malformed_looker_view_definition_measures,
            malformed_looker_view_definition_explore,
            malformed_looker_view_definition_dimension,
        ],
    )
    def test_dbt_model_creation(self, node):
        """Test creating a DbtModel instance"""
        model = DbtModel(**node)
        assert model.name == self.model_name
        assert model.resource_type == self.resource_type
        if "schema" in node:
            assert model.db_schema == self.db_schema
        else:
            assert model.db_schema == self.fallback_db_schema
        assert model.relation_name == self.relation_name
        assert model.unique_id == self.unique_id
        assert model.path == self.path
        assert isinstance(model.meta, DbtModelMeta)
        assert isinstance(model.meta.looker, DbtMetaLooker)
        assert isinstance(model.meta.looker.view, DbtMetaLookerBase)
        assert model.meta.looker.view.label == node.get("meta", {}).get(
            "looker", {}
        ).get("view", {}).get("label")
        assert len(model.columns) == len(node.get("columns", {}))
        for column in model.columns.values():
            assert isinstance(column, DbtModelColumn)
            assert isinstance(column.meta, DbtModelColumnMeta)
            assert isinstance(column.meta.looker, DbtMetaColumnLooker)
            assert isinstance(column.meta.looker.dimension, DbtMetaLookerDimension)

    def test_dbt_model_column_validation(self):
        """Test DbtModelColumn validation"""
        column = DbtModelColumn(
            name="test_col",
            lookml_name="test_col",
            lookml_long_name="test_col",
            description="Test column",
            data_type="STRING",
            meta=DbtModelColumnMeta(),
        )
        assert column.name == "test_col"
        assert column.data_type == "STRING"
        assert not column.nested
        assert not column.is_primary_key

    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
        ],
    )
    def test_yes_no_validator(self, value, expected):
        """Test yes/no validation in meta fields"""
        meta = DbtMetaLooker()
        meta.view = DbtMetaLookerBase(hidden=value)
        assert meta.view.hidden == expected


class TestDbtCatalog:
    @pytest.fixture
    def sample_catalog_node(self):
        return {
            "metadata": {
                "type": "table",
                "schema": "test_schema",
                "name": "test_table",
            },
            "columns": {"id": {"type": "INT64", "index": 1, "name": "id"}},
        }

    def test_catalog_node_creation(self, sample_catalog_node):
        """Test creating a DbtCatalogNode instance"""
        node = DbtCatalogNode(**sample_catalog_node)
        assert node.metadata.type == "table"
        assert node.metadata.db_schema == "test_schema"
        assert isinstance(node.columns["id"], DbtCatalogNodeColumn)
        assert node.columns["id"].type == "INT64"


class TestDbtExposure:
    name = "test_exposure"

    # Define a fixture for a well defined node
    well_defined_node = {
        "unique_id": "test_exposure",
        "resource_type": "exposure",
        "name": name,
        "description": "A test exposure",
        "url": "https://example.com",
        "refs": [{"name": "test_model"}],
        "tags": ["test"],
        "depends_on": {"macros": ["test_macro"], "nodes": ["test_model"]},
    }

    sparse_node = {
        "resource_type": "exposure",
        "unique_id": "test_exposure",
        "name": name,
        "refs": [{"name": "test_model"}],
    }

    # Using parametrize to test with both well_defined_node and sparse_node fixtures
    @pytest.mark.parametrize("node", [well_defined_node, sparse_node])
    def test_exposure_creation(self, node):
        """Test creating a DbtExposure instance"""
        object = node
        exposure = DbtExposure(**object)
        assert exposure.name == self.name
        assert len(exposure.refs) == len(node.get("refs", []))
        # Fix the assertion for tags: you expect a list from exposure.tags, not just the length
        assert exposure.tags == node.get("tags", [])
        assert exposure.resource_type == "exposure"

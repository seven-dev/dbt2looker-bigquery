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
)


class TestCaseBuilder:
    cases = []

    @staticmethod
    def create(
        cases,
        additional_case_data=None,
        expected_measures=0,
        expected_dimensions=0,
        id=None,
    ):
        def base_model():
            return {
                "resource_type": "model",
                "relation_name": "base.test_model",
                "schema": "my_schema",
                "name": "test_model",
                "unique_id": "test_model.id",
                "tags": [],
                "path": "models/test_model.sql",
                "description": "",
                "meta": {},
            }

        new_case = base_model()
        if additional_case_data:
            new_case.update(additional_case_data)
        cases.append(
            {
                "case": new_case,
                "expected_measures": expected_measures,
                "expected_dimensions": expected_dimensions,
                "id": id,
            }
        )

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
        "path": "models/test_model.sql",
    }
    create(cases, well_defined_model_params, 0, 1, "well_defined_model")

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
    create(cases, malformed_looker_dimension_params, 0, 1, "malformed_looker_dimension")

    malformed_looker_dimension_and_normal_params = {
        "columns": {
            "id": {
                "name": "id",
                "description": "Primary key",
                "data_type": "INT64",
                "meta": {"looker": {"dimension": {"hidden": "yes", "label": 1}}},
            },
            "name": {
                "name": "name",
                "description": "Name",
                "data_type": "STRING",
                "meta": {"looker": {"dimension": {"hidden": False, "label": "Name"}}},
            },
        }
    }
    create(
        cases,
        malformed_looker_dimension_and_normal_params,
        0,
        2,
        "malformed_looker_dimension_and_normal",
    )

    malformed_looker_dimension_with_measures_params = {
        "columns": {
            "id": {
                "name": "id",
                "description": "Primary key",
                "data_type": "INT64",
                "meta": {
                    "looker": {
                        "dimension": {"hidden": "yes", "label": 1},
                        "measure": {"sum", "count", "average"},
                    }
                },
            }
        }
    }
    create(
        cases,
        malformed_looker_dimension_with_measures_params,
        3,
        1,
        "malformed_looker_dimension_with_measures",
    )

    malformed_looker_dimension_with_measures_alt_params = {
        "columns": {
            "id": {
                "name": "id",
                "description": "Primary key",
                "data_type": "INT64",
                "meta": {
                    "looker": {
                        "dimension": {"hidden": "yes", "label": 1},
                        "measure": [
                            {"type": "sum", "label": "sum"},
                            {"type": "count", "label": "count"},
                            {"type": "average", "label": "average"},
                        ],
                    }
                },
            }
        }
    }
    create(
        cases,
        malformed_looker_dimension_with_measures_alt_params,
        3,
        1,
        "malformed_looker_dimension_with_measures_alt",
    )

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
    create(
        cases,
        malformed_looker_model_measure_params,
        0,
        1,
        "malformed_looker_model_measure",
    )

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
    create(
        cases,
        malformed_looker_view_definition_params,
        0,
        0,
        "malformed_looker_view_definition",
    )

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
    create(
        cases,
        malformed_looker_view_definition_measures_params,
        0,
        0,
        "malformed_looker_view_definition_measures",
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
    create(
        cases,
        malformed_looker_view_definition_explore_params,
        0,
        0,
        "malformed_looker_view_definition_explore",
    )

    malformed_looker_view_definition_dimension_params = {
        "meta": {
            "looker": {
                "dimensions": {
                    "hidden": "yes",
                    "label": 1,
                },
            },
        },
    }
    create(
        cases,
        malformed_looker_view_definition_dimension_params,
        0,
        0,
        "malformed_looker_view_definition_dimension",
    )

    malformed_looker_view_definition_dimension_too_many_params = {
        "meta": {
            "looker": {
                "explore": {
                    "label": {"hidden": "yes", "label": 1},
                    "group_label": {"hidden": "yes", "label": 1},
                    "description": ["hidden", "yes", "label", 1],
                },
                "measures": {
                    "type": "mellon",
                    "hidden": "yes",
                    "label": 1,
                },
                "dimensions": {
                    "hidden": "yes",
                    "label": 1,
                },
            },
            "other_key": {"tangerine": True},
        },
    }
    create(
        cases,
        malformed_looker_view_definition_dimension_too_many_params,
        0,
        0,
        "malformed_looker_view_definition_dimension_too_many_params",
    )


@pytest.mark.parametrize(
    "case", TestCaseBuilder.cases, ids=[case["id"] for case in TestCaseBuilder.cases]
)
def test_dbt_model_creation(case):
    path = "models/test_model.sql"
    relation_name = "base.test_model"
    resource_type = "model"
    db_schema = "my_schema"
    fallback_db_schema = "undefined schema"
    model_name = "test_model"
    unique_id = "test_model.id"
    case_data = case["case"]
    model = DbtModel(**case_data)
    assert model.name == model_name
    assert model.resource_type == resource_type
    if "schema" in case_data:
        assert model.db_schema == db_schema
    else:
        assert model.db_schema == fallback_db_schema
    assert model.relation_name == relation_name
    assert model.unique_id == unique_id
    assert model.path == path
    assert isinstance(model.meta, DbtModelMeta)
    assert isinstance(model.meta.looker, DbtMetaLooker)
    assert isinstance(model.meta.looker.view, DbtMetaLookerBase)
    assert len(model.columns) == case["expected_dimensions"]
    for column in model.columns.values():
        assert isinstance(column, DbtModelColumn)
        assert isinstance(column.meta, DbtModelColumnMeta)
        assert isinstance(column.meta.looker, DbtMetaColumnLooker)


class TestDbtModels:
    def test_dbt_model_column_validation(self):
        """Test DbtModelColumn validation"""
        column = DbtModelColumn(
            name="test_col",
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

    @pytest.fixture
    def alternate_sample_catalog_node(self):
        return {
            "metadata": {
                "type": "table",
                "schema": "test_schema",
                "name": "test_table",
            },
            "columns": {"id": {"type": "INTEGER", "index": 1, "name": "id"}},
        }

    def test_catalog_node_creation(self, sample_catalog_node):
        """Test creating a DbtCatalogNode instance"""
        node = DbtCatalogNode(**sample_catalog_node)
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

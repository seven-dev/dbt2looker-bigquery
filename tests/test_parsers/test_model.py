"""Tests for the model parser module."""

import pytest

from dbt2looker_bigquery.models.dbt import DbtManifest, DbtModel
from dbt2looker_bigquery.parsers.model import ModelParser


class TestModelParser:
    @pytest.fixture
    def sample_manifest(self):
        return DbtManifest(
            **{
                "metadata": {"adapter_type": "bigquery"},
                "nodes": {
                    "model.test.model1": {
                        "resource_type": "model",
                        "relation_name": "model1",
                        "schema": "test_schema",
                        "name": "model1",
                        "unique_id": "model.test.model1",
                        "tags": ["analytics"],
                        "description": "Test model 1",
                        "columns": {
                            "id": {
                                "name": "id",
                                "description": "Primary key",
                                "data_type": "INT64",
                                "meta": {"looker": {"hidden": False}},
                            }
                        },
                        "meta": {"looker": {"label": "Model 1"}},
                        "path": "models/test_model.sql",
                    },
                    "model.test.model2": {
                        "resource_type": "model",
                        "relation_name": "model2",
                        "schema": "test_schema",
                        "name": "model2",
                        "unique_id": "model.test.model2",
                        "tags": ["reporting"],
                        "description": "Test model 2",
                        "columns": {
                            "id": {
                                "name": "id",
                                "description": "Primary key",
                                "data_type": "INT64",
                                "meta": {"looker": {"hidden": False}},
                            }
                        },
                        "meta": {"looker": {"label": "Model 2"}},
                        "path": "models/test_model.sql",
                    },
                },
                "exposures": {},
            }
        )

    @pytest.fixture
    def parser(self, sample_manifest):
        return ModelParser(sample_manifest)

    def test_get_all_models(self, parser):
        """Test getting all models from manifest."""
        models = parser.get_all_models()
        assert len(models) == 2
        assert all(model.resource_type == "model" for model in models)
        assert {model.name for model in models} == {"model1", "model2"}

    def test_filter_models(self, parser):
        """Test filtering models with various criteria."""
        all_models = parser.get_all_models()

        # Test filtering by select_model
        filtered = parser.filter_models(all_models, select_model=["model1"])
        assert len(filtered) == 1
        assert filtered[0].name == "model1"

        filtered_multi = parser.filter_models(
            all_models, select_model=["model1", "model2"]
        )
        assert len(filtered_multi) == 2
        assert {model.name for model in filtered_multi} == {"model1", "model2"}

        # Test filtering by tag
        filtered = parser.filter_models(all_models, tag="analytics")
        assert len(filtered) == 1
        assert filtered[0].name == "model1"

        # Test filtering by exposed_names
        filtered = parser.filter_models(all_models, exposed_names=["model1"])
        assert len(filtered) == 1
        assert filtered[0].name == "model1"

    def test_tags_match(self, parser):
        """Test tag matching functionality."""
        model = DbtModel(
            resource_type="model",
            name="test",
            tags=["tag1", "tag2"],
            unique_id="model.test",
            relation_name="test",
            schema="test_schema",
            description="Test model",
            columns={},
            meta={"looker": {}},
            path="models/test.sql",
        )
        assert parser._tags_match("tag1", model) is True
        assert parser._tags_match("tag3", model) is False


class TestModelParser_labelled_with_typing_errors:
    @pytest.fixture
    def sample_manifest(self):
        return DbtManifest(
            **{
                "metadata": {"adapter_type": "bigquery"},
                "nodes": {
                    "model.dbt_test_data_gen.tv_data": {
                        "database": "example-bq-project",
                        "schema": "test",
                        "name": "tv_data",
                        "resource_type": "model",
                        "package_name": "dbt_test_data_gen",
                        "path": "tv/tv_data.sql",
                        "original_file_path": "models/tv/tv_data.sql",
                        "unique_id": "model.dbt_test_data_gen.tv_data",
                        "fqn": ["dbt_test_data_gen", "tv", "tv_data"],
                        "alias": "tv_data",
                        "checksum": {
                            "name": "sha256",
                            "checksum": "7f9ecf5d69459fe2a1fd1b68b9e5f4a357ccf39084c4b59c8334e34225f2d610",
                        },
                        "config": {
                            "enabled": True,
                            "alias": None,
                            "schema": None,
                            "database": None,
                            "tags": [],
                            "meta": {
                                "looker": {
                                    "label": "TV",
                                    "description": "A model that describes TV shows and seasons.",
                                    "hidden": True,
                                }
                            },
                            "group": None,
                            "materialized": "table",
                            "incremental_strategy": None,
                            "persist_docs": {"relation": True, "columns": True},
                            "post-hook": [],
                            "pre-hook": [],
                            "quoting": {},
                            "column_types": {},
                            "full_refresh": None,
                            "unique_key": None,
                            "on_schema_change": "ignore",
                            "on_configuration_change": "apply",
                            "grants": {},
                            "packages": [],
                            "docs": {"show": True, "node_color": None},
                            "contract": {"enforced": False, "alias_types": True},
                            "access": "protected",
                            "dbt-osmosis": "schema/{model}.yml",
                        },
                        "tags": [],
                        "description": "tv_data",
                        "columns": {
                            "show_id": {
                                "name": "show_id",
                                "description": "A unique identifier for each show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show ID",
                                            "group_label": "Basic Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_name": {
                                "name": "show_name",
                                "description": "The official name of the TV show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Name",
                                            "group_label": "Basic Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "season": {
                                "name": "season",
                                "description": "The sequential number of the season within the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Season Number",
                                            "group_label": "Season Information",
                                        }
                                    }
                                },
                                "data_type": "INT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "imdb_rating_score_avg": {
                                "name": "imdb_rating_score_avg",
                                "description": "The average IMDB rating score for the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "IMDB Rating Score (Avg)",
                                            "group_label": "Ratings",
                                            "value_format_name": "Decimal",
                                        }
                                    }
                                },
                                "data_type": "FLOAT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "release_date": {
                                "name": "release_date",
                                "description": "The release date of the show or season.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Release Date",
                                            "group_label": "Dates",
                                            "value_format_name": "Date",
                                        }
                                    }
                                },
                                "data_type": "DATE",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "end_date": {
                                "name": "end_date",
                                "description": "The end date of the show or season, if applicable.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "End Date",
                                            "group_label": "Dates",
                                            "value_format_name": "Date",
                                        }
                                    }
                                },
                                "data_type": "DATE",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_tag_array": {
                                "name": "show_tag_array",
                                "description": "A list of tags associated with the show, represented as an array.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Tags",
                                            "group_label": "Additional Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_array": {
                                "name": "show_seasons_array",
                                "description": "An array containing the seasons of the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Seasons Array",
                                            "group_label": "Additional Information",
                                        }
                                    }
                                },
                                "data_type": "INT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_publication_date_array": {
                                "name": "show_publication_date_array",
                                "description": "An array of publication dates for the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Publication Dates",
                                            "group_label": "Additional Information",
                                            "value_format_name": "Date",
                                        }
                                    }
                                },
                                "data_type": "DATE",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_publication_timestamp_array": {
                                "name": "show_publication_timestamp_array",
                                "description": "An array of publication timestamps for the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Publication Timestamps",
                                            "group_label": "Additional Information",
                                            "value_format_name": "Timestamp",
                                        }
                                    }
                                },
                                "data_type": "TIMESTAMP",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array": {
                                "name": "show_seasons_struct_array",
                                "description": "A structure containing detailed information about each season of the show, including episodes.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Seasons Struct Array",
                                            "group_label": "Additional Information",
                                        }
                                    }
                                },
                                "data_type": "RECORD",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.season_id": {
                                "name": "show_seasons_struct_array.season_id",
                                "description": "A unique identifier for each season within the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Season ID",
                                            "group_label": "Season Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.season_name": {
                                "name": "show_seasons_struct_array.season_name",
                                "description": "The name of each season in the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Season Name",
                                            "group_label": "Season Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.season_number": {
                                "name": "show_seasons_struct_array.season_number",
                                "description": "The number of the season in the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Season Number",
                                            "group_label": "Season Information",
                                        }
                                    }
                                },
                                "data_type": "INT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.episodes.episode_id": {
                                "name": "show_seasons_struct_array.episodes.episode_id",
                                "description": "A unique identifier for each episode within the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Episode ID",
                                            "group_label": "Episode Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.episodes.episode_title": {
                                "name": "show_seasons_struct_array.episodes.episode_title",
                                "description": "The title of each episode in the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Episode Title",
                                            "group_label": "Episode Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.episodes.episode_number": {
                                "name": "show_seasons_struct_array.episodes.episode_number",
                                "description": "The number of each episode in the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Episode Number",
                                            "group_label": "Episode Information",
                                        }
                                    }
                                },
                                "data_type": "INT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                        },
                        "meta": {},
                        "group": None,
                        "docs": {"show": True, "node_color": None},
                        "patch_path": "dbt_test_data_gen://models/tv/schema/tv_data.yml",
                        "build_path": None,
                        "unrendered_config": {
                            "dbt-osmosis": "schema/{model}.yml",
                            "materialized": "table",
                            "persist_docs": {"relation": True, "columns": True},
                            "meta": {
                                "looker": {
                                    "label": "TV",
                                    "description": "A model that describes TV shows and seasons.",
                                    "hidden": True,
                                }
                            },
                        },
                        "created_at": 1737842986.6129339,
                        "relation_name": "`example-bq-project`.`test`.`tv_data`",
                        "raw_code": "select\n    'string' as show_id,\n    'ding' as show_name,\n    1 as season,\n    2.0 as imdb_rating_score_avg,\n    cast('2024-01-01' as date) as release_date,\n    cast('2024-01-01' as date) as end_date,\n\n    ARRAY<STRING>[\n        'string1',\n        'string2',\n        'string3'\n    ] as show_tag_array,\n    ARRAY<INT64>[\n        1,\n        2,\n        3\n    ] as show_seasons_array,\n\n    ARRAY<DATE>[\n        cast('2024-01-01' as date),\n        cast('2024-01-02' as date)\n    ] as show_publication_date_array,\n\n    ARRAY<TIMESTAMP>[\n        cast('2024-01-01 00:00:00' as timestamp),\n        cast('2024-01-02 00:00:00' as timestamp)\n    ] as show_publication_timestamp_array,\n\n    ARRAY[\n        STRUCT(\n            'ding1' as season_id,\n            'season 1' AS season_name, \n            1 AS season_number, \n            ARRAY[\n                STRUCT('ding1ep1' as episode_id, 'episode 1' AS episode_title, 1 AS episode_number),\n                STRUCT('ding1ep2' as episode_id, 'episode 2' AS episode_title, 2 AS episode_number)\n            ] AS episodes\n        ),\n        STRUCT(\n            'ding2' as season_id,\n            'season 2' AS season_name, \n            2 AS season_number, \n            ARRAY[\n                STRUCT('ding2ep1' as episode_id,'episode 1' AS episode_title, 1 AS episode_number),\n                STRUCT('ding2ep2' as episode_id,'episode 2' AS episode_title, 2 AS episode_number)\n            ] AS episodes\n        ),\n        STRUCT(\n            'ding3' as season_id,\n            'season 3' AS season_name, \n            3 AS season_number, \n            ARRAY[\n                STRUCT('ding3ep1' as episode_id,'episode 1' AS episode_title, 1 AS episode_number),\n                STRUCT('ding3ep1' as episode_id,'episode 2' AS episode_title, 2 AS episode_number)\n            ] AS episodes\n        )\n    ] AS show_seasons_struct_array,",
                        "language": "sql",
                        "refs": [],
                        "sources": [],
                        "metrics": [],
                        "depends_on": {"macros": [], "nodes": []},
                        "compiled_path": "target/compiled/dbt_test_data_gen/models/tv/tv_data.sql",
                        "compiled": True,
                        "compiled_code": "select\n    'string' as show_id,\n    'ding' as show_name,\n    1 as season,\n    2.0 as imdb_rating_score_avg,\n    cast('2024-01-01' as date) as release_date,\n    cast('2024-01-01' as date) as end_date,\n\n    ARRAY<STRING>[\n        'string1',\n        'string2',\n        'string3'\n    ] as show_tag_array,\n    ARRAY<INT64>[\n        1,\n        2,\n        3\n    ] as show_seasons_array,\n\n    ARRAY<DATE>[\n        cast('2024-01-01' as date),\n        cast('2024-01-02' as date)\n    ] as show_publication_date_array,\n\n    ARRAY<TIMESTAMP>[\n        cast('2024-01-01 00:00:00' as timestamp),\n        cast('2024-01-02 00:00:00' as timestamp)\n    ] as show_publication_timestamp_array,\n\n    ARRAY[\n        STRUCT(\n            'ding1' as season_id,\n            'season 1' AS season_name, \n            1 AS season_number, \n            ARRAY[\n                STRUCT('ding1ep1' as episode_id, 'episode 1' AS episode_title, 1 AS episode_number),\n                STRUCT('ding1ep2' as episode_id, 'episode 2' AS episode_title, 2 AS episode_number)\n            ] AS episodes\n        ),\n        STRUCT(\n            'ding2' as season_id,\n            'season 2' AS season_name, \n            2 AS season_number, \n            ARRAY[\n                STRUCT('ding2ep1' as episode_id,'episode 1' AS episode_title, 1 AS episode_number),\n                STRUCT('ding2ep2' as episode_id,'episode 2' AS episode_title, 2 AS episode_number)\n            ] AS episodes\n        ),\n        STRUCT(\n            'ding3' as season_id,\n            'season 3' AS season_name, \n            3 AS season_number, \n            ARRAY[\n                STRUCT('ding3ep1' as episode_id,'episode 1' AS episode_title, 1 AS episode_number),\n                STRUCT('ding3ep1' as episode_id,'episode 2' AS episode_title, 2 AS episode_number)\n            ] AS episodes\n        )\n    ] AS show_seasons_struct_array,",
                        "extra_ctes_injected": True,
                        "extra_ctes": [],
                        "contract": {
                            "enforced": False,
                            "alias_types": True,
                            "checksum": None,
                        },
                        "access": "protected",
                        "constraints": [],
                        "version": None,
                        "latest_version": None,
                        "deprecation_date": None,
                    },
                    "model.dbt_test_data_gen.serve_tv_data": {
                        "database": "example-bq-project",
                        "schema": "test",
                        "name": "serve_tv_data",
                        "resource_type": "model",
                        "package_name": "dbt_test_data_gen",
                        "path": "tv/serve_tv_data.sql",
                        "original_file_path": "models/tv/serve_tv_data.sql",
                        "unique_id": "model.dbt_test_data_gen.serve_tv_data",
                        "fqn": ["dbt_test_data_gen", "tv", "serve_tv_data"],
                        "alias": "serve_tv_data",
                        "checksum": {
                            "name": "sha256",
                            "checksum": "e900cef673354f2b287f6b2ae47a0b6671dc79c97d00732988e0ecdd16e3800f",
                        },
                        "config": {
                            "enabled": True,
                            "alias": None,
                            "schema": None,
                            "database": None,
                            "tags": [],
                            "meta": {},
                            "group": None,
                            "materialized": "table",
                            "incremental_strategy": None,
                            "persist_docs": {"relation": True, "columns": True},
                            "post-hook": [],
                            "pre-hook": [],
                            "quoting": {},
                            "column_types": {},
                            "full_refresh": None,
                            "unique_key": None,
                            "on_schema_change": "ignore",
                            "on_configuration_change": "apply",
                            "grants": {},
                            "packages": [],
                            "docs": {"show": True, "node_color": None},
                            "contract": {"enforced": False, "alias_types": True},
                            "access": "protected",
                            "dbt-osmosis": "schema/{model}.yml",
                        },
                        "tags": [],
                        "description": "",
                        "columns": {
                            "show_id": {
                                "name": "show_id",
                                "description": "A unique identifier for each show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show ID",
                                            "group_label": "Basic Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_name": {
                                "name": "show_name",
                                "description": "The official name of the TV show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Name",
                                            "group_label": "Basic Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "season": {
                                "name": "season",
                                "description": "The sequential number of the season within the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Season Number",
                                            "group_label": "Season Information",
                                        }
                                    }
                                },
                                "data_type": "INT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "imdb_rating_score_avg": {
                                "name": "imdb_rating_score_avg",
                                "description": "The average IMDB rating score for the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "IMDB Rating Score (Avg)",
                                            "group_label": "Ratings",
                                            "value_format_name": "Decimal",
                                        }
                                    }
                                },
                                "data_type": "FLOAT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "release_date": {
                                "name": "release_date",
                                "description": "The release date of the show or season.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Release Date",
                                            "group_label": "Dates",
                                            "value_format_name": "Date",
                                        }
                                    }
                                },
                                "data_type": "DATE",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "end_date": {
                                "name": "end_date",
                                "description": "The end date of the show or season, if applicable.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "End Date",
                                            "group_label": "Dates",
                                            "value_format_name": "Date",
                                        }
                                    }
                                },
                                "data_type": "DATE",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_tag_array": {
                                "name": "show_tag_array",
                                "description": "A list of tags associated with the show, represented as an array.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Tags",
                                            "group_label": "Additional Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_array": {
                                "name": "show_seasons_array",
                                "description": "An array containing the seasons of the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Seasons Array",
                                            "group_label": "Additional Information",
                                        }
                                    }
                                },
                                "data_type": "INT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_publication_date_array": {
                                "name": "show_publication_date_array",
                                "description": "An array of publication dates for the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Publication Dates",
                                            "group_label": "Additional Information",
                                            "value_format_name": "Date",
                                        }
                                    }
                                },
                                "data_type": "DATE",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_publication_timestamp_array": {
                                "name": "show_publication_timestamp_array",
                                "description": "An array of publication timestamps for the show.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Publication Timestamps",
                                            "group_label": "Additional Information",
                                            "value_format_name": "Timestamp",
                                        }
                                    }
                                },
                                "data_type": "TIMESTAMP",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array": {
                                "name": "show_seasons_struct_array",
                                "description": "A structure containing detailed information about each season of the show, including episodes.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Show Seasons Struct Array",
                                            "group_label": "Additional Information",
                                        }
                                    }
                                },
                                "data_type": "RECORD",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.season_id": {
                                "name": "show_seasons_struct_array.season_id",
                                "description": "A unique identifier for each season within the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Season ID",
                                            "group_label": "Season Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.season_name": {
                                "name": "show_seasons_struct_array.season_name",
                                "description": "The name of each season in the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Season Name",
                                            "group_label": "Season Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.season_number": {
                                "name": "show_seasons_struct_array.season_number",
                                "description": "The number of the season in the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Season Number",
                                            "group_label": "Season Information",
                                        }
                                    }
                                },
                                "data_type": "INT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.episodes.episode_id": {
                                "name": "show_seasons_struct_array.episodes.episode_id",
                                "description": "A unique identifier for each episode within the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Episode ID",
                                            "group_label": "Episode Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.episodes.episode_title": {
                                "name": "show_seasons_struct_array.episodes.episode_title",
                                "description": "The title of each episode in the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Episode Title",
                                            "group_label": "Episode Information",
                                        }
                                    }
                                },
                                "data_type": "STRING",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                            "show_seasons_struct_array.episodes.episode_number": {
                                "name": "show_seasons_struct_array.episodes.episode_number",
                                "description": "The number of each episode in the structure.",
                                "meta": {
                                    "looker": {
                                        "dimension": {
                                            "label": "Episode Number",
                                            "group_label": "Episode Information",
                                        }
                                    }
                                },
                                "data_type": "INT64",
                                "constraints": [],
                                "quote": None,
                                "tags": [],
                            },
                        },
                        "meta": {},
                        "group": None,
                        "docs": {"show": True, "node_color": None},
                        "patch_path": "dbt_test_data_gen://models/tv/schema/serve_tv_data.yml",
                        "build_path": None,
                        "unrendered_config": {
                            "dbt-osmosis": "schema/{model}.yml",
                            "materialized": "table",
                            "persist_docs": {"relation": True, "columns": True},
                        },
                        "created_at": 1737842989.428729,
                        "relation_name": "`example-bq-project`.`test`.`serve_tv_data`",
                        "raw_code": "select\n    *\nfrom {{ ref('tv_data') }}",
                        "language": "sql",
                        "refs": [{"name": "tv_data", "package": None, "version": None}],
                        "sources": [],
                        "metrics": [],
                        "depends_on": {
                            "macros": [],
                            "nodes": ["model.dbt_test_data_gen.tv_data"],
                        },
                        "compiled_path": "target/compiled/dbt_test_data_gen/models/tv/serve_tv_data.sql",
                        "compiled": True,
                        "compiled_code": "select\n    *\nfrom `example-bq-project`.`test`.`tv_data`",
                        "extra_ctes_injected": True,
                        "extra_ctes": [],
                        "contract": {
                            "enforced": False,
                            "alias_types": True,
                            "checksum": None,
                        },
                        "access": "protected",
                        "constraints": [],
                        "version": None,
                        "latest_version": None,
                        "deprecation_date": None,
                    },
                },
                "sources": {},
                "exposures": {
                    "exposure.dbt_test_data_gen.tv_data": {
                        "name": "tv_data",
                        "resource_type": "exposure",
                        "package_name": "dbt_test_data_gen",
                        "path": "tv/exposure.yml",
                        "original_file_path": "models/tv/exposure.yml",
                        "unique_id": "exposure.dbt_test_data_gen.tv_data",
                        "fqn": ["dbt_test_data_gen", "tv", "tv_data"],
                        "type": "dashboard",
                        "owner": {"email": "mock_email@tv.com", "name": "test"},
                        "description": "exposed tables for tv data",
                        "label": None,
                        "maturity": None,
                        "meta": {},
                        "tags": [],
                        "config": {"enabled": True},
                        "unrendered_config": {},
                        "url": None,
                        "depends_on": {
                            "macros": [],
                            "nodes": ["model.dbt_test_data_gen.serve_tv_data"],
                        },
                        "refs": [
                            {"name": "serve_tv_data", "package": None, "version": None}
                        ],
                        "sources": [],
                        "metrics": [],
                        "created_at": 1737842986.610771,
                    }
                },
                "metrics": {},
                "groups": {},
                "selectors": {},
                "disabled": {},
                "parent_map": {
                    "model.dbt_test_data_gen.tv_data": [],
                    "model.dbt_test_data_gen.serve_tv_data": [
                        "model.dbt_test_data_gen.tv_data"
                    ],
                    "exposure.dbt_test_data_gen.tv_data": [
                        "model.dbt_test_data_gen.serve_tv_data"
                    ],
                },
                "child_map": {
                    "model.dbt_test_data_gen.tv_data": [
                        "model.dbt_test_data_gen.serve_tv_data"
                    ],
                    "model.dbt_test_data_gen.serve_tv_data": [
                        "exposure.dbt_test_data_gen.tv_data"
                    ],
                    "exposure.dbt_test_data_gen.tv_data": [],
                },
                "group_map": {},
                "saved_queries": {},
                "semantic_models": {},
                "unit_tests": {},
            }
        )

    @pytest.fixture
    def parser(self, sample_manifest):
        return ModelParser(sample_manifest)

    def test_get_all_models(self, parser):
        """Test getting all models from manifest."""
        models = parser.get_all_models()
        assert len(models) == 2
        assert all(model.resource_type == "model" for model in models)
        assert {model.name for model in models} == {"serve_tv_data", "tv_data"}

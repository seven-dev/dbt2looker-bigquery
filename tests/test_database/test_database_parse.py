from dbt2looker_bigquery.database.bigquery import BigQueryDatabase
from dbt2looker_bigquery.database.models.bigqueryTable import (
    BigQueryFieldSchema,
    BigQueryTableSchema,
)
from dbt2looker_bigquery.models.dbt import DbtCatalogNodeColumn


def test_parser():
    # Test
    complex_table_schema = BigQueryTableSchema(
        fields=[
            BigQueryFieldSchema(name="show_id", type="STRING", fields=None, mode=None),
            BigQueryFieldSchema(
                name="show_name", type="STRING", fields=None, mode=None
            ),
            BigQueryFieldSchema(name="season", type="INTEGER", fields=None, mode=None),
            BigQueryFieldSchema(
                name="imdb_rating_score_avg", type="FLOAT", fields=None, mode=None
            ),
            BigQueryFieldSchema(
                name="release_date", type="DATE", fields=None, mode=None
            ),
            BigQueryFieldSchema(name="end_date", type="DATE", fields=None, mode=None),
            BigQueryFieldSchema(
                name="show_tag_array", type="STRING", fields=None, mode="REPEATED"
            ),
            BigQueryFieldSchema(
                name="show_seasons_array", type="INTEGER", fields=None, mode="REPEATED"
            ),
            BigQueryFieldSchema(
                name="show_publication_date_array",
                type="DATE",
                fields=None,
                mode="REPEATED",
            ),
            BigQueryFieldSchema(
                name="show_publication_timestamp_array",
                type="TIMESTAMP",
                fields=None,
                mode="REPEATED",
            ),
            BigQueryFieldSchema(
                name="structured_information",
                type="RECORD",
                fields=[
                    BigQueryFieldSchema(
                        name="season_id", type="STRING", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="season_number", type="INTEGER", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="season_release_date", type="DATE", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="season_release_timestamp",
                        type="TIMESTAMP",
                        fields=None,
                        mode=None,
                    ),
                ],
                mode=None,
            ),
            BigQueryFieldSchema(
                name="array_structure_mixture",
                type="RECORD",
                fields=[
                    BigQueryFieldSchema(
                        name="episode_id", type="STRING", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="episode_title", type="STRING", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="episode_number", type="INTEGER", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="episode_release_date", type="DATE", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="episode_release_timestamp",
                        type="TIMESTAMP",
                        fields=None,
                        mode=None,
                    ),
                    BigQueryFieldSchema(
                        name="inside_array_struct",
                        type="RECORD",
                        fields=[
                            BigQueryFieldSchema(
                                name="episode_id", type="STRING", fields=None, mode=None
                            ),
                            BigQueryFieldSchema(
                                name="episode_title",
                                type="STRING",
                                fields=None,
                                mode=None,
                            ),
                            BigQueryFieldSchema(
                                name="episode_number",
                                type="INTEGER",
                                fields=None,
                                mode=None,
                            ),
                            BigQueryFieldSchema(
                                name="episode_release_date",
                                type="DATE",
                                fields=None,
                                mode=None,
                            ),
                            BigQueryFieldSchema(
                                name="episode_release_timestamp",
                                type="TIMESTAMP",
                                fields=None,
                                mode=None,
                            ),
                        ],
                        mode=None,
                    ),
                ],
                mode="REPEATED",
            ),
            BigQueryFieldSchema(
                name="show_seasons_struct_array",
                type="RECORD",
                fields=[
                    BigQueryFieldSchema(
                        name="season_id", type="STRING", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="season_name", type="STRING", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="season_number", type="INTEGER", fields=None, mode=None
                    ),
                    BigQueryFieldSchema(
                        name="episodes",
                        type="RECORD",
                        fields=[
                            BigQueryFieldSchema(
                                name="episode_id", type="STRING", fields=None, mode=None
                            ),
                            BigQueryFieldSchema(
                                name="episode_title",
                                type="STRING",
                                fields=None,
                                mode=None,
                            ),
                            BigQueryFieldSchema(
                                name="episode_number",
                                type="INTEGER",
                                fields=None,
                                mode=None,
                            ),
                        ],
                        mode="REPEATED",
                    ),
                ],
                mode="REPEATED",
            ),
        ]
    )

    db = BigQueryDatabase()

    # Assert
    columns = {
        "show_id": DbtCatalogNodeColumn(name="show_id", type="STRING"),
        "show_name": DbtCatalogNodeColumn(name="show_name", type="STRING"),
        "season": DbtCatalogNodeColumn(name="season", type="INTEGER"),
        "imdb_rating_score_avg": DbtCatalogNodeColumn(
            name="imdb_rating_score_avg", type="FLOAT"
        ),
        "release_date": DbtCatalogNodeColumn(name="release_date", type="DATE"),
        "end_date": DbtCatalogNodeColumn(name="end_date", type="DATE"),
        "show_tag_array": DbtCatalogNodeColumn(
            name="show_tag_array", type="ARRAY<STRING>"
        ),
        "show_seasons_array": DbtCatalogNodeColumn(
            name="show_seasons_array", type="ARRAY<INTEGER>"
        ),
        "show_publication_date_array": DbtCatalogNodeColumn(
            name="show_publication_date_array", type="ARRAY<DATE>"
        ),
        "show_publication_timestamp_array": DbtCatalogNodeColumn(
            name="show_publication_timestamp_array", type="ARRAY<TIMESTAMP>"
        ),
        "structured_information": DbtCatalogNodeColumn(
            name="structured_information",
            type="STRUCT<season_id STRING, season_number INTEGER, season_release_date DATE, season_release_timestamp TIMESTAMP>",
        ),
        "structured_information.season_id": DbtCatalogNodeColumn(
            name="structured_information.season_id", type="STRING"
        ),
        "structured_information.season_number": DbtCatalogNodeColumn(
            name="structured_information.season_number", type="INTEGER"
        ),
        "structured_information.season_release_date": DbtCatalogNodeColumn(
            name="structured_information.season_release_date", type="DATE"
        ),
        "structured_information.season_release_timestamp": DbtCatalogNodeColumn(
            name="structured_information.season_release_timestamp", type="TIMESTAMP"
        ),
        "array_structure_mixture": DbtCatalogNodeColumn(
            name="array_structure_mixture",
            type="ARRAY<STRUCT<episode_id STRING, episode_title STRING, episode_number INTEGER, episode_release_date DATE, episode_release_timestamp TIMESTAMP, inside_array_struct STRUCT<episode_id STRING, episode_title STRING, episode_number INTEGER, episode_release_date DATE, episode_release_timestamp TIMESTAMP>>>",
        ),
        "array_structure_mixture.episode_id": DbtCatalogNodeColumn(
            name="array_structure_mixture.episode_id", type="STRING"
        ),
        "array_structure_mixture.episode_number": DbtCatalogNodeColumn(
            name="array_structure_mixture.episode_number", type="INTEGER"
        ),
        "array_structure_mixture.episode_release_date": DbtCatalogNodeColumn(
            name="array_structure_mixture.episode_release_date", type="DATE"
        ),
        "array_structure_mixture.episode_release_timestamp": DbtCatalogNodeColumn(
            name="array_structure_mixture.episode_release_timestamp", type="TIMESTAMP"
        ),
        "array_structure_mixture.episode_title": DbtCatalogNodeColumn(
            name="array_structure_mixture.episode_title", type="STRING"
        ),
        "array_structure_mixture.inside_array_struct": DbtCatalogNodeColumn(
            name="array_structure_mixture.inside_array_struct",
            type="STRUCT<episode_id STRING, episode_title STRING, episode_number INTEGER, episode_release_date DATE, episode_release_timestamp TIMESTAMP>",
        ),
        "array_structure_mixture.inside_array_struct.episode_id": DbtCatalogNodeColumn(
            name="array_structure_mixture.inside_array_struct.episode_id", type="STRING"
        ),
        "array_structure_mixture.inside_array_struct.episode_number": DbtCatalogNodeColumn(
            name="array_structure_mixture.inside_array_struct.episode_number",
            type="INTEGER",
        ),
        "array_structure_mixture.inside_array_struct.episode_release_date": DbtCatalogNodeColumn(
            name="array_structure_mixture.inside_array_struct.episode_release_date",
            type="DATE",
        ),
        "array_structure_mixture.inside_array_struct.episode_release_timestamp": DbtCatalogNodeColumn(
            name="array_structure_mixture.inside_array_struct.episode_release_timestamp",
            type="TIMESTAMP",
        ),
        "array_structure_mixture.inside_array_struct.episode_title": DbtCatalogNodeColumn(
            name="array_structure_mixture.inside_array_struct.episode_title",
            type="STRING",
        ),
        "show_seasons_struct_array": DbtCatalogNodeColumn(
            name="show_seasons_struct_array",
            type="ARRAY<STRUCT<season_id STRING, season_name STRING, season_number INTEGER, episodes ARRAY<STRUCT<episode_id STRING, episode_title STRING, episode_number INTEGER>>>>",
        ),
        "show_seasons_struct_array.episodes": DbtCatalogNodeColumn(
            name="show_seasons_struct_array.episodes",
            type="ARRAY<STRUCT<episode_id STRING, episode_title STRING, episode_number INTEGER>>",
        ),
        "show_seasons_struct_array.episodes.episode_id": DbtCatalogNodeColumn(
            name="show_seasons_struct_array.episodes.episode_id", type="STRING"
        ),
        "show_seasons_struct_array.episodes.episode_number": DbtCatalogNodeColumn(
            name="show_seasons_struct_array.episodes.episode_number", type="INTEGER"
        ),
        "show_seasons_struct_array.episodes.episode_title": DbtCatalogNodeColumn(
            name="show_seasons_struct_array.episodes.episode_title", type="STRING"
        ),
        "show_seasons_struct_array.season_id": DbtCatalogNodeColumn(
            name="show_seasons_struct_array.season_id", type="STRING"
        ),
        "show_seasons_struct_array.season_name": DbtCatalogNodeColumn(
            name="show_seasons_struct_array.season_name", type="STRING"
        ),
        "show_seasons_struct_array.season_number": DbtCatalogNodeColumn(
            name="show_seasons_struct_array.season_number", type="INTEGER"
        ),
    }

    for translated_column_id, node in db._translate_schema_to_dbt_model(
        complex_table_schema
    ).columns.items():
        assert node == columns[translated_column_id]

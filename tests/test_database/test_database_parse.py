# from dbt2looker_bigquery.database.bigquery import BigQueryDatabase
# from dbt2looker_bigquery.database.models.bigqueryTable import (
#     BigQueryFieldSchema,
#     BigQueryTableSchema,
# )
# from dbt2looker_bigquery.models.dbt import DbtCatalogNodeColumn


# def test_parser():
#     # Test
#     complex_table_schema = BigQueryTableSchema(
#         fields=[
#             BigQueryFieldSchema(name="show_id", type="STRING", fields=None, mode=None),
#             BigQueryFieldSchema(
#                 name="show_name", type="STRING", fields=None, mode=None
#             ),
#             BigQueryFieldSchema(name="season", type="INTEGER", fields=None, mode=None),
#             BigQueryFieldSchema(
#                 name="imdb_rating_score_avg", type="FLOAT", fields=None, mode=None
#             ),
#             BigQueryFieldSchema(
#                 name="release_date", type="DATE", fields=None, mode=None
#             ),
#             BigQueryFieldSchema(name="end_date", type="DATE", fields=None, mode=None),
#             BigQueryFieldSchema(
#                 name="show_tag_array", type="STRING", fields=None, mode="REPEATED"
#             ),
#             BigQueryFieldSchema(
#                 name="show_seasons_array", type="INTEGER", fields=None, mode="REPEATED"
#             ),
#             BigQueryFieldSchema(
#                 name="show_publication_date_array",
#                 type="DATE",
#                 fields=None,
#                 mode="REPEATED",
#             ),
#             BigQueryFieldSchema(
#                 name="show_publication_timestamp_array",
#                 type="TIMESTAMP",
#                 fields=None,
#                 mode="REPEATED",
#             ),
#             BigQueryFieldSchema(
#                 name="structured_information",
#                 type="RECORD",
#                 fields=[
#                     BigQueryFieldSchema(
#                         name="season_id", type="STRING", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="season_number", type="INTEGER", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="season_release_date", type="DATE", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="season_release_timestamp",
#                         type="TIMESTAMP",
#                         fields=None,
#                         mode=None,
#                     ),
#                 ],
#                 mode=None,
#             ),
#             BigQueryFieldSchema(
#                 name="array_structure_mixture",
#                 type="RECORD",
#                 fields=[
#                     BigQueryFieldSchema(
#                         name="episode_id", type="STRING", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="episode_title", type="STRING", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="episode_number", type="INTEGER", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="episode_release_date", type="DATE", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="episode_release_timestamp",
#                         type="TIMESTAMP",
#                         fields=None,
#                         mode=None,
#                     ),
#                     BigQueryFieldSchema(
#                         name="inside_array_struct",
#                         type="RECORD",
#                         fields=[
#                             BigQueryFieldSchema(
#                                 name="episode_id", type="STRING", fields=None, mode=None
#                             ),
#                             BigQueryFieldSchema(
#                                 name="episode_title",
#                                 type="STRING",
#                                 fields=None,
#                                 mode=None,
#                             ),
#                             BigQueryFieldSchema(
#                                 name="episode_number",
#                                 type="INTEGER",
#                                 fields=None,
#                                 mode=None,
#                             ),
#                             BigQueryFieldSchema(
#                                 name="episode_release_date",
#                                 type="DATE",
#                                 fields=None,
#                                 mode=None,
#                             ),
#                             BigQueryFieldSchema(
#                                 name="episode_release_timestamp",
#                                 type="TIMESTAMP",
#                                 fields=None,
#                                 mode=None,
#                             ),
#                         ],
#                         mode=None,
#                     ),
#                 ],
#                 mode="REPEATED",
#             ),
#             BigQueryFieldSchema(
#                 name="show_seasons_struct_array",
#                 type="RECORD",
#                 fields=[
#                     BigQueryFieldSchema(
#                         name="season_id", type="STRING", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="season_name", type="STRING", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="season_number", type="INTEGER", fields=None, mode=None
#                     ),
#                     BigQueryFieldSchema(
#                         name="episodes",
#                         type="RECORD",
#                         fields=[
#                             BigQueryFieldSchema(
#                                 name="episode_id", type="STRING", fields=None, mode=None
#                             ),
#                             BigQueryFieldSchema(
#                                 name="episode_title",
#                                 type="STRING",
#                                 fields=None,
#                                 mode=None,
#                             ),
#                             BigQueryFieldSchema(
#                                 name="episode_number",
#                                 type="INTEGER",
#                                 fields=None,
#                                 mode=None,
#                             ),
#                         ],
#                         mode="REPEATED",
#                     ),
#                 ],
#                 mode="REPEATED",
#             ),
#         ]
#     )

#     db = BigQueryDatabase()

#     # Assert
#     columns = {
#         "show_id": DbtCatalogNodeColumn(
#             name="show_id", data_type="STRING", inner_types=["STRING"]
#         ),
#         "show_name": DbtCatalogNodeColumn(
#             name="show_name", data_type="STRING", inner_types=["STRING"]
#         ),
#         "season": DbtCatalogNodeColumn(
#             name="season", data_type="INT64", inner_types=["INT64"]
#         ),
#         "imdb_rating_score_avg": DbtCatalogNodeColumn(
#             name="imdb_rating_score_avg", data_type="FLOAT64", inner_types=["FLOAT64"]
#         ),
#         "release_date": DbtCatalogNodeColumn(
#             name="release_date", data_type="DATE", inner_types=["DATE"]
#         ),
#         "end_date": DbtCatalogNodeColumn(
#             name="end_date", data_type="DATE", inner_types=["DATE"]
#         ),
#         "show_tag_array": DbtCatalogNodeColumn(
#             name="show_tag_array", data_type="ARRAY", inner_types=["STRING"]
#         ),
#         "show_seasons_array": DbtCatalogNodeColumn(
#             name="show_seasons_array", data_type="ARRAY", inner_types=["INT64"]
#         ),
#         "show_publication_date_array": DbtCatalogNodeColumn(
#             name="show_publication_date_array", data_type="ARRAY", inner_types=["DATE"]
#         ),
#         "show_publication_timestamp_array": DbtCatalogNodeColumn(
#             name="show_publication_timestamp_array",
#             data_type="ARRAY",
#             inner_types=["TIMESTAMP"],
#         ),
#         "structured_information": DbtCatalogNodeColumn(
#             name="structured_information",
#             data_type="STRUCT",
#             inner_types=[
#                 "season_id STRING",
#                 "season_number INT64",
#                 "season_release_date DATE",
#                 "season_release_timestamp TIMESTAMP",
#             ],
#         ),
#         "structured_information.season_id": DbtCatalogNodeColumn(
#             name="structured_information.season_id",
#             data_type="STRING",
#             inner_types=["STRING"],
#         ),
#         "structured_information.season_number": DbtCatalogNodeColumn(
#             name="structured_information.season_number",
#             data_type="INT64",
#             inner_types=["INT64"],
#         ),
#         "structured_information.season_release_date": DbtCatalogNodeColumn(
#             name="structured_information.season_release_date",
#             data_type="DATE",
#             inner_types=["DATE"],
#         ),
#         "structured_information.season_release_timestamp": DbtCatalogNodeColumn(
#             name="structured_information.season_release_timestamp",
#             data_type="TIMESTAMP",
#             inner_types=["TIMESTAMP"],
#         ),
#         "array_structure_mixture": DbtCatalogNodeColumn(
#             name="array_structure_mixture",
#             data_type="ARRAY",
#             inner_types=[
#                 "episode_id STRING",
#                 "episode_number INT64",
#                 "episode_release_date DATE",
#                 "episode_release_timestamp TIMESTAMP",
#                 "episode_title STRING",
#                 "inside_array_struct STRUCT",
#                 "inside_array_struct.episode_id STRING",
#                 "inside_array_struct.episode_number INT64",
#                 "inside_array_struct.episode_release_date DATE",
#                 "inside_array_struct.episode_release_timestamp TIMESTAMP",
#                 "inside_array_struct.episode_title STRING",
#             ],
#         ),
#         "array_structure_mixture.episode_id": DbtCatalogNodeColumn(
#             name="array_structure_mixture.episode_id",
#             data_type="STRING",
#             inner_types=["STRING"],
#         ),
#         "array_structure_mixture.episode_number": DbtCatalogNodeColumn(
#             name="array_structure_mixture.episode_number",
#             data_type="INT64",
#             inner_types=["INT64"],
#         ),
#         "array_structure_mixture.episode_release_date": DbtCatalogNodeColumn(
#             name="array_structure_mixture.episode_release_date",
#             data_type="DATE",
#             inner_types=["DATE"],
#         ),
#         "array_structure_mixture.episode_release_timestamp": DbtCatalogNodeColumn(
#             name="array_structure_mixture.episode_release_timestamp",
#             data_type="TIMESTAMP",
#             inner_types=["TIMESTAMP"],
#         ),
#         "array_structure_mixture.episode_title": DbtCatalogNodeColumn(
#             name="array_structure_mixture.episode_title",
#             data_type="STRING",
#             inner_types=["STRING"],
#         ),
#         "array_structure_mixture.inside_array_struct": DbtCatalogNodeColumn(
#             name="array_structure_mixture.inside_array_struct",
#             data_type="STRUCT",
#             inner_types=[
#                 "episode_id STRING",
#                 "episode_number INT64",
#                 "episode_release_date DATE",
#                 "episode_release_timestamp TIMESTAMP",
#                 "episode_title STRING",
#             ],
#         ),
#         "array_structure_mixture.inside_array_struct.episode_id": DbtCatalogNodeColumn(
#             name="array_structure_mixture.inside_array_struct.episode_id",
#             data_type="STRING",
#             inner_types=["STRING"],
#         ),
#         "array_structure_mixture.inside_array_struct.episode_number": DbtCatalogNodeColumn(
#             name="array_structure_mixture.inside_array_struct.episode_number",
#             data_type="INT64",
#             inner_types=["INT64"],
#         ),
#         "array_structure_mixture.inside_array_struct.episode_release_date": DbtCatalogNodeColumn(
#             name="array_structure_mixture.inside_array_struct.episode_release_date",
#             data_type="DATE",
#             inner_types=["DATE"],
#         ),
#         "array_structure_mixture.inside_array_struct.episode_release_timestamp": DbtCatalogNodeColumn(
#             name="array_structure_mixture.inside_array_struct.episode_release_timestamp",
#             data_type="TIMESTAMP",
#             inner_types=["TIMESTAMP"],
#         ),
#         "array_structure_mixture.inside_array_struct.episode_title": DbtCatalogNodeColumn(
#             name="array_structure_mixture.inside_array_struct.episode_title",
#             data_type="STRING",
#             inner_types=["STRING"],
#         ),
#         "show_seasons_struct_array": DbtCatalogNodeColumn(
#             name="show_seasons_struct_array",
#             data_type="ARRAY",
#             inner_types=[
#                 "episodes ARRAY",
#                 "episodes.episode_id STRING",
#                 "episodes.episode_number INT64",
#                 "episodes.episode_title STRING",
#                 "season_id STRING",
#                 "season_name STRING",
#                 "season_number INT64",
#             ],
#         ),
#         "show_seasons_struct_array.episodes": DbtCatalogNodeColumn(
#             name="show_seasons_struct_array.episodes",
#             data_type="ARRAY",
#             inner_types=[
#                 "episode_id STRING",
#                 "episode_number INT64",
#                 "episode_title STRING",
#             ],
#         ),
#         "show_seasons_struct_array.episodes.episode_id": DbtCatalogNodeColumn(
#             name="show_seasons_struct_array.episodes.episode_id",
#             data_type="STRING",
#             inner_types=["STRING"],
#         ),
#         "show_seasons_struct_array.episodes.episode_number": DbtCatalogNodeColumn(
#             name="show_seasons_struct_array.episodes.episode_number",
#             data_type="INT64",
#             inner_types=["INT64"],
#         ),
#         "show_seasons_struct_array.episodes.episode_title": DbtCatalogNodeColumn(
#             name="show_seasons_struct_array.episodes.episode_title",
#             data_type="STRING",
#             inner_types=["STRING"],
#         ),
#         "show_seasons_struct_array.season_id": DbtCatalogNodeColumn(
#             name="show_seasons_struct_array.season_id",
#             data_type="STRING",
#             inner_types=["STRING"],
#         ),
#         "show_seasons_struct_array.season_name": DbtCatalogNodeColumn(
#             name="show_seasons_struct_array.season_name",
#             data_type="STRING",
#             inner_types=["STRING"],
#         ),
#         "show_seasons_struct_array.season_number": DbtCatalogNodeColumn(
#             name="show_seasons_struct_array.season_number",
#             data_type="INT64",
#             inner_types=["INT64"],
#         ),
#     }

#     for translated_column_id, node in db._translate_schema_to_dbt_model(
#         complex_table_schema
#     ).columns.items():
#         assert node == columns[translated_column_id]

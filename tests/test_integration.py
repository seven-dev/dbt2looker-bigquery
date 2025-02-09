import os
from pathlib import Path
import shutil
import re
from dbt2looker_bigquery.cli import Cli


class TestIntegration:
    def test_dry_run(self):
        self._spin_down()
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/standard",
                "--output-dir",
                "output/tests/",
                "--select",
                "example_retail_data__fact_daily_sales",
                "--use-table-name",
                "--folder-structure",
                "DBT_FOLDER",
                "--dry-run",
            ]
        )
        models = cli.parse(args)
        cli.generate(args, models)
        assert not any(os.scandir("output"))

        self._spin_down()

    def test_integration_all_hidden_count(self):
        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/standard",
                "--output-dir",
                "output/tests/",
                "--select",
                "example_retail_data__fact_daily_sales",
                "--use-table-name",
                "--folder-structure",
                "DBT_FOLDER",
                "--all-hidden",
            ]
        )
        file_path = "output/tests/example/retail_data/fact_daily_sales_v1.view.lkml"

        content = self._spin_up(
            cli,
            args,
            file_path,
        )
        self._test(
            content,
            "hidden: yes",
            "count",
            27,
        )
        self._test(
            content,
            "type:",
            "count",
            26,
        )
        self._test(
            content,
            "type: left_outer",
            "count",
            3,
        )
        self._test(
            content,
            "explore: example_retail_data__fact_daily_sales {",
            "include",
        )
        self._test(
            content,
            "view: example_retail_data__fact_daily_sales {",
            "count",
            1,
        )
        self._test(content, "view:", "count", 4)
        self._test(
            content,
            "join: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {",
            "include",
        )
        self._test(
            content,
            "d_iso_year",
            "count",
            2,
        )
        self._test(
            content,
            "d_iso",
            "count",
            4,
        )
        self._test(
            content,
            "d_iso_week_of_year",
            "count",
            2,
        )
        self._test(
            content,
            "required_joins: [example_retail_data__fact_daily_sales__sales]",
            "include",
        )

        self._spin_down()

    def test_integration_skip_explore_joins(self):
        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/standard",
                "--output-dir",
                "output/tests/",
                "--select",
                "example_retail_data__fact_daily_sales",
                "--skip-explore",
                "--use-table-name",
                "--folder-structure",
                "DBT_FOLDER",
            ]
        )
        file_path = "output/tests/example/retail_data/fact_daily_sales_v1.view.lkml"
        content = self._spin_up(
            cli,
            args,
            file_path,
        )
        self._test(
            content,
            "type:",
            "count",
            23,
        )
        self._test(
            content,
            "explore: example_retail_data__fact_daily_sales {",
            "exclude",
        )
        self._test(
            content,
            "view: example_retail_data__fact_daily_sales {",
            "count",
            1,
        )
        self._test(content, "view:", "count", 4)
        self._test(
            content,
            "join: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {",
            "exclude",
        )
        self._test(
            content,
            "d_iso_year",
            "count",
            2,
        )
        self._test(
            content,
            "d_iso",
            "count",
            4,
        )
        self._test(
            content,
            "d_iso_week_of_year",
            "count",
            2,
        )
        self._test(
            content,
            "hidden: yes",
            "count",
            3,
        )
        self._test(
            content,
            "required_joins: [example_retail_data__fact_daily_sales__sales]",
            "exclude",
        )

        self._spin_down()

    def test_integration_primary_key(self):
        # Initialize and run CLI
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/standard",
                "--output-dir",
                "output/tests/",
                "--select",
                "example_retail_data__fact_daily_sales",
                "--use-table-name",
                "--folder-structure",
                "DBT_FOLDER",
                "--implicit-primary-key",
            ]
        )
        file_path = "output/tests/example/retail_data/fact_daily_sales_v1.view.lkml"
        content = self._spin_up(
            cli,
            args,
            file_path,
        )
        self._test(
            content,
            "explore: example_retail_data__fact_daily_sales {",
            "include",
        )
        self._test(
            content,
            "view: example_retail_data__fact_daily_sales {",
            "count",
            1,
        )
        self._test(content, "view:", "count", 4)
        self._test(
            content,
            "view: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {",
            "both_once",
            "join: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {",
        )
        self._test(
            content,
            "d_iso_year",
            "count",
            2,
        )
        self._test(
            content,
            "d_iso",
            "count",
            4,
        )
        self._test(
            content,
            "d_iso_week_of_year",
            "count",
            2,
        )
        self._test(
            content,
            "hidden: yes",
            "count",
            4,
        )
        self._test(
            content,
            "required_joins: [example_retail_data__fact_daily_sales__sales]",
            "count",
            1,
        )
        self._test(
            content,
            "primary_key",
            "count",
            3,
        )
        self._spin_down()

    def test_integration_cases_sales(self):
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/standard",
                "--output-dir",
                "output/tests/",
                "--select",
                "example_retail_data__fact_daily_sales",
                "--use-table-name",
                "--folder-structure",
                "DBT_FOLDER",
            ]
        )
        file_path = "output/tests/example/retail_data/fact_daily_sales_v1.view.lkml"
        content = self._spin_up(
            cli,
            args,
            file_path,
        )
        self._test(
            content,
            "view: example_retail_data__fact_daily_sales {",
            "count",
            1,
        )
        self._test(content, "view:", "count", 4)
        self._test(
            content,
            "view: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {",
            "both_once",
            "join: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {",
        )
        self._test(
            content,
            "d_iso_year",
            "count",
            2,
        )
        self._test(
            content,
            "d_iso",
            "count",
            4,
        )
        self._test(
            content,
            "d_iso_week_of_year",
            "count",
            2,
        )
        self._test(
            content,
            "hidden: yes",
            "count",
            4,
        )
        self._test(
            content,
            "required_joins: [example_retail_data__fact_daily_sales__sales]",
            "count",
            1,
        )

        self._spin_down()

    def test_integration_show_arrays_and_structs(self):
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/labelled",
                "--output-dir",
                "output/tests/",
                "--use-table-name",
                "--folder-structure",
                "DBT_FOLDER",
                "--show-arrays-and-structs",
            ]
        )
        file_path = "output/tests/tv/serve_tv_data.view.lkml"
        content = self._spin_up(
            cli,
            args,
            file_path,
        )
        self._test(
            content,
            "hidden:",
            "count",
            1,  # explore
        )

        self._spin_down()

    def test_integration_label_count(self):
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/labelled",
                "--output-dir",
                "output/tests/",
                "--use-table-name",
                "--folder-structure",
                "DBT_FOLDER",
            ]
        )
        file_path = "output/tests/tv/serve_tv_data.view.lkml"
        content = self._spin_up(
            cli,
            args,
            file_path,
        )
        self._test(
            content,
            "label:",
            "count",
            48,
        )
        self._test(
            content,
            "group_label: ",
            "count",
            27,
        )
        self._test(content, "tv_data__show_tag_array", "count", 4)

        (
            self._test(
                content,
                """view: serve_tv_data__show_tag_array {
            dimension: show_tag_array {
                type: string
                sql: serve_tv_data__show_tag_array ;;
                description: "A list of tags associated with the show, represented as an array."
                group_label: "Additional Information"
                label: "Show Tags"
            }
            }""",
                "match",
            ),
        )
        self._test(
            content,
            """view: serve_tv_data__show_publication_timestamp_array {
            dimension_group: show_publication_timestamp_array {
                type: time
                sql: serve_tv_data__show_publication_timestamp_array ;;
                description: "An array of publication timestamps for the show."
                datatype: timestamp
                timeframes: [
                raw,
                time,
                time_of_day,
                date,
                week,
                month,
                quarter,
                year,
                ]
                convert_tz: yes
                group_label: "Additional Information"
                label: "Show Publication Timestamps"
            }

            set: s_show_publication_timestamp_array {
                fields: [
                show_publication_timestamp_array_raw,
                show_publication_timestamp_array_time,
                show_publication_timestamp_array_time_of_day,
                show_publication_timestamp_array_date,
                show_publication_timestamp_array_week,
                show_publication_timestamp_array_month,
                show_publication_timestamp_array_quarter,
                show_publication_timestamp_array_year,
                ]
            }
            }""",
            "match",
        )

        self._test(
            content,
            """view: serve_tv_data__show_seasons_struct_array {
            dimension: season_id {
                type: string
                sql: season_id ;;
                description: "A unique identifier for each season within the structure."
                group_label: "Season Information"
                label: "Season ID"
            }

            dimension: season_name {
                type: string
                sql: season_name ;;
                description: "The name of each season in the structure."
                group_label: "Season Information"
                label: "Season Name"
            }

            dimension: season_number {
                type: number
                sql: season_number ;;
                description: "The number of the season in the structure."
                group_label: "Season Information"
                label: "Season Number"
            }

            dimension: episodes {
                sql: episodes ;;
                description: "missing column from manifest.json, generated from catalog.json"
                hidden: yes
                tags: ["array"]
            }
            }""",
            "match",
        )

        self._spin_down()

    def test_osmosis_2(self):
        self._spin_down()

        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/osmosis2",
                "--output-dir",
                "output/osmosis2/",
                "--use-table-name",
                "--folder-structure",
                "DBT_FOLDER",
            ]
        )
        file_path = "output/osmosis2/tv/serve_tv_data.view.lkml"
        content = self._spin_up(
            cli,
            args,
            file_path,
        )
        self._test(
            content,
            "label:",
            "count",
            60,
        )
        self._test(
            content,
            "group_label: ",
            "count",
            39,
        )
        self._test(content, "tv_data__show_tag_array", "count", 4)

        (
            self._test(
                content,
                """explore: serve_tv_data {
                    hidden: yes
                    description: ""

                    join: serve_tv_data__show_tag_array {
                        relationship: one_to_many
                        sql: LEFT JOIN UNNEST(${serve_tv_data.show_tag_array}) AS serve_tv_data__show_tag_array ;;
                        type: left_outer
                        required_joins: []
                    }

                    join: serve_tv_data__show_seasons_array {
                        relationship: one_to_many
                        sql: LEFT JOIN UNNEST(${serve_tv_data.show_seasons_array}) AS serve_tv_data__show_seasons_array ;;
                        type: left_outer
                        required_joins: []
                    }

                    join: serve_tv_data__show_publication_date_array {
                        relationship: one_to_many
                        sql: LEFT JOIN UNNEST(${serve_tv_data.show_publication_date_array}) AS serve_tv_data__show_publication_date_array ;;
                        type: left_outer
                        required_joins: []
                    }

                    join: serve_tv_data__show_publication_timestamp_array {
                        relationship: one_to_many
                        sql: LEFT JOIN UNNEST(${serve_tv_data.show_publication_timestamp_array}) AS serve_tv_data__show_publication_timestamp_array ;;
                        type: left_outer
                        required_joins: []
                    }

                    join: serve_tv_data__array_structure_mixture {
                        relationship: one_to_many
                        sql: LEFT JOIN UNNEST(${serve_tv_data.array_structure_mixture}) AS serve_tv_data__array_structure_mixture ;;
                        type: left_outer
                        required_joins: []
                    }

                    join: serve_tv_data__show_seasons_struct_array {
                        relationship: one_to_many
                        sql: LEFT JOIN UNNEST(${serve_tv_data.show_seasons_struct_array}) AS serve_tv_data__show_seasons_struct_array ;;
                        type: left_outer
                        required_joins: []
                    }

                    join: serve_tv_data__show_seasons_struct_array__episodes {
                        relationship: one_to_many
                        sql: LEFT JOIN UNNEST(${serve_tv_data__show_seasons_struct_array.episodes}) AS serve_tv_data__show_seasons_struct_array__episodes ;;
                        type: left_outer
                        required_joins: [serve_tv_data__show_seasons_struct_array]
                    }
                    }""",
                "match",
            ),
        )
        self._test(
            content,
            """view: serve_tv_data__show_tag_array {
                        dimension: show_tag_array {
                            type: string
                            sql: serve_tv_data__show_tag_array ;;
                            description: "A list of tags associated with the show, represented as an array."
                            group_label: "Additional Information"
                            label: "Show Tags"
                        }
            }""",
            "match",
        )

        self._test(
            content,
            """  dimension: structured_information__season_id {
                    type: string
                    sql: ${TABLE}.structured_information.season_id ;;
                    description: ""
                }

                dimension: structured_information__season_number {
                    type: number
                    sql: ${TABLE}.structured_information.season_number ;;
                    description: ""
                }""",
            "match",
        )

        self._test(
            content,
            """view: serve_tv_data__array_structure_mixture {
                dimension: episode_id {
                    type: string
                    sql: episode_id ;;
                    description: ""
                }

                dimension: episode_title {
                    type: string
                    sql: episode_title ;;
                    description: ""
                }

                dimension: episode_number {
                    type: number
                    sql: episode_number ;;
                    description: ""
                }

                dimension: inside_array_struct__episode_id {
                    type: string
                    sql: ${TABLE}.inside_array_struct.episode_id ;;
                    description: ""
                }

                dimension: inside_array_struct__episode_title {
                    type: string
                    sql: ${TABLE}.inside_array_struct.episode_title ;;
                    description: ""
                }

                dimension: inside_array_struct__episode_number {
                    type: number
                    sql: ${TABLE}.inside_array_struct.episode_number ;;
                    description: ""
                }

                dimension: inside_array_struct {
                    type: string
                    sql: inside_array_struct ;;
                    description: "missing column from manifest.json, generated from catalog.json"
                    tags: ["struct"]
                    hidden: yes
                }

                dimension: episode_release_iso_year {
                    type: number
                    sql: Extract(isoyear from episode_release_date) ;;
                    description: "iso year for episode_release"
                    group_label: "Episode Release"
                    value_format_name: id
                }

                dimension: episode_release_iso_week_of_year {
                    type: number
                    sql: Extract(isoweek from episode_release_date) ;;
                    description: "iso year for episode_release"
                    group_label: "Episode Release"
                    value_format_name: id
                }

                dimension: inside_array_struct__episode_release_iso_year {
                    type: number
                    sql: Extract(isoyear from ${TABLE}.inside_array_struct.episode_release_date) ;;
                    description: "iso year for inside_array_struct__episode_release"
                    group_label: "Inside Array Struct  Episode Release"
                    value_format_name: id
                }

                dimension: inside_array_struct__episode_release_iso_week_of_year {
                    type: number
                    sql: Extract(isoweek from ${TABLE}.inside_array_struct.episode_release_date) ;;
                    description: "iso year for inside_array_struct__episode_release"
                    group_label: "Inside Array Struct  Episode Release"
                    value_format_name: id
                }

                dimension_group: episode_release {
                    type: time
                    sql: episode_release_date ;;
                    description: ""
                    datatype: date
                    timeframes: [
                    raw,
                    date,
                    day_of_month,
                    day_of_week,
                    day_of_week_index,
                    week,
                    week_of_year,
                    month,
                    month_num,
                    month_name,
                    quarter,
                    quarter_of_year,
                    year,
                    ]
                    convert_tz: no
                    group_label: "Episode Release"
                }

                dimension_group: episode_release_timestamp {
                    type: time
                    sql: episode_release_timestamp ;;
                    description: ""
                    datatype: timestamp
                    timeframes: [
                    raw,
                    time,
                    time_of_day,
                    date,
                    week,
                    month,
                    quarter,
                    year,
                    ]
                    convert_tz: yes
                    group_label: "Episode Release Timestamp"
                }

                dimension_group: inside_array_struct__episode_release {
                    type: time
                    sql: ${TABLE}.inside_array_struct.episode_release_date ;;
                    description: ""
                    datatype: date
                    timeframes: [
                    raw,
                    date,
                    day_of_month,
                    day_of_week,
                    day_of_week_index,
                    week,
                    week_of_year,
                    month,
                    month_num,
                    month_name,
                    quarter,
                    quarter_of_year,
                    year,
                    ]
                    convert_tz: no
                    group_label: "Inside Array Struct  Episode Release"
                }

                dimension_group: inside_array_struct__episode_release_timestamp {
                    type: time
                    sql: ${TABLE}.inside_array_struct.episode_release_timestamp ;;
                    description: ""
                    datatype: timestamp
                    timeframes: [
                    raw,
                    time,
                    time_of_day,
                    date,
                    week,
                    month,
                    quarter,
                    year,
                    ]
                    convert_tz: yes
                    group_label: "Inside Array Struct  Episode Release Timestamp"
                }

                set: s_episode_release {
                    fields: [
                    episode_release_raw,
                    episode_release_date,
                    episode_release_day_of_month,
                    episode_release_day_of_week,
                    episode_release_day_of_week_index,
                    episode_release_week,
                    episode_release_week_of_year,
                    episode_release_month,
                    episode_release_month_num,
                    episode_release_month_name,
                    episode_release_quarter,
                    episode_release_quarter_of_year,
                    episode_release_year,
                    episode_release_iso_year,
                    episode_release_iso_week_of_year,
                    ]
                }

                set: s_episode_release_timestamp {
                    fields: [
                    episode_release_timestamp_raw,
                    episode_release_timestamp_time,
                    episode_release_timestamp_time_of_day,
                    episode_release_timestamp_date,
                    episode_release_timestamp_week,
                    episode_release_timestamp_month,
                    episode_release_timestamp_quarter,
                    episode_release_timestamp_year,
                    ]
                }

                set: s_inside_array_struct__episode_release {
                    fields: [
                    inside_array_struct__episode_release_raw,
                    inside_array_struct__episode_release_date,
                    inside_array_struct__episode_release_day_of_month,
                    inside_array_struct__episode_release_day_of_week,
                    inside_array_struct__episode_release_day_of_week_index,
                    inside_array_struct__episode_release_week,
                    inside_array_struct__episode_release_week_of_year,
                    inside_array_struct__episode_release_month,
                    inside_array_struct__episode_release_month_num,
                    inside_array_struct__episode_release_month_name,
                    inside_array_struct__episode_release_quarter,
                    inside_array_struct__episode_release_quarter_of_year,
                    inside_array_struct__episode_release_year,
                    inside_array_struct__episode_release_iso_year,
                    inside_array_struct__episode_release_iso_week_of_year,
                    ]
                }

                set: s_inside_array_struct__episode_release_timestamp {
                    fields: [
                    inside_array_struct__episode_release_timestamp_raw,
                    inside_array_struct__episode_release_timestamp_time,
                    inside_array_struct__episode_release_timestamp_time_of_day,
                    inside_array_struct__episode_release_timestamp_date,
                    inside_array_struct__episode_release_timestamp_week,
                    inside_array_struct__episode_release_timestamp_month,
                    inside_array_struct__episode_release_timestamp_quarter,
                    inside_array_struct__episode_release_timestamp_year,
                    ]
                }
                }""",
            "match",
        )

        self._spin_down()

    def _spin_up(self, cli, args, file_path):
        models = cli.parse(args)
        cli.generate(args, models)
        return Path(file_path).read_text()

    def _spin_down(self):
        self._remove_all_in_folder(("output/"))

    def _test(self, content, pattern, test, param=1):
        if test == "include":
            assert pattern in content
        elif test == "exclude":
            assert pattern not in content
        elif test == "count":
            assert content.count(pattern) == param
        elif test == "match":
            content_normalized = re.sub(r"\s+", " ", content).strip()
            pattern_normalized = re.sub(r"\s+", " ", pattern).strip()
            assert pattern_normalized in content_normalized
        elif test == "both_once":
            assert content.count(pattern) == 1
            assert content.count(param) == 1
        else:
            raise ValueError("Invalid test type")

    def _remove_all_in_folder(self, folder_path):
        # Ensure the folder exists
        if not os.path.exists(folder_path):
            print(f"The folder {folder_path} does not exist.")
            return

        # List all files and directories in the folder
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                # If it's a file, remove it
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                # If it's a directory, remove it and all its contents
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

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

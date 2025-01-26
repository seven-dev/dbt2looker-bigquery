import os
from pathlib import Path

from dbt2looker_bigquery.cli import Cli


class TestIntegration:
    def test_integration_hidden_count(self):
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

        self._assert_integration_test(
            cli,
            args,
            "output/tests/example/retail_data/fact_daily_sales_v1.view.lkml",
            "hidden: yes",
            "count",
            4,
        )

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

        self._assert_integration_test(
            cli,
            args,
            "output/tests/tv/serve_tv_data.view.lkml",
            "label:",
            "count",
            50,
        )

    def test_integration_group_label_count(self):
        cli = Cli()
        parser = cli._init_argparser()
        args = parser.parse_args(
            [
                "--target-dir",
                "tests/fixtures/labelled",
                "--output-dir",
                "output/tests/",
                "--select",
                "",
                "--use-table-name",
                "--folder-structure",
                "DBT_FOLDER",
            ]
        )

        self._assert_integration_test(
            cli,
            args,
            "output/tests/tv/serve_tv_data.view.lkml",
            "group_label: ",
            "count",
            22,
        )

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

        self._assert_integration_test(
            cli,
            args,
            "output/tests/example/retail_data/fact_daily_sales_v1.view.lkml",
            "hidden: yes",
            "count",
            27,
        )

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
                "--folder-structure",
                "DBT_FOLDER",
            ]
        )
        # assert args.build_explore
        self._assert_integration_test(
            cli,
            args,
            "output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml",
            "explore: example_retail_data__fact_daily_sales {",
            "exclude",
        )

    def test_integration_with_an_explore(self):
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
                "--folder-structure",
                "DBT_FOLDER",
            ]
        )

        self._assert_integration_test(
            cli,
            args,
            "output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml",
            "explore: example_retail_data__fact_daily_sales {",
            "include",
        )

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
                "--folder-structure",
                "DBT_FOLDER",
                "--implicit-primary-key",
            ]
        )

        self._assert_integration_test(
            cli,
            args,
            "output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml",
            "primary_key",
            "count",
            3,
        )

    def test_integration_view_1_count(self):
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
                "--folder-structure",
                "DBT_FOLDER",
                "--implicit-primary-key",
            ]
        )

        self._assert_integration_test(
            cli,
            args,
            "output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml",
            "view: example_retail_data__fact_daily_sales {",
            "count",
            1,
        )

    def test_integration_view_count_number(self):
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
                "--folder-structure",
                "DBT_FOLDER",
                "--implicit-primary-key",
            ]
        )

        self._assert_integration_test(
            cli,
            args,
            "output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml",
            "view:",
            "count",
            4,
        )

    def test_integration_matching_join_view_naming(self):
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
                "--folder-structure",
                "DBT_FOLDER",
                "--implicit-primary-key",
            ]
        )

        self._assert_integration_test(
            cli,
            args,
            "output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml",
            "view: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {",
            "both_once",
            "join: example_retail_data__fact_daily_sales__sales__fact_transaction_keys {",
        )

    def test_integration_required_joins_count(self):
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
                "--folder-structure",
                "DBT_FOLDER",
                "--implicit-primary-key",
            ]
        )

        self._assert_integration_test(
            cli,
            args,
            "output/tests/example/retail_data/example_retail_data__fact_daily_sales.view.lkml",
            "required_joins: [example_retail_data__fact_daily_sales__sales]",
            "count",
            1,
        )

    def _assert_integration_test(
        self, cli, args, file_path, pattern: str, test: str, param=1
    ):
        models = cli.parse(args)
        cli.generate(args, models)
        content = Path(file_path).read_text()
        if test == "include":
            assert pattern in content
        elif test == "exclude":
            assert pattern not in content
        elif test == "count":
            assert content.count(pattern) == param
        elif test == "both_once":
            assert content.count(pattern) == 1
            assert content.count(param) == 1
        else:
            raise ValueError("Invalid test type")

        from rich import print

        print(content)
        os.remove(file_path)

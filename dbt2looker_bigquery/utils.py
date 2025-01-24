import json
import logging

from dbt2looker_bigquery.exceptions import CliError


class FileHandler:
    def read(self, file_path: str, is_json=True) -> dict:
        """Load file from disk. Default is to load as a JSON file

        Args:
            file_path: Path to the file

        Returns:
            Dictionary containing the JSON data OR raw contents
        """
        try:
            with open(file_path, "r") as f:
                raw_file = json.load(f) if is_json else f.read()
        except FileNotFoundError as e:
            logging.error(
                f"Could not find file at {file_path}. Use --target-dir to change the search path for the manifest.json file."
            )
            raise CliError("File not found") from e

        return raw_file

    def write(self, file_path: str, contents: str):
        """Write contents to a file

        Args:
            file_path (str): _description_
            contents (str): _description_

        Raises:
            CLIError: _description_
        """
        try:
            with open(file_path, "w") as f:
                f.truncate()  # Clear file to allow overwriting
                f.write(contents)
        except Exception as e:
            logging.error(f"Could not write file at {file_path}.")
            raise CliError("Could not write file") from e


class Sql:
    def validate_sql(self, sql: str) -> str:
        """Validate that a string is a valid Looker SQL expression.

        Args:
            sql: SQL expression to validate

        Returns:
            Validated SQL expression or None if invalid
        """
        sql = sql.strip()

        def check_if_has_dollar_syntax(sql):
            """Check if the string either has ${TABLE}.example or ${view_name}"""
            return "${" in sql and "}" in sql

        def check_expression_has_ending_semicolons(sql):
            """Check if the string ends with a semicolon"""
            return sql.endswith(";;")

        if check_expression_has_ending_semicolons(sql):
            logging.warning(
                f"SQL expression {sql} ends with semicolons. It is removed and added by lkml."
            )
            sql = sql.rstrip(";").rstrip(";").strip()

        if not check_if_has_dollar_syntax(sql):
            logging.warning(
                f"SQL expression {sql} does not contain $TABLE or $view_name"
            )
            return None
        else:
            return sql


class DotManipulation:
    """general . manipulation functions for adjusting strings to be used in looker"""

    def remove_dots(self, input_string: str) -> str:
        """replace all periods with a replacement string
        this is used to create unique names for joins
        """
        sign = "."
        replacement = "__"

        return input_string.replace(sign, replacement)

    def last_dot_only(self, input_string):
        """replace all but the last period with a replacement string
        this is used to create unique names for joins
        """
        sign = "."
        replacement = "__"

        # Splitting input_string into parts separated by sign (period)
        parts = input_string.split(sign)

        # If there's more than one part, we need to do replacements.
        if len(parts) > 1:
            # Joining all parts except for last with replacement,
            # and then adding back on final part.
            output_string = replacement.join(parts[:-1]) + sign + parts[-1]

            return output_string

        # If there are no signs at all or just one part,
        return input_string

    def textualize_dots(self, input_string: str) -> str:
        """Replace all periods with a human-readable " " """
        sign = "."
        replacement = " "

        return input_string.replace(sign, replacement)

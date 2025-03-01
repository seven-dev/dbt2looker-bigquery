"""LookML generator utilities."""

from typing import Optional

from dbt2looker_bigquery.enums import LookerBigQueryDataType
from dbt2looker_bigquery.models.dbt import DbtModelColumn


def map_bigquery_to_looker(column_type: str | None) -> Optional[str]:
    """Map BigQuery data type to Looker data type
    returns
    - None if not found
    - Looker data type as string
        number
        string
        date
        datetime
        timestamp
        yesno
    """
    if column_type:
        column_type = column_type.split("<")[0]  # STRUCT< or ARRAY<
        column_type = column_type.split("(")[0]  # Numeric(1,31)
    try:
        return LookerBigQueryDataType.get(column_type)
    except ValueError:
        return None


def get_sql_expression(column: DbtModelColumn, is_main_view: bool, view: dict) -> str:
    """Get name of column."""
    if column.is_inner_array_representation:
        return view.get("name")

    column_name = column.name

    if "." in column.name:
        if column.name.startswith(view.get("array_name", "")) and view.get(
            "array_name"
        ):
            # records in non array structs can be referenced directly
            column_name = column.name[len(view.get("array_name")) + 1 :]

    if not is_main_view and "." not in column_name:
        # dimensions in structs can be referenced without the table name
        return column_name

    # default sql formatting
    return f"${{TABLE}}.{column_name}"


class MetaAttributeApplier:
    def __init__(self, cli_args):
        self.cli_args = cli_args

    def apply_meta_attributes(
        self, target_dict: dict, obj: any, attributes: list, path: str = ""
    ) -> None:
        """Apply meta attributes from the given object to the target dictionary if they exist."""
        meta_obj = self._get_meta_object(obj, path)
        if meta_obj is not None:
            for attr in attributes:
                if hasattr(meta_obj, attr):
                    value = getattr(meta_obj, attr)
                    if value is not None:
                        meta_value = self._get_meta_value(value, attr)
                        target_dict[attr] = meta_value

        # The condition to add "hidden" should remain outside the 'if' check for None
        if self.cli_args.all_hidden:
            target_dict["hidden"] = "yes"

    def get_meta_attribute(self, obj: any, attr: str, path: str = "") -> any:
        """Get a meta attribute from the given object if it exists."""
        meta_obj = self._get_meta_object(obj, path)
        if meta_obj is not None and hasattr(meta_obj, attr):
            return getattr(meta_obj, attr)

    def _get_meta_object(self, obj, path):
        """Get the meta object by following the path"""
        if path == "":
            return obj
        parts = path.split(".")
        for part in parts:
            obj = getattr(obj, part, None)
            if obj is None:
                break
        return obj

    def _get_meta_value(self, value, attr):
        """Get the meta value based on the attribute type"""
        if attr == "value_format_name" and hasattr(value, "value"):
            meta_value = value.value
        elif isinstance(value, bool):
            meta_value = "yes" if value else "no"
        else:
            meta_value = value
        return meta_value

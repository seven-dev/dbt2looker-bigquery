"""LookML dimension generator module."""

from typing import Optional

from dbt2looker_bigquery.enums import (
    LookerDateTimeframes,
    LookerDateTimeTypes,
    LookerDateTypes,
    LookerScalarTypes,
    LookerTimeTimeframes,
)
from dbt2looker_bigquery.generators.utils import (
    get_column_name,
    map_bigquery_to_looker,
    MetaAttributeApplier,
)
from dbt2looker_bigquery.models.dbt import DbtModel, DbtModelColumn


class LookmlDimensionGenerator:
    """Lookml dimension generator."""

    def __init__(self, args):
        self._cli_args = args
        self._applier = MetaAttributeApplier(args)

    def _format_label(self, name: str | None, remove_date: bool = True) -> str:
        """Format a name into a human-readable label."""
        if name is None:
            return ""
        if remove_date:
            suffix = "_date"
            if name.endswith(suffix):
                name = name[: -len(suffix)]
        return name.replace("_", " ").title()

    def _adjust_dimension_group_name(self, name: str) -> str:
        """Adjust dimension group name."""
        suffix = "_date"
        if name.endswith(suffix):
            name = name[: -len(suffix)]
        return name

    def _create_iso_field(
        self, field_type: str, column: DbtModelColumn, sql: str
    ) -> dict:
        """Create an ISO year or week field."""
        label_type = field_type.replace("_of_year", "")
        field = {
            "name": f"{self._adjust_dimension_group_name(column.name)}_iso_{field_type}",
            "label": f"{self._format_label(self._adjust_dimension_group_name(column.name))} ISO {label_type.title()}",
            "type": "number",
            "sql": f"Extract(iso{label_type} from {sql})",
            "description": f"iso year for {self._adjust_dimension_group_name(column.name)}",
            "group_label": "D Date",
            "value_format_name": "id",
        }

        self._applier.apply_meta_attributes(
            field, column, ["group_label", "label", "hidden"], "meta.looker.dimension"
        )

        if field_type == "week_of_year":
            field["label"] = field["label"].replace("Week", "Week Of Year")
        return field

    def _get_looker_type(self, column: DbtModelColumn) -> str:
        """Get the category of a column's type."""
        looker_type = map_bigquery_to_looker(column.data_type)
        if looker_type in LookerDateTimeTypes:
            return "time"
        elif looker_type in LookerDateTypes:
            return "date"
        return "scalar"

    def _create_dimension(self, column: DbtModelColumn, sql: str) -> Optional[dict]:
        """Create a basic dimension dictionary."""
        data_type = map_bigquery_to_looker(column.data_type)
        if data_type is None:
            return None

        dimension = {"name": column.lookml_name}

        # Add type for scalar types (should come before sql)
        if data_type in LookerScalarTypes:
            dimension["type"] = data_type

        dimension |= {"sql": sql, "description": column.description or ""}

        # Add primary key attributes
        if column.is_primary_key:
            dimension["primary_key"] = "yes"

        # Handle array and struct types
        if "ARRAY" in f"{column.data_type}":
            dimension["hidden"] = "yes"
            dimension["tags"] = ["array"]
            dimension.pop("type", None)
        elif "STRUCT" in f"{column.data_type}":
            dimension["tags"] = ["struct"]

        self._applier.apply_meta_attributes(
            dimension,
            column,
            ["description", "group_label", "value_format_name", "label", "hidden"],
            "meta.looker.dimension",
        )
        return dimension

    def lookml_dimension_group(
        self,
        column: DbtModelColumn,
        looker_type: str,
        main_view: bool,
    ) -> tuple:
        """Create dimension group for date/time fields."""
        if map_bigquery_to_looker(column.data_type) is None:
            return None, None, None

        if looker_type == "date":
            convert_tz = "no"
            timeframes = LookerDateTimeframes.values()
            column_name_adjusted = self._adjust_dimension_group_name(column.name)
        elif looker_type == "time":
            convert_tz = "yes"
            timeframes = LookerTimeTimeframes.values()
            column_name_adjusted = self._adjust_dimension_group_name(column.name)
        else:
            return None, None, None

        sql = get_column_name(column, main_view)

        dimensions = []
        dimension_group = {
            "name": column_name_adjusted,
            "type": looker_type,
            "sql": sql,
            "description": column.description,
            "datatype": map_bigquery_to_looker(column.data_type),
            "timeframes": timeframes,
            "convert_tz": convert_tz,
        }
        self._applier.apply_meta_attributes(
            dimension_group,
            column,
            ["group_label", "label", "hidden"],
            "meta.looker.dimension",
        )

        dimension_group_set = {
            "name": f"s_{column_name_adjusted}",
            "fields": [
                f"{column_name_adjusted}_{looker_time_timeframe}"
                for looker_time_timeframe in timeframes
            ],
        }

        if looker_type == "date":
            iso_year = self._create_iso_field("year", column, sql)
            iso_week_of_year = self._create_iso_field("week_of_year", column, sql)
            dimensions = [iso_year, iso_week_of_year]
            dimension_group_set["fields"].extend(
                [f"{column.name}_iso_year", f"{column.name}_iso_week_of_year"]
            )

        return dimension_group, dimension_group_set, dimensions

    # def _create_single_array_dimension(self, column: DbtModelColumn) -> dict:
    #     """Create a dimension for a simple array type."""
    #     data_type = map_bigquery_to_looker(column.inner_types[0])
    #     return {
    #         "name": column.lookml_name,
    #         "type": data_type,
    #         "sql": column.lookml_name,
    #         "description": column.description or "",
    #     }

    # def _is_single_type_array(self, column: DbtModelColumn) -> bool:
    #     """Check if column is a simple array type."""
    #     return column.data_type == "ARRAY" and (
    #         len(column.inner_types) == 1 and " " not in column.inner_types[0]
    #     )

    def _add_dimension_to_dimension_group(
        self, model: DbtModel, dimensions: list = None, main_view: bool = True
    ):
        """Add dimensions to dimension groups."""
        for column in model.columns.values():
            if column.data_type == "DATE":
                _, _, dimension_group_dimensions = self.lookml_dimension_group(
                    column, "date", main_view
                )
                if dimension_group_dimensions:
                    dimensions.extend(dimension_group_dimensions)

    def lookml_dimensions_from_model(
        self, column_list: list[DbtModelColumn], is_main_view: bool
    ) -> tuple:
        """Generate dimensions from model."""
        dimensions = []

        if self._cli_args.implicit_primary_key:
            # add primary keys on the first column, override if there is a primary key in constraints
            primary_key = True
            for column in column_list:
                column.is_primary_key = primary_key
                primary_key = None

        # Then add regular dimensions
        for column in column_list:
            if column.data_type == "DATETIME":
                continue

            if column.data_type == "DATE":
                continue

            if column.data_type is None:
                continue

            column_name = get_column_name(column, is_main_view)
            dimension = self._create_dimension(column, column_name)

            if dimension is not None:
                dimensions.append(dimension)

        return dimensions

    def lookml_dimension_groups_from_model(
        self, columns: list[DbtModelColumn], is_main_view: bool
    ) -> dict:
        """Generate dimension groups from model."""
        dimension_groups = []
        dimension_group_sets = []

        # First add ISO date dimensions for main view only
        # if not include_names:  # Only for main view
        # self._add_dimension_to_dimension_group(model, dimensions, table_format_sql)

        for column in columns:
            looker_type = self._get_looker_type(column)
            if looker_type in ("time", "date"):
                dimension_group, dimension_group_set, _ = self.lookml_dimension_group(
                    column=column,
                    looker_type=looker_type,
                    main_view=is_main_view,
                )
                if dimension_group:
                    dimension_groups.append(dimension_group)
                if dimension_group_set:
                    dimension_group_sets.append(dimension_group_set)

        return {
            "dimension_groups": dimension_groups or None,
            "dimension_group_sets": dimension_group_sets or None,
        }

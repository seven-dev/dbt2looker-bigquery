from dbt2looker_bigquery.enums import LookerMeasureType, LookerScalarTypes
from dbt2looker_bigquery.generators.utils import get_column_name, map_bigquery_to_looker
from dbt2looker_bigquery.models.dbt import DbtModelColumn
from dbt2looker_bigquery.models.looker import DbtMetaLookerMeasure
from dbt2looker_bigquery.generators.utils import MetaAttributeApplier


class LookmlMeasureGenerator:
    """Lookml dimension generator."""

    def __init__(self, args):
        self._cli_args = args
        self._applier = MetaAttributeApplier(args)

    def _apply_measure_attributes(
        self, measure_dict: dict, measure: DbtMetaLookerMeasure
    ) -> None:
        """Apply measure attributes to the measure dictionary."""
        direct_attributes = [
            "approximate",
            "approximate_threshold",
            "can_filter",
            "tags",
            "alias",
            "convert_tz",
            "suggestable",
            "precision",
            "percentile",
            "group_label",
            "label",
            "description",
        ]

        for attr in direct_attributes:
            value = getattr(measure, attr, None)
            if value is not None:
                measure_dict[attr] = value

        # Special handling for value_format_name which is an enum
        if measure.value_format_name is not None:
            measure_dict["value_format_name"] = measure.value_format_name.value

        # Special handling for hidden attribute
        if measure.hidden is not None:
            measure_dict["hidden"] = "yes" if measure.hidden else "no"

    def _lookml_measure(
        self,
        column: DbtModelColumn,
        measure: DbtMetaLookerMeasure,
        is_main_view: bool,
    ) -> dict:
        """Create a LookML measure from a DBT model column and measure."""
        if measure.type.value not in [t.value for t in LookerMeasureType]:
            return None

        m = {
            "name": f"m_{measure.type.value}_{column.name}",
            "type": measure.type.value,
            "sql": get_column_name(column, is_main_view),
            "description": measure.description
            or f"{measure.type.value} of {column.name}",
        }
        # Apply all measure attributes
        self._applier.apply_meta_attributes(
            m,
            measure,
            [
                "approximate",
                "approximate_threshold",
                "can_filter",
                "tags",
                "alias",
                "convert_tz",
                "suggestable",
                "precision",
                "percentile",
                "group_label",
                "value_format_name",
                "label",
                "description",
            ],
        )

        # Handle SQL distinct key
        if measure.sql_distinct_key is not None:
            m["sql_distinct_key"] = measure.sql_distinct_key

        # Handle filters
        if measure.filters:
            m["filters"] = [
                {"field": f.filter_dimension, "value": f.filter_expression}
                for f in measure.filters
            ]

        return m

    def lookml_measures_from_model(
        self, column_list: list[DbtModelColumn], is_main_view: bool
    ) -> list:
        """Generate measures from model."""
        lookml_measures = []

        for column in column_list:
            if (
                map_bigquery_to_looker(column.data_type) in LookerScalarTypes.values()
                and hasattr(column.meta, "looker")
                and hasattr(column.meta.looker, "measures")
                and column.meta.looker.measures
            ):
                lookml_measures.extend(
                    self._lookml_measure(column, measure, is_main_view)
                    for measure in column.meta.looker.measures
                )

        return lookml_measures

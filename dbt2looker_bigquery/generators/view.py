"""LookML view generator module."""

from typing import Dict

from dbt2looker_bigquery.models.dbt import DbtModel
from dbt2looker_bigquery.utils import DotManipulation


class LookmlViewGenerator:
    """LookML view generator."""

    def __init__(self, args):
        self._cli_args = args
        self._dot = DotManipulation()

    def _create_view(
        self,
        model: DbtModel,
        depth: int,
        column_list: list,
        view_name: str,
        view_label: str,
        dimension_generator,
        measure_generator,
    ) -> dict:
        """Create a view definition."""
        # Build view dict in specific order to match expected LookML output
        view = {
            "name": view_name,
            "label": view_label,
        }

        if depth == 0:
            view["sql_table_name"] = model.relation_name
            is_main_view = True
        else:
            is_main_view = False

        dimensions = dimension_generator.lookml_dimensions_from_model(
            column_list, is_main_view
        )

        if dimensions:
            view["dimensions"] = dimensions

        if dimension_groups := dimension_generator.lookml_dimension_groups_from_model(
            column_list, is_main_view
        ).get("dimension_groups"):
            view["dimension_groups"] = dimension_groups

        if measures := measure_generator.lookml_measures_from_model(
            column_list, is_main_view
        ):
            view["measures"] = measures

        if sets := dimension_generator.lookml_dimension_groups_from_model(
            column_list, is_main_view
        ).get("dimension_group_sets"):
            view["sets"] = sets

        return view

    def generate(
        self,
        model: DbtModel,
        base_view_name: str,
        base_view_label: str,
        dimension_generator,
        measure_generator,
        grouped_columns: dict,
    ) -> Dict:
        """Generate a view for a model."""
        views = []

        for key, column_list in grouped_columns.items():
            prepath = key[1]
            depth = key[0]

            view_name = (
                self._dot.remove_dots(f"{base_view_name}.{prepath}")
                if prepath
                else base_view_name
            )
            view_label = (
                self._dot.textualize_dots(f"{base_view_label} : {prepath}")
                if prepath
                else base_view_label
            )

            view = self._create_view(
                model,
                depth,
                column_list,
                view_name,
                view_label,
                dimension_generator,
                measure_generator,
            )
            views.append(view)

        return views

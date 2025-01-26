"""LookML view generator module."""

from typing import Dict

from dbt2looker_bigquery.models.dbt import DbtModel
from dbt2looker_bigquery.utils import DotManipulation, StructureGenerator
from dbt2looker_bigquery.generators.utils import MetaAttributeApplier


class LookmlViewGenerator:
    """LookML view generator."""

    def __init__(self, args):
        self._cli_args = args
        self._dot = DotManipulation()
        self._structure_generator = StructureGenerator(args)
        self._applier = MetaAttributeApplier(args)

    def _build_view(
        self,
        view,
        model: DbtModel,
        depth: int,
        column_list: list,
        dimension_generator,
        measure_generator,
    ) -> dict:
        """Create a view definition."""
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
        dimension_generator,
        measure_generator,
    ) -> Dict:
        """Generate a view for a model."""
        views = []
        grouped_columns = self._structure_generator.process_model(model)

        base_view = {"name": model.name}
        self._applier.apply_meta_attributes(
            base_view, model, ["label", "hidden"], "meta.looker.view"
        )

        for key, column_list in grouped_columns.items():
            prepath = key[1]
            depth = key[0]
            iteration_view = base_view.copy()
            if depth > 0:
                iteration_view["name"] = self._dot.remove_dots(
                    f"{iteration_view['name']}.{prepath}"
                )
                if hasattr(iteration_view, "label"):
                    iteration_view["label"] = self._dot.textualize_dots(
                        f"{iteration_view['label']} : {prepath}"
                    )
                else:
                    iteration_view["label"] = self._dot.textualize_dots(
                        f"{iteration_view["name"]} : {prepath}"
                    )

            view = self._build_view(
                iteration_view,
                model,
                depth,
                column_list,
                dimension_generator,
                measure_generator,
            )
            views.append(view)

        return views

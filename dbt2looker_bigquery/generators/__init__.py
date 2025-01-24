"""LookML Generator implementations."""

import os
from typing import Dict

from dbt2looker_bigquery.generators.dimension import LookmlDimensionGenerator
from dbt2looker_bigquery.generators.explore import LookmlExploreGenerator
from dbt2looker_bigquery.generators.measure import LookmlMeasureGenerator
from dbt2looker_bigquery.generators.view import LookmlViewGenerator
from dbt2looker_bigquery.models.dbt import DbtModel
from dbt2looker_bigquery.generators.structure import StructureGenerator


class LookmlGenerator:
    """Main LookML generator that coordinates dimension, view, and explore generation."""

    def __init__(self, cli_args):
        self._cli_args = cli_args
        self.dimension_generator = LookmlDimensionGenerator(cli_args)
        self.view_generator = LookmlViewGenerator(cli_args)
        self.explore_generator = LookmlExploreGenerator(cli_args)
        self.measure_generator = LookmlMeasureGenerator(cli_args)
        self.structure_generator = StructureGenerator(cli_args)

    def _get_view_label(self, model: DbtModel) -> str:
        """Get the view label from the model metadata or name."""
        # Check looker meta label first
        if hasattr(model.meta.looker, "label") and model.meta.looker.label is not None:
            return model.meta.looker.label

        # Fall back to model name if available
        return model.name.replace("_", " ").title() if hasattr(model, "name") else None

    def _get_file_path(self, model: DbtModel, base_view_name: str) -> str:
        """Get the file path for the LookML view."""
        if self._cli_args.folder_structure == "BIGQUERY_DATASET":
            file_path = model.db_schema
            if self._cli_args.remove_prefix_from_dataset:
                file_path = file_path.replace(
                    f"{self._cli_args.remove_prefix_from_dataset}.", ""
                )
        elif self._cli_args.folder_structure == "DBT_FOLDER":
            file_path = os.path.join(model.path.split(model.name)[0])

        if self._cli_args.use_table_name:
            file_name = model.relation_name.split(".")[-1].strip("`")
        else:
            file_name = base_view_name

        return f"{file_path}/{file_name}.view.lkml"

    def generate(self, model: DbtModel) -> Dict:
        """Generate LookML for a model."""

        # Get view name
        base_view_name = (
            model.relation_name.split(".")[-1].strip("`")
            if self._cli_args.use_table_name
            else model.name
        )
        # Get view label
        base_view_label = self._get_view_label(model)

        grouped_columns = self.structure_generator.generate(model)

        # Create views
        views = self.view_generator.generate(
            model=model,
            base_view_name=base_view_name,
            base_view_label=base_view_label,
            dimension_generator=self.dimension_generator,
            measure_generator=self.measure_generator,
            grouped_columns=grouped_columns,
        )

        # Create LookML base
        lookml = {
            "view": [views],
        }

        # Create explore if needed
        if (
            self._cli_args.build_explore
        ):  # When build_explore is True, we should generate the explore
            # Create explore
            explore = self.explore_generator.generate(
                model=model,
                base_view_name=base_view_name,
                base_view_label=base_view_label,
                grouped_columns=grouped_columns,
            )
            if explore:
                lookml["explore"] = explore

        return self._get_file_path(model, base_view_name), lookml


__all__ = ["LookmlGenerator"]

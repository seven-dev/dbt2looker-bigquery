"""LookML explore generator module."""

from dbt2looker_bigquery.models.dbt import DbtModel
from dbt2looker_bigquery.utils import DotManipulation, StructureGenerator


class LookmlExploreGenerator:
    """Lookml explore generator."""

    def __init__(self, args):
        self._cli_args = args
        self._dot = DotManipulation()
        self._structure_generator = StructureGenerator(args)

    def get_reduced_paths(self, input_string):
        # Split the input string by periods
        parts = input_string.split(".")

        # Initialize the result list
        result = []

        # Construct the reduced paths from the parts
        for i in range(len(parts) - 1, 0, -1):
            reduced_path = ".".join(parts[:i])
            result.append(self._dot.remove_dots(reduced_path))

        return result

    def generate_joins(self, model, structure):
        join_list = []

        base_name = (
            model.relation_name.split(".")[-1].strip("`")
            if self._cli_args.use_table_name
            else model.name
        )

        for key, _ in structure.items():
            depth = key[0]
            if depth > 0:
                prepath = key[1]
                view_base = f"{base_name}.{prepath}"
                view_name = self._dot.remove_dots(view_base)
                join_name = self._dot.last_dot_only(view_base)
                join_sql = f"LEFT JOIN UNNEST(${{{join_name}}}) AS {view_name}"

                depth = key[0]
                if depth > 1:
                    required_joins = self.get_reduced_paths(prepath)
                else:
                    required_joins = []

                # Add to list
                join_list.append(
                    {
                        "name": view_name,
                        "relationship": "one_to_many",
                        "sql": join_sql,
                        "type": "left_outer",
                        "required_joins": required_joins,
                    }
                )
        return join_list

    def generate(
        self,
        model: DbtModel,
        base_view_name: str,
        base_view_label: str,
    ) -> dict:
        """Create the explore definition."""
        # default behavior is to hide the view
        hidden = "yes"
        if (
            hasattr(model, "meta")
            and hasattr(model.meta, "looker")
            and hasattr(model.meta.looker, "view")
            and hasattr(model.meta.looker.view, "hidden")
        ):
            hidden = "no" if not model.meta.looker.view.hidden else "yes"

        # Create explore
        explore = {
            "name": base_view_name,
            "label": base_view_label,
            "hidden": hidden,
        }

        grouped_columns = self._structure_generator.process_model(model)

        # if joins exist we need to explore them
        if joins := self.generate_joins(model, grouped_columns):
            explore["joins"] = joins
            return explore
        else:
            return None

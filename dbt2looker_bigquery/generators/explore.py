"""LookML explore generator module."""

from dbt2looker_bigquery.models.dbt import DbtModel


class LookmlExploreGenerator:
    """Lookml explore generator."""

    def __init__(self, args):
        self._cli_args = args

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

    def remove_dots(self, input_string):
        """replace all periods with a replacement string
        this is used to create unique names for joins
        """
        sign = "."
        replacement = "__"

        return input_string.replace(sign, replacement)

    def get_reduced_paths(self, input_string):
        # Split the input string by periods
        parts = input_string.split(".")

        # Initialize the result list
        result = []

        # Construct the reduced paths from the parts
        for i in range(len(parts) - 1, 0, -1):
            reduced_path = ".".join(parts[:i])
            result.append(self.remove_dots(reduced_path))

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
                view_name = self.remove_dots(view_base)
                join_name = self.last_dot_only(view_base)
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
        self, model: DbtModel, view_name: str, view_label: str, grouped_columns: dict
    ) -> dict:
        """Create the explore definition."""
        # default behavior is to hide the view
        hidden = "yes"
        if (
            hasattr(model, "meta")
            and hasattr(model.meta, "looker")
            and hasattr(model.meta.looker, "hidden")
        ):
            hidden = "no" if not model.meta.looker.hidden else "yes"

        # Create explore
        explore = {
            "name": view_name,
            "label": view_label,
            "from": view_name,
            "hidden": hidden,
        }

        # if joins exist we need to explore them
        if joins := self.generate_joins(model, grouped_columns):
            explore["joins"] = joins
            return explore
        else:
            return None

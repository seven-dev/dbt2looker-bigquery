class StructureGenerator:
    """Split columns into groups for views and joins"""

    def __init__(self, args):
        self._cli_args = args

    def process_model(self, model):
        """Process the model to group columns for views and joins"""
        grouped_data = {}

        for column in model.columns.values():
            depth = column.name.count(".")
            prepath = ".".join(column.name.split(".")[:-1])
            key = (depth, prepath)

            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(column)

            # Add arrays as columns in two depth levels
            if column.data_type == "ARRAY" and len(column.inner_types) == 1:
                depth += 1
                prepath = column.name
                key = (depth, prepath)
                if key not in grouped_data:
                    grouped_data[key] = []
                column_copy = column.model_copy()
                column_copy.is_inner_array_representation = True
                grouped_data[key].append(column_copy)

        return grouped_data

    def generate(self, model):
        """Process a model to group columns for views and joins"""
        grouped_data = self.process_model(model)
        return grouped_data

from dbt2looker_bigquery.database.bigquery import BigQueryDatabase

from typing import List, Optional, Tuple

from dbt2looker_bigquery.models.dbt import (
    DbtModel,
    DbtModelColumn,
    DbtModelColumnMeta,
)


class DatabaseCatalog:
    """Database substitution for Catalog"""

    def __init__(self):
        """Initialize with catalog data."""
        self._database = BigQueryDatabase()
        self._model = None

    def process_model(self, model: DbtModel) -> Optional[DbtModel]:
        """Process a model by updating its columns with catalog information."""
        processed_columns = {}

        self._model = self._database.get_dbt_table_schema(model)
        for column_name, column in model.columns.items():
            if processed_column := self._update_column_with_inner_types(
                column, model.unique_id
            ):
                processed_columns[column_name] = processed_column

        if model.unique_id in self._model:
            for column_name, column in self._model.columns.items():
                if (
                    column_name not in processed_columns
                    and "ARRAY" in f"{column.data_type}"
                    and column.data_type is not None
                ):
                    processed_columns[column_name] = self._create_missing_array_column(
                        column_name, column.data_type, column.inner_types or []
                    )

        if processed_columns:
            return model.model_copy(update={"columns": processed_columns})
        return None

    def _create_missing_array_column(
        self, column_name: str, data_type: str, inner_types: List[str]
    ) -> DbtModelColumn:
        """Create a new column model for array columns missing from manifest."""
        return DbtModelColumn(
            name=column_name,
            description="missing column from manifest.json, generated from catalog.json",
            data_type=data_type,
            inner_types=inner_types,
            meta=DbtModelColumnMeta(),
        )

    def _get_catalog_column_info(
        self, model_id: str, column_name: str
    ) -> Tuple[Optional[str], List[str]]:
        """Get column type information from catalog."""
        if not self._model or column_name.lower() not in self._model.columns:
            raise ValueError(f"Column {column_name} not found in model {model_id}")

        column = self._model.columns[column_name.lower()]
        return column.data_type, column.inner_types or []

    def _update_column_with_inner_types(
        self, column: DbtModelColumn, model_id: str
    ) -> Optional[DbtModelColumn]:
        """Update a column with type information from catalog."""
        data_type, inner_types = self._get_catalog_column_info(model_id, column.name)
        if data_type is not None:
            return column.model_copy(
                update={"data_type": data_type, "inner_types": inner_types}
            )
        return None

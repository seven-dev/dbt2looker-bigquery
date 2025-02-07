import google.auth
from google.auth.transport.requests import Request
import requests
from dbt2looker_bigquery.database.models.bigqueryTable import BigQueryTableSchema

from dbt2looker_bigquery.models.dbt import DbtCatalogNode


class BigQueryDatabase:
    def _fetch_table_schema(
        self, project_id: str, dataset_id: str, table_id: str
    ) -> BigQueryTableSchema:
        """Fetch the schema of a BigQuery table and parse it into a Pydantic model."""
        credentials, _ = google.auth.default()

        credentials.refresh(Request())

        url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

        headers = {"Authorization": f"Bearer {credentials.token}"}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        table_info = response.json()

        schema = BigQueryTableSchema(fields=table_info["schema"]["fields"])

        from rich import print

        print(schema)
        return schema

    def _translate_schema_to_dbt_model(self, schema: BigQueryTableSchema) -> dict:
        """Translate a BigQueryTableSchema to a dbt model schema."""

        def recurse_types(field, include_name=False):
            if field.type == "RECORD":
                inner_types = []
                for sub_field in field.fields:
                    inner_types.append(recurse_types(sub_field, include_name=True))
                if field.mode == "REPEATED":
                    type = f"ARRAY<STRUCT<{', '.join(inner_types)}>>"
                else:
                    type = f"STRUCT<{', '.join(inner_types)}>"
            else:
                if field.mode == "REPEATED":
                    type = f"ARRAY<{field.type}>"
                else:
                    type = field.type

            if include_name:
                type = f"{field.name} {type}"
            return type

        def recursively_flatten_fields(fields, prefix=""):
            flat_fields = []
            for field in fields:
                field.name = prefix + field.name
                if field.fields:
                    flat_fields.extend(
                        recursively_flatten_fields(field.fields, field.name + ".")
                    )
                flat_fields.append(field)
            return flat_fields

        def recurse_type_fields(fields):
            for field in fields:
                field.type = recurse_types(field)
                if field.fields:
                    recurse_type_fields(field.fields)

        recurse_type_fields(schema.fields)
        schema.fields = recursively_flatten_fields(schema.fields)

        dict_schema = schema.model_dump()
        catalog_nodes = {}
        for field in dict_schema.get("fields"):
            catalog_nodes[field.get("name")] = field
        catalog_schema = {}
        catalog_schema["columns"] = catalog_nodes

        return DbtCatalogNode(**catalog_schema)

    def get_dbt_table_schema(self, model) -> BigQueryTableSchema:
        """get the schema of a dbt table and parse it into a common dbt model schema."""

        table_id = model.unique_id.split(".")[-1]
        schema = self._fetch_table_schema(model.database, model.db_schema, table_id)
        catalog_schema = self._translate_schema_to_dbt_model(schema)

        return catalog_schema

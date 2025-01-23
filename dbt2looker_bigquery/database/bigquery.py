# import google.auth
# from google.auth.transport.requests import Request
# import requests

# from dbt2looker_bigquery.database.models.bigqueryTable import BigQueryTableSchema


# class BigQueryDatabase:
#     def _fetch_table_schema(
#         project_id: str, dataset_id: str, table_id: str
#     ) -> BigQueryTableSchema:
#         """Fetch the schema of a BigQuery table and parse it into a Pydantic model."""
#         # Set up your authentication
#         credentials, _ = google.auth.default()

#         # Refresh the token
#         credentials.refresh(Request())

#         # Construct the URL
#         url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

#         # Make the request
#         headers = {"Authorization": f"Bearer {credentials.token}"}
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()  # Raise an error for bad status codes

#         # Extract schema information
#         table_info = response.json()

#         # Validate and parse schema using Pydantic
#         schema = BigQueryTableSchema(fields=table_info["schema"]["fields"])

#         return schema

#     def get_native_table_schema(self, table_name: str) -> BigQueryTableSchema:
#         """Fetch the schema of a dbt table and parse it into a Pydantic model."""
#         # Extract project_id, dataset_id, and table_id from the table_name
#         project_id, dataset_id, table_id = table_name.split(".")

#         # Fetch the schema
#         return self._fetch_table_schema(project_id, dataset_id, table_id)

#     def _translate_schema_to_dbt_model(self, schema: BigQueryTableSchema) -> dict:
#         """Translate a BigQueryTableSchema to a dbt model schema."""
#         # Convert the schema to a dbt model schema
#         dbt_schema = schema.to_dbt_model_schema()
#         return dbt_schema

#     def get_dbt_table_schema(self, table_name: str) -> BigQueryTableSchema:
#         """get the schema of a dbt table and parse it into a common dbt model schema."""
#         bigquery_schema = self.get_native_table_schema(table_name)

#         # Convert the schema to a dbt model schema
#         dbt_schema = bigquery_schema.to_dbt_model_schema()

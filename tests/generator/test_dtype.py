from dbt2looker_bigquery.models import SupportedDbtAdapters
from dbt2looker_bigquery.generator import map_adapter_type_to_looker

# Mock for logging to test if warning is logged correctly
from unittest.mock import patch

def test_map_adapter_type_to_looker_valid_input():
    adapter_type = SupportedDbtAdapters.bigquery
    column_type = 'INT64'
    expected_output = 'number'
    assert map_adapter_type_to_looker(adapter_type, column_type) == expected_output

def test_map_adapter_type_to_looker_unsupported_column():
    adapter_type = SupportedDbtAdapters.bigquery
    column_type = 'UNSUPPORTED_TYPE'
    assert map_adapter_type_to_looker(adapter_type, column_type) is None
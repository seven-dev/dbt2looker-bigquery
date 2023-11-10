import pytest
from dbt2looker_bigquery.models import DbtModelColumn, SupportedDbtAdapters
from dbt2looker_bigquery.generator import NotImplementedError, lookml_date_time_dimension_group, looker_time_timeframes, map_adapter_type_to_looker

@pytest.fixture
def mock_column():
    return DbtModelColumn(name="test_column", description="Test column", data_type="TIMESTAMP")

def test_lookml_date_time_dimension_group_valid_input(mock_column):
    adapter_type = SupportedDbtAdapters.bigquery
    expected_output = {
        'name': mock_column.name,
        'type': 'time',
        'sql': f'${{TABLE}}.{mock_column.name}',
        'description': mock_column.description,
        'datatype': map_adapter_type_to_looker(adapter_type, mock_column.data_type),
        'timeframes': looker_time_timeframes,
        'convert_tz': 'yes'
    }
    assert lookml_date_time_dimension_group(mock_column, adapter_type) == expected_output

def test_lookml_date_time_dimension_group_unsupported_data_type(mock_column):
    mock_column.data_type = "UNSUPPORTED_TYPE"
    adapter_type = SupportedDbtAdapters.bigquery
    with pytest.raises(NotImplementedError):
        lookml_date_time_dimension_group(mock_column, adapter_type)
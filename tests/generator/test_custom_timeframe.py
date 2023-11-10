from unittest.mock import MagicMock
import pytest

# Assuming your models are structured as in the provided code
from dbt2looker_bigquery.generator import lookml_date_time_dimension_group, map_adapter_type_to_looker, models

class MockLookerMeta:
    def __init__(self, timeframes):
        self.timeframes = timeframes

class MockMeta:
    def __init__(self, looker):
        self.looker = looker

@pytest.fixture
def mock_column():
    # Create the nested meta.looker structure with custom timeframes
    looker_meta = MockLookerMeta(timeframes=['raw', 'year', 'quarter'])
    meta = MockMeta(looker=looker_meta)

    # Create the DbtModelColumn mock with the nested meta
    mock = MagicMock(spec=models.DbtModelColumn)
    mock.name = "test_column"
    mock.description = "Test description"
    mock.data_type = "TIMESTAMP"
    mock.meta = meta

    return mock


def test_lookml_date_time_dimension_group_custom_timeframes(mock_column):
    adapter_type = models.SupportedDbtAdapters.bigquery  # Replace with actual enum if different
    expected_timeframes = mock_column.meta.looker.timeframes

    result = lookml_date_time_dimension_group(mock_column, adapter_type)

    assert 'timeframes' in result
    assert result['timeframes'] == expected_timeframes
import logging

import lkml
from . import models
from rich import print
class NotImplementedError(Exception):
    pass

LOOKER_DTYPE_MAP = {
    'bigquery': {
        'INT64':     'number',
        'INTEGER':   'number',
        'FLOAT':     'number',
        'FLOAT64':   'number',
        'NUMERIC':   'number',
        'BIGNUMERIC': 'number',
        'BOOLEAN':   'yesno',
        'STRING':    'string',
        'TIMESTAMP': 'timestamp',
        'DATETIME':  'datetime',
        'DATE':      'date',
        'TIME':      'string',    # Can time-only be handled better in looker?
        'BOOL':      'yesno',
        'ARRAY':     'string',
        'GEOGRAPHY': 'string',
        'BYTES':     'string',
        'ARRAY<STRING>': 'string',
        'ARRAY<INT64>': 'string',
        'ARRAY<FLOAT64>': 'string',
        'ARRAY<NUMERIC>': 'string',
        'ARRAY<BOOLEAN>': 'string',
        'ARRAY<TIMESTAMP>': 'string',
        'ARRAY<DATETIME>': 'string',
        'ARRAY<DATE>': 'string',
        'ARRAY<TIME>': 'string',
        'ARRAY<BOOL>': 'string',
        'ARRAY<BYTES>': 'string',
        'ARRAY<GEOGRAPHY>': 'string',
    }
}

looker_date_time_types = ['datetime', 'timestamp']
looker_date_types = ['date']
looker_scalar_types = ['number', 'yesno', 'string']

looker_date_timeframes = [
    'date',
    'day_of_month',
    'day_of_week',
    'day_of_week_index',
    'week',
    'week_of_year',
    'month_name',
    'month',
    'month_num',
    'quarter',
    'quarter_of_year',
    'year',
]

looker_time_timeframes = [
    'raw',
    'time',
    'time_of_day',
]

def map_adapter_type_to_looker(adapter_type: models.SupportedDbtAdapters, column_type: str):
    looker_type = LOOKER_DTYPE_MAP[adapter_type].get(column_type)
    if (column_type is not None) and (looker_type is None):
        logging.warning(f'Column type {column_type} not supported for conversion from {adapter_type} to looker. No dimension will be created.')
    return looker_type

# def lookml_date_time_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters):
#     if map_adapter_type_to_looker(adapter_type, column.data_type) is None:
#         raise NotImplementedError()
#     else:
#         date_time_group = {
#             'name': column.name,
#             'type': 'time',
#             'sql': f'${{TABLE}}.{column.name}',
#             'description': column.description,
#             'datatype': map_adapter_type_to_looker(adapter_type, column.data_type),
#             'timeframes': looker_time_timeframes,
#             'convert_tz': 'yes'
#         }
#         if column.meta.looker.timeframes != None:
#             date_time_group['timeframes'] = column.meta.looker.timeframes
                
#         date_time_group_set = {
#             'set':  {
#             'name' : f'dateset_{column.name}',
#             'fields': [
#                 f"{column.name}_{looker_time_timeframe}" for looker_time_timeframe in date_time_group['timeframes']
#             ]
#         }
#         }

#         date_time_group_list = []
#         date_time_group_list.append(date_time_group)
#         date_time_group_list.append(date_time_group_set)
        
#         return date_time_group_list

# def lookml_date_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters):
#     if map_adapter_type_to_looker(adapter_type, column.data_type) is None:
#         raise NotImplementedError()
#     else:
        
#         date_group = {
#             'name': column.name,
#             'type': 'time',
#             'sql': f'${{TABLE}}.{column.name}',
#             'description': column.description,
#             'datatype': map_adapter_type_to_looker(adapter_type, column.data_type),
#             'timeframes': looker_date_timeframes,
#             'convert_tz': 'no'
#         }

#         date_group_set = {
#             'set':  {
#             'name' : f'dateset_{column.name}',
#             'fields': [
#                 f"{column.name}_{looker_time_timeframe}" for looker_time_timeframe in date_group['timeframes']
#             ]
#         }
#         }

#         date_group_list = []
#         date_group_list.append(date_group)
#         date_group_list.append(date_group_set)

#         return date_group_list

def lookml_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters, type: str):

    if map_adapter_type_to_looker(adapter_type, column.data_type) is None:
        raise NotImplementedError()
    else:
        if type == 'time':
            convert_tz = 'yes'
            timeframes = looker_time_timeframes
        elif type == 'date':
            convert_tz = 'no'
            timeframes = looker_date_timeframes
        else:
            raise NotImplementedError()

        dimension_group = {
            'name': column.name,
            'type': 'time',
            'sql': f'${{TABLE}}.{column.name}',
            'description': column.description,
            'datatype': map_adapter_type_to_looker(adapter_type, column.data_type),
            'timeframes': timeframes,
            'convert_tz': convert_tz
        }

        dimension_group_set = {
            'set':  {
            'name' : f's_{column.name}',
            'fields': [
                f"{column.name}_{looker_time_timeframe}" for looker_time_timeframe in timeframes
            ]
        }
        }

        return dimension_group, dimension_group_set


def lookml_dimension_groups_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):

    dimension_groups = []
    dimension_group_sets = []

    for column in model.columns.values(): 
        if map_adapter_type_to_looker(adapter_type, column.data_type) in looker_date_time_types: 
            dimension_group, dimension_set = lookml_dimension_group(column, adapter_type, 'time')            
        elif map_adapter_type_to_looker(adapter_type, column.data_type) in looker_date_types:
            dimension_group, dimension_set = lookml_dimension_group(column, adapter_type, 'date')            
        else:
            continue

        dimension_groups.append(dimension_group)
        dimension_group_sets.append(dimension_set)

    return {'dimension_groups' : dimension_groups, 'dimension_group_sets': dimension_group_sets}

def lookml_dimensions_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    dimensions = []
    is_first_dimension = True  # Flag to identify the first dimension

    for column in model.columns.values():
        is_hidden = False

        if map_adapter_type_to_looker(adapter_type, column.data_type) in looker_scalar_types:
            dimension = {
                'name': column.name,
                'type': map_adapter_type_to_looker(adapter_type, column.data_type),
                'sql': f'${{TABLE}}.{column.name}',
                'description': column.description,
            }

            if is_first_dimension:
                dimension['primary_key'] = 'yes'
                is_first_dimension = False  # Unset the flag after processing the first dimension
                is_hidden = True
                dimension['value_format_name'] = 'id'

            if column.meta.looker.group_label != None:
                dimension['group_label'] = column.meta.looker.group_label
            if column.meta.looker.label  != None:
                dimension['label'] = column.meta.looker.label

            if column.meta.looker.group_label != None:
                dimension['hidden'] = 'yes' if column.meta.looker.hidden == True else 'no'
            elif is_hidden:
                dimension['hidden'] = 'yes'

            if column.meta.looker.value_format_name != None:
                dimension['value_format_name'] = column.meta.looker.value_format_name.value

            dimensions.append(dimension)

    return dimensions

def lookml_measures_from_model(model: models.DbtModel):
    # Initialize an empty list to hold all lookml measures.
    lookml_measures = []

    # Iterate over all columns in the model.
    for column in model.columns.values():
        if hasattr(column.meta, 'looker_measures'):
            # For each measure found in the combined dictionary, create a lookml_measure.
            for measure in column.meta.looker_measures:
                # Call the lookml_measure function and append the result to the list.
                lookml_measures.append(lookml_measure(column, measure))

    # Return the list of lookml measures.
    return lookml_measures

def lookml_measure(column: models.DbtModelColumn, measure: models.DbtMetaMeasure):
    m = {
        'name': f'm_{measure.type.value}_{column.name}',
        'type': measure.type.value,
        'sql': f'${{{column.name}}}',
        'description': f'{measure.type.value} of {column.name}',
    }

    # inherit the value format, or overwrite it
    if measure.value_format_name != None:
        m['value_format_name'] = measure.value_format_name.value
    elif column.meta.looker.value_format_name != None:
        m['value_format_name'] = column.meta.looker.value_format_name.value        

    # allow configuring advanced lookml measures
    if measure.sql != None:
        m['sql'] = measure.sql
        m['type'] = 'number'

    if measure.group_label != None:
        m['group_label'] = measure.group_label
    if measure.label != None:
        m['label'] = measure.label
    if measure.hidden != None:
        m['hidden'] = measure.hidden.value
    return m

def lookml_view_from_dbt_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    ''' Create a looker view from a dbt model '''
    lookml = {
        'view': {
            'name': model.name,
            'sql_table_name': model.relation_name,
            'dimensions': lookml_dimensions_from_model(model, adapter_type),
            'dimension_groups': lookml_dimension_groups_from_model(model, adapter_type).get('dimension_groups'),
            'sets' : lookml_dimension_groups_from_model(model, adapter_type).get('dimension_group_sets'),
            'measures': lookml_measures_from_model(model),
        }
    }

    # Add 'label' only if it exists
    if hasattr(model.meta.looker, 'label'):
        lookml['view']['label'] = model.meta.looker.label

    logging.debug(
        f'Created view from model %s with %d measures, %d dimensions, %d dimension groups',
        model.name,
        len(lookml['view']['measures']),
        len(lookml['view']['dimensions']),
        len(lookml['view']['dimension_groups']),
    )

    try: 
        contents = lkml.dump(lookml)
    except TypeError as e:
        print(f"{e} : {lookml}")
    filename = f'{model.name}.view.lkml'
    return models.LookViewFile(filename=filename, contents=contents, schema=model.db_schema)
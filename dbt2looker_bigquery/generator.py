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
        'ARRAY':     'array',
        'STRUCT':    'struct',
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
    if adapter_type == 'bigquery' and column_type:
        column_type = column_type.split('<')[0]

    looker_type = LOOKER_DTYPE_MAP[adapter_type].get(column_type)
    if (column_type is not None) and (looker_type is None):
        logging.warning(f'Column type {column_type} not supported for conversion from {adapter_type} to looker. No dimension will be created.')
    return looker_type

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
            'name' : f's_{column.name}',
            'fields': [
                f"{column.name}_{looker_time_timeframe}" for looker_time_timeframe in timeframes
            ]
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
        if 'season_episode' in model.name:
            # print(f"{model.name} Column {column.name} has data type {column.data_type}")
            if column.data_type is None:
                continue
                # print(f"{model.name} Column {column.name} has no data type")
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


def process_columns(model):
    parent_group = {}

    # Initialize parent_group with model columns.
    for column in model.columns.values():
        if column.nested:
            # Add nested columns under their direct parents.
            parent_group.setdefault(column.parent_name, []).append(column.name)

    def flatten_parents(parents_dict):
        updates = {}

        for parent, children in list(parents_dict.items()):
            if "." in parent:
                top_parent, sub_parent = parent.split(".", 1)

                updates.setdefault(top_parent, [])

                new_entry = {sub_parent: children}
                existing_entry = next((item for item in updates[top_parent] if isinstance(item, dict) and sub_parent in item), None)

                if existing_entry:
                    existing_entry[sub_parent].extend(children)
                else:
                    updates[top_parent].append(new_entry)

                del parents_dict[parent]

        for key, value_list in updates.items():
            current_children = parents_dict.get(key, []) or []

            for value_item in value_list:
                value_exists = any("." in k for k in value_item)

                if isinstance(value_item, dict):
                    child_key = next(iter(value_item))

                    for existing_item in current_children:
                        if isinstance(existing_item, dict) and child_key in existing_item:
                            existing_item[child_key].extend(value_item[child_key])
                            value_exists = True
                            break

                    if not value_exists:
                        current_children.append(value_item)

                elif not isinstance(value_item, dict):
                    current_children.append(value_item)

            parents_dict[key] = current_children

        return any("." in k for k in parents_dict)

    while flatten_parents(parent_group):
        pass

    return parent_group

def flatten_parents(parents_dict):
    updates = {}

    for parent, children in list(parents_dict.items()):
        if "." in parent:
            top_parent, sub_parent = parent.split(".", 1)

            updates.setdefault(top_parent, [])

            new_entry = {sub_parent: children}
            existing_entry = next((item for item in updates[top_parent] if isinstance(item, dict) and sub_parent in item), None)

            if existing_entry:
                existing_entry[sub_parent].extend(children)
            else:
                updates[top_parent].append(new_entry)

            del parents_dict[parent]

    for key, value_list in updates.items():
        current_children = parents_dict.get(key, []) or []

        for value_item in value_list:
            if isinstance(value_item, dict):
                child_key = next(iter(value_item))
                current_children.append({child_key: value_item[child_key]})
            else:
                current_children.append(value_item)

        parents_dict[key] = current_children

    return any("." in k for k in parents_dict)

def lookml_view_from_dbt_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    ''' Create a looker view from a dbt model '''
    parent_group = process_columns(model)

    if parent_group:
        print(parent_group)

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
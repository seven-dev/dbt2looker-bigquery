import lkml
from . import models
from rich import print
import logging

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
        'GEOGRAPHY': 'string',
        'BYTES':     'string',
        'ARRAY':     'string',
        'STRUCT':    'string',
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

def lookml_dimension_group(column: models.DbtModelColumn, adapter_type: models.SupportedDbtAdapters, type: str, table_format_sql=True, model=None):

    table_format_sql = True
    
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
            'name': column.lookml_name,
            'type': 'time',
            'sql': f'${{TABLE}}.{column.name}' if table_format_sql else f'{model.name}__{column.name}',
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


def lookml_dimension_groups_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters, include_names=None, exclude_names=[]):

    dimension_groups = []
    dimension_group_sets = []
    table_format_sql = True

    for column in model.columns.values(): 

        if include_names:
            table_format_sql = False
            if column.inner_types is not None:
                if len(column.inner_types) == 1:
                    column.data_type = column.inner_types[0]
            if column.name not in include_names:
                continue
        if len(exclude_names) > 0:
            if column.name in exclude_names:
                continue

        if map_adapter_type_to_looker(adapter_type, column.data_type) in looker_date_time_types: 
            dimension_group, dimension_set = lookml_dimension_group(column, adapter_type, 'time', table_format_sql, model)            
        elif map_adapter_type_to_looker(adapter_type, column.data_type) in looker_date_types:
            dimension_group, dimension_set = lookml_dimension_group(column, adapter_type, 'date', table_format_sql, model)            
        else:
            continue

        dimension_groups.append(dimension_group)
        dimension_group_sets.append(dimension_set)

    return {'dimension_groups' : dimension_groups, 'dimension_group_sets': dimension_group_sets}

def lookml_dimensions_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters, include_names=None, exclude_names=[]):
    dimensions = []
    is_first_dimension = True  # Flag to identify the first dimension
    table_format_sql = True
    is_hidden = False

    for column in model.columns.values():

        if include_names:
            table_format_sql = False
            # print(column)
            if column.inner_types is not None:
                if len(column.inner_types) == 1:
                    column.data_type = column.inner_types[0]
                    is_first_dimension = False

            if column.name not in include_names:
                continue

        if len(exclude_names) > 0:
            # we want to exclude nested data within arrays
            # but we want to retain the array itself
            if column.name in exclude_names and not (len(column.inner_types) == 1 and column.inner_types is not None):
                print(f"excluding {column.name}")
                print(f"inner types {column.inner_types}")
                print(f"exclude names {exclude_names}")
                print(f"len(inner_types) {len(column.inner_types)}")
                continue

        if map_adapter_type_to_looker(adapter_type, column.data_type) in looker_scalar_types:

            dimension = {
                'name': column.lookml_name,
                'type': map_adapter_type_to_looker(adapter_type, column.data_type),
                'sql': f'${{TABLE}}.{column.name}' if table_format_sql else f'{model.name}__{column.name}',
                'description': column.description,
            }
            
            if 'ARRAY' in column.data_type :
                dimension['hidden'] = 'yes'
                dimension['tags'] = ['array']
                dimension.pop('type')

            if 'STRUCT' in column.data_type :
                dimension['hidden'] = 'yes'
                dimension['tags'] = ['struct']
                # dimension.pop('type')

            if is_first_dimension == True:
                dimension['primary_key'] = 'yes'
                is_first_dimension = False  # Unset the flag after processing the first dimension
                is_hidden = True
                dimension['value_format_name'] = 'id'

            if column.meta.looker.group_label != None:
                dimension['group_label'] = column.meta.looker.group_label
            if column.meta.looker.label  != None:
                dimension['label'] = column.meta.looker.label

            if column.meta.looker.hidden != None:
                dimension['hidden'] = 'yes' if column.meta.looker.hidden == True else 'no'
            elif is_hidden:
                dimension['hidden'] = 'yes'

            if column.meta.looker.value_format_name != None:
                dimension['value_format_name'] = column.meta.looker.value_format_name.value

            is_hidden = False
            dimensions.append(dimension)
    return dimensions

def lookml_measures_from_model(model: models.DbtModel, include_names=None, exclude_names=[]):
    # Initialize an empty list to hold all lookml measures.
    lookml_measures = []
    table_format_sql = True

    # Iterate over all columns in the model.
    for column in model.columns.values():
        
        if include_names:
            table_format_sql = False
            if column.name not in include_names:
                continue
        if len(exclude_names) > 0:

            if column.name in exclude_names:
                continue

        if hasattr(column.meta, 'looker_measures'):
            # For each measure found in the combined dictionary, create a lookml_measure.
            for measure in column.meta.looker_measures:
                # Call the lookml_measure function and append the result to the list.
                lookml_measures.append(lookml_measure(column, measure, table_format_sql, model))

    # Return the list of lookml measures.
    return lookml_measures

def lookml_measure(column: models.DbtModelColumn, measure: models.DbtMetaMeasure, table_format_sql, model):
    
    m = {
        'name': f'm_{measure.type.value}_{column.name}',
        'type': measure.type.value,
        'sql':  f'${{{column.name}}}' if table_format_sql else f'${{{model.name}__{column.name}}}',
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


def extract_array_models(columns: list[models.DbtModelColumn])->list[models.DbtModelColumn]:
    ''' Process columns to determine if they are nested 
        and if so, what the parent group is
    '''
    array_list = []

    # Initialize parent_list with all columns that are arrays
    for column in columns:
        if column.data_type is not None:
            if 'ARRAY' == column.data_type:
                array_list.append(column)
    return array_list



def group_strings(all_columns:list[models.DbtModelColumn], array_columns:list[models.DbtModelColumn]):
    nested_columns = {}

    # def find_closest_parent(model : models.DbtModelColumn, array_models:list[models.DbtModelColumn]):
    #     """
    #     Find the closest parent from the parent_list for the target string.
    #     """
    #     for array in array_models:
    #         if len(array.inner_types) == 1:
    #             if model.name == array.name:
    #                 return array
    #         else:
    #             if model.name.startswith(array.name) and model.name != array.name:
    #                 return array
    #     return None
    
    def remove_parts(input_string):
        parts = input_string.split('.')
        modified_parts = parts[:-1]
        result = '.'.join(modified_parts)
        return result
    
    def recurse(parent: models.DbtModelColumn, all_columns:list[models.DbtModelColumn], level = 0):
        structure = {
            'column' : parent,
            'children' : []
        }

        print(f"level {level}, {parent.name}")
        for column in all_columns:
            # singleton array handling
            if column.name == parent.name:
                if column.inner_types is not None:
                    if len(column.inner_types) == 1:
                        print(f"column {column.name} is a array child of {parent.name}")
                        structure['children'].append({column.name : {'column' : column, 'children' : []}})

            # descendant handling
            elif remove_parts(column.name) == parent.name:
                print(f"column {column.name} is a direct descendant of {parent.name}")

                structure['children'].append({column.name : recurse(
                            parent = column,
                            all_columns = [d for d in all_columns if d.name.startswith(column.name)],
                            level=level+1)})                

            elif column.name.startswith(parent.name):
                print(f"column {column.name} is a nested child of {parent.name}")

                structure['children'].append({column.name : recurse(
                            parent = column,
                            all_columns = [d for d in all_columns if d.name.startswith(column.name)],
                            level=level+1)})                

        return structure

    for parent in array_columns:
        # start with the top level arrays
        if not '.' in parent.name:
            nested_columns[parent.name] = recurse(parent, all_columns)

    return nested_columns


def lookml_view_from_dbt_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    ''' Create a looker view from a dbt model 
        if the model has nested arrays, create a view for each array
        and an explore that joins them together
    '''
    array_models = extract_array_models(model.columns.values())
    structure = group_strings(model.columns.values(), array_models)
    lookml = {}
    lookml_list = []

    # Add 'label' only if it exists
    if hasattr(model.meta.looker, 'label'):
        view_label = model.meta.looker.label
    elif hasattr(model, 'name'):
        view_label = model.name.replace("_", " ").title()
    else:
        view_label = "MISSING"

    def recurse_views(structure, d):
        view_list = []
        used_names = []

        for parent, children in structure.items():
            children_names = []

            for child_strucure in children['children']:
                for child_name, child_dict in child_strucure.items():
                    children_names.append(child_name)
                    if len((child_dict['children'])) > 0:
                        recursed_view_list, recursed_names = recurse_views(child_strucure, d=d+1)
                        view_list.extend(recursed_view_list)
                        used_names.extend(recursed_names)
            print(f"adding view for {parent} d {d}")
            view_list.append(
                {
                    'name': model.name + "__" + parent.replace('.','__') ,
                    'label': view_label + " : " + parent.replace("_", " ").title(),
                    'dimensions': lookml_dimensions_from_model(model, adapter_type, include_names=children_names),
                    'dimension_groups': lookml_dimension_groups_from_model(model, adapter_type, include_names=children_names).get('dimension_groups'),
                    'sets' : lookml_dimension_groups_from_model(model, adapter_type, include_names=children_names).get('dimension_group_sets'),
                    'measures': lookml_measures_from_model(model, include_names=children_names),
                }
            )
            used_names.extend(children_names)
        return view_list, used_names

    # this is for handling arrays
    if structure:
        view_list, used_names = recurse_views(structure, 1)
        print(view_list)
        lookml_list.append(view_list)

    lookml_view = [
        {
            'name': model.name,
            'label': view_label,
            'sql_table_name': model.relation_name,
            'dimensions': lookml_dimensions_from_model(model, adapter_type, exclude_names=used_names),
            'dimension_groups': lookml_dimension_groups_from_model(model, adapter_type, exclude_names=used_names).get('dimension_groups'),
            'sets' : lookml_dimension_groups_from_model(model, adapter_type, exclude_names=used_names).get('dimension_group_sets'),
            'measures': lookml_measures_from_model(model, exclude_names=used_names),
        }
    ]

    lookml_list.append(lookml_view)

    def rael(input_string):
        ''' replace all but the last period with a replacement string 
            this is used to create unique names for joins
        '''
        sign = '.'
        replacement = '__'
        
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

    def recurse_joins(structure, parent_name):
        join_list = []
        for parent, children in structure.items():
            for child_strucure in children['children']:
                for child_name, child_dict in child_strucure.items():
                    if len((child_dict['children'])) > 0:
                        recursed_join_list = recurse_joins(child_strucure, child_name)
                        join_list.extend(recursed_join_list)
            join_list.append(
                {
                    'sql' : f'LEFT JOIN UNNEST(${{{rael(model.name+"."+parent)}}}) AS {model.name}__{parent.replace(".","__")}',
                    'relationship': 'one_to_many',
                    'name': model.name + "__" + parent.replace('.','__'),
                }
            )
        return join_list

    if len(array_models) > 0:
        lookml_explore = [
        {
            'name': model.name,
            'joins': [],
            'hidden': 'yes'
        }
        ]
        lookml_explore[0]['joins'].extend(recurse_joins(structure, model.name))
        lookml = {
            'explore': lookml_explore,
            'view': lookml_list,
        }        
    else:
        lookml = {
            'view': lookml_list,
        }

    try: 
        contents = lkml.dump(lookml)
    except TypeError as e:
        logging.warning(f"TYPEERROR {e}")
    filename = f'{model.name}.view.lkml'
    return models.LookViewFile(filename=filename, contents=contents, schema=model.db_schema)
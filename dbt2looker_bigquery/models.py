from enum import Enum
from typing import Union, Dict, List, Optional
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
from pydantic import BaseModel, Field, PydanticValueError, validator, root_validator
from . import looker_enums
from rich import print
# dbt2looker utility types
class UnsupportedDbtAdapterError(PydanticValueError):
    code = 'unsupported_dbt_adapter'
    msg_template = '{wrong_value} is not a supported dbt adapter'

class SupportedDbtAdapters(str, Enum):
    ''' Supported dbt adapters, only bigquery for now. '''
    bigquery = 'bigquery'

class DbtNode(BaseModel):
    ''' A dbt node. extensible to models, seeds, etc.'''
    unique_id: str
    resource_type: str

class DbtCatalogNodeMetadata(BaseModel):
    ''' Metadata about a dbt catalog node '''
    type: str
    db_schema: str = Field(..., alias='schema')
    name: str
    comment: Optional[str]
    owner: Optional[str]

class DbtCatalogNodeColumn(BaseModel):
    ''' A column in a dbt catalog node '''
    type: str
    comment: Optional[str]
    index: int
    name: str

class DbtCatalogNode(BaseModel):
    ''' A dbt catalog node '''
    metadata: DbtCatalogNodeMetadata
    columns: Dict[str, DbtCatalogNodeColumn]

    @validator('columns')
    def case_insensitive_column_names(cls, v: Dict[str, DbtCatalogNodeColumn]):
        return {
            name.lower(): column.copy(update={'name': column.name.lower()})
            for name, column in v.items()
        }

class DbtCatalog(BaseModel):
    ''' A dbt catalog '''
    nodes: Dict[str, DbtCatalogNode]

class LookViewFile(BaseModel):
    ''' A file in a looker view directory '''
    filename: str
    contents: str

class DbtMetaLooker(BaseModel):
    ''' Looker-specific metadata for a dbt model '''
    hidden: Optional[bool] = None
    label: Optional[str] = None
    group_label: Optional[str] = None
    value_format_name: Optional[looker_enums.LookerValueFormatName] = None

class DbtMetaMeasure(DbtMetaLooker):
    ''' A measure defined in a dbt model'''
    type: looker_enums.LookerMeasureType
    description: Optional[str] = Field(None, alias='description')
    sql: Optional[str] = None

class DbtModelColumnMeta(BaseModel):
    ''' Metadata about a column in a dbt model '''
    looker: Optional[DbtMetaLooker] = DbtMetaLooker()
    looker_measures: Optional[List[DbtMetaMeasure]] = []

class DbtModelColumn(BaseModel):
    ''' A column in a dbt model '''
    name: str
    description: Optional[str]
    data_type: Optional[str]
    meta: Optional[DbtModelColumnMeta] = DbtModelColumnMeta()
    nested: Optional[bool] = False
    parent_name: Optional[str] = None

    # Root validator
    @root_validator(pre=True)
    def set_nested_and_parent_name(cls, values):
        name = values.get('name', '')
        # If there's a dot in the name, it's a nested field
        if '.' in name:
            parent, _, nested_name = name.rpartition('.')
            values['nested'] = True
            values['parent_name'] = parent
            values['name'] = nested_name  # Update the name to just the nested field's name
        return values

class DbtModelMeta(BaseModel):
    ''' Metadata about a dbt model '''
    pass

class DbtModel(DbtNode):
    ''' A dbt model '''
    resource_type: Literal['model']
    relation_name: str
    db_schema: str = Field(..., alias='schema')
    name: str
    description: str
    columns: Dict[str, DbtModelColumn]
    tags: List[str]
    meta: DbtModelMeta

    @validator('columns')
    def case_insensitive_column_names(cls, v: Dict[str, DbtModelColumn]):
        new_columns = {}
        for name, column in v.items():
            # Skip the entry if the name is not a string
            if not isinstance(name, str):
                raise TypeError(f"The key {name} is not a string.")
            # Skip the entry if the column is a dict, not a DbtModelColumn instance
            if isinstance(column, dict):
                new_columns[name] = column
            else:
                # Ensure the column is a DbtModelColumn instance before proceeding
                if not isinstance(column, DbtModelColumn):
                    raise TypeError(f"The value for key {name} is not a DbtModelColumn instance.")
                # Lowercase the name and update the column name
                new_columns[name.lower()] = column.copy(update={'name': column.name.lower()})
        return new_columns


class DbtManifestMetadata(BaseModel):
    adapter_type: str

    @validator('adapter_type')
    def adapter_must_be_supported(cls, v):
        try:
            SupportedDbtAdapters(v)
        except ValueError:
            raise UnsupportedDbtAdapterError(wrong_value=v)
        return v

class DbtManifest(BaseModel):
    nodes: Dict[str, Union[DbtModel, DbtNode]]
    metadata: DbtManifestMetadata

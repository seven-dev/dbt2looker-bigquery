import logging
from typing import Dict, Optional, List
from . import models as models

def parse_catalog_nodes(raw_catalog: dict):
    catalog = models.DbtCatalog(**raw_catalog)
    return catalog.nodes

def parse_adapter_type(raw_manifest: dict):
    manifest = models.DbtManifest(**raw_manifest)
    return manifest.metadata.adapter_type

def tags_match(query_tag: str, model: models.DbtModel) -> bool:
    try:
        return query_tag in model.tags
    except AttributeError:
        return False
    except ValueError:
        return query_tag == model.tags

def parse_models(raw_manifest: dict, tag=None, exposures_only=False) -> List[models.DbtModel]:
    '''Parse dbt models from manifest and filter by tag if provided'''

    manifest = models.DbtManifest(**raw_manifest)

    for node in manifest.nodes.values():
        if node.resource_type == 'model':
            logging.debug('Found model %s', node)

    all_models: List[models.DbtModel] = [
        node
        for node in manifest.nodes.values()
        if node.resource_type == 'model' and hasattr(node, 'name')
    ]

    if exposures_only == True:
        all_exposures = [
            node
            for node in manifest.exposures.values()
            if node.resource_type == 'exposure' and hasattr(node, 'name')
        ]
        exposed_model_names: List[str] = get_exposed_models(all_exposures)
        

    for model in all_models:
        if not hasattr(model, 'name'):
            logging.error('Cannot parse model with id: "%s" - is the model file empty?', model.unique_id)
            raise SystemExit('Failed')

    if tag is None and exposures_only == False:
        return all_models
    elif tag is None and exposures_only == True:
        return [model for model in all_models if model.name in exposed_model_names]
    elif tag is not None and exposures_only == False:
        return [model for model in all_models if tags_match(tag, model)]
    elif tag is not None and exposures_only == True:
        return [model for model in all_models if tags_match(tag, model) and model.name in exposed_model_names]
    else:
        logging.error('Invalid combination of arguments')
        raise SystemExit('Failed')


def check_models_for_missing_column_types(dbt_typed_models: List[models.DbtModel]):
    for model in dbt_typed_models:
        if all([col.data_type is None for col in model.columns.values()]):
            logging.debug('Model %s has no typed columns, no dimensions will be generated. %s', model.unique_id, model)

def get_column_type_from_catalog(catalog_nodes: Dict[str, models.DbtCatalogNode], model_id: str, column_name: str):
    node = catalog_nodes.get(model_id)
    column = None if node is None else node.columns.get(column_name)
    return None if column is None else column.type

def get_exposed_models(exposures: List[models.DbtExposure]) -> list:
    # Print exposures
    exposed_models = []
    for exposure in exposures:
            for ref in exposure.refs:
                exposed_models.append(ref.name)

    unique_exposed_models = list(set(exposed_models))
    return unique_exposed_models

def parse_typed_models(raw_manifest: dict, raw_catalog: dict, tag: Optional[str] = None, exposures_only: bool = False):
    catalog_nodes = parse_catalog_nodes(raw_catalog)
    dbt_models = parse_models(raw_manifest, tag=tag, exposures_only=exposures_only)
    adapter_type = parse_adapter_type(raw_manifest)
    logging.debug('Found manifest entries for %d models', len(dbt_models))

    for model in dbt_models:
        logging.debug(
            'Model %s has %d columns with %d measures',
            model.name,
            len(model.columns),
            sum([len(col.meta.looker_measures) for col in model.columns.values()])
        )
        if model.unique_id not in catalog_nodes:
            logging.warning(
                f'Model {model.unique_id} not found in catalog. No looker view will be generated. '
                f'Check if model has materialized in {adapter_type} at {model.relation_name}')

    # Update dbt models with data types from catalog
    dbt_typed_models = [
        model.model_copy(update={'columns': {
            column.name: column.model_copy(update={
                'data_type': get_column_type_from_catalog(catalog_nodes, model.unique_id, column.name)
            })
            for column in model.columns.values()
        }})
        for model in dbt_models
        if model.unique_id in catalog_nodes
    ]
    logging.debug('Found catalog entries for %d models', len(dbt_typed_models))
    logging.debug('Catalog entries missing for %d models', len(dbt_models) - len(dbt_typed_models))
    check_models_for_missing_column_types(dbt_typed_models)
    return dbt_typed_models



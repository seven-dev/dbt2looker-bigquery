from dbt2looker_bigquery.generators.utils import MetaAttributeApplier
from unittest.mock import Mock


def test_apply():
    # Test data
    cli_args = Mock()
    cli_args.all_hidden = False
    target_dict = {"name": "1"}
    obj = Mock()
    obj.meta = Mock()
    obj.meta.looker.view.label = "value1"
    obj.meta.looker.view.hidden = True
    attributes = ["label", "hidden"]
    # Initialize the class
    meta_attribute_applier = MetaAttributeApplier(cli_args)

    # Call the method
    meta_attribute_applier.apply_meta_attributes(
        target_dict, obj, attributes, "meta.looker.view"
    )

    # Assert the results
    assert len(target_dict) == 3
    assert target_dict["name"] == "1"
    assert target_dict["label"] == "value1"
    assert target_dict["hidden"] == "yes"


def test_apply_meta_overwrite():
    # Test data
    cli_args = Mock()
    cli_args.all_hidden = False
    target_dict = {"label": "1"}
    obj = Mock()
    obj.meta = Mock()
    obj.meta.label = "2"
    attributes = ["label"]

    # Initialize the class
    meta_attribute_applier = MetaAttributeApplier(cli_args)

    # Call the method
    meta_attribute_applier.apply_meta_attributes(target_dict, obj, attributes, "meta")

    # Assert the results
    assert target_dict["label"] == "2"


def test_missing_apply_meta_attributes():
    # Test data
    cli_args = Mock()
    cli_args.all_hidden = False
    target_dict = {}
    obj = {"meta": None}
    attributes = ["label"]

    # Initialize the class
    meta_attribute_applier = MetaAttributeApplier(cli_args)

    # Call the method
    meta_attribute_applier.apply_meta_attributes(target_dict, obj, attributes, "meta")

    # Assert the results
    assert type(target_dict) is dict
    assert len(target_dict) == 0
    assert "label" not in target_dict


def test_apply_meta_attributes():
    # Test data
    cli_args = Mock()
    cli_args.all_hidden = False
    target_dict = {}
    obj = Mock()
    obj.meta = Mock()
    obj.meta.attr1 = "value1"
    obj.meta.attr2 = True
    attributes = ["attr1", "attr2"]

    # Initialize the class
    meta_attribute_applier = MetaAttributeApplier(cli_args)

    # Call the method
    meta_attribute_applier.apply_meta_attributes(target_dict, obj, attributes, "meta")

    # Assert the results
    assert target_dict["attr1"] == "value1"
    assert target_dict["attr2"] == "yes"


def test_apply_meta_attributes_with_all_hidden():
    # Test data
    cli_args = Mock()
    cli_args.all_hidden = True
    target_dict = {}
    obj = Mock()
    obj.meta = Mock()
    obj.meta.attr1 = "value1"
    attributes = ["attr1"]

    # Initialize the class
    meta_attribute_applier = MetaAttributeApplier(cli_args)

    # Call the method
    meta_attribute_applier.apply_meta_attributes(target_dict, obj, attributes, "meta")

    # Assert the results
    assert target_dict["attr1"] == "value1"
    assert target_dict["hidden"] == "yes"


def test_get_meta_object():
    # Test data
    obj = Mock()
    obj.meta = Mock()
    obj.meta.attr1 = "value1"
    path = "meta"

    # Initialize the class
    meta_attribute_applier = MetaAttributeApplier(Mock())

    # Call the method
    result = meta_attribute_applier._get_meta_object(obj, path)

    # Assert the results
    assert result == obj.meta


def test_get_meta_value():
    # Test data
    value = "value1"
    attr = "attr1"

    # Initialize the class
    meta_attribute_applier = MetaAttributeApplier(Mock())

    # Call the method
    result = meta_attribute_applier._get_meta_value(value, attr)

    # Assert the results
    assert result == "value1"

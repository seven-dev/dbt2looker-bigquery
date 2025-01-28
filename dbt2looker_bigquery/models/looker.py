from typing import List, Optional, Union
import logging
from pydantic import BaseModel, Field, model_validator, field_validator, ValidationError
import warnings
from dbt2looker_bigquery.warnings import DeprecationWarning

from dbt2looker_bigquery.enums import (
    # LookerJoinType,
    LookerMeasureType,
    # LookerRelationshipType,
    LookerTimeFrame,
    LookerValueFormatName,
)


class LookerView(BaseModel):
    """Looker metadata for a view"""

    name: str
    sql_table_name: Optional[str] = Field(default=None)
    label: Optional[str] = Field(default=None)
    hidden: Optional[bool] = Field(default=None)
    dimensions: Optional[List[dict]] = Field(default=[])
    measures: Optional[List[dict]] = Field(default=[])
    sets: Optional[List[dict]] = Field(default=[])
    dimension_groups: Optional[List[dict]] = Field(default=[])

    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            raise KeyError(
                f"{key} is not a valid attribute for {self.__class__.__name__}"
            )


class DbtMetaLookerBase(BaseModel):
    label: Optional[str] = None
    hidden: Optional[bool] = None
    description: Optional[str] = None


class DbtMetaLookerMeasureFilter(BaseModel):
    filter_dimension: str
    filter_expression: str


class DbtMetaLookerViewElement(DbtMetaLookerBase):
    """Looker metadata for a view element."""

    value_format_name: Optional[LookerValueFormatName] = Field(default=None)
    filters: Optional[List[DbtMetaLookerMeasureFilter]] = None
    group_label: Optional[str] = None

    @field_validator("value_format_name", mode="before")
    def validate_format_name(cls, value):
        if value is not None:
            if isinstance(value, str):
                value = value.strip()
                if not LookerValueFormatName.has_value(value):
                    logging.warning(
                        f"Invalid value for value_format_name [{value}]. Setting to None."
                    )
                    return None
                else:
                    return LookerValueFormatName(value)
        return value


class DbtMetaLookerMeasure(DbtMetaLookerViewElement):
    """Looker metadata for a measure."""

    # Required fields
    type: LookerMeasureType

    # Common optional fields
    name: Optional[str] = None

    # Fields specific to certain measure types
    approximate: Optional[bool] = None  # For count_distinct
    approximate_threshold: Optional[int] = None  # For count_distinct
    precision: Optional[int] = None  # For average, sum
    sql_distinct_key: Optional[str] = None  # For count_distinct
    percentile: Optional[int] = None  # For percentile measures

    @model_validator(mode="before")
    def validate_measure_attributes(cls, values):
        """Validate that measure attributes are compatible with the measure type."""
        if type(values) is dict:
            measure_type = values.get("type")

            if any(
                v is not None
                for v in [
                    values.get("approximate"),
                    values.get("approximate_threshold"),
                    values.get("sql_distinct_key"),
                ]
            ) and measure_type not in (
                LookerMeasureType.COUNT_DISTINCT,
                LookerMeasureType.SUM_DISTINCT,
            ):
                raise ValueError(
                    "approximate, approximate_threshold, and sql_distinct_key can only be used with distinct measures"
                )

            if values.get(
                "percentile"
            ) is not None and not measure_type.value.startswith("percentile"):
                raise ValueError("percentile can only be used with percentile measures")

            if values.get("precision") is not None and measure_type not in [
                LookerMeasureType.AVERAGE,
                LookerMeasureType.SUM,
            ]:
                raise ValueError(
                    "precision can only be used with average or sum measures"
                )

        return values


class DbtMetaLookerDimension(DbtMetaLookerViewElement):
    """Looker-specific metadata for a dimension on a dbt model column

    meta:
        looker:
            dimension:
                hidden: True
                label: "Blog Info"
                group_label: "Blog Info"
                description: "Blog Info"
                value_format_name: decimal_0
                filters:
                    - path: "/blog%"

    """

    convert_tz: Optional[bool] = Field(default=None)
    timeframes: Optional[List[LookerTimeFrame]] = Field(default=None)
    can_filter: Optional[Union[bool, str]] = Field(default=None)

    @field_validator("timeframes", mode="before")
    def check_enums(cls, values):
        if values is not None:
            if isinstance(values, list[str]):
                timeframes = values
                valid_timeframes = [
                    tf for tf in timeframes if isinstance(tf, LookerTimeFrame)
                ]
                if len(valid_timeframes) < len(timeframes):
                    invalid_timeframes = set(timeframes) - set(valid_timeframes)
                    logging.warning(
                        f"Invalid values for timeframes: {invalid_timeframes}. They will be excluded."
                    )
                    values["timeframes"] = valid_timeframes

        return values


class DbtMetaLookerDerivedMeasure(DbtMetaLookerMeasure):
    """Looker metadata for a derived measure."""

    sql: str


class DbtMetaLookerDerivedDimension(DbtMetaLookerDimension):
    """Looker metadata for a derived dimension."""

    sql: str


class DbtMetaColumnLooker(DbtMetaLookerViewElement):
    """Looker metadata for a column in a dbt model"""

    dimension: Optional[DbtMetaLookerDimension] = DbtMetaLookerDimension()
    measures: List[Union[DbtMetaLookerMeasure, BaseModel]] = []

    @model_validator(mode="before")
    def warn_outdated(cls, values):
        dimension_attrs = {
            "label": values.get("label"),
            "hidden": values.get("hidden"),
            "description": values.get("description"),
            "value_format_name": values.get("value_format_name"),
            "group_label": values.get("group_label"),
        }

        outdated_attrs = any(dimension_attrs.values())
        if outdated_attrs and not values.get("dimension"):
            values["dimension"] = dimension_attrs
            warnings.warn(
                "Putting dimension attributes in 'looker' is outdated and should be moved to 'looker: dimension:'.",
                DeprecationWarning,
            )

        validated_dimension = None
        dimension = values.get("dimension")
        if dimension:
            values.pop("dimension")
            try:
                if type(dimension) is DbtMetaLookerDimension:
                    validated_dimension = dimension
                else:
                    validated_dimension = DbtMetaLookerDimension(**dimension)
            except (ValidationError, TypeError) as e:
                logging.warning(f"Invalid dimension: {dimension}. Error: {str(e)}")
                validated_dimension = None
            if validated_dimension:
                values["dimension"] = validated_dimension

        measures = values.get("measures", [])
        if type(measures) is not list:
            measures = [measures]

        validated_measures = []
        for potential_measure in measures:
            if type(potential_measure) is str:
                # If the measure is just a string, assume it's the measure type
                potential_measure = {"type": potential_measure}
            try:
                if type(potential_measure) is DbtMetaLookerMeasure:
                    validated_measures.append(potential_measure)
                else:
                    validated_measure = DbtMetaLookerMeasure(**potential_measure)
                    validated_measures.append(validated_measure)
            except (ValidationError, TypeError) as e:
                logging.warning(
                    f"Invalid measure: {potential_measure}. Error: {str(e)}"
                )
        if validated_measures:
            values["measures"] = validated_measures

        return values


class DbtMetaLookerExplore(BaseModel):
    group_label: Optional[str] = None


class DbtMetaLooker(BaseModel):
    """Looker metadata for a model."""

    view: Optional[DbtMetaLookerBase] = DbtMetaLookerBase()
    explore: Optional[DbtMetaLookerExplore] = DbtMetaLookerExplore()
    measures: Optional[List[DbtMetaLookerDerivedMeasure]] = []
    dimensions: Optional[List[DbtMetaLookerDerivedDimension]] = []

    @model_validator(mode="before")
    def warn_outdated(cls, values):
        dimension_attrs = {
            "label": values.get("label"),
            "hidden": values.get("hidden"),
        }

        outdated_attrs = any(dimension_attrs.values())
        if outdated_attrs and not values.get("view"):
            values["view"] = dimension_attrs
            warnings.warn(
                "Putting view attributes in 'looker' is outdated and should be moved to 'looker: view:'.",
                DeprecationWarning,
                source="values.name",
            )

        validated_view = None
        view = values.get("view")
        if view:
            values.pop("view")
            try:
                if type(view) is DbtMetaLookerBase:
                    validated_view = view
                else:
                    validated_view = DbtMetaLookerBase(**view)
            except (ValidationError, TypeError) as e:
                logging.warning(f"Invalid view: {view}. Error: {str(e)}")
                validated_view = None
            if validated_view:
                values["view"] = validated_view

        validated_explore = None
        explore = values.get("explore")
        if explore:
            values.pop("explore")
            try:
                if type(explore) is DbtMetaLookerBase:
                    validated_explore = explore
                else:
                    validated_explore = DbtMetaLookerBase(**explore)
            except (ValidationError, TypeError) as e:
                logging.warning(f"Invalid explore: {explore}. Error: {str(e)}")
                validated_explore = None
            if validated_explore:
                values["explore"] = validated_explore

        dimensions = values.get("dimensions", [])
        if type(dimensions) is not list:
            dimensions = [dimensions]

        validated_dimensions = []
        for potential_dimension in dimensions:
            values.pop("dimensions")
            try:
                if type(potential_dimension) is DbtMetaLookerDerivedDimension:
                    validated_dimensions.append(potential_dimension)
                else:
                    validated_dimension = DbtMetaLookerDerivedDimension(
                        **potential_dimension
                    )
                    validated_dimensions.append(validated_dimension)
            except (ValidationError, TypeError) as e:
                logging.warning(
                    f"Invalid measure: {potential_dimension}. Error: {str(e)}"
                )
        if validated_dimensions:
            values["dimensions"] = validated_dimensions

        measures = values.get("measures", [])
        if type(measures) is not list:
            measures = [measures]

        validated_measures = []
        for potential_measure in measures:
            values.pop("measures")
            try:
                if type(potential_measure) is DbtMetaLookerDerivedMeasure:
                    validated_measures.append(potential_measure)
                else:
                    validated_measure = DbtMetaLookerDerivedMeasure(**potential_measure)
                    validated_measures.append(validated_measure)
            except (ValidationError, TypeError) as e:
                logging.warning(
                    f"Invalid measure: {potential_measure}. Error: {str(e)}"
                )
        if validated_measures:
            values["measures"] = validated_measures

        return values

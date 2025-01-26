from typing import List, Optional, Union
import logging
from pydantic import BaseModel, Field, model_validator, field_validator

from dbt2looker_bigquery.enums import (
    # LookerJoinType,
    LookerMeasureType,
    # LookerRelationshipType,
    LookerTimeFrame,
    LookerValueFormatName,
)


class LookViewFile(BaseModel):
    """A file in a looker view directory"""

    filename: str
    contents: str
    db_schema: str = Field(..., alias="schema")


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
        measure_type = values.get("type")

        # Validate type-specific attributes
        if (
            any(
                v is not None
                for v in [
                    values.get("approximate"),
                    values.get("approximate_threshold"),
                    values.get("sql_distinct_key"),
                ]
            )
            and measure_type != LookerMeasureType.COUNT_DISTINCT
        ):
            raise ValueError(
                "approximate, approximate_threshold, and sql_distinct_key can only be used with count_distinct measures"
            )

        if values.get("percentile") is not None and not measure_type.value.startswith(
            "percentile"
        ):
            raise ValueError("percentile can only be used with percentile measures")

        if values.get("precision") is not None and measure_type not in [
            LookerMeasureType.AVERAGE,
            LookerMeasureType.SUM,
        ]:
            raise ValueError("precision can only be used with average or sum measures")

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

    dimension: Optional[DbtMetaLookerDimension] = None
    measures: Optional[List[DbtMetaLookerMeasure]] = Field(default=[])

    @model_validator(mode="before")
    def warn_outdated(cls, values):
        if values.get("looker_measures"):
            logging.warning(
                "The 'looker: looker_measures' field is outdated and should be moved to 'looker: measures:'"
            )
            values["measures"] = values.get("looker_measures")

        label = None
        hidden = None
        description = None
        value_format_name = None
        group_label = None
        if values.get("label"):
            label = values.get("label")
        if values.get("hidden"):
            hidden = values.get("hidden")
        if values.get("description"):
            description = values.get("description")
        if values.get("value_format_name"):
            value_format_name = values.get("value_format_name")
        if values.get("group_label"):
            group_label = values.get("group_label")

        if label or hidden or description or value_format_name:
            if "dimension" not in values or values["dimension"] is None:
                values["dimension"] = {}
                values["dimension"]["label"] = label if label else None
                values["dimension"]["group_label"] = (
                    group_label if group_label else None
                )
                values["dimension"]["hidden"] = hidden if hidden else None
                values["dimension"]["description"] = (
                    description if description else None
                )
                values["dimension"]["value_format_name"] = (
                    value_format_name if value_format_name else None
                )
                logging.warning(
                    "Deprecation_warning: Looker dimension metadata should be moved to 'looker: dimension:'"
                )
        return values


# class DbtMetaLookerJoin(BaseModel):
#     """Looker-specific metadata for joins on a dbt model

#     meta:
#       looker:
#         description: "Page views for Hubble landing page"
#         label: "Page Views"
#         joins:
#           - join: users                               # Reference another dbt model to join to
#             sql_on: "${users.id} = ${pages.user_id}"  # Sql string containing join clause
#             type: left_outer                          # Optional - left_outer is default
#             relationship: many_to_one                 # Relationship type
#     """

#     join_model: Optional[str] = Field(default=None)
#     sql_on: Optional[str] = Field(default=None)
#     type: Optional[LookerJoinType] = Field(default=None)
#     relationship: Optional[LookerRelationshipType] = Field(default=None)


class DbtMetaLooker(DbtMetaLookerBase):
    """Looker metadata for a model."""

    view: Optional[DbtMetaLookerBase] = None
    explore: Optional[DbtMetaLookerBase] = None
    measures: Optional[List[DbtMetaLookerDerivedMeasure]] = Field(default=[])
    dimensions: Optional[List[DbtMetaLookerDerivedDimension]] = Field(default=[])

    # Component fields
    # joins: Optional[List[DbtMetaLookerJoin]] = Field(default=[])

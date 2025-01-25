from typing import List, Optional, Union
import logging
from pydantic import BaseModel, Field, model_validator

from dbt2looker_bigquery.enums import (
    LookerJoinType,
    LookerMeasureType,
    LookerRelationshipType,
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
    description: Optional[str] = None
    hidden: Optional[bool] = None


class DbtMetaLookerDimension(DbtMetaLookerBase):
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
    group_label: Optional[str] = Field(default=None)
    value_format_name: Optional[LookerValueFormatName] = Field(default=None)
    timeframes: Optional[List[LookerTimeFrame]] = Field(default=None)
    can_filter: Optional[Union[bool, str]] = Field(default=None)

    @model_validator(mode="before")
    def check_enums(cls, values):
        # Check if the value for `value_format_name` is valid
        value_format_name = values.get("value_format_name")
        if value_format_name and not isinstance(
            value_format_name, LookerValueFormatName
        ):
            logging.warning(
                f"Invalid value for value_format_name: {value_format_name}. It will be excluded."
            )
            values.pop("value_format_name", None)

        # Check if values for `timeframes` are valid
        timeframes = values.get("timeframes", [])
        if timeframes:
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


class DbtMetaLookerMeasureFilter(BaseModel):
    filter_dimension: str
    filter_expression: str


class DbtMetaLookerMeasure(DbtMetaLookerBase):
    """Looker metadata for a measure."""

    # Required fields
    type: LookerMeasureType

    # Common optional fields
    name: Optional[str] = None
    group_label: Optional[str] = None
    value_format_name: Optional[LookerValueFormatName] = None
    filters: Optional[List[DbtMetaLookerMeasureFilter]] = None

    # Fields specific to certain measure types
    approximate: Optional[bool] = None  # For count_distinct
    approximate_threshold: Optional[int] = None  # For count_distinct
    precision: Optional[int] = None  # For average, sum
    sql_distinct_key: Optional[str] = None  # For count_distinct
    percentile: Optional[int] = None  # For percentile measures

    @model_validator(mode="before")
    def check_enums(cls, values):
        # Validate value_format_name
        value_format_name = values.get("value_format_name")
        if value_format_name and not isinstance(
            value_format_name, LookerValueFormatName
        ):
            logging.warning(
                f"Invalid value for value_format_name: {value_format_name}. It will be excluded."
            )
            values.pop("value_format_name", None)

        return values

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


class DbtMetaLookerJoin(BaseModel):
    """Looker-specific metadata for joins on a dbt model

    meta:
      looker:
        description: "Page views for Hubble landing page"
        label: "Page Views"
        joins:
          - join: users                               # Reference another dbt model to join to
            sql_on: "${users.id} = ${pages.user_id}"  # Sql string containing join clause
            type: left_outer                          # Optional - left_outer is default
            relationship: many_to_one                 # Relationship type
    """

    join_model: Optional[str] = Field(default=None)
    sql_on: Optional[str] = Field(default=None)
    type: Optional[LookerJoinType] = Field(default=None)
    relationship: Optional[LookerRelationshipType] = Field(default=None)


class DbtMetaLooker(BaseModel):
    """Looker metadata for a model."""

    # Component fields
    view: Optional[DbtMetaLookerBase] = None
    dimension: Optional[DbtMetaLookerDimension] = None
    measures: Optional[List[DbtMetaLookerMeasure]] = Field(default=[])
    joins: Optional[List[DbtMetaLookerJoin]] = Field(default=[])

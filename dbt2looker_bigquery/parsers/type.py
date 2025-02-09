"""Schema parsing functionality for BigQuery schema strings.

This module provides tools for parsing BigQuery schema strings into structured
field definitions. It handles nested structures (STRUCT) and complex types (ARRAY)
while maintaining the hierarchical relationships between fields.
"""

import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import List
from dbt2looker_bigquery.enums import BigqueryType


@dataclass
class SchemaField:
    """Represents a field in the BigQuery schema with its name, type, and path."""

    name: str
    type_str: str
    path: List[str]

    def __str__(self) -> str:
        """Returns the complete field representation as it would appear in a schema."""
        path_str = ".".join(self.path + ([self.name] if self.name else []))
        return f"{path_str} {self.type_str}".strip()


class TypeParser:
    """Parser for dbt BigQuery schema strings that handles nested structures and complex types."""

    def _map_type(self, type_str: str) -> str:
        """Maps BigQuery type variations to uniform Bigquery types."""

        type_str = type_str.upper()

        if type_str == "INTEGER":
            return "INT64"
        elif type_str == "FLOAT":
            return "FLOAT64"
        elif type_str == "BOOL":
            return "BOOLEAN"
        else:
            return type_str

    def __init__(self):
        self._fields: List[SchemaField] = []
        self._current_path: List[str] = []

    def _find_inner_content(self, text: str) -> str:
        """Extracts content within angle brackets."""
        match = re.search(r"<(.*)>", text)
        if match:
            return match.group(1)
        else:
            return text

    def _split_fields(self, text: str) -> List[str]:
        """Splits content on top-level commas."""
        result = []
        current = []
        level = 0

        for char in text + ",":
            if char == "<":
                level += 1
            elif char == ">":
                level -= 1
            elif char == "," and level == 0:
                if current:
                    result.append("".join(current).strip())
                current = []
                continue
            current.append(char)

        return [f for f in result if f]

    def _normalize_numerics(self, type_str: str) -> str:
        """Remove (1,2) formatting from NUMERICS."""
        if "NUMERIC" in type_str:
            return re.sub(r"NUMERIC\(\d+,\s*\d+\)", "NUMERIC", type_str)
        return type_str

    def _process_type(self, type_str: str) -> tuple[str, str, bool]:
        """Processes a type string to determine its structure.
        Returns: (data_type, inner_type_str, has_struct)"""

        is_struct = False
        inner_type_str = type_str

        if type_str.startswith(BigqueryType.ARRAY.value + "<"):
            inner_type_str = self._find_inner_content(type_str)

            if inner_type_str.startswith(BigqueryType.STRUCT.value + "<"):
                is_struct = True
                inner_type_str = self._find_inner_content(inner_type_str)

        if type_str.startswith(BigqueryType.STRUCT.value + "<"):
            is_struct = True
            inner_type_str = self._find_inner_content(type_str)

        return inner_type_str, is_struct

    @contextmanager
    def _path_context(self, name: str):
        """Context manager for tracking field paths."""
        if name:
            self._current_path.append(name)
        try:
            yield
        finally:
            if name:
                self._current_path.pop()

    def _add_field(self, name: str, type_str: str):
        """Adds a field to the result list."""
        self._fields.append(
            SchemaField(
                name=name,
                type_str=self._map_type(type_str),
                path=self._current_path.copy(),
            )
        )

    def _process_fields(self, content: str):
        """Iteratively processes field definitions."""
        for field in self._split_fields(content):
            name, type_str = field.split(" ", 1)
            inner_type_str, has_struct = self._process_type(type_str.strip())

            if has_struct:
                self._add_field(name, inner_type_str)
                with self._path_context(name):
                    self._process_fields(inner_type_str)
            else:
                self._add_field(name, type_str)

    def get_data_type(self, schema_str: str) -> str:
        """Returns the outer data type for a schema string."""
        schema_str = self._normalize_numerics(schema_str)

        if "<" not in schema_str:
            return self._map_type(schema_str.strip())
        else:
            return self._map_type(schema_str.split("<")[0].strip())

    def get_inner_types(self, schema_str: str) -> List[str]:
        """Returns the outer data type and a list of inner types for a schema string."""
        self._fields = []
        self._current_path = []

        schema_str = self._normalize_numerics(schema_str)

        inner_type_str, is_struct = self._process_type(schema_str)

        if is_struct:
            self._process_fields(inner_type_str)
        else:
            self._add_field(
                "",
                inner_type_str,
            )

        inner_type_list = sorted(str(field) for field in self._fields)

        return inner_type_list

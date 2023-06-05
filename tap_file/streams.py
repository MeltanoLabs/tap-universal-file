"""Stream type classes for tap-file."""

from __future__ import annotations
from functools import cached_property

import csv
from typing import Any, Generator

import fsspec

from tap_file.client import FileStream


class CSVStream(FileStream):
    """Stream for reading CSVs."""

    name = "CSV"

    def get_rows(self) -> Generator[dict[str | Any, str | Any], None, None]:
        """Retrive all rows from all CSVs.

        Yields:
            A dictionary containing information about a row in a CSV.
        """
        for file in self.get_files():
            with self.filesystem.open(path = file, mode = "rt") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row

    @cached_property
    def schema(self) -> dict[str, dict]:
        """Create a schema for a CSV file.

        Each column in the CSV will have its own entry in the schema. All entries will 
        be of the form: `'FIELD_NAME': {'type': ['null', 'string']}`

        Returns:
            A schema representing a CSV.
        """

        properties = {}

        for file in self.get_files():
            with self.filesystem.open(path = file, mode = "rt") as f:
                reader = csv.DictReader(f)
                for field in reader.fieldnames:
                    properties.update({field: {"type": ["null", "string"]}})

        return {"properties": properties}
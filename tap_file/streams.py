"""Stream type classes for tap-file."""

from __future__ import annotations

import csv
import json
import re
from functools import cached_property
from typing import Any, Generator

from tap_file.client import FileStream


class DelimitedStream(FileStream):
    """Stream for reading CSVs and TSVs."""

    def get_rows(self) -> Generator[dict[str | Any, str | Any], None, None]:
        """Retrive all rows from all *SVs.

        Yields:
            A dictionary containing information about a row in a *SV.
        """
        for reader in self._get_readers():
            for row in reader:
                yield row

    @cached_property
    def schema(self) -> dict[str, dict]:
        """Create a schema for a *SV file.

        Each column in the *SV will have its own entry in the schema. All entries will
        be of the form: `'FIELD_NAME': {'type': ['null', 'string']}`

        Returns:
            A schema representing a *SV.
        """
        properties = {}

        for reader in self._get_readers():
            for field in reader.fieldnames:
                properties.update({field: {"type": ["null", "string"]}})

        return {"properties": properties}

    def _get_readers(self) -> Generator[csv.DictReader[str], None, None]:
        quote_character: str = self.config["quote_character"]

        for file in self.get_files():
            if self.config["delimiter"] == "detect":
                if re.match(".*\\.csv.*", file):
                    delimiter = ","
                elif re.match(".*\\.tsv.*", file):
                    delimiter = "\t"
                else:
                    msg = (
                        "Configuration option 'delimiter' is set to 'detect' but a "
                        "non-csv non-tsv file is present. Please manually specify "
                        "'delimiter'."
                    )
                    raise RuntimeError(msg)
            else:
                delimiter = self.config["delimiter"]

            with self.filesystem.open(
                path=file,
                mode="rt",
                compression=self.get_compression(file=file),
            ) as f:
                yield csv.DictReader(f, delimiter=delimiter, quotechar=quote_character)


class JSONLStream(FileStream):
    """Stream for reading JSON files."""

    def get_rows(self) -> Generator[dict[str, Any], None, None]:
        """Retrive all rows from all JSONL files.

        Yields:
            A dictionary containing information about a row in a JSONL file.
        """
        for file in self.get_files():
            with self.filesystem.open(
                path=file,
                mode="rt",
                compression=self.get_compression(file=file),
            ) as f:
                for row in f:
                    yield self._pre_process(json.loads(row))

    @cached_property
    def schema(self) -> dict[str, dict]:
        """Create a schema for a JSONL file.

        The format of the schema will depend on the jsonl_type_coercion_strategy config
        option, but will always be a dictionary of field names and associated types.

        Returns:
            A schema representing a JSONL file.
        """
        properties = {}
        for field in self._get_fields():
            properties.update(self._get_property(field=field))
        return {"properties": properties}

    def _get_property(self, field: str) -> dict[str, dict[str, list[str]]]:
        strategy = self.config["jsonl_type_coercion_strategy"]
        if strategy == "any":
            return {
                field: {
                    "type": [
                        "null",
                        "boolean",
                        "integer",
                        "number",
                        "string",
                        "array",
                        "object",
                    ],
                },
            }
        if strategy == "string":
            return {field: {"type": ["null", "string"]}}
        if strategy == "blob":
            return {field: {"type": ["null", "object"]}}
        msg = f"The coercion strategy '{strategy}' is not valid."
        raise ValueError(msg)

    def _get_fields(self) -> Generator[str, None, None]:
        strategy = self.config["jsonl_sampling_strategy"]
        if strategy == "first":
            try:
                yield from next(self.get_rows())
            except StopIteration:
                return
            return
        if strategy == "all":
            msg = f"The sampling strategy '{strategy}' has not been implemented."
            raise NotImplementedError(msg)
        msg = f"The sampling strategy '{strategy}' is not valid."
        raise ValueError(msg)

    def _pre_process(self, row: dict[str, Any]) -> dict[str, Any]:
        strategy = self.config["jsonl_type_coercion_strategy"]
        if strategy == "any":
            return row
        if strategy == "string":
            for entry in row:
                row[entry] = str(row[entry])
            return row
        if strategy == "blob":
            return {"record": row}
        msg = f"The coercion strategy '{strategy}' is not valid."
        raise ValueError(msg)

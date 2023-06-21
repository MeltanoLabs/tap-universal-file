"""Stream type classes for tap-file."""

from __future__ import annotations

import csv
import json
from functools import cached_property
from typing import Any, Generator, Iterable

from tap_file.client import FileStream


class SeparatedValuesStream(FileStream):
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
        """Get all *SV readers.

        Raises:
            StopIteration: To end iteration early if no more readers exist.

        Yields:
            A *SV reader.
        """
        file_type: str = self.config["file_type"]
        delimiter: str = self.config["delimiter"]
        delimiters: dict[str, str] = {
            "csv": "," if delimiter == "detect" else delimiter,
            "tsv": "\t" if delimiter == "detect" else delimiter,
        }

        if file_type == "detect":
            for reader in self._get_readers_helper(
                delimiter=delimiters["csv"],
                regex=".*\\.csv.*",
            ):
                yield reader
            for reader in self._get_readers_helper(
                delimiter=delimiters["tsv"],
                regex=".*\\.tsv.*",
            ):
                yield reader
        elif file_type in {"csv", "tsv"}:
            for reader in self._get_readers_helper(delimiter=delimiters[file_type]):
                yield reader
        else:
            raise StopIteration

    def _get_readers_helper(
        self,
        delimiter: str,
        regex: str | None = None,
    ) -> Generator[csv.DictReader[str], None, None]:
        """Get a subset of *SV readers matching certain criteria.

        Yields:
            A *SV reader.
        """
        quote_character: str = self.config["quote_character"]

        for file in self.get_files(regex=regex):
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
        file_type: str = self.config["file_type"]

        if file_type == "detect":
            regex = None
        elif file_type == "jsonl":
            regex = ".*\\.jsonl.*"
        else:
            raise StopIteration

        for file in self.get_files(regex=regex):
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
                        "object",
                        "integer",
                        "array",
                        "number",
                        "boolean",
                        "string",
                    ],
                },
            }
        if strategy == "string":
            return {field: {"type": ["null", "string"]}}
        if strategy == "blob":
            return {field: {"type": ["null", "object"]}}
        msg = f"The coercion strategy '{strategy}' is not valid."
        raise ValueError(msg)

    def _get_fields(self) -> Iterable[str]:
        strategy = self.config["jsonl_sampling_strategy"]
        if strategy == "first":
            return list(next(self.get_rows()))
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

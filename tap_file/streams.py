"""Stream type classes for tap-file."""

from __future__ import annotations

import csv
import json
import re
from typing import Any, Generator

import avro
import avro.datafile
import avro.io
import avro.schema

from tap_file.client import FileStream


class DelimitedStream(FileStream):
    """Stream for reading CSVs and TSVs."""

    def get_rows(self) -> Generator[dict[str | Any, str | Any], None, None]:
        """Retrive all rows from all *SVs.

        Yields:
            A dictionary containing information about a row in a *SV.
        """
        header_skip = self.config["header_skip"]
        footer_skip = self.config["footer_skip"]

        for reader_dict in self._get_readers():
            reader = reader_dict["reader"]
            if footer_skip != 0:
                reader = list(reader)
                total_lines = len(reader)
            line_number = 1
            for row in reader:
                if line_number > header_skip and (
                    True
                    if footer_skip == 0
                    else line_number <= total_lines - footer_skip
                ):
                    yield self.add_additional_info(
                        row,
                        reader_dict["file_name"],
                        line_number,
                    )
                line_number += 1

    def get_properties(self) -> dict:
        """Get a list of properties for a *SV file, to be used in creating a schema.

        Each column in the *SV will have its own entry in the schema. All entries will
        be of the form: `'FIELD_NAME': {'type': ['null', 'string']}`

        Returns:
            A list of properties representing a *SV file.
        """
        properties = {}

        for reader_dict in self._get_readers():
            reader = reader_dict["reader"]
            for field in reader.fieldnames:
                properties.update({field: {"type": ["null", "string"]}})

        return properties

    def _get_readers(
        self,
    ) -> Generator[dict[str, str | csv.DictReader[str]], None, None]:
        quote_character: str = self.config["quote_character"]
        override_headers: list | None = self.config.get("override_headers", None)

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
                yield {
                    "reader": csv.DictReader(
                        f,
                        delimiter=delimiter,
                        quotechar=quote_character,
                        fieldnames=override_headers,
                    ),
                    "file_name": file,
                }


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
                line_number = 1
                for row in f:
                    yield self.add_additional_info(
                        self._pre_process(json.loads(row)),
                        file,
                        line_number,
                    )
                    line_number += 1

    def get_properties(self) -> dict:
        """Get a list of properties for a JSONL file, to be used in creating a schema.

        The format of the schema will depend on the jsonl_type_coercion_strategy config
        option, but will always be a dictionary of field names and associated types.

        Returns:
            A list of properties representing a JSONL file.
        """
        properties = {}
        for field in self._get_fields():
            properties.update(self._get_property(field=field))
        return properties

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
        if strategy == "envelope":
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
        if strategy == "envelope":
            return {"record": row}
        msg = f"The coercion strategy '{strategy}' is not valid."
        raise ValueError(msg)


class AvroStream(FileStream):
    """Stream for reading Avro files."""

    def get_rows(self) -> Generator[dict[str, Any], None, None]:
        """Retrive all rows from all Avro files.

        Yields:
            A dictionary containing information about a row in a Avro file.
        """
        for reader_dict in self._get_readers():
            reader = reader_dict["reader"]
            line_number = 1
            for row in reader:
                yield self.add_additional_info(
                    self._pre_process(row),
                    reader_dict["file_name"],
                    line_number,
                )
                line_number += 1

    def get_properties(self) -> dict:
        """Get a list of properties for an Avro file, to be used in creating a schema.

        Returns:
            A list of properties representing an Avro file.
        """
        properties = {}
        for field in self._get_fields():
            properties.update(self._get_property(field))
        return properties

    def _get_fields(self) -> Generator[dict | str, None, None]:
        strategy = self.config["avro_type_coercion_strategy"]
        if strategy == "convert":
            for reader_dict in self._get_readers():
                reader = reader_dict["reader"]
                for field in json.loads(reader.schema)["fields"]:
                    yield field
            return
        if strategy == "envelope":
            yield "record"
            return
        msg = f"The coercion strategy '{strategy}' is not valid."
        raise ValueError(msg)

    def _get_property(self, field: dict | str) -> dict[str, dict[str, list[str]]]:
        strategy = self.config["avro_type_coercion_strategy"]
        if strategy == "convert":
            return {field["name"]: {"type": [self._type_convert(field["type"])]}}
        if strategy == "envelope":
            return {field: {"type": ["null", "object"]}}
        msg = f"The coercion strategy '{strategy}' is not valid."
        raise ValueError(msg)

    def _type_convert(self, field_type: str) -> str:
        if field_type in {"null", "boolean", "string"}:
            return field_type
        if field_type in {"int", "long"}:
            return "integer"
        if field_type in {"float", "double"}:
            return "number"
        if field_type == "bytes":
            return "string"
        if field_type == {"record", "enum", "array", "map", "union", "fixed"}:
            msg = f"The field type '{field_type} has not been implemented."
            raise NotImplementedError(msg)
        msg = f"An invalid field type of '{field_type}' was detected."
        raise RuntimeError(msg)

    def _pre_process(self, row: dict[str, Any]) -> dict[str, Any]:
        strategy = self.config["avro_type_coercion_strategy"]
        if strategy == "convert":
            return row
        if strategy == "envelope":
            return {"record": row}
        msg = f"The coercion strategy '{strategy}' is not valid."
        raise ValueError(msg)

    def _get_readers(
        self,
    ) -> Generator[dict[str, str | avro.datafile.DataFileReader], None, None]:
        for file in self.get_files():
            with self.filesystem.open(
                path=file,
                mode="rb",
                compression=self.get_compression(file=file),
            ) as f:
                yield {
                    "reader": avro.datafile.DataFileReader(f, avro.io.DatumReader()),
                    "file_name": file,
                }

"""Stream type classes for tap-file."""

from __future__ import annotations

import csv
from functools import cached_property
from typing import Any, Generator

from tap_file.client import FileStream


class SeparatedValuesStream(FileStream):
    """Stream for reading CSVs and TSVs."""

    def get_rows(self) -> Generator[dict[str | Any, str | Any], None, None]:
        """Retrive all rows from all *SVs.

        Yields:
            A dictionary containing information about a row in a *SV.
        """
        for reader in self.get_readers():
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

        for reader in self.get_readers():
            for field in reader.fieldnames:
                properties.update({field: {"type": ["null", "string"]}})

        return {"properties": properties}

    def get_readers(self) -> Generator[csv.DictReader[str], None, None]:
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
            for reader in self.get_readers_helper(
                delimiter=delimiters["csv"],
                regex=".*\\.csv.*",
            ):
                yield reader
            for reader in self.get_readers_helper(
                delimiter=delimiters["tsv"],
                regex=".*\\.tsv.*",
            ):
                yield reader
        elif file_type in {"csv", "tsv"}:
            for reader in self.get_readers_helper(delimiter=delimiters[file_type]):
                yield reader
        else:
            raise StopIteration

    def get_readers_helper(
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

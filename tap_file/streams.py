"""Stream type classes for tap-file."""

from __future__ import annotations

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
            with fsspec.open(file, "rt") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row

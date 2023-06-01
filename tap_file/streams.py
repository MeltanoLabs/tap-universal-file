"""Stream type classes for tap-file."""

from __future__ import annotations
import csv

from pathlib import Path
from typing import Any, Iterable
import fsspec

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_file.client import FileStream


class CSVStream(FileStream):
    """Stream for reading CSVs."""

    name = "CSV"
    
    def get_rows(self) -> Iterable[dict[str | Any, str | Any]]:
        for file in self.get_files():
            with fsspec.open(file, "rt") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row

    

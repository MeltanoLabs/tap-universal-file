"""File tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_file import streams


class TapFile(Tap):
    """File tap class."""

    name = "tap-file"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "protocol",
            th.StringType,
            required=True,
            description="The protocol to use to retrieve data. One of `file` or `s3`.",
        ),
        th.Property(
            "filepath",
            th.StringType,
            required=True,
            description="The path to obtain files from. Example: `/foo/bar`",
        ),
        th.Property(
            "format",
            th.StringType,
            description=(
                "The file format to return data on. One of `csv`, `tsv`, `json`, ",
                "`avro`, or `detect`.",
            ),
        ),
        th.Property(
            "delimiter",
            th.StringType,
            default=",",
            description=(
                "The delimiter used between records in a file. Any singular charater ",
                "or the special value `detect`.",
            ),
        ),
        th.Property(
            "file_regex",
            th.StringType,
            description=(
                "A regex pattern to only include certain files. Example: `*\\.csv`",
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.FileStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CSVStream(self),
        ]


if __name__ == "__main__":
    TapFile.cli()

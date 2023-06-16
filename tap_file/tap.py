"""File tap class."""

from __future__ import annotations

import os
import tempfile

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_file import streams


class TapFile(Tap):
    """File tap class."""

    name = "tap-file"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "stream_name",
            th.StringType,
            required=False,
            default="file",
            description="The name of the stream that is output by the tap.",
        ),
        th.Property(
            "protocol",
            th.StringType,
            required=True,
            allowed_values=["file", "s3"],
            description="The protocol to use to retrieve data. One of `file` or `s3`.",
        ),
        th.Property(
            "filepath",
            th.StringType,
            required=True,
            description=(
                "The path to obtain files from. Examples: `/foo/bar`, `s3-bucket-name`."
            ),
        ),
        th.Property(
            "file_regex",
            th.RegexType,
            description=(
                "A regex pattern to only include certain files. Example: `.*\\.csv`."
            ),
        ),
        th.Property(
            "cache_filepath",
            th.StringType,
            default=tempfile.gettempdir(),
            description=(
                "The location to store cached files when `protocol!=file`. If left "
                "blank, caching will not be used and the entire contents of each file "
                "will be fetched for each read operation."
            ),
        ),
        th.Property(
            "s3_anonymous_connection",
            th.BooleanType,
            default=False,
            description=(
                "Whether to use an anonymous S3 connection, without any credentials. "
                "Ignored if `protocol!=s3`."
            ),
        ),
        th.Property(
            "AWS_ACCESS_KEY_ID",
            th.StringType,
            default=os.getenv("AWS_ACCESS_KEY_ID"),
            description=(
                "The access key to use when authenticating to S3. Ignored if "
                "`protocol!=s3` or `s3_anonymous_connection=True`. Defaults to the "
                "value of the environment variable of the same name."
            ),
        ),
        th.Property(
            "AWS_SECRET_ACCESS_KEY",
            th.StringType,
            default=os.getenv("AWS_SECRET_ACCESS_KEY"),
            description=(
                "The access key secret to use when authenticating to S3. Ignored if "
                "`protocol!=s3` or `s3_anonymous_connection=True`. Defaults to the "
                "value of the environment variable of the same name."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.FileStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        name = self.config["stream_name"]
        return [
            streams.CSVStream(self, name = name),
        ]


if __name__ == "__main__":
    TapFile.cli()

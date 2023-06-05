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
            allowed_values=["file", "s3"],
            description="The protocol to use to retrieve data. One of `file` or `s3`.",
        ),
        th.Property(
            "filepath",
            th.StringType,
            required=True,
            description="The path to obtain files from. Example: `/foo/bar`.",
        ),
        th.Property(
            "file_regex",
            th.RegexType,
            description=(
                "A regex pattern to only include certain files. Example: `.*\\.csv$`."
            ),
        ),
        th.Property(
            "s3_anonymous_connection",
            th.BooleanType,
            default=False,
            description=(
                "Whether to use an anonymous S3 connection, without the use of any "
                "credentials. Ignored if `protocol!=s3`."
            ),
        ),
        th.Property(
            "s3_access_key",
            th.StringType,
            description=(
                "The access key to use when authenticating to S3. Ignored if "
                "`protocol!=s3` or `s3_anonymous_connection=True`."
            ),
        ),
        th.Property(
            "s3_access_key_secret",
            th.StringType,
            default=True,
            description=(
                "The access key secret to use when authenticating to S3. Ignored if "
                "`protocol!=s3`or `s3_anonymous_connection=True`."
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

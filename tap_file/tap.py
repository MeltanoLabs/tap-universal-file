"""File tap class."""

from __future__ import annotations

import os

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
                "The path to obtain files from. Example: `/foo/bar`. Or, for "
                "`protocol==s3`, use `s3-bucket-name` instead."
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
            "file_type",
            th.RegexType,
            default="delimited",
            description=(
                "Can be any of `delimited`, `jsonl`, or `avro`. Indicates the type of "
                "file to sync, where `delimited` is for CSV/TSV files and similar. "
                "Note that *all* files will be read as that type, regardless of file "
                "extension. To only read from files with a matching file extension, "
                "appropriately configure `file_regex`."
            ),
        ),
        th.Property(
            "compression",
            th.StringType,
            allowed_values=["none", "zip", "bz2", "gzip", "lzma", "xz", "detect"],
            default="detect",
            description=(
                "The encoding to use to decompress data. One of `zip`, `bz2`, `gzip`, "
                "`lzma`, `xz`, `none`, or `detect`. If set to `none` or any encoding, "
                "that setting will be applied to *all* files, regardless of file "
                "extension. If set to `detect`, encodings will be applied based on "
                "file extension."
            ),
        ),
        th.Property(
            "additional_info",
            th.BooleanType,
            default=True,
            description=(
                "If `True`, each row in tap's output will have two additional columns: "
                "`_sdc_file_name` and `_sdc_line_number`. If `False`, these columns "
                "will not be present."
            ),
        ),
        th.Property(
            "delimited_error_handling",
            th.StringType,
            allowed_values=["fail", "ignore"],
            default="fail",
            description=(
                "The method with which to handle improperly formatted records in "
                "delimited files. `fail` will cause the tap to fail if an improperly "
                "formatted record is detected. `ignore` will ignore the fact that it "
                "is improperly formatted and process it anyway."
            ),
        ),
        th.Property(
            "delimited_delimiter",
            th.StringType,
            default="detect",
            description=(
                "The character used to separate records in a delimited file. Can be "
                "any character or the special value `detect`. If a character is "
                "provided, all delimited files will use that value. `detect` will use "
                "`,` for `.csv` files, `\\t` for `.tsv` files, and fail if other file "
                "types are present."
            ),
        ),
        th.Property(
            "delimited_quote_character",
            th.StringType,
            default='"',
            description=(
                "The character used to indicate when a record in a delimited file "
                "contains a delimiter character."
            ),
        ),
        th.Property(
            "delimited_header_skip",
            th.IntegerType,
            default=0,
            description=(
                "The number of initial rows to skip at the beginning of each delimited "
                "file."
            ),
        ),
        th.Property(
            "delimited_footer_skip",
            th.IntegerType,
            default=0,
            description=(
                "The number of initial rows to skip at the end of each delimited file."
            ),
        ),
        th.Property(
            "delimited_override_headers",
            th.ArrayType(th.StringType),
            description=(
                "An optional array of headers used to override the default column "
                "name in delimited files, allowing for headerless files to be "
                "correctly read."
            ),
        ),
        th.Property(
            "jsonl_error_handling",
            th.StringType,
            allowed_values=["fail", "ignore"],
            default="fail",
            description=(
                "The method with which to handle improperly formatted records in "
                "jsonl files. `fail` will cause the tap to fail if an improperly "
                "formatted record is detected. `ignore` will ignore the fact that it "
                "is improperly formatted and process it anyway."
            ),
        ),
        th.Property(
            "jsonl_sampling_strategy",
            th.StringType,
            allowed_values=["first", "all"],
            default="first",
            description=(
                "The strategy determining how to read the keys in a JSONL file. Must "
                "be one of `first` or `all`. Currently, only `first` is supported, "
                "which will assume that the first record in a file is representative "
                "of all keys."
            ),
        ),
        th.Property(
            "jsonl_type_coercion_strategy",
            th.StringType,
            allowed_values=["any", "string", "envelope"],
            default="any",
            description=(
                "The strategy determining how to construct the schema for JSONL files "
                "when the types represented are ambiguous. Must be one of `any`, "
                "`string`, or `envelope`. `any` will provide a generic schema for all "
                "keys, allowing them to be any valid JSON type. `string` will require "
                "all keys to be strings and will convert other values accordingly. "
                "`envelope` will deliver each JSONL row as a JSON object with no "
                "internal schema."
            ),
        ),
        th.Property(
            "avro_type_coercion_strategy",
            th.StringType,
            allowed_values=["convert", "envelope"],
            default="convert",
            description=(
                "The strategy determining how to construct the schema for Avro files "
                "when conversion between schema types is ambiguous. Must be one of "
                "`convert` or `envelope`. `convert` will attempt to convert from Avro "
                "Schema to JSON Schema and will fail if a type can't be easily "
                "coerced. `envelope` will wrap each record in an object without "
                "providing an internal schema for the record."
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
        th.Property(
            "caching_strategy",
            th.StringType,
            default="once",
            allowed_values=["none", "once", "persistent"],
            description=(
                "*DEVELOPERS ONLY* The caching method to use when `protocol!=file`. "
                "One of `none`, `once`, or `persistent`. `none` does not use caching "
                "at all. `once` (the default) will cache all files for the duration of "
                "the tap's invocation, then discard them upon completion. `peristent` "
                "will allow caches to persist between invocations of the tap, storing "
                "them in your OS's temp directory. It is recommended that you do not "
                "modify this setting."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.FileStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        name = self.config["stream_name"]
        file_type = self.config["file_type"]
        if file_type == "delimited":
            return [streams.DelimitedStream(self, name=name)]
        if file_type == "jsonl":
            return [streams.JSONLStream(self, name=name)]
        if file_type == "avro":
            return [streams.AvroStream(self, name=name)]
        if file_type in {"csv", "tsv", "txt"}:
            msg = f"'{file_type}' is not a valid file_type. Did you mean 'delimited'?"
            raise ValueError(msg)
        if file_type in {"json", "ndjson"}:
            msg = f"'{file_type}' is not a valid file_type. Did you mean 'jsonl'?"
            raise ValueError(msg)
        msg = f"'{file_type}' is not a valid file_type."
        raise ValueError(msg)


if __name__ == "__main__":
    TapFile.cli()

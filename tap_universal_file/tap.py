"""File tap class."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk._singerlib import Catalog
from singer_sdk.helpers._util import read_json_file
from singer_sdk.mapper import PluginMapper

if TYPE_CHECKING:
    from pathlib import PurePath

    import click

from tap_universal_file import streams


def one_of(allowed_values: list) -> str:
    """Creates a string listing allowed values.

    Args:
        allowed_values: The allowed values.

    Returns:
        A string listing each allowed value.
    """
    if len(allowed_values) == 1:
        return "Must be `" + allowed_values[0] + "`"
    if len(allowed_values) == 2:  # noqa: PLR2004
        return (
            "Must be either `" + allowed_values[0] + "` or `" + allowed_values[1] + "`"
        )
    to_return = "Must be one of "
    for i in range(len(allowed_values) - 1):
        to_return += "`" + allowed_values[i] + "`, "
    to_return += "or `" + allowed_values[-1] + "`"
    return to_return


class TapUniversalFile(Tap):
    """File tap class."""

    name = "tap-universal-file"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "stream_name",
            th.StringType,
            default="file",
            description="The name of the stream that is output by the tap.",
        ),
        th.Property(
            "protocol",
            th.StringType,
            required=True,
            allowed_values=(allowed_values := ["file", "s3"]),
            description=(
                f"The protocol to use to retrieve data. {one_of(allowed_values)}."
            ),
        ),
        th.Property(
            "file_path",
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
                "Must be one of `delimited`, `jsonl`, or `avro`. Indicates the type "
                "of file to sync, where `delimited` is for CSV/TSV files and "
                "similar. Note that *all* files will be read as that type, "
                "regardless of file extension. To only read from files with a "
                "matching file extension, appropriately configure `file_regex`."
            ),
        ),
        th.Property(
            "schema",
            th.StringType,
            description=(
                "The declarative schema to use for the stream. It can be the schema "
                "itself or a path to a json file containing the schema. If not "
                "provided, the schema will be inferred from the data following "
                "the coercion strategy."
            ),
        ),
        th.Property(
            "compression",
            th.StringType,
            allowed_values=(
                allowed_values := [
                    "none",
                    "zip",
                    "bz2",
                    "gzip",
                    "lzma",
                    "xz",
                    "detect",
                ]
            ),
            default="detect",
            description=(
                "The encoding used to decompress data. "
                f"{one_of(allowed_values)}. If set to `none` or any encoding, "
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
                "If `True`, each row in tap's output will have three additional "
                "columns: `_sdc_file_name`, `_sdc_line_number`, and "
                "`_sdc_last_modified`. If `False`, these columns will not be "
                "present. Incremental replication requires `additional_info==True`."
            ),
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description=(
                "Used in place of state. Files that were last modified before the "
                "`start_date` wwill not be synced."
            ),
        ),
        th.Property(
            "delimited_error_handling",
            th.StringType,
            allowed_values=(allowed_values := ["fail", "ignore"]),
            default="fail",
            description=(
                "The method with which to handle improperly formatted records in "
                f"delimited files. {one_of(allowed_values)}. `fail` will "
                "cause the tap to fail if an improperly formatted record is "
                "detected. `ignore` will ignore the fact that it is improperly "
                "formatted and process it anyway."
            ),
        ),
        th.Property(
            "delimited_delimiter",
            th.StringType,
            default="detect",
            description=(
                "The character used to separate records in a delimited file. Can "
                "ne any character or the special value `detect`. If a character is "
                "provided, all delimited files will use that value. `detect` will "
                "use `,` for `.csv` files, `\\t` for `.tsv` files, and fail if "
                "other file types are present."
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
                "The number of initial rows to skip at the beginning of each "
                "delimited file."
            ),
        ),
        th.Property(
            "delimited_footer_skip",
            th.IntegerType,
            default=0,
            description=(
                "The number of initial rows to skip at the end of each delimited "
                "file."
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
            allowed_values=(allowed_values := ["fail", "ignore"]),
            default="fail",
            description=(
                "The method with which to handle improperly formatted records in "
                f"jsonl files. {one_of(allowed_values)}. `fail` will cause "
                "the tap to fail if an improperly formatted record is detected. "
                "`ignore` will ignore the fact that it is improperly formatted and "
                "process it anyway."
            ),
        ),
        th.Property(
            "jsonl_sampling_strategy",
            th.StringType,
            allowed_values=(allowed_values := ["first", "all"]),
            default="first",
            description=(
                "The strategy determining how to read the keys in a JSONL file. "
                f"{one_of(allowed_values)}. Currently, only `first` is "
                "supported, which will assume that the first record in a file is "
                "representative of all keys."
            ),
        ),
        th.Property(
            "jsonl_type_coercion_strategy",
            th.StringType,
            allowed_values=(allowed_values := ["any", "string", "envelope"]),
            default="any",
            description=(
                "The strategy determining how to construct the schema for JSONL "
                "files when the types represented are ambiguous.  "
                f"{one_of(allowed_values)}. `any` will provide a generic "
                "schema for all keys, allowing them to be any valid JSON type. "
                "`string` will require all keys to be strings and will convert "
                "other values accordingly. `envelope` will deliver each JSONL "
                "row as a JSON object with no internal schema."
            ),
        ),
        th.Property(
            "avro_type_coercion_strategy",
            th.StringType,
            allowed_values=(allowed_values := ["convert", "envelope"]),
            default="convert",
            description=(
                "The strategy deciding how to convert Avro Schema to JSON Schema "
                f"when the conversion is ambiguous. {one_of(allowed_values)}. "
                "`convert` will attempt to convert from Avro Schema to JSON Schema "
                "and will fail if a type can't be easily coerced. `envelope` will "
                "wrap each record in an object without providing an internal"
                "schema for the record."
            ),
        ),
        th.Property(
            "s3_anonymous_connection",
            th.BooleanType,
            default=False,
            description=(
                "Whether to use an anonymous S3 connection, without any "
                "credentials. Ignored if `protocol!=s3`."
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
                "The access key secret to use when authenticating to S3. Ignored "
                "if `protocol!=s3` or `s3_anonymous_connection=True`. Defaults to "
                "the value of the environment variable of the same name."
            ),
        ),
        th.Property(
            "caching_strategy",
            th.StringType,
            default="once",
            allowed_values=["none", "once", "persistent"],
            description=(
                "*DEVELOPERS ONLY* The caching method to use when "
                "`protocol!=file`. One of `none`, `once`, or `persistent`. `none` "
                "does not use caching at all. `once` (the default) will cache all "
                "files for the duration of the tap's invocation, then discard them "
                "upon completion. `peristent` will allow caches to persist between "
                "invocations of the tap, storing them in your OS's temp directory. "
                "It is recommended that you do not modify this setting."
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
        schema = self.config.get("schema", None)
        if file_type == "delimited":
            return [streams.DelimitedStream(self, name=name, schema=schema)]
        if file_type == "jsonl":
            return [streams.JSONLStream(self, name=name, schema=schema)]
        if file_type == "avro":
            return [streams.AvroStream(self, name=name, schema=schema)]
        if file_type in {"csv", "tsv", "txt"}:
            msg = f"'{file_type}' is not a valid file_type. Did you mean 'delimited'?"
            raise ValueError(msg)
        if file_type in {"json", "ndjson"}:
            msg = f"'{file_type}' is not a valid file_type. Did you mean 'jsonl'?"
            raise ValueError(msg)
        msg = f"'{file_type}' is not a valid file_type."
        raise ValueError(msg)

    @classmethod
    def cb_discover(
        cls: type[Tap],
        ctx: click.Context,
        param: click.Option,  # noqa: ARG003
        value: bool,  # noqa: FBT001
    ) -> None:
        """CLI callback to run the tap in discovery mode and pass state into tap."""
        if not value:
            return

        config_args = ctx.params.get("config", ())
        state = ctx.params.get("state", ())
        config_files, parse_env_config = cls.config_from_cli_args(*config_args)
        tap = cls(
            config=config_files,  # type: ignore[arg-type]
            state=state,
            parse_env_config=parse_env_config,
            validate_config=False,
        )
        tap.run_discovery()
        ctx.exit()

    def __init__(  # noqa: PLR0913
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        catalog: PurePath | str | dict | Catalog | None = None,
        state: PurePath | str | dict | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Initialize the tap, but create state before running discovery."""
        # Call grandparent (PluginBase) __init__ method.
        super(Tap, self).__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        # Declare private members
        self._streams: dict[str, Stream] | None = None
        self._input_catalog: Catalog | None = None
        self._state: dict[str, Stream] = {}
        self._catalog: Catalog | None = None  # Tap's working catalog

        # Process input catalog
        if isinstance(catalog, Catalog):
            self._input_catalog = catalog
        elif isinstance(catalog, dict):
            self._input_catalog = Catalog.from_dict(catalog)  # type: ignore[arg-type]
        elif catalog is not None:
            self._input_catalog = Catalog.from_dict(read_json_file(catalog))

        # Initialize mapper
        self.mapper: PluginMapper
        self.mapper = PluginMapper(
            plugin_config=dict(self.config),
            logger=self.logger,
        )

        # Process state. In parent (Tap), state is processed after registering from
        # catalog (see below), which causes issues.
        state_dict: dict = {}
        if isinstance(state, dict):
            state_dict = state
        elif state:
            state_dict = read_json_file(state)
        self.load_state(state_dict)

        # Register from catalog
        self.mapper.register_raw_streams_from_catalog(self.catalog)


if __name__ == "__main__":
    TapUniversalFile.cli()

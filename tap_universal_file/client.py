"""Custom client handling, including FileStream base class."""

from __future__ import annotations

import re
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generator, Iterable

from singer_sdk.streams import Stream

from tap_universal_file.files import FilesystemManager

if TYPE_CHECKING:
    from os import PathLike

    import singer_sdk._singerlib as singer
    from singer_sdk.tap_base import Tap


class FileStream(Stream):
    """Stream class for File streams."""

    def __init__(
        self,
        tap: Tap,
        schema: str | PathLike | dict[str, Any] | singer.Schema | None = None,
        name: str | None = None,
    ) -> None:
        """Duplicates superclass functionality but runs replication config before init.

        Raises:
            RuntimeError: If replication config is invalid.
        """
        # Define starting_replication_key_value based on state, stream_name, and
        # start_date. This has to be done before stream initialization below so that
        # state can be used during the discovery process.
        self.starting_replication_key_value: str | None = None
        if tap.state:
            stream_name = tap.config["stream_name"]
            if stream_name not in tap.state["bookmarks"]:
                msg = (
                    "State was passed so incremental replication is assumed. However, "
                    f"no state was found for a stream_name of {stream_name}."
                )
                raise RuntimeError(msg)
            self.starting_replication_key_value = tap.state["bookmarks"][stream_name][
                "replication_key_value"
            ]
        else:
            self.starting_replication_key_value = tap.config.get("start_date", None)

        super().__init__(tap, schema, name)

        # If _sdc_last_modified is not in the stream, incremental replication cannot
        # be used.
        if not (
            self.starting_replication_key_value is None
            or self.config["additional_info"]
        ):
            msg = "Incremental replication requires additional_info to be True."
            raise RuntimeError(msg)

        # This is set to a constant because the tap only supports _sdc_last_modified as
        # an incremental replication key, not custom values.
        self.replication_key = "_sdc_last_modified"

    @property
    def is_sorted(self) -> bool:
        """The stream returns records in order."""
        return True

    @cached_property
    def fs_manager(self) -> FilesystemManager:
        """A filesystem manager as a wrapper for all aspects of the filesystem.

        Returns:
            A FilesystemManager object with appropriate protocol and configuration.
        """
        return FilesystemManager(self.config, self.logger)

    @cached_property
    def schema(self) -> dict:
        """Orchestrates schema creation for all streams.

        Returns:
            A schema constructed using the get_properties() method of whichever stream
            is currently in use.
        """
        properties = self.get_properties()
        additional_info = self.config["additional_info"]
        if additional_info:
            properties.update({"_sdc_file_name": {"type": "string"}})
            properties.update({"_sdc_line_number": {"type": "integer"}})
            properties.update(
                {"_sdc_last_modified": {"type": "string", "format": "date-time"}},
            )
        return {"properties": properties}

    def add_additional_info(
        self,
        row: dict,
        file_name: str,
        line_number: int,
        last_modified: str,
    ) -> dict:
        """Adds _sdc-prefixed additional columns to a row, dependent on config.

        Args:
            row: The row to add info to.
            file_name: The name of the file that the row came from.
            line_number: The line number of the row within its file.
            last_modified: The last_modified date of the row's file.

        Returns:
            A dictionary representing a row containing additional information columns.
        """
        additional_info = self.config["additional_info"]
        if additional_info:
            row.update({"_sdc_file_name": file_name})
            row.update({"_sdc_line_number": line_number})
            row.update({"_sdc_last_modified": last_modified})
        return row

    def get_rows(self) -> Generator[dict[str | Any, str | Any], None, None]:
        """Gets rows of all files that should be synced.

        Raises:
            NotImplementedError: This must be implemented by a subclass.

        Yields:
            A dictionary representing a row to be synced.
        """
        msg = "get_rows must be implemented by subclass."
        raise NotImplementedError(msg)

    def get_properties(self) -> dict:
        """Gets properties for the purpose of schema generation.

        Raises:
            NotImplementedError: This must be implemented by a subclass.

        Returns:
            A dictionary representing a series of properties for schema generation.
        """
        msg = "get_properties must be implemented by subclass."
        raise NotImplementedError(msg)

    def get_compression(self, file: str) -> str | None:
        """Determines what compression encoding is appropraite for a given file.

        Args:
            file: The file to determine the encoding of.

        Returns:
            A string representing the appropriate compression encoding, or `None` if no
            compression is needed or if a compression encoding can't be determined.
        """
        compression: str = self.config["compression"]
        if compression == "none":
            return None
        if compression != "detect":
            return compression
        encoding = None
        if re.match(".*\\.zip$", file):
            encoding = "zip"
        elif re.match(".*\\.bz2$", file):
            encoding = "bz2"
        elif re.match(".*\\.gz(ip)?$", file):
            encoding = "gzip"
        elif re.match(".*\\.lzma$", file):
            encoding = "lzma"
        elif re.match(".*\\.xz$", file):
            encoding = "xz"
        return encoding

    def get_records(
        self,
        context: dict | None,  # noqa: ARG002
    ) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        get_records() currently doesn't do anything other than return each row in a call
        to get_rows(). Therefore, an alternative implementation would be to put the
        functionality for each subclass's version of get_rows() into its own version of
        get_records() and do away with get_rows() entirely. This method was chosen to
        preempt some sort of post-processing that might need to be applied.
        TODO: Remove explanation if alternative implementation is chosen or
        post-processing is added.

        Args:
            context: Stream partition or context dictionary.
        """
        yield from self.get_rows()

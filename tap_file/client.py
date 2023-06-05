"""Custom client handling, including FileStream base class."""

from __future__ import annotations

import re
from functools import cached_property
from typing import Any, Generator, Iterable

import fsspec
from pathlib import Path
from singer_sdk.streams import Stream


class FileStream(Stream):
    """Stream class for File streams."""

    @cached_property
    def schema(self) -> dict[str, dict]:
        """Gets a schema for the current stream.

        Raises:
            NotImplementedError: This must be implemented by a subclass.

        Returns:
            A dictionary in the format of a Singer schema.
        """
        msg = "schema must be implemented by subclass."
        raise NotImplementedError(msg)

    @cached_property
    def filesystem(self) -> fsspec.AbstractFileSystem:
        """A fsspec filesytem.

        Raises:
            ValueError: If the supplied protocol is not supported.

        Returns:
            A fileystem object of the appropriate type for the user-supplied protocol.
        """

        protocol = self.config["protocol"]

        if(protocol == "file"):
            return fsspec.filesystem("file")
        if(protocol == "s3"):
            if self.config["s3_anonymous_connection"]:
                return fsspec.filesystem("s3", anon = True)
            return fsspec.filesystem(
                "s3", 
                anon = False, 
                key = self.config["s3_access_key"], 
                secret = self.config["s3_access_key_secret"]
                )
        
        msg = f"Protocol '{protocol}' is not valid."
        raise ValueError(msg)

    def get_files(self) -> Generator[str, None, None]:
        """Gets file names to be synced.

        Yields:
            The name of a file to be synced, matching a regex pattern, if one has been
                configured.
        """

        for file in self.filesystem.ls(self.config['filepath'], detail=False):
            if "file_regex" in self.config:
                # RegEx is currently checked against basename rather than full path. 
                # Fullpath matching could be added if recursive subdirectory syncing is
                # implemented.
                if re.match(self.config["file_regex"], Path(file).name):
                    yield file
            else:
                yield file

    def get_rows(self) -> Generator[dict[str | Any, str | Any], None, None]:
        """Gets rows of all files that should be synced.

        Raises:
            NotImplementedError: This must be implemented by a subclass.

        Yields:
            A dictionary representing a row to be synced.
        """
        msg = "get_rows must be implemented by subclass."
        raise NotImplementedError(msg)

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
        for row in self.get_rows():
            yield row

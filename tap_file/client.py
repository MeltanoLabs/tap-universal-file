"""Custom client handling, including FileStream base class."""

from __future__ import annotations

import re
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Iterable

if TYPE_CHECKING:
    import fsspec

from singer_sdk.streams import Stream

from tap_file.files import FilesystemManager


class FileStream(Stream):
    """Stream class for File streams."""

    @cached_property
    def filesystem(self) -> fsspec.AbstractFileSystem:
        """A fsspec filesytem.

        Raises:
            ValueError: If the supplied protocol is not supported.

        Returns:
            A fileystem object of the appropriate type for the user-supplied protocol.
        """
        return FilesystemManager(self.config, self.logger).get_filesystem()

    def get_files(self, regex: str | None = None) -> Generator[str, None, None]:
        """Gets file names to be synced.

        Yields:
            The name of a file to be synced, matching a regex pattern, if one has been
                configured.
        """
        empty = True

        for file in self.filesystem.ls(self.config["filepath"], detail=True):
            # RegEx is currently checked against basename rather than full path.
            # Fullpath matching could be added if recursive subdirectory syncing is
            # implemented.
            if file["type"] == "directory" or file["size"] == 0:
                continue
            if "file_regex" in self.config and not re.match(
                self.config["file_regex"],
                Path(file["name"]).name,
            ):
                continue
            if regex is not None and not re.match(regex, Path(file["name"]).name):
                continue
            empty = False
            yield file["name"]

        if empty:
            msg = (
                "No files found. Choose a different `filepath` or try a more lenient "
                "`file_regex`."
            )
            raise RuntimeError(msg)


    def get_rows(self) -> Generator[dict[str | Any, str | Any], None, None]:
        """Gets rows of all files that should be synced.

        Raises:
            NotImplementedError: This must be implemented by a subclass.

        Yields:
            A dictionary representing a row to be synced.
        """
        msg = "get_rows must be implemented by subclass."
        raise NotImplementedError(msg)

    def get_compression(self, file: str) -> str | None:  # noqa: PLR0911
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
        if re.match(".*\\.zip$", file):
            return "zip"
        if re.match(".*\\.bz2$", file):
            return "bz2"
        if re.match(".*\\.gz(ip)?$", file):
            return "gzip"
        if re.match(".*\\.lzma$", file):
            return "lzma"
        if re.match(".*\\.xz$", file):
            return "xz"
        return None

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

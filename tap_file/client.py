"""Custom client handling, including FileStream base class."""

from __future__ import annotations

import re
from functools import cached_property
from pathlib import Path
from typing import Any, Generator, Iterable

import fsspec
from singer_sdk.streams import Stream


class FileStream(Stream):
    """Stream class for File streams."""

    @cached_property
    def filesystem(self) -> fsspec.AbstractFileSystem:  # noqa: PLR0911
        """A fsspec filesytem.

        TODO: Move this logic to an external class if support for further protocols is
        added.

        Raises:
            ValueError: If the supplied protocol is not supported.

        Returns:
            A fileystem object of the appropriate type for the user-supplied protocol.
        """
        protocol = self.config["protocol"]

        if protocol == "file":
            return fsspec.filesystem("file")
        if protocol == "s3":
            cache_filepath = self.config.get("cache_filepath", None)

            # A user specified anonymous connection overrides all else, allowing for a
            # uncredentialed requests even when credentials are available.
            if self.config["s3_anonymous_connection"]:
                if cache_filepath:
                    return fsspec.filesystem(
                        "filecache",
                        target_protocol="s3",
                        target_options={"anon": True},
                        cache_storage=cache_filepath,
                    )
                self.logger.warning(
                    "Caching is not being used. The entire contents of each resource "
                    "will be fetched during each read operation, which could be "
                    "expensive.",
                )
                return fsspec.filesystem("s3", anon=True)

            # If values are present in config, use them. If not, attempt to resolve
            # through boto3.
            if (
                "AWS_ACCESS_KEY_ID" in self.config
                and "AWS_SECRET_ACCESS_KEY" in self.config
            ):
                if cache_filepath:
                    return fsspec.filesystem(
                        "filecache",
                        target_protocol="s3",
                        target_options={
                            "anon": False,
                            "key": self.config["AWS_ACCESS_KEY_ID"],
                            "secret": self.config["AWS_SECRET_ACCESS_KEY"],
                        },
                        cache_storage=cache_filepath,
                    )
                self.logger.warning(
                    "Caching is not being used. The entire contents of each resource "
                    "will be fetched during each read operation, which could be "
                    "expensive.",
                )
                return fsspec.filesystem(
                    "s3",
                    anon=False,
                    key=self.config["AWS_ACCESS_KEY_ID"],
                    secret=self.config["AWS_SECRET_ACCESS_KEY"],
                )

            # Using boto3 credential resolution.
            self.logger.warning(
                "Defaulting to boto3 credential resolution. To force an anonymous "
                "connection, set 's3_anonymous_connection' to True."
                "Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#environment-variables",
            )
            if cache_filepath:
                return fsspec.filesystem(
                    "filecache",
                    target_protocol="s3",
                    target_options={"anon": False},
                    cache_storage=cache_filepath,
                )
            self.logger.warning(
                "Caching is not being used. The entire contents of each resource will "
                "be fetched during each read operation, which could be expensive.",
            )
            return fsspec.filesystem("s3", anon=False)

        msg = f"Protocol '{protocol}' is not valid."
        raise ValueError(msg)

    def get_files(self, regex: str | None = None) -> Generator[str, None, None]:
        """Gets file names to be synced.

        Yields:
            The name of a file to be synced, matching a regex pattern, if one has been
                configured.
        """
        for file in self.filesystem.ls(self.config["filepath"], detail=False):
            # RegEx is currently checked against basename rather than full path.
            # Fullpath matching could be added if recursive subdirectory syncing is
            # implemented.
            if "file_regex" in self.config and not re.match(
                self.config["file_regex"],
                Path(file).name,
            ):
                continue
            if regex is not None and not re.match(regex, Path(file).name):
                continue
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
        if re.match(".*\\.(gzip|gz)$", file):
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

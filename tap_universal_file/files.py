"""Handling for the creation and usage of filesystems."""

from __future__ import annotations

import datetime
import re
import tempfile
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator

if TYPE_CHECKING:
    import logging

import fsspec


class FilesystemManager:
    """A wrapper for managing fsspec filessystems."""

    def __init__(self, config: dict[str, Any], logger: logging.Logger) -> None:
        """Initialize a new FilesystemManager instance.

        Args:
            config: Configuration to create filesystems with.
            logger: Logger to pipe warnings and errors into.
        """
        self.config: dict[str, Any] = config
        self.logger: logging.Logger = logger

    @cached_property
    def protocol(self) -> str:
        """Protocol from config."""
        return self.config["protocol"]

    @cached_property
    def filesystem(self) -> fsspec.AbstractFileSystem:
        """Gets a filesystem with appropriate protocol and caching.

        Raises:
            ValueError: If an invalid protocol or caching method is supplied.

        Returns:
            An fsspec filesystem.
        """
        caching_strategy = self.config["caching_strategy"]

        if self.protocol == "file":
            return fsspec.filesystem("file")

        if caching_strategy == "once":
            # Creating a filecache without specifying cache_storage location will cause
            # the cache to be discarded after the filesystem is closed.
            # Docs: https://filesystem-spec.readthedocs.io/en/latest/features.html#caching-files-locally
            return fsspec.filesystem(
                "filecache",
                target_protocol=self.protocol,
                target_options=self._get_args(),
            )
        if caching_strategy == "persistent":
            return fsspec.filesystem(
                "filecache",
                target_protocol=self.protocol,
                target_options=self._get_args(),
                cache_storage=tempfile.gettempdir(),
            )
        if caching_strategy == "none":
            # When caching is not used, the protocol's arguments have to be
            # star-unpacked because fsspec.filesystem accepts them directly instead of
            # as a dictionary.
            return fsspec.filesystem(
                protocol=self.protocol,
                **self._get_args(),
            )
        msg = f"The caching strategy '{caching_strategy} is invalid."
        raise ValueError(msg)

    def get_files(
        self,
        starting_replication_key_value: str | None = None,
    ) -> Generator[dict, None, None]:
        """Gets file names to be synced.

        Args:
            starting_replication_key_value: _description_. Defaults to None.

        Raises:
            RuntimeError: If no files match the configured regex pattern or replication
            key value.

        Yields:
            The name of a file to be synced, matching a regex pattern, if one has been
                configured.
        """
        none_found = True
        none_synced = True

        file_dict_list = []

        for file_path in self.filesystem.find(self.config["file_path"]):
            file = self.filesystem.info(file_path)
            if (
                file["type"] == "directory"  # Ignore nested folders.
                or file["size"] == 0  # Ignore empty files.
                or (  # Ignore files not matching the configured file_regex
                    "file_regex" in self.config
                    and not re.match(
                        self.config["file_regex"],
                        file["name"],
                    )
                )
            ):
                continue
            none_found = False
            file_dict_list.append(
                {"name": file["name"], "last_modified": self._get_last_modified(file)},
            )

        # Sort the files so that is_sorted can be True. This allows the tap to pick up
        # where it left off if interrupted.
        file_dict_list = sorted(file_dict_list, key=lambda k: k["last_modified"])

        # Only yield files when no replication key is present or when the file is newer
        # than the replication key value, as a datetime.
        for file_dict in file_dict_list:
            if starting_replication_key_value is None or file_dict[
                "last_modified"
            ] >= datetime.datetime.strptime(
                starting_replication_key_value,
                r"%Y-%m-%dT%H:%M:%S%z",  # ISO-8601
            ):
                none_synced = False
                yield file_dict
                continue

        if none_found:
            msg = (
                "No files found. Choose a different `file_path` or try a more lenient "
                "`file_regex`."
            )
            raise RuntimeError(msg)
        if none_synced:
            msg = (
                "Current state precludes files being synced as none have been modified "
                "since state was last updated."
            )
            self.logger.warning(msg)

    def _get_last_modified(self, file: dict) -> datetime.datetime | None:
        """Finds the last modified date from a file dictionary.

        The implementation for the last modified date of a file varies by fsspec
        protocol. This method takes protocol into account and processes the last
        modified date of a file accordingly so that a datetime object is returned.

        Args:
            file: The dictionary containing information about the file.

        Returns:
            The file's last modified date.
        """
        if self.protocol == "file":
            return datetime.datetime.fromtimestamp(
                int(file["mtime"]),
                datetime.timezone.utc,
            )
        if self.protocol == "s3":
            return file["LastModified"]
        msg = f"The protocol '{self.protocol}' is invalid."
        raise ValueError(msg)

    def _get_args(self) -> dict[str, Any]:
        """Gets the fsspec arguments for a certain set of configuration options.

        Some fsspec implementations require additional configuration. The behavior
        here is abstracted to a dedicated method for two reasons:
          - When further protocols are added, the logic to determine their arguments
                would become unwieldy if left in the main method.
          - Creating a consistent dictionary of arguments allows the dictionary to be
                used both when provided directly as an fsspec argument to a cached
                implementation and when unpacked (**) for a direct implementation.

        Returns:
            A dictionary containing fsspec arguments.
        """
        if self.protocol == "s3":
            if self.config["s3_anonymous_connection"]:
                return {"anon": True}
            if (
                "AWS_ACCESS_KEY_ID" in self.config
                and "AWS_SECRET_ACCESS_KEY" in self.config
            ):
                return {
                    "anon": False,
                    "key": self.config["AWS_ACCESS_KEY_ID"],
                    "secret": self.config["AWS_SECRET_ACCESS_KEY"],
                }
            return {"anon": False}
        msg = f"The protocol '{self.protocol}' is invalid."
        raise ValueError(msg)

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
        self._check_config()
        caching_strategy = self.config["caching_strategy"]

        if self.protocol == "file":
            return fsspec.filesystem("file")

        if caching_strategy == "once":
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
            return fsspec.filesystem(
                protocol=self.protocol,
                **self._get_args(),
            )
        return None

    def get_files(
        self,
        starting_replication_key_value: str | None = None,
    ) -> Generator[dict, None, None]:
        """Gets file names to be synced.

        Yields:
            The name of a file to be synced, matching a regex pattern, if one has been
                configured.
        """
        none_found = True
        none_synced = True

        file_dict_list = []

        for file in self.filesystem.ls(self.config["filepath"], detail=True):
            if (
                file["type"] == "directory"
                or file["size"] == 0
                or (
                    "file_regex" in self.config
                    and not re.match(
                        self.config["file_regex"],
                        Path(file["name"]).name,
                    )
                )
            ):
                continue
            none_found = False
            file_dict = {}
            file_dict.update(
                {"name": file["name"], "last_modified": self._get_last_modified(file)},
            )
            file_dict_list.append(file_dict)

        file_dict_list = sorted(
            file_dict_list,
            key=lambda k: k["last_modified"],
            reverse=True,
        )

        for file_dict in file_dict_list:
            if starting_replication_key_value is None or file_dict[
                "last_modified"
            ] >= datetime.datetime.strptime(
                starting_replication_key_value,
                r"%Y-%m-%dT%H:%M:%S%z",
            ):
                none_synced = False
                yield file_dict
                continue
            break

        if none_found:
            msg = (
                "No files found. Choose a different `filepath` or try a more lenient "
                "`file_regex`."
            )
            raise RuntimeError(msg)
        if none_synced:
            msg = (
                "Current state precludes files being synced as none have been modified "
                "since state was last updated."
            )
            raise RuntimeError(msg)

    def _get_last_modified(self, file: dict) -> datetime.datetime | None:
        if self.protocol == "file":
            return datetime.datetime.fromtimestamp(
                int(file["mtime"]),
                datetime.timezone.utc,
            )
        if self.protocol == "s3":
            return file["LastModified"]
        return None

    def _get_args(self) -> dict[str, Any]:
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
        return None

    def _check_config(self) -> None:
        caching_strategy = self.config["caching_strategy"]

        if self.protocol not in {"file", "s3"}:
            msg = f"Protocol '{self.protocol}' is not valid."
            raise ValueError(msg)

        if caching_strategy not in {"none", "once", "persistent"}:
            msg = f"Caching strategy '{caching_strategy}' is not valid."
            raise ValueError(msg)

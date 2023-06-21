"""Handling for the creation and usage of filesystems."""

from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING, Any

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

    def get_filesystem(self) -> fsspec.AbstractFileSystem:
        """Gets a filesystem with appropriate protocol and caching.

        Raises:
            ValueError: If an invalid protocol or caching method is supplied.

        Returns:
            An fsspec filesystem.
        """
        self._check_config()
        protocol = self.config["protocol"]
        caching_strategy = self.config["caching_strategy"]

        if protocol == "file":
            return fsspec.filesystem("file")

        if caching_strategy == "once":
            return fsspec.filesystem(
                "filecache",
                target_protocol=self.config["protocol"],
                target_options=self._get_args(),
            )
        if caching_strategy == "persistent":
            return fsspec.filesystem(
                "filecache",
                target_protocol=self.config["protocol"],
                target_options=self._get_args(),
                cache_storage=tempfile.gettempdir(),
            )
        if caching_strategy == "none":
            return fsspec.filesystem(
                protocol=self.config["protocol"],
                **self._get_args(),
            )
        return None

    def _get_args(self) -> dict[str, Any]:
        protocol = self.config["protocol"]
        if protocol == "s3":
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
        protocol = self.config["protocol"]
        caching_strategy = self.config["caching_strategy"]

        if protocol not in {"file", "s3"}:
            msg = f"Protocol '{protocol}' is not valid."
            raise ValueError(msg)

        if caching_strategy not in {"none", "once", "persistent"}:
            msg = f"Caching strategy '{caching_strategy}' is not valid."
            raise ValueError(msg)

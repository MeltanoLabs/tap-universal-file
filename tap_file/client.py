"""Custom client handling, including FileStream base class."""

from __future__ import annotations

import re
from functools import cached_property
from typing import Any, Generator, Iterable, Literal

import fsspec
from click import Path
from singer_sdk.streams import Stream


class FileStream(Stream):
    """Stream class for File streams."""

    json_type = Literal["integer", "number", "boolean", "string"]

    @cached_property
    def schema(self) -> dict[str, dict]:
        """Gets a schema for the current stream.

        Returns:
            A dictionary in the format of a Singer schema.
        """
        # Sample the files and find counts for each potential data type.
        properties = self.count_samples()

        self.logger.info("Establishing schema.")

        # For each property, determine the dominant data type.
        for key, value in properties.items():
            # If there's only one data type detected through sampling, this would
            # indicate a clearly dominant data type, so it should be preferred.
            if len(value) == 1:
                if value.get("integer", 0) > 0:
                    value_type = "integer"
                elif value.get("number", 0) > 0:
                    value_type = "number"
                elif value.get("boolean", 0) > 0:
                    value_type = "boolean"
                elif value.get("string", 0) > 0:
                    value_type = "string"
                else:
                    self.logger.warning(
                        "Unknown data type for key '{key}'. Defaulting to string.",
                        extra={
                            "key": key,
                        },
                    )
                    value_type = "string"

            # If both integers and floats have been detected, prefer floats because they
            # are the "wider" data type.
            elif (
                len(value) == 2  # noqa: PLR2004
                and value.get("integer", 0) > 0
                and value.get("number", 0) > 0
            ):
                value_type = "number"

            # If the above does not allow a data type to be determined, fallback to
            # string because it is a general-case solution. However, a warning is logged
            # because this is not ideal behavior.
            else:
                self.logger.warning(
                    "Ambiguous data type for key '{key}'. Defaulting to string.",
                    extra={
                        "key": key,
                    },
                )
                value_type = "string"
            properties[key] = {
                "type": ["null", value_type],
            }

        self.logger.info("Schema complete.")

        # Return the properties, appropriately keyed as for a Singer schema.
        return {"properties": properties}

    @cached_property
    def url(self) -> str:
        """Returns a URL to the folder containing the files to be synced.

        Returns:
            A URL for fsspec ingestion. Protocol is always followed by `://` for fsspec.
                A trailing `/` is added for easy concatenation with file names.
        """
        return f"{self.config['protocol']}://{self.config['filepath']}/"

    def get_samples(
        self,
        sample_rate: int,
        max_samples: int,
    ) -> list[dict[str | Any, str | Any]]:
        """Samples the files returns their data.

        Args:
            sample_rate: Determines how often samples are taken, with a value of N
                causing every Nth row to be sampled.
            max_samples: The maximum number of samples to take before returning, to
            limit the time spent sampling large datasets.

        Returns:
            A list of samples.
        """
        samples = []
        current_row = 0  # Tracks total rows read, not reset between files.

        for row in self.get_rows():
            if current_row % sample_rate == 0:
                samples.append(row)
            current_row += 1
            if len(samples) >= max_samples:
                return samples
        return samples

    def get_files(self) -> Generator[str, None, None]:
        """Gets file names to be synced.

        Yields:
            The name of a file to be synced, matching a regex pattern, if one has been
                configured.
        """
        fs = fsspec.filesystem("file")
        files = fs.ls(self.url, detail=False)
        for file in files:
            if "file_regex" in self.config:
                # RegEx is checked again basename rather than full path for an easier
                # user experience. Could potentially add opt-in fullpath matching if
                # recursive subdirectory syncing is implemented.
                if re.match(self.config["file_regex"], Path.name(file)):
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

    def count_samples(self) -> dict:
        """Pull a small number of rows from files and check their data types.

        Returns:
            A dictionary with information on predicted data types.
        """
        to_return = {}

        self.logger.info("Beginning sample.")

        for sample in self.get_samples(99, 20):  # TODO: replace hardcoded sample rate.
            for key, value in sample.items():
                if key not in to_return:
                    to_return[key] = {}
                datatype = self.get_datatype(value)
                if datatype is not None:
                    to_return[key][datatype] = to_return[key].get(datatype, 0) + 1

        self.logger.info("Sample complete.")

        return to_return

    def get_datatype(self, value: Any | None) -> json_type | None:
        """Tries to coerce value to various types and returns the first match.

        Integer is checked before float because it is the "narrower" data type. Boolean
        could be checked either first or last since 0 and 1 are not boolean values in
        this context.

        Args:
            value: A value whose datatype should be found.

        Returns:
            A string indicating a data type or None if value is None or blank.
        """
        if value is None or not str(value).strip():
            return None
        try:
            int(value)
        except (ValueError, TypeError):
            pass
        else:
            return "integer"
        try:
            float(value)
        except (ValueError, TypeError):
            pass
        else:
            return "number"
        # Catches True, False, "true", "false", "True", and "False", among others.
        if str(value).lower() in ("true", "false"):
            return "boolean"
        return "string"

    def convert_value(
        self,
        value: Any | None,
        convert_to: json_type,
    ) -> int | float | bool | str | None:
        """Converts a value to a specified data type.

        Args:
            value: The value to be converted.
            convert_to: The data type to convert to. One of "integer", "number",
                "boolean", or "string".

        Raises:
            ValueError: If convert_to is "boolean" but value cannot be coerced to
                boolean, or if convert_to is not one of the allowed values.

        Returns:
            The value argument, after being converted to the appropriate data type.
        """
        if value is None or not str(value).strip():
            return None

        if convert_to == "integer":
            return int(value)

        if convert_to == "number":
            return float(value)

        if convert_to == "boolean":
            # Catches True, False, "true", "false", "True", and "False", among others.
            if str(value).lower() in ("true", "false"):
                return str(value).lower() == "true"
            msg = f'Value "{value!s}" could not be coerced to boolean.'
            raise ValueError(msg)

        if convert_to == "string":
            return str(value)

        msg = (
            "Invalid value for convert_to. Must be one of 'integer', 'number', ",
            "'boolean', or 'string'.",
        )
        raise ValueError(msg)

    def convert_row(
        self,
        row: dict[str | Any, str | Any],
        schema: dict[str, dict],
    ) -> dict[str | Any, str | Any]:
        """Converts all values in a row of data so they conform to a specified schema.

        Args:
            row: A dictionary representing a row of data.
            schema: A Singer schema.

        Returns:
            The row, with all values in the row aligning to the schema.
        """
        for key, value in row.items():
            # It shouldn't happen, but if the row has a value that doesn't have a type
            # in the schema, default to string.
            if key in schema["properties"] and "type" in schema["properties"][key]:
                convert_to = schema["properties"][key]["type"]
            else:
                self.logger.warning(
                    "No schema for key '{key}'; defaulting to string.",
                    extra={
                        "key": key,
                    },
                )
                convert_to = "string"

            # If convert_to has more than one value, remove any nulls and then take the
            # first element, which should be a singular standard json schema type.
            if isinstance(convert_to, list):
                if "null" in convert_to:
                    convert_to.remove("null")
                if len(convert_to) >= 2:  # noqa: PLR2004
                    self.logger.warning(
                        (
                            "Ambiguous schema. Converting value '{value}' to ",
                            "'{convert_type}'.",
                        ),
                        extra={
                            "value": value,
                            "convert_type": convert_to[0],
                        },
                    )
                convert_to = convert_to[0]
            row[key] = self.convert_value(value, convert_to)
        return row

    def get_records(
        self,
        context: dict | None,  # noqa: ARG002
    ) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.
        """
        for row in self.get_rows():
            yield self.convert_row(row, self.schema)

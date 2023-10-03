"""Tests standard tap features using the built-in SDK tests library."""
# flake8: noqa
import io
import json
import os
import shutil
import tempfile
from collections import defaultdict
from contextlib import redirect_stdout
from pathlib import Path

import pytest
from singer_sdk.testing import get_tap_test_class

from tap_universal_file.tap import TapUniversalFile


# Helper functions


def data_dir() -> str:
    """Gets the directory in tests/data where data is stored.

    Returns:
        A str representing a file path to tests/data.
    """
    return str(Path(__file__).parent / Path("./data"))


base_file_config = {
    "protocol": "file",
    "file_path": data_dir(),
}


def execute_tap(config: dict = None, state: dict = None):
    """Executes a TapUniversalFile tap.

    Args:
        config: Configuration for the tap.
        state: State input for the tap.

    Returns:
        A dictionary containing messages about the tap's invocation, including, schema,
        records (both messages about them and the records themselves), and state.
    """
    schema_messages: list[dict] = []
    record_messages: list[dict] = []
    state_messages: list[dict] = []
    records: defaultdict = defaultdict(list)

    stdout_buf = io.StringIO()
    with redirect_stdout(stdout_buf):
        tap_config = config if config is not None else {}
        TapUniversalFile(config=tap_config, state=state).run_sync_dry_run(dry_run_record_limit=None)
    stdout_buf.seek(0)

    for message in [
        json.loads(line) for line in stdout_buf.read().strip().split("\n") if line
    ]:
        if message:
            if message["type"] == "STATE":
                state_messages.append(message)
                continue
            if message["type"] == "SCHEMA":
                schema_messages.append(message)
                continue
            if message["type"] == "RECORD":
                stream_name = message["stream"]
                record_messages.append(message)
                records[stream_name].append(message["record"])
                continue
    return {
        "schema_messages": schema_messages,
        "record_messages": record_messages,
        "state_messages": state_messages,
        "records": records,
    }


# Run standard built-in tap tests from the SDK on a simple csv.

sample_config = base_file_config.copy()
sample_config.update({"file_regex": "^.*fruit_records\\.csv"})

TestTapUniversalFile = get_tap_test_class(
    tap_class=TapUniversalFile,
    config=sample_config,
)


# Run custom tests


def test_sdc_fields_present():
    modified_config = base_file_config.copy()
    modified_config.update(
        {"file_regex": "^.*fruit_records\\.csv$", "additional_info": True}
    )
    messages = execute_tap(modified_config)
    properties = messages["schema_messages"][0]["schema"]["properties"]
    assert properties["_sdc_line_number"], "_sdc_line_number is not present in schema"
    assert properties["_sdc_file_name"], "_sdc_file_name is not present in schema"


def test_delimited_execution():
    modified_config = base_file_config.copy()
    modified_config.update(
        {"file_type": "delimited", "file_regex": "^.*fruit_records\\.csv$"},
    )
    execute_tap(modified_config)


def test_jsonl_execution():
    modified_config = base_file_config.copy()
    modified_config.update(
        {
            "file_type": "jsonl",
            "file_regex": "^.*\\/employees\\.jsonl$",
            "jsonl_sampling_strategy": "first",
            "jsonl_type_coercion_strategy": "string",
        },
    )
    execute_tap(modified_config)


def test_avro_execution():
    modified_config = base_file_config.copy()
    modified_config.update(
        {
            "file_type": "avro",
            "file_regex": "^.*athletes\\.avro$",
            "avro_type_coercion_strategy": "convert",
        },
    )
    execute_tap(modified_config)


def test_s3_execution():
    s3_config = {
        "protocol": "s3",
        "file_path": "derek-tap-filetesting/2023",
        "file_regex": "^.*airtravel\\.csv$",
    }
    execute_tap(s3_config)


def test_compression_execution():
    modified_config = base_file_config.copy()
    modified_config.update(
        {
            "file_regex": "^.*fruit_records",
            "compression": "detect",
            "delimited_delimiter": ",",
        },
    )
    execute_tap(modified_config)


def test_header_footer_execution():
    modified_config = base_file_config.copy()
    modified_config.update(
        {
            "file_regex": "^.*cats\\.csv$",
            "delimited_header_skip": 3,
            "delimited_footer_skip": 3,
        },
    )
    execute_tap(modified_config)


def test_malformed_delimited_fail():
    modified_config = base_file_config.copy()
    modified_config.update(
        {"file_regex": "^.*cats\\.csv$", "delimited_error_handling": "fail"}
    )
    with pytest.raises(RuntimeError, match="^Error processing.*"):
        execute_tap(modified_config)


def test_malformed_delimited_ignore():
    modified_config = base_file_config.copy()
    modified_config.update(
        {"file_regex": "^.*cats\\.csv$", "delimited_error_handling": "ignore"}
    )
    execute_tap(modified_config)


def test_malformed_jsonl_fail():
    modified_config = base_file_config.copy()
    modified_config.update(
        {
            "file_regex": "^.*malformed_employees\\.jsonl$",
            "file_type": "jsonl",
            "jsonl_error_handling": "fail",
            "jsonl_type_coercion_strategy": "string",
        }
    )
    with pytest.raises(RuntimeError, match="^Error processing.*"):
        execute_tap(modified_config)


def test_malformed_jsonl_ignore():
    modified_config = base_file_config.copy()
    modified_config.update(
        {
            "file_regex": "^.*malformed_employees\\.jsonl$",
            "file_type": "jsonl",
            "jsonl_error_handling": "ignore",
            "jsonl_type_coercion_strategy": "string",
        }
    )
    execute_tap(modified_config)


def test_incremental_sync_using_state():
    # create tmp dir to isolate test and avoid conflicts with other tests in the future
    with tempfile.TemporaryDirectory() as tmp_dir:
        print('created temporary directory', tmp_dir)
        # copy the source file to the temp directory
        shutil.copy2(data_dir() + "/old_hardware.csv", tmp_dir)
        config = {
            "protocol": "file",
            "file_path": tmp_dir,
            "stream_name": "file",
            "file_regex": ".*hardware\\.csv$",
            "file_type": "delimited",
        }

        # run tap for the first time, it should sync all the records from old_hardware.csv
        messages = execute_tap(config)
        assert len(messages["records"]["file"]) == 5, "Improper number of records returned"

        # copy new_hardware.csv to the temp directory, simulating new data coming in the source folder
        shutil.copy2(data_dir() + "/new_hardware.csv", tmp_dir)

        # run the tap again with the same config and state from previous execution, it should sync only the new records
        messages = execute_tap(config, state=messages["state_messages"][0]["value"])
        assert len(messages["records"]["file"]) == 6, "Improper number of records returned. It should return " \
                                                      "only new records"


def test_incremental_sync_using_state_with_no_changes():
    # create tmp dir to isolate test and avoid conflicts with other tests in the future
    with tempfile.TemporaryDirectory() as tmp_dir:
        print('created temporary directory', tmp_dir)
        # copy the source file to the temp directory
        shutil.copy2(data_dir() + "/old_hardware.csv", tmp_dir)
        config = {
            "protocol": "file",
            "file_path": tmp_dir,
            "stream_name": "file",
            "file_regex": ".*hardware\\.csv$",
            "file_type": "delimited",
        }

        # run tap for the first time, it should sync all the records from old_hardware.csv
        messages = execute_tap(config)
        assert len(messages["records"]["file"]) == 5, "Improper number of records returned"

        # run the tap again with the same config and state from previous execution, it should not sync any records
        # since there were no changes in the source folder
        messages = execute_tap(config, state=messages["state_messages"][0]["value"])
        assert len(messages["records"]["file"]) == 0, "Improper number of records returned. It should return " \
                                                      "0 records because there were no changes in the source folder."


def test_incremental_sync():
    os.utime(data_dir() + "/old_hardware.csv", (1641124800, 1641124800))
    os.utime(data_dir() + "/new_hardware.csv", (1641211200, 1641211200))
    modified_config = base_file_config.copy()
    modified_config.update(
        {
            "stream_name": "file",
            "file_regex": ".*hardware\\.csv$",
            "file_type": "delimited",
        }
    )
    messages = execute_tap(modified_config)
    assert len(messages["records"]["file"]) == 11, "Improper number of records returned"
    start_date = messages["state_messages"][0]["value"]["bookmarks"]["file"][
        "replication_key_value"
    ]
    modified_config.update({"start_date": start_date})
    messages = execute_tap(modified_config)
    assert len(messages["records"]["file"]) == 0, "Improper number of records returned. It should return " \
                                                  "0 records because there no new data were added."

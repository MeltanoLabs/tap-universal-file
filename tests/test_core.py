"""Tests standard tap features using the built-in SDK tests library."""
# flake8: noqa
import io
import json
from collections import defaultdict
from contextlib import redirect_stdout
from pathlib import Path

from singer_sdk.testing import get_tap_test_class

from tap_file.tap import TapFile

# Helper functions


def data_dir() -> str:
    """Gets the directory in tests/data where data is stored.

    Returns:
        A str representing a filepath to tests/data.
    """
    return str(Path(__file__).parent / Path("./data"))


base_file_config = {
    "protocol": "file",
    "filepath": data_dir(),
}


def execute_tap(config: dict = {}):
    """Executes a TapFile tap.

    Args:
        config: Configuration for the tap. Defaults to {}.

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
        TapFile(config=config).run_sync_dry_run(dry_run_record_limit=None)
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
sample_config.update({"file_regex": "fruit_records\\.csv"})

TestTapFile = get_tap_test_class(
    tap_class=TapFile,
    config=sample_config,
)

# Run custom tests

sdc_config = base_file_config.copy()
sdc_config.update({"file_regex": "^fruit_records\\.csv$", "additional_info": True})
delimited_config = base_file_config.copy()
delimited_config.update(
    {"file_type": "delimited", "file_regex": "^fruit_records\\.csv$"},
)
jsonl_config = base_file_config.copy()
jsonl_config.update(
    {
        "file_type": "jsonl",
        "file_regex": "^employees\\.jsonl$",
        "jsonl_sampling_strategy": "first",
        "jsonl_type_coercion_strategy": "string",
    },
)
avro_config = base_file_config.copy()
avro_config.update(
    {
        "file_type": "avro",
        "file_regex": "^athletes\\.avro$",
        "avro_type_coercion_strategy": "convert",
    },
)
s3_config = {"protocol": "s3", "filepath": "tap-file-taptesting/grocery"}
compression_config = base_file_config.copy()
compression_config.update(
    {
        "file_regex": "fruit_records",
        "compression": "detect",
        "delimited_delimiter": ",",
    },
)
header_footer_config = base_file_config.copy()
header_footer_config.update(
    {
        "file_regex": "^cats\\.csv$",
        "delimited_header_skip": 3,
        "delimited_footer_skip": 3,
    },
)


def test_sdc_fields_present():
    messages = execute_tap(sdc_config)
    properties = messages["schema_messages"][0]["schema"]["properties"]
    assert properties["_sdc_line_number"], "_sdc_line_number is not present in schema"
    assert properties["_sdc_file_name"], "_sdc_file_name is not present in schema"


def test_delimited_execution():
    execute_tap(delimited_config)


def test_jsonl_execution():
    execute_tap(jsonl_config)


def test_avro_execution():
    execute_tap(avro_config)


def test_s3_execution():
    execute_tap(s3_config)


def test_compression_execution():
    execute_tap(compression_config)


def test_header_footer_execution():
    execute_tap(header_footer_config)

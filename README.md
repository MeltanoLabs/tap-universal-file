# tap-file

**IMPORTANT**: This tap is still under development and should not be used in its current form. <!-- TODO: remove disclaimer when feature-complete. -->

`tap-file` is a Singer tap for File.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

An example GitHub installation command:

```bash
pipx install git+https://github.com/MeltanoLabs/tap-file.git
```

## Configuration

### Accepted Config Options

| Setting                | Required | Default | Description |
|:-----------------------|:--------:|:-------:|:------------|
| protocol               | True     | None    | The protocol to use to retrieve data. One of `file` or `s3`. |
| filepath               | True     | None    | The path to obtain files from. Example: `/foo/bar`. |
| file_regex             | False    | None    | A regex pattern to only include certain files. Example: `.*\.csv`. |
| s3_anonymous_connection| False    |       0 | Whether to use an anonymous S3 connection, without the use of any credentials. Ignored if `protocol!=s3`. |
| s3_access_key          | False    | None    | The access key to use when authenticating to S3. Ignored if `protocol!=s3` or `s3_anonymous_connection=True`. |
| s3_access_key_secret   | False    |       1 | The access key secret to use when authenticating to S3. Ignored if `protocol!=s3`or `s3_anonymous_connection=True`. |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-file --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-file` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-file --version
tap-file --help
tap-file --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-file` CLI interface directly using `poetry run`:

```bash
poetry run tap-file --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-file
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-file --version
# OR run a test `elt` pipeline:
meltano elt tap-file target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

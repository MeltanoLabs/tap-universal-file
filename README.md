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
| cache_filepath         | False    | None    | The location to store cached files when `protocol!=file`. If left blank, caching will not be used and the entire contents of each resource will be fetched for each read operation. |
| s3_anonymous_connection| False    |       0 | Whether to use an anonymous S3 connection, without the use of any credentials. Ignored if `protocol!=s3`. |
| AWS_ACCESS_KEY_ID      | False    | $AWS_ACCESS_KEY_ID | The access key to use when authenticating to S3. Ignored if `protocol!=s3` or `s3_anonymous_connection=True`. Defaults to the value of the environment variable of the same name. |
| AWS_SECRET_ACCESS_KEY  | False    | $AWS_SECRET_ACCESS_KEY | The access key secret to use when authenticating to S3. Ignored if `protocol!=s3`or `s3_anonymous_connection=True`. Defaults to the value of the environment variable of the same name. |
| stream_maps            | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config      | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled     | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth   | False    | None    | The max depth to flatten schemas. | <!-- Manually added entries begin below. -->
| batch_config           | False    | None    | Object containing batch configuration information, as specified in the [Meltano documentation](https://sdk.meltano.com/en/latest/batch.html). Has two child objects: `encoding` and `storage`. |
| batch_config.encoding  | False    | None    | Object containing information about how to encode batch information. Has two child entries: `format` and `compression`. |
| batch_config.storage   | False    | None    | Object containing information about how batch files should be stored. Has two child entries: `root` and `prefix`. |
| batch_config.encoding.format       | False    | None    | Format to store batch files in. Example: `jsonl`. |
| batch_config.encoding.compression  | False    | None    | Method with which to compress batch files. Example: `gzip`. |
| batch_config.storage.root          | False    | None    | Location to store batch files. Examples: `file:///foo/bar`, `file://output`, `s3://bar/foo`. Note that the triple-slash is not a typo: it indicates an absolute filepath. |
| batch_config.storage.prefix        | False    | None    | Prepended to the names of all batch files. Example: `batch-`.  |

### Additional S3 Dependency

If you use `protocol=s3` and/or if you use batching to send data to S3, you will need to add the additional dependency `s3`. For example, you could update `meltano.yml` to have `pip_url: -e .[s3]`.

### Sample Batching Config

Here is an example `meltano.yml` entry to configure batch files, and then the same sample configuration in JSON.
```yml
config:
  # ... other config options ...
  batch_config:
    encoding:
      format: jsonl
      compression: gzip
    storage:
      root: file:///foo/bar
      prefix: batch-
```
```json
{
  // ... other config options ...
  "batch_config": {
    "encoding": {
      "format": "jsonl",
      "compression": "gzip",
    },
    "storage": {
      "root": "file:///foo/bar",
      "prefix": "batch-",
    }
  }
}
```

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

#### S3

If you use S3, either for fetching files or for batching, you will need to obtain an access key and secret from AWS IAM. Specifically, `protocol=s3` requires the ListBucket and GetObject permissions, and batching requires the PutObject permission. <!-- TODO: Expand this section with more detailed instructions -->

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

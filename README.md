# tap-universal-file

**IMPORTANT**: This tap is still under development and should not be used in its current form. <!-- TODO: remove disclaimer when feature-complete. -->

`tap-universal-file` is a Singer tap for File.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

An example GitHub installation command:

```bash
pipx install git+https://github.com/MeltanoLabs/tap-universal-file.git
```

## Configuration

### Accepted Config Options

| Setting                     | Required | Default | Description |
|:----------------------------|:--------:|:-------:|:------------|
| stream_name                 | False    | file    | The name of the stream that is output by the tap. |
| protocol                    | True     | None    | The protocol to use to retrieve data. Must be either `file` or `s3`. |
| file_path                    | True     | None    | The path to obtain files from. Example: `/foo/bar`. Or, for `protocol==s3`, use `s3-bucket-name` instead. |
| file_regex                  | False    | None    | A regex pattern to only include certain files. Example: `.*\.csv`. |
| file_type                   | False    | delimited | Must be one of `delimited`, `jsonl`, or `avro`. Indicates the type of file to sync, where `delimited` is for CSV/TSV files and similar. Note that *all* files will be read as that type, regardless of file extension. To only read from files with a matching file extension, appropriately configure `file_regex`. |
| schema                      | False    | None    | The declarative schema to use for the stream. It can be the schema itself or a path to a json file containing the schema, see [example](https://github.com/meltano/hub/blob/c4b541bbdc36b1b6efffa1eb6022367d8de43e3a/schemas/plugin_definitions/hub_metadata.schema.json). If not provided, the schema will be inferred. |
| compression                 | False    | detect  | The encoding used to decompress data. Must be one of `none`, `zip`, `bz2`, `gzip`, `lzma`, `xz`, or `detect`. If set to `none` or any encoding, that setting will be applied to *all* files, regardless of file extension. If set to `detect`, encodings will be applied based on file extension. |
| additional_info             | False    |       1 | If `True`, each row in tap's output will have three additional columns: `_sdc_file_name`, `_sdc_line_number`, and `_sdc_last_modified`. If `False`, these columns will not be present. Incremental replication requires `additional_info==True`. |
| start_date                  | False    | None    | Used in place of state. Files that were last modified before the `start_date` wwill not be synced. |
| delimited_error_handling    | False    | fail    | The method with which to handle improperly formatted records in delimited files. Must be either `fail` or `ignore`. `fail` will cause the tap to fail if an improperly formatted record is detected. `ignore` will ignore the fact that it is improperly formatted and process it anyway. |
| delimited_delimiter         | False    | detect  | The character used to separate records in a delimited file. Can ne any character or the special value `detect`. If a character is provided, all delimited files will use that value. `detect` will use `,` for `.csv` files, `\t` for `.tsv` files, and fail if other file types are present. |
| delimited_quote_character   | False    | "       | The character used to indicate when a record in a delimited file contains a delimiter character. |
| delimited_header_skip       | False    |       0 | The number of initial rows to skip at the beginning of each delimited file. |
| delimited_footer_skip       | False    |       0 | The number of initial rows to skip at the end of each delimited file. |
| delimited_override_headers  | False    | None    | An optional array of headers used to override the default column name in delimited files, allowing for headerless files to be correctly read. |
| jsonl_error_handling        | False    | fail    | The method with which to handle improperly formatted records in jsonl files. Must be either `fail` or `ignore`. `fail` will cause the tap to fail if an improperly formatted record is detected. `ignore` will ignore the fact that it is improperly formatted and process it anyway. |
| jsonl_sampling_strategy     | False    | first   | The strategy determining how to read the keys in a JSONL file. Must be either `first` or `all`. Currently, only `first` is supported, which will assume that the first record in a file is representative of all keys. |
| jsonl_type_coercion_strategy| False    | any     | The strategy determining how to construct the schema for JSONL files when the types represented are ambiguous.  Must be one of `any`, `string`, or `envelope`. `any` will provide a generic schema for all keys, allowing them to be any valid JSON type. `string` will require all keys to be strings and will convert other values accordingly. `envelope` will deliver each JSONL row as a JSON object with no internal schema. |
| avro_type_coercion_strategy | False    | convert | The strategy deciding how to convert Avro Schema to JSON Schema when the conversion is ambiguous. Must be either `convert` or `envelope`. `convert` will attempt to convert from Avro Schema to JSON Schema and will fail if a type can't be easily coerced. `envelope` will wrap each record in an object without providing an internalschema for the record. |
| s3_anonymous_connection     | False    |       0 | Whether to use an anonymous S3 connection, without any credentials. Ignored if `protocol!=s3`. |
| AWS_ACCESS_KEY_ID           | False    | $AWS_ACCESS_KEY_ID    | The access key to use when authenticating to S3. Ignored if `protocol!=s3` or `s3_anonymous_connection=True`. Defaults to the value of the environment variable of the same name. |
| AWS_SECRET_ACCESS_KEY       | False    | $AWS_SECRET_ACCESS_KEY    | The access key secret to use when authenticating to S3. Ignored if `protocol!=s3` or `s3_anonymous_connection=True`. Defaults to the value of the environment variable of the same name. |
| caching_strategy            | False    | once    | *DEVELOPERS ONLY* The caching method to use when `protocol!=file`. One of `none`, `once`, or `persistent`. `none` does not use caching at all. `once` (the default) will cache all files for the duration of the tap's invocation, then discard them upon completion. `peristent` will allow caches to persist between invocations of the tap, storing them in your OS's temp directory. It is recommended that you do not modify this setting. |
| stream_maps                 | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config           | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled          | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth        | False    | None    | The max depth to flatten schemas. | <!-- Manually added entries begin below. -->
| batch_config           | False    | None    | Object containing batch configuration information, as specified in the [Meltano documentation](https://sdk.meltano.com/en/latest/batch.html). Has two child objects: `encoding` and `storage`. |
| batch_config.encoding  | False    | None    | Object containing information about how to encode batch information. Has two child entries: `format` and `compression`. |
| batch_config.storage   | False    | None    | Object containing information about how batch files should be stored. Has two child entries: `root` and `prefix`. |
| batch_config.encoding.format       | False    | None    | Format to store batch files in. Example: `jsonl`. |
| batch_config.encoding.compression  | False    | None    | Method with which to compress batch files. Example: `gzip`. |
| batch_config.storage.root          | False    | None    | Location to store batch files. Examples: `file:///foo/bar`, `file://output`, `s3://bar/foo`. Note that the triple-slash is not a typo: it indicates an absolute file path. |
| batch_config.storage.prefix        | False    | None    | Prepended to the names of all batch files. Example: `batch-`.  |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-universal-file --about
```


### Regular Expressions

To allow configuration for which files are synced, this tap supports the use of regular expressions to match file paths. First, the tap will find the directory specified by the `file_path` config option. Then it will compare the provided regular expression to the full file path of each file in that directory.

To demonstrate this, consider the following directory structure and suppose that you want to sync only the file `apple.csv`.

```
.
└── top-level/
    ├── alpha/
    └── bravo/
        ├── apple.csv
        ├── pineapple.csv
        └── orange.csv
```

If you set `file_path` to be `/top-level/bravo` and you've set `file_regex` to be `^apple\.csv$`, you won't sync any files. That's because the regular expression you provide is compared against the string `"top-level/bravo/apple.csv"`. Instead, correct values for `file_regex` include `^.*\/apple\.csv$`, `^.*bravo\/apple\.csv$`, or `^top-level\/bravo\/apple\.csv$`. Alternatively, to sync both `apple.csv` and `pineapple.csv`, you could use `^.*\/(pine)?apple\.csv$`.

### Using S3

Some additional configuration is needed when using Amazon S3.

#### Additional Dependency

If you use `protocol==s3` and/or if you use batching to send data to S3, you will need to add the additional dependency `s3`. For example, you could update `meltano.yml` to have `pip_url: -e .[s3]`.

#### Sample Batching Config

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
```python
{
  # ... other config options ...
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

#### Authentication and Authorization

If you use `protocol==s3` and/or if you use batching to send data to S3, you will need to obtain an access key and secret from AWS IAM. Specifically, `protocol==s3` requires the ListBucket and GetObject permissions, and batching requires the PutObject permission.

You can create a policy that grants the requisite permissions with the following JSON:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME/*"
        }
    ]
}
```

You can generate an access key for a specific account using the following command:

```bash
aws iam create-access-key --user-name=YOUR_ACCOUNT_NAME
```

If you already have two access keys for an account, you will have to delete one of them first. You can delete an access key using the following command:

```bash
aws iam delete-access-key --user-name=YOUR_ACCOUNT_NAME --access-key-id=YOUR_ACCESS_KEY_ID
```

#### Subfolders

To sync a subfolder in S3, add it like you would add any other file path. For example to sync all files in `foo` subfolder of the S3 bucket named `bar-bucket`, set `file_path==bar-bucket/foo`.

### Incremental Replication

If this tap is provided a state or `start_date`, it assumes that incremental replication is desired, in which case only files most recently modified will be synced. Attempting to override this behavior in `meltano.yml` can cause unintended behavior due this tap's use of state during the discovery process. Further note that this tap does not support incremental replication on any column other than `_sdc_last_modified`.

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

To authnticate or authorize using S3, see [S3 Authentication and Authorization](#authentication-and-authorization) above

## Usage

You can easily run `tap-universal-file` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-universal-file --version
tap-universal-file --help
tap-universal-file --config CONFIG --discover > ./catalog.json
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

You can also test the `tap-universal-file` CLI interface directly using `poetry run`:

```bash
poetry run tap-universal-file --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-universal-file
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-universal-file --version
# OR run a test `elt` pipeline:
meltano elt tap-universal-file target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

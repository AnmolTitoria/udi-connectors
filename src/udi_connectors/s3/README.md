# S3 Connector

The platform's data-lake connector — the only one registered as **both** a
source and a target. As a target it lands batches from any other connector
into S3 (raw or curated zone). As a source it's a dependency-free fallback
reader over an already-landed prefix, for when Athena/Glue isn't
provisioned (see [`athena`](../athena/README.md) for the Glue-backed query
path).

- Module: `udi_connectors.s3`
- Registry name: `"s3"`
- Role: **source and target**
- Backing library: `boto3`

## Config (`S3Config`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `bucket_name` | `str` | — | **required** |
| `prefix` | `str` | `""` | Root prefix under the bucket |
| `region` | `str` | `"us-east-1"` | |
| `endpoint_url` | `str \| None` | `None` | For S3-compatible stores (MinIO, etc.) |
| `access_key` / `secret_key` / `session_token` | `str \| None` | `None` | Falls back to the default boto3 credential chain — **not** a `SecretStr`, no protection against these leaking into a `repr()`/log of the config object |
| `file_format` | `parquet\|csv\|jsonl` | `"parquet"` | |
| `compression` | `snappy\|gzip\|none` | `"snappy"` | Ignored for parquet (compression is set on the Parquet writer directly) |
| `batch_size` | `int` | `100000` | Used to chunk upsert merges into files |
| `max_concurrent_uploads` | `int` | `5` | Concurrent `put_object` calls in append mode |
| `retry_count` / `retry_delay` | `int` / `float` | `3` / `1.0` | Exponential backoff per upload |
| `connection_id` | `str \| None` | `None` | Enables the zone-partitioned key layout (see below) |
| `zone` | `raw\|curated` | `"raw"` | Default zone when a batch doesn't carry its own via `BatchMetadata.zone` |
| `incremental_column` | `str \| None` | `None` | Unused by the S3 source reader today (no native cursor) |
| `checkpoint_file` | `str \| None` | `None` | |

As with every connector, once a connection using these credentials is saved
through `udi-etl-app`'s API, `secret_key`/`session_token` are encrypted at
rest with an app-level key before being written to the metadata database
(`access_key` is not, since it isn't sensitive on its own) — see that
repo's `docs/ARCHITECTURE.md` for the current state of credential storage.
Prefer an IAM role or the default credential chain over
`access_key`/`secret_key` where possible.

## Example

```json
{
  "bucket_name": "dash-data-migration",
  "region": "ap-south-1",
  "access_key": "YOUR_AWS_KEY",
  "secret_key": "YOUR_AWS_SECRET",
  "file_format": "parquet",
  "compression": "snappy"
}
```

## Key layout

```
{prefix}/{zone}/{connection_id}/{table_name}/dt={extracted_at:%Y-%m-%d}/{batch_id}.ext
```

If `connection_id` is **not** set, the connector falls back to the original
flat layout `{table_name}/{batch_id}.ext` — this keeps existing callers that
don't opt into the zone/connection_id convention landing exactly where they
always have.

## Behavior — as a target (`load`)

- **`load(batches, table_name, mode="append")`** serializes each incoming
  `Batch` to the configured `file_format`/`compression` and uploads it to
  its key, bounded by a semaphore of `max_concurrent_uploads`, with
  exponential-backoff retries (`retry_count`, `retry_delay * 2**attempt`).
- **`load(..., mode="upsert", merge_keys=[...])`** reads back every existing
  object under the table's prefix, concatenates them with the incoming
  batches, drops existing rows whose `merge_keys` match an incoming row,
  deletes the old objects, and rewrites the merged result in
  `batch_size`-row chunks. Object stores have no native `MERGE`, so this is
  a delete-then-rewrite compaction — fine at moderate table sizes; a real
  table format (Iceberg/Delta/Hudi) is the right answer once this needs to
  scale further.
- **`clear(table_name)`** deletes every object under the table's zone/
  connection prefix (used to sweep a `__staging` increment once it's been
  folded into the published dataset).
- **`dataset_exists(table_name)`** / **`get_schema(table_name)`** check/read
  by listing (or reading one of) the objects under the table's prefix.

## Behavior — as a source (`extract`)

- **`list_tables()`** lists "directories" (common prefixes) one level under
  `{prefix}/{zone}/{connection_id}/`, filtering out the `__staging`
  convention used internally by the pipeline module.
- **`extract(table_name, columns=None, filter_predicate=None)`** lists every
  object under the table's prefix (optionally substring-filtered by
  `filter_predicate` against the key) and yields one `Batch` per object,
  read back with `pyarrow.parquet` / `pyarrow.csv` / `pyarrow.json`
  depending on `file_format`.
- **`supports_incremental()`** returns `False` — there's no native cursor on
  a flat prefix; incremental orchestration happens at the pipeline layer
  instead.

## Failure modes

- Bucket/connection errors raise `ConnectorError` (`retryable=True` for
  `ClientError`s, `False` for anything else at connect time).
- Upload failures retry up to `retry_count` times before raising.

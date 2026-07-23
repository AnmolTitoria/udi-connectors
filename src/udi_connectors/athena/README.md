# Athena Connector

Source connector that runs SQL against **Amazon Athena** (AWS Glue Data
Catalog) and streams the results back as Arrow-backed `Batch` objects. It's
also used as a read path for the S3 lake once tables are crawled/registered
in Glue.

- Module: `udi_connectors.athena`
- Registry name: `"athena"`
- Role: **source** only
- Backing library: `boto3` (Athena, no persistent connection — the client is
  reused across queries)

## Config (`AthenaConfig`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `region` | `str` | `"us-east-1"` | |
| `catalog` | `str` | `"AwsDataCatalog"` | Glue Data Catalog name |
| `database` | `str` | — | **required** — Glue database to query |
| `workgroup` | `str` | `"primary"` | Verified with `get_work_group` on connect |
| `output_location` | `str \| None` | `None` | S3 path for query results; required unless the workgroup has a default result configuration |
| `access_key` / `secret_key` / `session_token` | `str \| None` | `None` | Omit to fall back to the default boto3 credential chain (IAM role, env vars, `~/.aws/credentials`) |
| `batch_size` | `int` | `1000` | Rows per `Batch` yielded from `extract()` |
| `poll_interval` | `float` | `1.0` | Seconds between query-status polls |
| `max_poll_attempts` | `int` | `300` | Query is treated as timed out after this many polls |
| `incremental_column` | `str \| None` | `None` | Adds `ORDER BY <col>` and, with a checkpoint, `WHERE <col> > <last>` |
| `checkpoint_file` | `str \| None` | `None` | Path to a `CheckpointFile` used to resume incremental extracts |

## Example

```json
{
  "name": "My Athena",
  "source_type": "athena",
  "database": "analytics",
  "workgroup": "primary",
  "output_location": "s3://my-athena-results/",
  "region": "us-east-1"
}
```

## Behavior

- **`connect()`** builds a `boto3` Athena client and calls `get_work_group`
  to fail fast on bad credentials/workgroup.
- **`list_databases()` / `list_tables()`** page through
  `list_databases` / `list_table_metadata` against `catalog`/`database`.
- **`get_schema(table_name)`** reads Glue column metadata via
  `get_table_metadata` and maps Hive types (`bigint`, `decimal`, `timestamp`,
  …) to Arrow types; returns `None` if the table isn't registered.
- **`extract(table_name, columns=None, filter_predicate=None)`** builds a
  `SELECT ... FROM "database"."table"` statement (adding the incremental
  `WHERE`/`ORDER BY` clauses described above), submits it with
  `start_query_execution`, polls until `SUCCEEDED`/`FAILED`/`CANCELLED`, then
  pages `get_query_results` and yields one `Batch` per `batch_size` rows.
  The repeated header row Athena returns on the first results page is
  stripped automatically.
- **`extract_sql(sql, table_name)`** runs an arbitrary SQL statement (e.g. a
  manual transform) through the same polling/streaming path.
- **`execute_query(sql, max_rows=1000)`** is a non-streaming helper for
  ad-hoc queries — returns `{"columns", "rows", "row_count"}` directly.
- **`dataset_exists(table_name)`** checks whether a Glue table is
  registered. This is informational only (Athena is a query engine, not a
  write target here) — it's used to report Glue-side schema drift after a
  curated write, since evolving the Glue table itself is a manual/crawler
  step.
- **`supports_incremental()`** returns `True`.

## Failure modes

- Connection/workgroup errors raise `ConnectionError`.
- Query failures (`FAILED`/`CANCELLED` state) and timeouts raise
  `ConnectorError` with `retryable=True`.

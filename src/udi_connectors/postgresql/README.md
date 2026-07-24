# PostgreSQL Connector

Source connector for PostgreSQL using the native `psycopg` driver (via
`psycopg_pool`) with a named server-side cursor for streaming extraction, so
large tables don't have to be materialized in memory.

For MySQL/MSSQL/Oracle/SQLite (or Postgres via SQLAlchemy instead of native
`psycopg`), see the [`sql`](../sql/README.md) connector.

- Module: `udi_connectors.postgresql`
- Registry name: `"postgresql"`
- Role: **source** only
- Backing library: `psycopg_pool` (async by default; sync mode via
  `PostgreSQLConnector(use_async=False)`)

## Config (`PostgreSQLConfig`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `host` | `str` | `"localhost"` | |
| `port` | `int` | `5432` | |
| `database` | `str` | — | **required** |
| `username` | `str` | — | **required** |
| `password` | `SecretStr` | — | **required** |
| `ssl_mode` | `disable\|allow\|prefer\|require\|verify-ca\|verify-full` | `"prefer"` | |
| `ssl_cert` / `ssl_key` / `ssl_root_cert` | `str \| None` | `None` | Client cert/key/CA paths |
| `pool_min_size` / `pool_max_size` | `int` | `2` / `10` | |
| `pool_timeout` | `float` | `30.0` | |
| `batch_size` | `int` | `20000` | Rows per `fetchmany()` / `Batch` |
| `cursor_name` | `str \| None` | `None` | Defaults to `export_<table_name>` |
| `incremental_column` | `str \| None` | `None` | Adds `ORDER BY <col>` and, with a checkpoint, `WHERE <col> > <last>` |
| `checkpoint_file` | `str \| None` | `None` | |

`password` being `SecretStr` only keeps it out of `repr()`/log output for
this in-memory config object — it is **not** encryption on its own. Callers
still get the raw value via `.get_secret_value()` (see `connection_string`
in `config.py`). Separately, once a connection is saved through
`udi-etl-app`'s API, its config (including this field) is encrypted at rest
with an app-level key before being written to the metadata database — see
that repo's `docs/ARCHITECTURE.md` for the current state of credential
storage.

## Example

```json
{
  "name": "My Aiven PG",
  "source_type": "postgresql",
  "host": "pg-1234.aivencloud.com",
  "port": 27726,
  "database": "defaultdb",
  "username": "avnadmin",
  "password": "your-password",
  "ssl_mode": "require"
}
```

## Behavior

- **`connect()`** opens a `psycopg_pool` connection pool (async or sync) and
  runs `SELECT 1` to fail fast on bad credentials.
- **`list_tables()`** queries `information_schema.tables`, excluding
  `pg_catalog`/`information_schema`.
- **`list_databases()`** queries `pg_database` for non-template databases.
- **`get_schema(table_name)`** reads `information_schema.columns` and maps
  Postgres types (`integer`, `numeric`, `timestamp with time zone`, `jsonb`,
  …) to Arrow types.
- **`extract(table_name, columns=None, filter_predicate=None)`** opens a
  named server-side cursor (`DECLARE CURSOR` under the hood via psycopg) for
  `SELECT <columns> FROM <table> [WHERE ...] [ORDER BY ...]` and pulls rows
  via `fetchmany(batch_size)` in a loop, yielding one `Batch` per fetch.
  Incremental filtering works the same way as the other SQL-ish connectors:
  `ORDER BY <incremental_column>`, plus a `WHERE <col> > <checkpoint>` once a
  checkpoint exists.
- **`supports_incremental()`** returns `True`.

## Failure modes

- Connection errors raise `ConnectionError`.
- Extract errors raise `ConnectorError` with `retryable=True`.

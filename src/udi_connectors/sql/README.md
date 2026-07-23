# SQL Connector

Generic source connector for any SQLAlchemy-supported relational database —
MySQL, MSSQL, Oracle, SQLite, and PostgreSQL. Use this when you need a
dialect other than Postgres; for native Postgres, the
[`postgresql`](../postgresql/README.md) connector (server-side cursor via
`psycopg`) is more efficient.

- Module: `udi_connectors.sql`
- Registry name: `"sql"`
- Role: **source** only
- Backing library: SQLAlchemy (async engine), with per-dialect async/sync
  driver pairs

## Supported dialects

| `dialect` | Async driver | Sync driver |
|---|---|---|
| `postgresql` | `postgresql+asyncpg` | `postgresql+psycopg2` |
| `mysql` | `mysql+asyncmy` | `mysql+pymysql` |
| `mssql` | `mssql+aioodbc` | `mssql+pyodbc` |
| `oracle` | `oracle+oracledb` | `oracle+oracledb` |
| `sqlite` | `sqlite+aiosqlite` | `sqlite+pysqlite` |

Override with `driver` in config to use a different DBAPI driver string.

## Config (`SQLConfig`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `dialect` | `postgresql\|mysql\|mssql\|oracle\|sqlite` | `"postgresql"` | |
| `host` | `str` | `"localhost"` | |
| `port` | `int` | `5432` | |
| `database` | `str` | — | **required** |
| `username` | `str` | — | **required** |
| `password` | `SecretStr` | — | **required** |
| `driver` | `str \| None` | `None` | Overrides the default driver for `dialect` |
| `extra_params` | `dict` | `{}` | Passed through as URL query params |
| `pool_size` / `max_overflow` | `int` | `10` / `20` | |
| `pool_timeout` | `float` | `30.0` | |
| `batch_size` | `int` | `20000` | Rows per result partition / `Batch` |
| `incremental_column` | `str \| None` | `None` | Adds `ORDER BY <col>` and, with a checkpoint, `WHERE <col> > <last>` |
| `checkpoint_file` | `str \| None` | `None` | |

## Example

```json
{
  "name": "My MySQL",
  "source_type": "sql",
  "dialect": "mysql",
  "host": "localhost",
  "port": 3306,
  "database": "mydb",
  "username": "root",
  "password": "pass"
}
```

## Behavior

- **`connect()`** builds a SQLAlchemy async engine from the dialect/driver
  map, using `pool_size`/`max_overflow`/`pool_timeout` and
  `pool_pre_ping=True`, then runs `SELECT 1` to verify connectivity.
- **`list_tables()`** / **`get_schema(table_name)`** use SQLAlchemy's
  `inspect()` (`get_table_names()`, `get_columns()`), mapping generic SQL
  type names to Arrow types.
- **`list_databases()`** runs a dialect-specific query (`pg_database` for
  Postgres, `SHOW DATABASES` for MySQL, `sys.databases` for MSSQL); other
  dialects (Oracle, SQLite) just return the configured `database`.
- **`extract(table_name, columns=None, filter_predicate=None)`** builds
  `SELECT <columns> FROM <table> [WHERE ...] [ORDER BY ...]` (same
  incremental-column/checkpoint pattern as the other source connectors),
  streams it with `session.stream()`, and yields one `Batch` per
  `batch_size`-row partition.
- **`supports_incremental()`** returns `True`.

## Failure modes

- Connection errors raise `ConnectionError`.
- Extract errors raise `ConnectorError` with `retryable=True`.

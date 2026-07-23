# udi-connectors

Concrete source/target connector implementations for UDI, plus the registry
that ties them together (`create_source`, `create_target`, `list_sources`,
`list_targets`, `migrate_all`).

Depends on [`udi-packages`](../udi-packages) for the shared
`SourceConnector`/`TargetConnector` protocols, `Batch`, `BaseConfig`, etc.
Consumed by [`udi-etl-app`](../udi-etl-app), which exposes these connectors
over an HTTP API.

## Connectors

Each connector has its own README covering its config fields, behavior, and
failure modes:

| Type | Kind | Module | Docs |
|---|---|---|---|
| PostgreSQL | source | `udi_connectors.postgresql` | [README](src/udi_connectors/postgresql/README.md) |
| MongoDB | source | `udi_connectors.mongodb` | [README](src/udi_connectors/mongodb/README.md) |
| SQL (MySQL/MSSQL/Oracle/SQLite via SQLAlchemy) | source | `udi_connectors.sql` | [README](src/udi_connectors/sql/README.md) |
| File upload | source | `udi_connectors.file_upload` | [README](src/udi_connectors/file_upload/README.md) |
| Amazon Athena | source | `udi_connectors.athena` | [README](src/udi_connectors/athena/README.md) |
| S3 | source + target | `udi_connectors.s3` | [README](src/udi_connectors/s3/README.md) |

## Install (editable, for local dev alongside udi-packages)

```bash
uv sync
```

This assumes `udi-packages` is checked out as a sibling directory
(`../udi-packages`), per the `[tool.uv.sources]` path dependency in
`pyproject.toml`. If you split it into a separate remote, swap that for a git
dependency.

## Tests

```bash
uv run pytest
```

The PostgreSQL and MongoDB tests expect local containers (previously provided
by the monorepo's root `docker-compose.yml`, now living in `udi-etl-app`) —
bring up your own `postgres:16` on `5433` and `mongo:7` on `27017`, or copy
that compose file here.

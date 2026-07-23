# MongoDB Connector

Source connector for MongoDB collections. Defaults to the async `motor`
driver; can be switched to synchronous `pymongo` by constructing
`MongoDBConnector(use_async=False)`.

- Module: `udi_connectors.mongodb`
- Registry name: `"mongodb"`
- Role: **source** only
- Backing library: `motor` (async, default) / `pymongo` (sync)

## Config (`MongoDBConfig`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `connection_string` | `str` | — | **required**, e.g. `mongodb://localhost:27017` |
| `database` | `str` | — | **required** |
| `collection` | `str \| None` | `None` | Informational — `extract()` takes the collection name as an explicit argument |
| `max_pool_size` | `int` | `100` | |
| `min_pool_size` | `int` | `10` | |
| `batch_size` | `int` | `20000` | Documents per `Batch` |
| `incremental_field` | `str \| None` | `None` | Sort + `$gt` filter field for incremental extracts |
| `last_checkpoint` | `dict \| None` | `None` | |
| `checkpoint_file` | `str \| None` | `None` | Path to a `CheckpointFile` used with `incremental_field` |
| `read_preference` | `"primary" \| "secondary" \| "nearest"` | `"primary"` | |

## Example

```json
{
  "name": "My MongoDB",
  "source_type": "mongodb",
  "connection_string": "mongodb://localhost:27017",
  "database": "mydb"
}
```

## Behavior

- **`connect()`** opens a Motor (or PyMongo) client and pings `admin` to
  confirm the connection is live.
- **`list_databases()` / `list_tables()`** list database/collection names.
- **`get_schema(collection_name)`** samples up to 1000 documents and infers
  an Arrow schema from them via pandas.
- **`extract(collection_name, filter_dict=None, projection=None)`**:
  - Every document is passed through `_serialize_doc`, which recursively
    stringifies nested dicts/lists and non-`None` scalars — this keeps BSON's
    flexible typing from breaking a fixed Arrow schema per batch.
  - If `incremental_field` is set, results are sorted by it ascending, and
    if a `checkpoint_file` is also set, an existing checkpoint value adds
    `{incremental_field: {"$gt": checkpoint}}` to the filter.
  - Documents are streamed in chunks of `batch_size` (cursor batch size is
    also set to `batch_size`) and converted to a `Batch` per chunk.
- **`supports_incremental()`** returns `True`.

## Failure modes

- Connection errors raise `ConnectionError`.
- Extract errors raise `ConnectorError` with `retryable=True`.

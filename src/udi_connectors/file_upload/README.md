# File Upload Connector

Source connector that reads files from a local directory and emits one row
per file (name, size, type, and optionally extracted text content). No
external service or database involved — it's a filesystem walker.

- Module: `udi_connectors.file_upload`
- Registry name: `"file_upload"`
- Role: **source** only
- Backing library: stdlib `pathlib`; optional `pdfplumber` (PDF text) and
  `python-docx` (`.docx` text) — only imported if those file types are hit

## Config (`FileUploadConfig`)

| Field | Type | Default | Notes |
|---|---|---|---|
| `input_dir` | `str` | — | **required** — must exist and be a directory |
| `file_pattern` | `str` | `"*"` | Glob pattern, ignored if `files` is set |
| `files` | `list[str] \| None` | `None` | Explicit relative paths to pick, instead of globbing |
| `recursive` | `bool` | `False` | Use `rglob` instead of `glob` when scanning by pattern |
| `batch_size` | `int` | `100` | Files per `Batch` |
| `include_content` | `bool` | `True` | Read file contents into a `content` column |
| `checkpoint_file` | `str \| None` | `None` | Skips files already recorded as processed |

## Example

```json
{
  "name": "My Files",
  "source_type": "file_upload",
  "input_dir": "C:/data/files"
}
```

## Behavior

- **`connect()`** just validates `input_dir` exists and is a directory.
- **`list_tables()` / `list_databases()`** both return `["files"]` — there's
  no real table/database concept for a flat file source.
- **`get_schema()`** returns `filename` (string), `size` (int64), `type`
  (string), plus `content` (string) if `include_content` is set.
- **`extract()`** resolves the file list (either `files` explicitly, or a
  `glob`/`rglob` scan by `file_pattern`), sorts it for determinism, and then
  for each file:
  - `filename` is stored relative to `input_dir`
  - `content` is populated only for recognized text extensions (code,
    config, markup, etc. — see `TEXT_EXTENSIONS` in `connector.py`), `.pdf`
    (via `pdfplumber`), and `.docx` (via `python-docx`); anything else gets
    `content = None`. Extraction failures are swallowed and also yield `None`
    rather than failing the batch.
  - Files are yielded in `Batch`es of `batch_size`.
- **Checkpointing**: if `checkpoint_file` is set, the connector reads a list
  of already-processed relative paths (keyed by table name or the folder
  name) from the `CheckpointFile` and skips them. This is list-based
  dedup, not a column comparison — hence `supports_incremental()` reports
  `False` even though a checkpoint file is supported.

## Failure modes

- A missing/non-directory `input_dir` raises `ConnectorError`
  (`retryable=False`) at `connect()` time.
- An empty result set yields zero batches rather than erroring.

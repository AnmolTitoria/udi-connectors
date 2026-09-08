import asyncio
from collections.abc import AsyncIterator

import pyarrow as pa
from faker import Faker
from udi_connectors._registry import Source
from udi_packages import Batch, BatchMetadata, ConnectorError, ExtractResult

from .config import PromptDataConfig
from .fields import build_field_specs, generate_rows, parse_prompt

TABLE_NAME = "generated_data"


@Source("prompt_data", icon="sparkles", category="Generated")
class PromptDataConnector:
    """Generates a synthetic dataset from a free-text prompt describing the
    desired columns and row count (e.g. "300 rows with name, email,
    signup_date, amount"). Column types are inferred from field names and
    filled in locally with Faker — no external API calls, no cost."""

    Config = PromptDataConfig

    def __init__(self):
        self._config: PromptDataConfig | None = None

    async def connect(self, config: PromptDataConfig) -> None:
        self._config = config

    async def disconnect(self) -> None:
        self._config = None

    async def test_connection(self) -> bool:
        return self._config is not None

    async def list_tables(self, config: PromptDataConfig) -> list[str]:
        return [TABLE_NAME]

    async def list_databases(self, config: PromptDataConfig) -> list[str]:
        return ["generated"]

    async def get_schema(self, table_name: str = TABLE_NAME) -> pa.Schema:
        cfg = self._config
        prompt = cfg.prompt if cfg else ""
        field_names, _ = parse_prompt(prompt, cfg.row_count if cfg else 100)
        specs = build_field_specs(field_names)
        return pa.schema([pa.field(spec.name, spec.arrow_type) for spec in specs])

    async def get_checkpoint(self, table_name: str) -> dict | None:
        return None

    def supports_incremental(self) -> bool:
        return False

    async def extract(
        self,
        table_name: str,
        config: PromptDataConfig,
        columns: list[str] | None = None,
        filter_predicate: str | None = None,
    ) -> ExtractResult:
        cfg = config or self._config
        if not cfg:
            raise ConnectorError("PromptDataConnector is not connected", "prompt_data", retryable=False)

        field_names, row_count = parse_prompt(cfg.prompt, cfg.row_count)
        specs = build_field_specs(field_names)
        if columns:
            wanted = set(columns)
            specs = [s for s in specs if s.name in wanted] or specs
        schema = pa.schema([pa.field(spec.name, spec.arrow_type) for spec in specs])
        faker = Faker()
        if cfg.seed is not None:
            faker.seed_instance(cfg.seed)

        async def batch_generator() -> AsyncIterator[Batch]:
            produced = 0
            batch_num = 0
            while produced < row_count:
                n = min(cfg.batch_size, row_count - produced)
                columns_data = generate_rows(specs, produced, n, faker)
                table = pa.table(columns_data, schema=schema)
                yield Batch(
                    data=table,
                    metadata=BatchMetadata(
                        source_name="prompt_data",
                        table_name=table_name,
                        batch_id=f"{table_name}_{batch_num}",
                        row_count=n,
                        byte_size=table.nbytes,
                        schema=table.schema,
                    ),
                )
                produced += n
                batch_num += 1
                await asyncio.sleep(0)

        return ExtractResult(batches=batch_generator())

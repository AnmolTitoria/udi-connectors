import pyarrow as pa
import pytest
from udi_packages import Batch, BatchMetadata
from udi_connectors.s3 import S3Config, S3Connector

pytest.importorskip("moto")


@pytest.fixture(autouse=True)
def _aws_ctx():
    from moto import mock_aws
    with mock_aws():
        import boto3
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield


@pytest.fixture
async def conn(s3_bucket, s3_region):
    c = S3Connector()
    await c.connect(S3Config(bucket_name=s3_bucket, region=s3_region))
    yield c
    await c.disconnect()


def _make_batch(table_name: str, rows: int, batch_id: int = 0) -> Batch:
    import pandas as pd
    data = {"id": range(rows), "name": [f"n_{i}" for i in range(rows)]}
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    return Batch(
        data=table,
        metadata=BatchMetadata(
            source_name="test",
            table_name=table_name,
            batch_id=f"{table_name}_{batch_id}",
            row_count=rows,
            byte_size=table.nbytes,
            schema=table.schema,
        ),
    )


class TestS3Connector:
    async def test_connect(self, conn):
        assert conn._client is not None

    async def test_test_connection(self, conn):
        assert await conn.test_connection()

    async def test_load_single_batch(self, conn, s3_bucket):
        batch = _make_batch("users", 10)

        async def batch_gen():
            yield batch

        result = await conn.load(batch_gen(), "users")
        assert result.rows_loaded == 10
        assert result.batch_count == 1
        assert len(result.errors) == 0

    async def test_load_multiple_batches(self, conn, s3_bucket):
        async def batch_gen():
            for i in range(3):
                yield _make_batch("orders", 5, i)

        result = await conn.load(batch_gen(), "orders")
        assert result.rows_loaded == 15
        assert result.batch_count == 3
        assert len(result.errors) == 0

    async def test_load_empty_batches(self, conn, s3_bucket):
        async def empty_gen():
            return
            yield  # pragma: no cover

        result = await conn.load(empty_gen(), "empty")
        assert result.rows_loaded == 0
        assert result.batch_count == 0


class TestXlsxFormat:
    async def test_xlsx_round_trip(self, s3_bucket, s3_region):
        c = S3Connector()
        await c.connect(S3Config(bucket_name=s3_bucket, region=s3_region, file_format="xlsx"))
        try:
            batch = _make_batch("people", 5)

            async def batch_gen():
                yield batch

            result = await c.load(batch_gen(), "people")
            assert result.rows_loaded == 5
            assert len(result.errors) == 0

            extracted = await c.extract("people", c._config)
            batches = [b async for b in extracted.batches]
            assert sum(b.data.num_rows for b in batches) == 5
            assert batches[0].data.schema.names == ["id", "name"]
        finally:
            await c.disconnect()

    async def test_xlsx_with_timezone_aware_datetime_column(self, s3_bucket, s3_region):
        c = S3Connector()
        await c.connect(S3Config(bucket_name=s3_bucket, region=s3_region, file_format="xlsx"))
        try:
            import pandas as pd
            df = pd.DataFrame({
                "id": [1, 2],
                "created_at": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"]),
            })
            table = pa.Table.from_pandas(df)
            batch = Batch(
                data=table,
                metadata=BatchMetadata(
                    source_name="test",
                    table_name="events",
                    batch_id="events_0",
                    row_count=2,
                    byte_size=table.nbytes,
                    schema=table.schema,
                ),
            )

            async def batch_gen():
                yield batch

            result = await c.load(batch_gen(), "events")
            assert result.rows_loaded == 2
            assert len(result.errors) == 0
        finally:
            await c.disconnect()

    async def test_read_columns_from_existing_xlsx_template(self, conn, s3_bucket):
        batch = _make_batch("template", 3)

        async def batch_gen():
            yield batch

        conn._config.file_format = "xlsx"
        await conn.load(batch_gen(), "template")

        keys = await conn._list_object_keys("template", conn._config)
        assert len(keys) == 1

        columns = await conn.read_columns(keys[0])
        assert columns == ["id", "name"]


class TestExtractLimit:
    async def test_extract_without_limit_reads_everything(self, conn, s3_bucket):
        async def batch_gen():
            for i in range(3):
                yield _make_batch("orders", 5, i)

        await conn.load(batch_gen(), "orders")

        result = await conn.extract("orders", conn._config)
        batches = [b async for b in result.batches]
        assert sum(b.data.num_rows for b in batches) == 15

    async def test_extract_with_limit_stops_early(self, conn, s3_bucket):
        async def batch_gen():
            for i in range(3):
                yield _make_batch("orders", 5, i)

        await conn.load(batch_gen(), "orders")

        result = await conn.extract("orders", conn._config, limit=7)
        batches = [b async for b in result.batches]
        total = sum(b.data.num_rows for b in batches)
        assert total == 7

    async def test_extract_limit_larger_than_data_reads_everything(self, conn, s3_bucket):
        async def batch_gen():
            yield _make_batch("orders", 5)

        await conn.load(batch_gen(), "orders")

        result = await conn.extract("orders", conn._config, limit=100)
        batches = [b async for b in result.batches]
        assert sum(b.data.num_rows for b in batches) == 5

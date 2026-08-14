"""Real end-to-end test: seed two FK/PK-related PostgreSQL tables, extract
both with PostgreSQLConnector, dump them to real S3 with S3Connector, then
read the parquet objects back and verify the row counts and the foreign-key
relationship survived the round trip.

Requires a running PostgreSQL test container (`docker compose up -d postgres`
in udi-etl-app, port 5433) and a real S3 bucket + AWS credentials. Skipped
unless both TEST_PG and E2E_S3_BUCKET are set. See tests/e2e/README.md for
the S3/IAM prerequisites shared with test_s3_athena_e2e.py.
"""

import asyncio
import datetime
import os
import platform
import uuid

import pytest
from dotenv import load_dotenv
from udi_connectors.postgresql import PostgreSQLConfig, PostgreSQLConnector
from udi_connectors.s3 import S3Config, S3Connector

load_dotenv()

pytestmark = pytest.mark.skipif(
    not (os.environ.get("TEST_PG") and os.environ.get("E2E_S3_BUCKET")),
    reason=(
        "Set TEST_PG=1 (+ run: docker compose up -d postgres) and "
        "E2E_S3_BUCKET=<bucket> (+ real AWS credentials) to run the "
        "PostgreSQL -> S3 e2e test."
    ),
)

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("E2E_S3_BUCKET")

NUM_CUSTOMERS = 20
NUM_ORDERS = 60


@pytest.fixture(scope="session")
def event_loop_policy():
    if platform.system() == "Windows":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


@pytest.fixture
def pg_config():
    return PostgreSQLConfig(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5433")),
        database=os.getenv("PG_DATABASE", "testdb"),
        username=os.getenv("PG_USERNAME", "test"),
        password=os.getenv("PG_PASSWORD", "test"),
        ssl_mode=os.getenv("PG_SSLMODE", "prefer"),
        batch_size=100,
    )


async def _pg_exec(pg_config: PostgreSQLConfig, sql: str):
    import psycopg
    async with await psycopg.AsyncConnection.connect(pg_config.connection_string) as conn:
        await conn.execute(sql)


async def _pg_executemany(pg_config: PostgreSQLConfig, sql: str, params: list[tuple]):
    import psycopg
    async with await psycopg.AsyncConnection.connect(pg_config.connection_string) as conn:
        async with conn.cursor() as cur:
            await cur.executemany(sql, params)


@pytest.fixture
async def seeded_related_tables(pg_config):
    """Two related tables: e2e_customers (PK) <- e2e_orders (FK on customer_id)."""
    await _pg_exec(pg_config, """
        CREATE TABLE IF NOT EXISTS e2e_customers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )
    """)
    await _pg_exec(pg_config, """
        CREATE TABLE IF NOT EXISTS e2e_orders (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER NOT NULL REFERENCES e2e_customers(id),
            amount INTEGER NOT NULL,
            order_date DATE NOT NULL
        )
    """)

    await _pg_executemany(
        pg_config,
        "INSERT INTO e2e_customers (name, email) VALUES (%s, %s)",
        [(f"customer_{i}", f"customer_{i}@test.com") for i in range(NUM_CUSTOMERS)],
    )
    await _pg_executemany(
        pg_config,
        "INSERT INTO e2e_orders (customer_id, amount, order_date) VALUES (%s, %s, %s)",
        [
            (
                (i % NUM_CUSTOMERS) + 1,
                (i + 1) * 100,
                datetime.date(2026, 1, 1) + datetime.timedelta(days=i % 28),
            )
            for i in range(NUM_ORDERS)
        ],
    )
    yield
    # FK means orders must drop before customers.
    await _pg_exec(pg_config, "DROP TABLE IF EXISTS e2e_orders")
    await _pg_exec(pg_config, "DROP TABLE IF EXISTS e2e_customers")


@pytest.fixture
def run_id() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture
async def s3_conn():
    conn = S3Connector()
    await conn.connect(
        S3Config(bucket_name=BUCKET, region=REGION, file_format="parquet", compression="snappy")
    )
    yield conn
    await conn.disconnect()


async def _dump_table_to_s3(pg_config, s3_conn, pg_table: str, s3_table: str):
    pg = PostgreSQLConnector(use_async=True)
    await pg.connect(pg_config)
    try:
        result = await pg.extract(pg_table, pg_config)
        return await s3_conn.load(result.batches, s3_table)
    finally:
        await pg.disconnect()


class TestPostgresToS3EndToEnd:
    async def test_fk_related_tables_round_trip_through_s3(
        self, pg_config, seeded_related_tables, s3_conn, run_id
    ):
        customers_table = f"e2e_customers_{run_id}"
        orders_table = f"e2e_orders_{run_id}"

        try:
            customers_result = await _dump_table_to_s3(pg_config, s3_conn, "e2e_customers", customers_table)
            orders_result = await _dump_table_to_s3(pg_config, s3_conn, "e2e_orders", orders_table)

            assert customers_result.rows_loaded == NUM_CUSTOMERS
            assert not customers_result.errors
            assert orders_result.rows_loaded == NUM_ORDERS
            assert not orders_result.errors

            # Read both back from S3 and confirm the FK relationship (every
            # order's customer_id resolves to a seeded customer) survived
            # the extract -> dump -> read round trip.
            customers_extract = await s3_conn.extract(customers_table, s3_conn._config)
            customer_ids = {
                row["id"] async for b in customers_extract.batches for row in b.data.to_pylist()
            }
            assert len(customer_ids) == NUM_CUSTOMERS

            orders_extract = await s3_conn.extract(orders_table, s3_conn._config)
            order_rows = [row async for b in orders_extract.batches for row in b.data.to_pylist()]
            assert len(order_rows) == NUM_ORDERS
            assert all(row["customer_id"] in customer_ids for row in order_rows)
        finally:
            await s3_conn.clear(customers_table)
            await s3_conn.clear(orders_table)

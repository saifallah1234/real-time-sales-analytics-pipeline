# PostgreSQL CDC Analytics MVP

This project models an eventual PostgreSQL → Debezium → Kafka → Snowflake → dbt
pipeline. The MVP ingestion command currently polls PostgreSQL directly and merges
new or updated rows into `CDC_DATABASE.RAW_DATA`. PostgreSQL deletes are deliberately
left for the later Debezium implementation.

## Source tables

The ingestion registry contains exactly these eight tables:

1. `customer`
2. `supplier`
3. `product`
4. `sales_invoice`
5. `sales_invoice_item`
6. `purchase_bill`
7. `purchase_bill_item`
8. `inventory_movement`

## Setup

Install the locked dependencies and create a local environment file:

```bash
uv sync
cp .env.example .env
```

Fill in the PostgreSQL and Snowflake credentials in `.env`. The Snowflake role must
be able to alter the existing raw tables and create tables in `CDC_DATABASE.RAW_DATA`.

### Apply the `updated_at` migration

On a brand-new Docker volume, the migration runs automatically after the existing
initialization scripts. For an existing volume, apply it explicitly as the PostgreSQL
owner because `/docker-entrypoint-initdb.d/` is only processed for a new data directory.

Bash:

```bash
docker exec -i cdc_postgres psql -U postgres -d cdc_project < postgres/init/03_add_updated_at.sql
```

PowerShell:

```powershell
Get-Content -Raw postgres/init/03_add_updated_at.sql | docker exec -i cdc_postgres psql -U postgres -d cdc_project
```

## Run ingestion

Ingest all tables:

```bash
uv run cdc-ingest
# Equivalent:
uv run python -m ingestion
```

Ingest selected tables:

```bash
uv run cdc-ingest --table customer --table sales_invoice
```

The command holds a PostgreSQL session advisory lock for its entire run. Each table
uses a fixed `(previous cutoff - overlap, PostgreSQL clock_timestamp()]` window,
loads a temporary Snowflake table in bounded batches, and then atomically merges the
raw target and its checkpoint. An empty window still advances to the captured upper
cutoff. Earlier tables remain committed if a later table fails, making a retry safe.

The default five-minute overlap handles ordinary late commits. A source transaction
lasting longer than the overlap can still be missed; logical replication will remove
that timestamp-polling limitation later.

## Verify and transform

Checkpoint metrics distinguish extracted candidates (which can include overlap
replays) from rows actually inserted or updated:

```sql
SELECT *
FROM CDC_DATABASE.RAW_DATA.INGESTION_CHECKPOINTS
ORDER BY SOURCE_TABLE;
```

Run dbt separately, as Airflow will eventually orchestrate these as distinct tasks:

```bash
uv run dbt build
```

## Smoke test

1. Run ingestion once and record the raw-table values and checkpoints.
2. Insert one PostgreSQL row and update a different row.
3. Run ingestion and verify one insert/update in the corresponding raw tables.
4. Run ingestion again without source changes; extracted candidates may be replayed
   by the overlap, but inserted and updated counts should be zero.
5. Delete a PostgreSQL row and confirm it remains in Snowflake.
6. Run `uv run dbt build` and confirm the existing models and tests still pass.

Local unit tests do not require live database credentials:

```bash
uv run python -m unittest discover -s tests -v
```

## CDC load test

The automated customer load test exercises PostgreSQL through Debezium, Kafka,
the Snowflake landing table, and the triggered raw-table merge. It clears the
reserved customer IDs `900001` through `901000`, inserts 1,000 rows in one
transaction, then updates 100 rows and deletes a separate 50 rows. The command
prints a pass/fail summary and saves a JSON report under `reports/`.

Run it from the ingestion project after the source and Snowflake sink connectors
and Snowflake merge tasks are running:

```bash
cd airflow_astro/dags/ingestion_logic
uv run cdc-load-test
```

From the repository root it can also be invoked with:

```bash
python -m airflow_astro.dags.ingestion_logic.ingestion.load_test_cli
```

The existing PostgreSQL and Snowflake variables in `.env` are reused. Optional
load-test settings are:

```ini
KAFKA_CONNECT_URL=http://localhost:8083
LOAD_TEST_LANDING_SCHEMA=CDC_LANDING
LOAD_TEST_TIMEOUT_SECONDS=300
LOAD_TEST_POLL_INTERVAL_SECONDS=1
LOAD_TEST_CLEANUP_QUIET_SECONDS=5
LOAD_TEST_REPORT_DIR=reports
```

The configured Snowflake role needs `SELECT` on the landing and raw customer
tables and `DELETE` on the reserved raw-table range. A run exits nonzero on a
failed assertion, timeout, database error, unavailable Connect REST API, or a
connector/task that is not running.

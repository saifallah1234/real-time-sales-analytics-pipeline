from .tables import TableSpec


CHECKPOINT_TABLE = "INGESTION_CHECKPOINTS"


def postgres_extract_sql(spec: TableSpec, schema: str, first_run: bool) -> str:
    columns = ", ".join(spec.all_columns)
    if first_run:
        predicate = "updated_at <= %(upper_bound)s"
    else:
        predicate = (
            "updated_at > %(lower_bound)s\n"
            "  AND updated_at <= %(upper_bound)s"
        )
    return (
        f"SELECT {columns}\n"
        f"FROM {schema}.{spec.name}\n"
        f"WHERE {predicate}\n"
        f"ORDER BY updated_at, {spec.primary_key}"
    )


def snowflake_merge_sql(spec: TableSpec, target: str, stage: str) -> str:
    columns = spec.all_columns
    projected = ", ".join(columns)
    update_columns = [column for column in columns if column != spec.primary_key]
    updates = ", ".join(f"target.{c} = source.{c}" for c in update_columns)
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"source.{c}" for c in columns)
    return f"""MERGE INTO {target} AS target
USING (
    SELECT {projected}
    FROM {stage}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {spec.primary_key}
        ORDER BY updated_at DESC
    ) = 1
) AS source
ON target.{spec.primary_key} = source.{spec.primary_key}
WHEN MATCHED
 AND (target.updated_at IS NULL OR source.updated_at > target.updated_at)
THEN UPDATE SET {updates}
WHEN NOT MATCHED
THEN INSERT ({insert_columns}) VALUES ({insert_values})"""


def checkpoint_merge_sql(checkpoint_table: str) -> str:
    return f"""MERGE INTO {checkpoint_table} AS target
USING (
    SELECT
        %s::VARCHAR AS source_table,
        %s::TIMESTAMP_TZ AS successful_cutoff,
        %s::NUMBER AS extracted_row_count,
        %s::NUMBER AS inserted_row_count,
        %s::NUMBER AS updated_row_count
) AS source
ON target.source_table = source.source_table
WHEN MATCHED THEN UPDATE SET
    successful_cutoff = source.successful_cutoff,
    extracted_row_count = source.extracted_row_count,
    inserted_row_count = source.inserted_row_count,
    updated_row_count = source.updated_row_count,
    ingested_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    source_table,
    successful_cutoff,
    extracted_row_count,
    inserted_row_count,
    updated_row_count,
    ingested_at
) VALUES (
    source.source_table,
    source.successful_cutoff,
    source.extracted_row_count,
    source.inserted_row_count,
    source.updated_row_count,
    CURRENT_TIMESTAMP()
)"""

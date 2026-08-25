from airflow.sdk import dag, task

from pendulum import datetime

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from dags.ingestion_logic.ingestion.pipeline import run_ingestion


@dag(
    dag_id="ingestion_dag",
    start_date=datetime(2026, 8, 25),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "ingestion"],
)
def ingestion_dag():


    @task
    def ingest() -> None:
        for result in run_ingestion():
            print(
                f"{result.table}: extracted={result.extracted_row_count}, "
                f"inserted={result.inserted_row_count}, "
                f"updated={result.updated_row_count}, "
                f"cutoff={result.successful_cutoff.isoformat()}"
            )

    ingestion_task = ingest()

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="dbt_dag",  
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True,
    )

    ingestion_task >> trigger_dbt_dag




ingestion_dag()

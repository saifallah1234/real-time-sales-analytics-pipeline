from airflow.sdk import dag, task

from pendulum import datetime

from dags.ingestion_logic.ingestion.monitoring import OK, run_monitoring, summarize


@dag(
    dag_id="monitoring_dag",
    start_date=datetime(2026, 10, 15),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "monitoring"],
)
def monitoring_dag():

    @task
    def collect_health_checks() -> dict[str, int]:
        # Failing checks are recorded for Grafana; they do not fail the task.
        run_id, results = run_monitoring()
        for result in results:
            if result.status != OK:
                print(f"{result.status.upper()} {result.check_name} {result.subject}: {result.detail}")
        counts = summarize(results)
        print(f"run {run_id}: {counts}")
        return counts

    collect_health_checks()


monitoring_dag()

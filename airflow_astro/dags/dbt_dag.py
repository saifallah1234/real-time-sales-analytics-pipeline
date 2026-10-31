import os

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.operators.local import DbtRunOperationLocalOperator
from cosmos.profiles import RedshiftUserPasswordProfileMapping


DBT_PROJECT_DIR = "/usr/local/airflow/dags/dbt/cdc_analytics"
DBT_EXECUTABLE = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

# redshift_conn: set AIRFLOW_CONN_REDSHIFT_CONN in airflow_astro/.env (see README); dbname comes from the URI path.
profile_config = ProfileConfig(
    profile_name="cdc_analytics",
    target_name="dev",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_conn",
        profile_args={"schema": "analytics"},
    )
)

dbt_redshift_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    # Uses the mounted dbt_packages/ instead of running `dbt deps` in every task (run `dbt deps` once after cloning).
    operator_args={"install_deps": False},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
    render_config=RenderConfig(should_detach_multiple_parents_tests=True, dbt_deps=False),
    schedule="0 */2 * * *",
    catchup=False,
    max_active_runs=1,  # runs must not overlap: each one refreshes the same streaming MV
    dag_id="dbt_dag",
)

# Refresh the streaming MV once per run, before any model reads it.
with dbt_redshift_dag:
    dbt_roots = [task for task in dbt_redshift_dag.tasks if not task.upstream_list]
    refresh_landing = DbtRunOperationLocalOperator(
        task_id="refresh_landing",
        macro_name="refresh_landing",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE,
        install_deps=False,
    )
    refresh_landing >> dbt_roots

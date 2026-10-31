import os

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping


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
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/cdc_analytics",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    render_config=RenderConfig(should_detach_multiple_parents_tests=True),
    schedule="*/15 * * * *",
    catchup=False,
    dag_id="dbt_dag",
)

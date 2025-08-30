"""DAG de orquestração dos modelos dbt usando Cosmos."""
from datetime import datetime
from cosmos import DbtDag, ExecutionConfig, ProjectConfig, ProfileConfig

project_config = ProjectConfig("/opt/airflow/dbt/project")
profile_config = ProfileConfig("/opt/airflow/dbt/profiles")
execution_config = ExecutionConfig()

dbt_cosmos = DbtDag(
    dag_id="dbt_cosmos_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
)

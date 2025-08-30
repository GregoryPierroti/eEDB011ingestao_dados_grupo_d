"""DAG de orquestração dos modelos dbt usando Cosmos.

Atualizado para a assinatura recente do ProfileConfig (exige target_name)
e para usar "schedule" em vez de schedule_interval.
"""
import os
from datetime import datetime
from cosmos import DbtDag, ExecutionConfig, ProjectConfig, ProfileConfig

# Caminhos montados no container do Airflow
PROJECT_DIR = "/opt/airflow/dbt/project"
PROFILES_YML = "/opt/airflow/dbt/profiles/profiles.yml"

project_config = ProjectConfig(PROJECT_DIR)

# dbt_project.yml indica profile: my_postgres_profile; em profiles.yml o target é "dev"
profile_config = ProfileConfig(
    profile_name="my_postgres_profile",
    target_name="dev",
    profiles_yml_filepath=PROFILES_YML,
)

execution_config = ExecutionConfig()

if os.getenv("ENABLE_COSMOS", "false").lower() == "true":
    dbt_cosmos = DbtDag(
        dag_id="dbt_cosmos_dag",
        schedule="@daily",
        start_date=datetime(2023, 1, 1),
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
    )

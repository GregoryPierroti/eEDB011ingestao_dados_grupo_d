"""
DAG: Executa dbt seed, depois 3 modelos em paralelo, e por último mod_final.

Fluxo:
  seed -> [mod_empregados, mod_reclamacoes, mod_bancos] -> mod_final

Observações:
- Este DAG usa BashOperator para chamar dbt diretamente, mantendo compatibilidade ampla.
- Assume que o diretório do projeto e profiles estão montados no container em /opt/airflow/dbt.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Caminhos dentro do container do Airflow (ajuste se necessário)
DBT_PROJECT_DIR = "/opt/airflow/dbt/project"
RUNTIME_PROJECT_DIR = "/opt/airflow/runtime_dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt/profiles"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 0,
}

# dbt instalado via pip para o usuário airflow costuma ir em ~/.local/bin
# Garantimos que o PATH inclua esse diretório para todas as tasks
ENV_BASE = {
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    # Redireciona artefatos e logs para diretórios graváveis do container
    "DBT_LOG_PATH": "/opt/airflow/logs/dbt",
    "DBT_TARGET_PATH": "/opt/airflow/logs/dbt_target",
    "DBT_PACKAGES_INSTALL_PATH": "/opt/airflow/logs/dbt_packages",
}

with DAG(
    dag_id="dbt_seed_parallel_models_then_final",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    # Prepara uma cópia do projeto para escrita dentro do container
    prepare_project = BashOperator(
        task_id="prepare_project",
        bash_command=(
            f"rm -rf {RUNTIME_PROJECT_DIR} && "
            f"mkdir -p {RUNTIME_PROJECT_DIR} && "
            # copia conteúdo mantendo estrutura
            f"cp -a {DBT_PROJECT_DIR}/. {RUNTIME_PROJECT_DIR} && "
            # garante permissões de escrita
            f"chmod -R u+rwX {RUNTIME_PROJECT_DIR}"
        ),
    )

    # 0) dbt deps (instala pacotes, ex: dbt_expectations)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"mkdir -p /opt/airflow/logs/dbt /opt/airflow/logs/dbt_target /opt/airflow/logs/dbt_packages && "
            f"cd {RUNTIME_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR}"
        ),
        env=ENV_BASE,
    )

    # 1) dbt seed
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"mkdir -p /opt/airflow/logs/dbt /opt/airflow/logs/dbt_target /opt/airflow/logs/dbt_packages && "
            f"cd {RUNTIME_PROJECT_DIR} && "
            f"dbt seed --profiles-dir {DBT_PROFILES_DIR} --full-refresh"
        ),
        env=ENV_BASE,
    )

    # 2) 3 modelos em paralelo
    dbt_run_empregados = BashOperator(
        task_id="dbt_run_mod_empregados",
        bash_command=(
            f"mkdir -p /opt/airflow/logs/dbt /opt/airflow/logs/dbt_target /opt/airflow/logs/dbt_packages && "
            f"cd {RUNTIME_PROJECT_DIR} && "
            # Inclui dependências ascendentes (+) para garantir que stg/int existam
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select +mod_empregados"
        ),
        env=ENV_BASE,
    )

    dbt_run_reclamacoes = BashOperator(
        task_id="dbt_run_mod_reclamacoes",
        bash_command=(
            f"mkdir -p /opt/airflow/logs/dbt /opt/airflow/logs/dbt_target /opt/airflow/logs/dbt_packages && "
            f"cd {RUNTIME_PROJECT_DIR} && "
            # Inclui dependências ascendentes (+) para garantir que stg/int existam
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select +mod_reclamacoes"
        ),
        env=ENV_BASE,
    )

    dbt_run_bancos = BashOperator(
        task_id="dbt_run_mod_bancos",
        bash_command=(
            f"mkdir -p /opt/airflow/logs/dbt /opt/airflow/logs/dbt_target /opt/airflow/logs/dbt_packages && "
            f"cd {RUNTIME_PROJECT_DIR} && "
            # Inclui dependências ascendentes (+) para garantir que stg/int existam
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select +mod_bancos"
        ),
        env=ENV_BASE,
    )

    # 3) modelo final dependente dos três
    dbt_run_final = BashOperator(
        task_id="dbt_run_mod_final",
        bash_command=(
            f"mkdir -p /opt/airflow/logs/dbt /opt/airflow/logs/dbt_target /opt/airflow/logs/dbt_packages && "
            f"cd {RUNTIME_PROJECT_DIR} && "
            # Inclui dependências ascendentes (+) por segurança
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select +mod_final"
        ),
        env=ENV_BASE,
    )

    # Orquestração: prepare -> deps -> seed -> paralelos -> final
    prepare_project >> dbt_deps >> dbt_seed >> [dbt_run_empregados, dbt_run_reclamacoes, dbt_run_bancos] >> dbt_run_final

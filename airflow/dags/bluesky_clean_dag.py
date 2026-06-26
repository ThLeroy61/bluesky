"""
DAG de nettoyage Bluesky : posts → bluesky_posts_clean.

Lance le pipeline Kedro 'data_engineering' via la CLI kedro, dans un
BashOperator. Le projet Kedro est bind-monté sur /opt/kedro-clean
(cf docker-compose.yaml).

Pourquoi BashOperator plutôt que kedro-airflow :
  - Mapping 1:1 avec ce qu'on lance en local (kedro run --pipeline=...)
  - Aucun générateur intermédiaire à comprendre/maintenir
  - Pour 4 nodes, un seul task suffit : la granularité au node n'apporte rien
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

KEDRO_RUN_CMD = (
    "cd /opt/kedro-clean && "
    "kedro run --pipeline=data_engineering"
)


with DAG(
    dag_id="bluesky_clean",
    description="Nettoyage des posts via Kedro (data_engineering) → bluesky_posts_clean.",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bluesky", "cleaning", "kedro"],
    max_active_runs=1,
) as dag:

    run_kedro_cleaning = BashOperator(
        task_id="run_kedro_data_engineering",
        bash_command=KEDRO_RUN_CMD,
    )
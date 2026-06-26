from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=90),
}

with DAG(
    dag_id="bluesky_vectors",
    description="Vectorisation TF-IDF+LSA des posts nettoyés → bluesky_posts_vectors.",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bluesky", "vectors", "kedro"],
    max_active_runs=1,
) as dag:

    run_kedro_vectors = BashOperator(
        task_id="run_kedro_vectors",
        bash_command="cd /opt/kedro-clean && kedro run --pipeline=vectors",
    )

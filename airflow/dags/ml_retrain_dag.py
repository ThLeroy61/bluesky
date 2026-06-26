from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bluesky_ml_retrain",
    description="Clustering KMeans sur les embeddings LSA → bluesky_posts_clusters.",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bluesky", "ml", "kedro"],
    max_active_runs=1,
) as dag:

    run_kedro_ml = BashOperator(
        task_id="run_kedro_ml",
        bash_command="cd /opt/kedro-clean && kedro run --pipeline=ml",
    )

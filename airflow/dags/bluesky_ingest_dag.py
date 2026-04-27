"""
DAG d'ingestion Bluesky → PostgreSQL.

Reproduit la logique de import_data/app.py mais en tâches Airflow séparées :
chaque source (auteur, timeline, catégorie de recherche) devient une tâche
indépendante avec son propre retry et ses propres logs. Si une catégorie
plante (rate limit, token expiré non rafraîchi, etc.), les autres continuent.

Imports dynamiques :
  Le code source du pipeline n'est pas dans un package installé mais dans
  /opt/import_data (bind-mount déclaré dans docker-compose.yaml). On ajoute
  ce chemin à sys.path AVANT les imports du module.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

# Le pipeline d'ingestion vit dans /opt/import_data (bind mount depuis l'hôte).
# Il faut l'ajouter à sys.path avant d'importer functions, getItems, app.
sys.path.insert(0, "/opt/import_data")

from airflow.decorators import dag, task  # noqa: E402

import functions  # noqa: E402
import getItems   # noqa: E402
from app import get_author_feed, get_timeline  # noqa: E402


DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="bluesky_ingest",
    description="Ingestion des posts Bluesky (auteurs ciblés, timeline, recherches par mots-clés) vers PostgreSQL.",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bluesky", "ingestion", "postgres"],
    max_active_runs=1,
)
def bluesky_ingest():

    @task
    def ensure_table() -> str:
        """Crée la table 'posts' si elle n'existe pas. Idempotent."""
        if not functions.LOGIN or not functions.PASS:
            raise RuntimeError("LOGIN_BLUESKY et PASS_BLUESKY non définis dans .env")
        functions.ensure_table()
        return "ready"

    @task
    def fetch_author_feed(handle: str) -> None:
        """Récupère le fil d'un auteur ciblé."""
        get_author_feed(handle)

    @task
    def fetch_timeline_task() -> None:
        """Récupère le fil d'actualité général du compte connecté."""
        get_timeline()

    @task
    def fetch_search_category(category: str, queries: list[str]) -> None:
        """Récupère tous les posts pour une catégorie de mots-clés."""
        print(f"=== Catégorie : {category} ({len(queries)} requêtes) ===")
        for q in queries:
            getItems.get_search_post(q, limit=100)

    # ── Graphe ────────────────────────────────────────────────────────────────
    ready = ensure_table()

    # Une tâche par auteur ciblé (TARGET défini dans functions.py).
    # .override(task_id=...) donne un nom lisible dans l'UI Airflow plutôt que
    # fetch_author_feed, fetch_author_feed__1, etc.
    author_tasks = [
        fetch_author_feed.override(
            task_id=f"author__{t['handle'].replace('.', '_')}"
        )(t["handle"])
        for t in functions.TARGET
    ]

    timeline_task = fetch_timeline_task()

    # Une tâche par catégorie de recherche (politics, health, tech, ...).
    search_tasks = [
        fetch_search_category.override(task_id=f"search__{cat}")(cat, queries)
        for cat, queries in functions.SEARCH_QUERIES.items()
    ]

    # Toutes les tâches d'ingestion attendent que la table existe,
    # mais sont indépendantes entre elles → parallélisation possible.
    ready >> author_tasks
    ready >> timeline_task
    ready >> search_tasks


bluesky_ingest()
import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.cluster import MiniBatchKMeans
from sqlalchemy import create_engine, text

MODELS_DIR = Path("/opt/kedro-clean/data/06_models")
KMEANS_PATH = MODELS_DIR / "kmeans.pkl"

# Fichier de metadata : nb de vecteurs au dernier fit du KMeans.
# Même logique que pour LSA dans nodes_vectors.py.
KMEANS_FIT_COUNT_PATH = MODELS_DIR / "kmeans_fit_count.txt"


def _build_con_string() -> str:
    user = os.getenv("POSTGRES_USER", "airflow")
    pwd  = os.getenv("POSTGRES_PASSWORD", "airflow")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "bluesky_clean")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _last_fit_count() -> int:
    if KMEANS_FIT_COUNT_PATH.exists():
        return int(KMEANS_FIT_COUNT_PATH.read_text().strip())
    return 0


def _save_kmeans(kmeans: MiniBatchKMeans, corpus_size: int) -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(kmeans, KMEANS_PATH)
    KMEANS_FIT_COUNT_PATH.write_text(str(corpus_size))
    print(f"[ML] KMeans sauvegardé ({corpus_size} vecteurs au fit).")


def load_vectors(refit_threshold: float) -> dict:
    engine = create_engine(_build_con_string())

    # Posts sans cluster assigné
    new_query = text("""
        SELECT v.post_id, v.embedding::text
        FROM bluesky_posts_vectors v
        LEFT JOIN bluesky_posts_clusters c ON v.post_id = c.post_id
        WHERE c.post_id IS NULL
    """)

    total_query = text("SELECT COUNT(*) FROM bluesky_posts_vectors")

    with engine.connect() as conn:
        new_df      = pd.read_sql(new_query, conn)
        total_count = conn.execute(total_query).scalar()

    print(f"[ML] Vecteurs total : {total_count} | Sans cluster : {len(new_df)}")

    n_at_fit    = _last_fit_count()
    n_since_fit = total_count - n_at_fit
    ratio       = (n_since_fit / n_at_fit) if n_at_fit > 0 else 1.0
    needs_refit = not KMEANS_PATH.exists() or ratio >= refit_threshold

    if needs_refit:
        print(f"[ML] Refit KMeans (ratio={ratio:.1%} >= seuil={refit_threshold:.1%}). Chargement de tous les vecteurs...")
        all_query = text("SELECT post_id, embedding::text FROM bluesky_posts_vectors")
        with engine.connect() as conn:
            all_df = pd.read_sql(all_query, conn)
    else:
        print(f"[ML] Pas de refit (ratio={ratio:.1%} < seuil={refit_threshold:.1%}). Predict uniquement.")
        all_df = pd.DataFrame(columns=["post_id", "embedding"])

    return {
        "new_df":       new_df,
        "all_df":       all_df,
        "needs_refit":  needs_refit,
        "total_count":  total_count,
        "refit_threshold": refit_threshold,
    }


def _parse_embeddings(df: pd.DataFrame) -> np.ndarray:
    return np.array([
        list(map(float, row.strip("[]").split(",")))
        for row in df["embedding"]
    ])


def train_clustering(payload: dict, n_clusters: int, kmeans_batch_size: int) -> pd.DataFrame:
    new_df      = payload["new_df"]
    all_df      = payload["all_df"]
    needs_refit = payload["needs_refit"]
    total_count = payload["total_count"]

    if new_df.empty and not needs_refit:
        print("[ML] Aucun nouveau post à clusteriser.")
        return pd.DataFrame(columns=["post_id", "cluster_id"])

    if needs_refit:
        fit_df = all_df if not all_df.empty else new_df
        X_fit  = _parse_embeddings(fit_df)

        # n_clusters ne peut pas dépasser le nombre de samples
        k = min(n_clusters, len(X_fit))
        print(f"[ML] Entraînement MiniBatchKMeans(k={k}) sur {len(X_fit)} vecteurs...")

        kmeans = MiniBatchKMeans(
            n_clusters=k,
            random_state=42,
            batch_size=kmeans_batch_size,
            n_init="auto",
        )
        kmeans.fit(X_fit)
        _save_kmeans(kmeans, total_count)

        # Après un refit, on réassigne TOUS les posts (les anciens clusters
        # ne sont plus comparables avec le nouvel espace).
        labels = kmeans.predict(X_fit)
        result = fit_df[["post_id"]].copy()
        result["cluster_id"] = labels.astype(int)
        print(f"[ML] {len(result)} posts clusterisés (refit complet).")
        return result

    # Pas de refit : on charge le modèle et on prédit uniquement les nouveaux
    kmeans = joblib.load(KMEANS_PATH)
    X_new  = _parse_embeddings(new_df)
    labels = kmeans.predict(X_new)

    result = new_df[["post_id"]].copy()
    result["cluster_id"] = labels.astype(int)
    print(f"[ML] {len(result)} nouveaux posts clusterisés.")
    return result


def upsert_clusters(df: pd.DataFrame) -> None:
    if df.empty:
        print("[ML] Rien à upserter.")
        return

    engine = create_engine(_build_con_string())
    rows   = df[["post_id", "cluster_id"]].to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TEMPORARY TABLE clusters_tmp (
                post_id    TEXT,
                cluster_id INTEGER
            ) ON COMMIT DROP
        """))
        conn.execute(
            text("INSERT INTO clusters_tmp (post_id, cluster_id) VALUES (:post_id, :cluster_id)"),
            rows,
        )
        result = conn.execute(text("""
            INSERT INTO bluesky_posts_clusters (post_id, cluster_id, assigned_at)
            SELECT post_id, cluster_id, NOW()
            FROM clusters_tmp
            ON CONFLICT (post_id) DO UPDATE SET
                cluster_id  = EXCLUDED.cluster_id,
                assigned_at = NOW()
        """))

    print(f"[ML] {result.rowcount} clusters upsertés dans bluesky_posts_clusters.")

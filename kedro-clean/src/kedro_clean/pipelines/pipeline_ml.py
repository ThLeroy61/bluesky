from kedro.pipeline import Pipeline, node

from kedro_clean.pipelines.nodes_ml import (
    load_vectors,
    train_clustering,
    upsert_clusters,
)
from kedro_clean.pipelines.nodes_nlp import (
    load_posts_for_sentiment,
    run_sentiment,
    upsert_sentiment,
    load_posts_for_fakenews,
    run_fakenews,
    upsert_fakenews,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            # ── Clustering KMeans ─────────────────────────────────────────────
            node(
                func=load_vectors,
                inputs="params:refit_threshold",
                outputs="vectors_payload",
                name="load_vectors",
            ),
            node(
                func=train_clustering,
                inputs=[
                    "vectors_payload",
                    "params:n_clusters",
                    "params:kmeans_batch_size",
                ],
                outputs="clusters_df",
                name="train_clustering",
            ),
            node(
                func=upsert_clusters,
                inputs="clusters_df",
                outputs=None,
                name="upsert_clusters",
            ),

            # ── Analyse émotionnelle ──────────────────────────────────────────
            node(
                func=load_posts_for_sentiment,
                inputs=None,
                outputs="sentiment_posts",
                name="load_posts_for_sentiment",
            ),
            node(
                func=run_sentiment,
                inputs=["sentiment_posts", "params:sentiment_batch_size"],
                outputs="sentiment_scores",
                name="run_sentiment",
            ),
            node(
                func=upsert_sentiment,
                inputs="sentiment_scores",
                outputs=None,
                name="upsert_sentiment",
            ),

            # ── Détection de fake news ────────────────────────────────────────
            node(
                func=load_posts_for_fakenews,
                inputs=None,
                outputs="fakenews_posts",
                name="load_posts_for_fakenews",
            ),
            node(
                func=run_fakenews,
                inputs=["fakenews_posts", "params:fakenews_batch_size"],
                outputs="fakenews_scores",
                name="run_fakenews",
            ),
            node(
                func=upsert_fakenews,
                inputs="fakenews_scores",
                outputs=None,
                name="upsert_fakenews",
            ),
        ]
    )

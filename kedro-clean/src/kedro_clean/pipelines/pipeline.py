from kedro.pipeline import Pipeline, node

from kedro_clean.pipelines.nodes import (
    extract_text_signals,
    clean_text,
    validate_quality,
    upsert_posts,
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            # posts_raw vient du catalog (SQL incrémental sur la table 'posts').
            # Plus besoin de normalize_mongo_posts : les types arrivent déjà
            # propres depuis PostgreSQL.
            node(
                func=extract_text_signals,
                inputs="posts_raw",
                outputs="posts_signals",
                name="extract_text_signals",
            ),
            node(
                func=clean_text,
                inputs="posts_signals",
                outputs="posts_cleaned",
                name="clean_text",
            ),
            node(
                func=validate_quality,
                inputs=[
                    "posts_cleaned",
                    "params:min_text_length",
                    "params:allowed_languages",
                ],
                outputs="posts_ready",
                name="validate_quality",
            ),
            node(
                func=upsert_posts,
                inputs="posts_ready",
                outputs="postgres_posts_final",
                name="upsert_into_postgres",
            ),
        ]
    )
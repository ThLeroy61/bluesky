from kedro.pipeline import Pipeline, node

from kedro_clean.pipelines.nodes_vectors import (
    load_posts_to_vectorize,
    compute_vectors,
    upsert_vectors,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=load_posts_to_vectorize,
                inputs="params:refit_threshold",
                outputs="posts_to_vectorize",
                name="load_posts_to_vectorize",
            ),
            node(
                func=compute_vectors,
                inputs="posts_to_vectorize",
                outputs="posts_with_embeddings",
                name="compute_vectors",
            ),
            node(
                func=upsert_vectors,
                inputs="posts_with_embeddings",
                outputs=None,
                name="upsert_vectors",
            ),
        ]
    )

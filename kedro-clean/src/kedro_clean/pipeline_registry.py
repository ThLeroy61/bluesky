from __future__ import annotations

from kedro.pipeline import Pipeline
from kedro_clean.pipelines import pipeline_vectors
from kedro_clean.pipelines import pipeline_ml


from kedro_clean.pipelines.pipeline import create_pipeline as create_data_pipeline


def register_pipelines() -> dict[str, Pipeline]:
    data_pipeline = create_data_pipeline()

    return {
        "data_engineering": data_pipeline,
        "vectors": pipeline_vectors.create_pipeline(),
        "ml": pipeline_ml.create_pipeline(),
        "__default__": data_pipeline,
    }
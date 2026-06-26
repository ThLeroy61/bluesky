import os
from pathlib import Path

from kedro.framework.hooks import hook_impl


class EnergyTrackingHooks:
    def __init__(self) -> None:
        self._tracker = None

    def _start(self, pipeline_name: str | None) -> None:
        from codecarbon import OfflineEmissionsTracker

        output_dir = Path(os.getenv("EMISSIONS_DIR", "/opt/kedro-clean/data/08_reporting"))
        output_dir.mkdir(parents=True, exist_ok=True)

        self._tracker = OfflineEmissionsTracker(
            country_iso_code="FRA",
            project_name=pipeline_name or "__default__",
            output_dir=str(output_dir),
            output_file="emissions.csv",
            measure_power_secs=15,
            log_level="error",
            save_to_file=True,
        )
        self._tracker.start()

    def _stop(self) -> None:
        if self._tracker is not None:
            try:
                self._tracker.stop()
            finally:
                self._tracker = None

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog) -> None:
        # run_params["pipeline_name"] = nom passé à `kedro run --pipeline=...`
        self._start(run_params.get("pipeline_name"))

    @hook_impl
    def after_pipeline_run(self, run_params, run_result, pipeline, catalog) -> None:
        self._stop()

    @hook_impl
    def on_pipeline_error(self, error, run_params, pipeline, catalog) -> None:
        # On arrête proprement le tracker même si le pipeline plante,
        # pour ne pas perdre la mesure partielle ni laisser un thread tourner.
        self._stop()

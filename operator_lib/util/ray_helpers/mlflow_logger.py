import numbers
import time
import typing
from contextlib import contextmanager


import mlflow
from ray.train import Checkpoint, UserCallback

class TrainMlflowLogger(UserCallback):
    def __init__(self, tracking_uri: str, experiment_name: str, run_id: str):
        self._tracking_uri = tracking_uri
        self._experiment_name = experiment_name
        self._run_id = run_id

    def _ensure_started(self):
        # Ray callbacks are serialized across processes; a cached _started flag
        # alone is not reliable. Reconcile against the actual active run.
        mlflow.set_tracking_uri(self._tracking_uri)
        mlflow.set_experiment(self._experiment_name)
        active_run = mlflow.active_run()
        if active_run is None:
            mlflow.start_run(run_id=self._run_id, log_system_metrics=True)
        elif active_run.info.run_id != self._run_id:
            mlflow.end_run(status="KILLED")
            mlflow.start_run(run_id=self._run_id, log_system_metrics=True)

    def set_tags(self, tags: typing.Dict[str, typing.Any]):
        self._ensure_started()
        mlflow.set_tags(tags)

    def log_params(self, params: typing.Dict[str, typing.Any]):
        self._ensure_started()
        mlflow.log_params(params)

    def log_metrics(self, metrics: typing.Dict[str, typing.Any], step: typing.Optional[int] = None):
        self._ensure_started()
        numeric_metrics = {k: float(v) for k, v in metrics.items() if isinstance(v, numbers.Number)}
        if numeric_metrics:
            mlflow.log_metrics(numeric_metrics, step=step)

    def log_dict(self, dictionary: typing.Dict[str, typing.Any], artifact_file: str):
        self._ensure_started()
        mlflow.log_dict(dictionary, artifact_file)

    def log_text(self, text: str, artifact_file: str):
        self._ensure_started()
        mlflow.log_text(text, artifact_file)

    def log_stage_timing(self, stage_name: str, seconds: float, step: typing.Optional[int] = None):
        safe_stage = stage_name.replace(" ", "_")
        self.log_metrics({f"timing.{safe_stage}.seconds": float(seconds)}, step=step)

    @contextmanager
    def trace(
        self,
        stage_name: str,
        step: typing.Optional[int] = None,
        inputs: typing.Optional[typing.Dict[str, typing.Any]] = None,
        attributes: typing.Optional[typing.Dict[str, typing.Any]] = None,
    ):
        self._ensure_started()
        started_at = time.perf_counter()

        # MLflow manual tracing API (GenAI tracing). If unavailable, fall back to metrics-only timing.
        if hasattr(mlflow, "start_span"):
            with mlflow.start_span(name=stage_name) as span:
                if inputs:
                    span.set_inputs(inputs)
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(str(key), value)

                try:
                    yield span
                except Exception as exc:
                    span.set_attribute("error", True)
                    span.set_attribute("error.type", type(exc).__name__)
                    span.set_attribute("error.message", str(exc))
                    raise
                finally:
                    elapsed_seconds = time.perf_counter() - started_at
                    span.set_attribute("duration_seconds", float(elapsed_seconds))
                    self.log_stage_timing(stage_name, elapsed_seconds, step=step)
            return

        try:
            yield None
        finally:
            elapsed_seconds = time.perf_counter() - started_at
            self.log_stage_timing(stage_name, elapsed_seconds, step=step)

    def _aggregate_metrics(self, worker_metrics: typing.List[typing.Dict[str, typing.Any]]) -> typing.Dict[str, float]:
        if not worker_metrics:
            return {}

        aggregated: typing.Dict[str, float] = {}
        keys = set().union(*(m.keys() for m in worker_metrics))
        for key in keys:
            values = [m.get(key) for m in worker_metrics if isinstance(m.get(key), numbers.Number)]
            if values:
                aggregated[key] = float(sum(values) / len(values))
        return aggregated

    def after_report(self, run_context, metrics: typing.List[typing.Dict[str, typing.Any]], checkpoint: typing.Optional[Checkpoint]):
        self._ensure_started()

        aggregated_metrics = self._aggregate_metrics(metrics)
        step = None
        if aggregated_metrics:
            epoch_value = aggregated_metrics.get("epoch")
            if isinstance(epoch_value, numbers.Number):
                step = int(epoch_value)
            self.log_metrics(aggregated_metrics, step=step)

        if checkpoint is not None:            
            with checkpoint.as_directory() as checkpoint_dir:
                mlflow.log_artifacts(checkpoint_dir, artifact_path=f"checkpoints/{step}")

    def after_exception(self, run_context, worker_exceptions: typing.Dict[int, Exception]):
        self.finish(status="FAILED")

    def finish(self, status: str = "FINISHED"):
        if mlflow.active_run() is not None:
            mlflow.end_run(status=status)

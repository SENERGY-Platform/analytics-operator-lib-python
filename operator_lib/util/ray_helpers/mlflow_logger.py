import numbers
import typing


import mlflow
from ray.train import Checkpoint, UserCallback

class TrainMlflowLogger(UserCallback):
    def __init__(self, tracking_uri: str, experiment_name: str, run_id: str):
        self._tracking_uri = tracking_uri
        self._experiment_name = experiment_name
        self._run_id = run_id
        self._started = False

    def _ensure_started(self):
        if self._started:
            return
        mlflow.set_tracking_uri(self._tracking_uri)
        mlflow.set_experiment(self._experiment_name)
        mlflow.start_run(run_id=self._run_id)
        self._started = True

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
        if aggregated_metrics:
            step = None
            epoch_value = aggregated_metrics.get("epoch")
            if isinstance(epoch_value, numbers.Number):
                step = int(epoch_value)
            mlflow.log_metrics(aggregated_metrics, step=step)

        if checkpoint is not None:
            with checkpoint.as_directory() as checkpoint_dir:
                mlflow.log_artifacts(checkpoint_dir, artifact_path="checkpoints/latest")

    def after_exception(self, run_context, worker_exceptions: typing.Dict[int, Exception]):
        self.finish(status="FAILED")

    def finish(self, status: str = "FINISHED"):
        if self._started and mlflow.active_run() is not None:
            mlflow.end_run(status=status)
        self._started = False
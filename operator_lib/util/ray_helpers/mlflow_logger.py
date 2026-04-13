import numbers
import typing


import mlflow
from ray.train import Checkpoint, UserCallback

class TrainMlflowLoggerCallback(UserCallback):
    def __init__(self, tracking_uri: str, experiment_name: str, run_name: str):
        self._tracking_uri = tracking_uri
        self._experiment_name = experiment_name
        self._run_name = run_name
        self._started = False

    def _ensure_started(self):
        if self._started:
            return
        mlflow.set_tracking_uri(self._tracking_uri)
        mlflow.set_experiment(self._experiment_name)
        # mlflow.start_run(run_name=self._run_name)
        self._started = True

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
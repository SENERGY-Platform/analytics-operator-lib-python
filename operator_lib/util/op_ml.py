"""
   Copyright 2026 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

__all__ = ("MLOperator",)

from .helpers.mlflow_logger import TrainMlflowLogger

from .op_base import OperatorBase
from operator_lib.util.logger import logger
from operator_lib.util.config import MissingConfigValueError
from . import clock

import typing
import datetime
import typing
import abc
import json
import math
import os
import tempfile
import time
import mlflow
from mlflow import MlflowClient
from mlflow.environment_variables import MLFLOW_RUN_ID
from mlflow.pyfunc import PyFuncModel, PythonModel
import datetime
import ray
from ray.runtime_env import RuntimeEnv
import mlflow


class MLOperator(OperatorBase):
    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        ray.shutdown()
        mlflow.set_tracking_uri(self.config.mlflow_url)
        self.model_id = f"pipeline-{self.get_pipeline_id()}_operator-{self.get_operator_id()}"
        mlflow.set_experiment(self.model_id)
        if self.config.test_end:
            # A data split with a test end on the config: the evaluation phase
            # replaces the deployment's train-or-skip logic entirely. init()
            # returns once the replay is recorded; it never calls
            # __load_model(), so a registered production model plays no part.
            self.__evaluate()
            return
        model = self.__load_model()
        if model is None:
            self.__wrap_training()

    def __update_model(self, model: PythonModel):
        # Started before the outer trace, not inside it: the with-statement
        # below reads self.__mlflow_logger the moment it is evaluated, so a
        # start_run happening in the old position -- inside the block --
        # came too late to matter. __mlflow_logger.trace's own
        # _ensure_started does a fluent start_run(run_id=...) against
        # whatever run the logger still names; if that is ODE's run
        # (__evaluate ended it, but a stale logger reference would still
        # name it), tracing here would reopen exactly the run this file's
        # phase separation just closed. Starting fresh first means the
        # logger used below already names the run this call is about to
        # write into.
        #
        # This also fixes a pre-existing defect on the deployment path:
        # after a training pass registered a model, __run went back to None
        # while __mlflow_logger still pointed at the run __update_model had
        # just ended, so a later run() traced its work into that stale,
        # ended run while log_model/register_model landed in a second run
        # start_run() opened without a name. Starting the run before
        # anything traces keeps the logger and the run it writes to in
        # agreement.
        if self.__run is None:
            self.__start_run()

        with self.__mlflow_logger.trace("update_model"):
            with self.__mlflow_logger.trace("log model"):
                new_model = mlflow.pyfunc.log_model(
                    artifact_path=self.model_id,
                    python_model=model,
                )
            with self.__mlflow_logger.trace("register model"):
                created_model_version = mlflow.register_model(
                    new_model.model_uri, self.model_id)

            with self.__mlflow_logger.trace("set alias"):
                client = MlflowClient()
                client.set_registered_model_alias(
                    self.model_id, "production", created_model_version.version)

            with self.__mlflow_logger.trace("update local model"):
                self.model = mlflow.pyfunc.load_model(
                    f"models:/{self.model_id}@production")
        mlflow.end_run()
        self.__run = None

    def __load_model(self) -> typing.Optional[PyFuncModel]:
        try:
            self.model = mlflow.pyfunc.load_model(
                f"models:/{self.model_id}@production")
        except Exception:
            self.model = None
        return self.model

    @abc.abstractmethod
    def infer(self, model: typing.Optional[PyFuncModel], data: typing.Dict[str, typing.Any], selector: str, device_id: str, timestamp: datetime.datetime) -> typing.Tuple[typing.Optional[datetime.datetime], typing.Optional[typing.Any], typing.Optional[PythonModel]]:
        """
        Subclasses must override this method.
        It will be called for each message. The current model will be provided. If your ML algorithm changes the model on each inference, you can return the new model as the second return value. This will update the model in mlflow and set it as the current model. If you return None as the second return value, the model is not updated. You should only return a new model if it is actually updated.
        :param model: The current model
        :param data: Dictionary containing data extracted from a message.
        :param selector: Name of a selector identifying the extracted data.
        :param device_id: ID of the device the message originates from
        :param timestamp: Kafka stored message timestamp.
        :return: Result data or None.
        """
        pass

    @abc.abstractmethod
    def train(self, model: typing.Optional[PyFuncModel], logger: TrainMlflowLogger) -> typing.Optional[PythonModel]:
        """
        Subclasses must override this method.
        Training is called if no model is present in mlflow or you return True in need_retraining. If a model already exists it is provided as a parameter. You can return a new model which is then registered in mlflow and set as the current model. If you return None, the current model is not updated.
        :param model: The current model
        :return: Result data or None.
        """
        return None

    @abc.abstractmethod
    def need_retraining(self, model: typing.Optional[PyFuncModel]) -> bool:
        """
        Subclasses must override this method.
        This method is called after each inference. Therefore, computation should be fast. If re-computaiton should not be done after every message, consider setting a timer to avoid frequent recomputation.
        If you return True, the training method is called to update the model.
        :param model: The current model
        :return: Result data or None.
        """
        return False

    def __start_run(self):
        # A run handed over in MLFLOW_RUN_ID keeps the name its creator gave it.
        # mlflow's fluent start_run resumes that run and passes whatever run_name it
        # is given straight to update_run_info, so offering one here renames somebody
        # else's run -- and the only way to leave the name alone is not to offer one.
        # A deployment sets no MLFLOW_RUN_ID and still opens the named run below.
        run_name = None
        if MLFLOW_RUN_ID.get() is None:
            run_name = f"{self.model_id}@{datetime.datetime.now().isoformat(timespec='microseconds')}"
        self.__run = mlflow.start_run(run_name=run_name)
        # Kept beyond the run object itself: __update_model ends the run and
        # sets __run back to None once a trained model is registered, but the
        # evaluation still needs to log to this same run after that happens.
        self.__run_id = self.__run.info.run_id
        self.__mlflow_logger = TrainMlflowLogger(
            self.config.mlflow_url, self.model_id, self.__run.info.run_id)

    def __wrap_training(self):
        self.__start_run()

        with self.__mlflow_logger.trace("ray init"):
            logger.debug(
                f"Initializing Ray. This might take a while, even if you see log messages below...")
            ray.init(address=self.config.ray_url)
        with self.__mlflow_logger.trace("train"):
            model = self.train(self.model, self.__mlflow_logger)
        ray.shutdown()
        if model is not None:
            self.__update_model(model)

    def __evaluate(self):
        """
        The evaluation phase: train once under the split (whatever the
        registry holds), then replay the test window through infer(), message
        by message, with the clock set to each message's own time so a bounded
        read inside infer() sees exactly what would have been available then.

        Triggered from init() when the config carries test_end; see the module
        docstring on operator_lib.util.clock for why training_end -- read here
        from the clock rather than the config -- is driver state and not
        something a reader receives on its own.
        """
        training_end = clock.fixed()
        if training_end is None:
            raise MissingConfigValueError(
                "config value 'test_end' is set without 'training_end'; an "
                "evaluation needs both bounds")
        test_end = clock.parse_time(self.config.test_end)
        if not test_end > training_end:
            raise ValueError(
                f"test_end {test_end.isoformat()} is not after training_end "
                f"{training_end.isoformat()}")

        # The registry is not consulted: a registered production model from an
        # earlier run was trained on some other split, and the point of the
        # evaluation is that training happens under this one. The clock is
        # still at training_end here (set by OperatorBase.init, untouched
        # since), so every provide_historic_data call inside train() ends
        # there.
        self.model = None
        self.__wrap_training()

        # __wrap_training leaves the run open whenever train() returned None
        # (__update_model, which ends it, never ran); end it here
        # unconditionally -- mlflow.end_run() is a no-op when nothing is
        # active, so this is safe either way -- so no active fluent run
        # survives into the replay. self.__run_id (captured in __start_run,
        # untouched by __update_model) still names the run just closed;
        # everything the replay records goes through MlflowClient against
        # that id from here on, never through the fluent API again.
        mlflow.end_run()
        self.__run = None
        self.__mlflow_logger = None
        run_id = self.__run_id

        # A launch hands the run over in MLFLOW_RUN_ID so that
        # mlflow.start_run() without an explicit run_id resumes it -- which
        # is exactly what let a fluent call inside infer() during the replay
        # land in the run get_experiment_results reads back to a model.
        # Unsetting it here, once, before the first infer() below, is what
        # closes that path: a reader who deletes this line reopens the hole,
        # and every test still passes except the isolation test in
        # tests/test_evaluation.py that pins it.
        MLFLOW_RUN_ID.unset()

        # The phase transition, stamped on the run so that the other side can
        # filter on it.
        #
        # Unsetting the variable above takes nothing from code that already
        # read it: op.py is imported before init() runs, so an operator can
        # keep `RUN = os.environ.get("MLFLOW_RUN_ID")` at module level and
        # then call MlflowClient().log_metric(RUN, ...) from inside infer(),
        # writing into this very run with the boundary fully in place. ODE
        # withholds every metric stamped at or after this instant, whatever it
        # is called, which is the check that survives that route.
        #
        # Unix milliseconds, because that is the time base MLflow stamps
        # metrics with and the comparison is against those stamps. Read after
        # end_run() and before the first infer() below, which is what makes it
        # later than every metric the training phase logged and earlier than
        # anything the replay can write. Moving this line in either direction
        # breaks a control that nothing in this repository can see failing.
        training_ended_at = int(time.time() * 1000)
        MlflowClient().set_tag(
            run_id, "operator_lib.training_ended_at", str(training_ended_at))

        messages = 0
        results_count = 0
        prediction_rows = []

        import pandas as pd
        merged = pd.DataFrame(columns=["time", "topic", "selector", "device_id"])
        completed = False
        try:
            ray.init(address=self.config.ray_url)
            # Lazy imports: operator_lib.util.helpers is already pulled in as a
            # side effect of the TrainMlflowLogger import at the top of this
            # module, which is what makes read_input_window safe to import
            # here without importing operator_lib.util.helpers.data at load
            # time of operator_lib.util itself -- the same reasoning
            # helpers/kafka.py gives for its own lazy import of
            # operator_lib.util.gen_identifiers.
            from .helpers.data import read_input_window
            from operator_lib.util import get_selector

            inputs = read_input_window(training_end, test_end)
            dests_by_topic = {}
            frames = []
            for topic, frame in inputs:
                frame = frame.copy()
                selector = get_selector(topic.mappings, self.selectors) if self.selectors else None
                device_id = topic.filterValue if topic.filterType == "DeviceId" else None
                frame["topic"] = topic.name
                frame["selector"] = selector
                frame["device_id"] = device_id
                # Topic names are taken as unique among one operator's input
                # topics, the same assumption __provide_historic_data's own
                # topic loop already makes.
                dests_by_topic[topic.name] = [m.dest for m in topic.mappings]
                frames.append(frame)

            if frames:
                merged = pd.concat(frames, ignore_index=True)
                # Ties keep topic order: pd.concat above preserves the frames'
                # own list order, and a stable sort does not disturb that among
                # equal times.
                merged = merged.sort_values("time", kind="stable").reset_index(drop=True)

            total = len(merged)
            for _, row in merged.iterrows():
                at = _as_utc(row["time"])
                clock.set_fixed(at)
                dests = dests_by_topic.get(row["topic"], [])
                data = {dest: _plain_value(row[dest]) for dest in dests}
                dt_result, result, model = self.infer(
                    self.model, data, row["selector"], row["device_id"], at)
                # The one part of run() this replay keeps: a model infer()
                # returns becomes the current model. need_retraining() is
                # deliberately never called here -- retraining inside the test
                # window would train on test data, which is the thing the
                # split forbids.
                if model is not None:
                    self.__update_model(model)
                messages += 1
                if result is not None:
                    results_count += 1
                    # A None result timestamp means "now", as in run(), where
                    # the producer stamps the output with the current time;
                    # the replay's now is this message's own time.
                    if dt_result is None:
                        dt_result = at
                prediction_rows.append({
                    "time": at.isoformat(),
                    "topic": row["topic"],
                    "selector": row["selector"],
                    "device_id": row["device_id"],
                    "result_time": dt_result.isoformat() if dt_result is not None else "",
                    "result": json.dumps(result, default=str) if result is not None else "",
                })
                if messages % 1000 == 0:
                    logger.info(f"evaluation: replayed {messages}/{total} messages")
            logger.info(f"evaluation: replayed {messages}/{total} messages")
            completed = True
        finally:
            clock.set_fixed(training_end)
            ray.shutdown()

            # Recorded through MlflowClient against the explicit run_id
            # captured above, never through the fluent API: nothing here may
            # resume, or accidentally open, a run of its own the way
            # __resume_run once did. Writing to an already-terminated run is
            # allowed -- MLflow's store guards lifecycle_stage, not status --
            # so this works whether or not something else closed it since.
            #
            # No metric is computed here either: what the target is and how
            # far ahead a forecast looks are the evaluation protocol's to
            # declare, not this library's -- it does not read
            # evaluation.yaml.
            client = MlflowClient()
            client.set_tag(run_id, "operator_lib.history_end", training_end.isoformat())
            client.set_tag(run_id, "operator_lib.test_end", test_end.isoformat())
            # Exactly these four: what get_experiment_results
            # (evaluationParams in the other repository) reads and nothing
            # else. evaluation.completed is gone -- ODE never read it, and
            # whether the replay finished is the run's terminal status,
            # which is where ODE already looks.
            client.log_param(run_id, "evaluation.messages", messages)
            client.log_param(run_id, "evaluation.results", results_count)
            client.log_param(run_id, "evaluation.window_start", training_end.isoformat())
            client.log_param(run_id, "evaluation.window_end", test_end.isoformat())

            # The test-window metric, from what is already in memory above --
            # prediction_rows and merged -- and nothing else: no new read, no
            # new artefact. `inputs` is read too, for the mapping source that
            # resolves evaluation_target_series; it was built before this
            # try's loop and is still in scope here, but may not exist at all
            # if the run failed before that point, which is one more reason
            # this whole call is guarded rather than trusted. A failure here
            # must not fail the replay that already completed (or already
            # failed for its own reason, which this must not shadow) -- so it
            # is caught and turned into the status reason instead of
            # propagating.
            try:
                metric_status, metric_name, metric_value, metric_n = \
                    _compute_evaluation_metric(
                        self.config, prediction_rows, merged, inputs)
            except Exception as ex:
                logger.exception(
                    "evaluation: metric computation raised; logging the "
                    "reason instead of a metric")
                metric_status, metric_name, metric_value, metric_n = (
                    f"error: {ex}", None, None, None)
            client.log_param(run_id, "evaluation.metric_status", metric_status)
            if metric_status == "computed":
                client.log_param(run_id, "evaluation.metric_name", metric_name)
                client.log_param(run_id, "evaluation.metric_value", metric_value)
                client.log_param(run_id, "evaluation.metric_n", metric_n)

            # inputs.csv is gone: it carried platform measurements from the
            # test window, and the singleuser pod that would read it back
            # reaches MLflow without a token -- an unconfirmed cell asking
            # for none could pull it down. predictions.csv is the one
            # artifact now; the scoring script fetches the actuals itself,
            # through timescale-wrapper, on its own credential.
            with tempfile.TemporaryDirectory() as tmp_dir:
                predictions_path = os.path.join(tmp_dir, "predictions.csv")
                pd.DataFrame(
                    prediction_rows,
                    columns=["time", "topic", "selector", "device_id", "result_time", "result"],
                ).to_csv(predictions_path, index=False)
                client.log_artifact(run_id, predictions_path, artifact_path="evaluation")

            client.set_terminated(run_id, status="FINISHED" if completed else "FAILED")
            # The exception that set completed=False, if any, propagates from
            # here -- this finally only records the run, it does not swallow
            # the failure that ended the replay early.

    def train_once(self) -> typing.Optional[PyFuncModel]:
        """
        Run a single training, regardless of whether a model is already registered. This is what
        init does when it finds none. Public so that a caller outside the deployment lifecycle -- a
        development run, an evaluation run -- can ask for it explicitly.
        :return: The current model, unchanged if train returned None.
        """
        self.__wrap_training()
        return self.model

    def run(self, data: typing.Dict[str, typing.Any], selector: str, device_id: str, timestamp: datetime.datetime):
        """
        The method will be called by the Operator Lib. It should not be called or overridden by subclasses. Subclasses should implement the infer and train method to provide ML functionality.
        """
        dt_result, result, model = self.infer(
            self.model, data, selector, device_id, timestamp)
        if model is not None:
            self.__update_model(model)
        if self.need_retraining(self.model):
            self.__wrap_training()
        return dt_result, result


def _as_utc(value) -> datetime.datetime:
    """
    A replayed row's `time` value as an aware UTC datetime.

    Frames returned by every reader are tz-naive UTC after normalisation (see
    the module comments in helpers/ts_wrapper.py and helpers/timescale.py), so
    the common case is naive and treated as already being UTC; an aware value,
    should one reach here some other way, is converted instead of assumed.
    """
    dt = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def _plain_value(value):
    """
    NaN/NaT become None, a numpy scalar becomes native Python via `.item()`,
    everything else passes through unchanged.

    `value != value` is the NaN/NaT self-inequality rather than
    `pandas.isna()`, so this needs no pandas import of its own -- it runs once
    per replayed field, which the evaluation's own risk (millions of rows in a
    long test window) is reason enough not to add one.
    """
    if value is None:
        return None
    try:
        if value != value:
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        return value.item()
    return value


_EVALUATION_METRICS = ("mae", "rmse")
# A fixed origin for bucket truncation, so that a prediction's result_time and
# the target series' own timestamps -- read from unrelated places, at
# unrelated moments -- fall into the same bucket whenever they land in the
# same resolution-wide interval. Which epoch is used does not matter on its
# own, only that both sides of the metric use the same one.
_BUCKET_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def _bucket_start(at: datetime.datetime, resolution: datetime.timedelta) -> datetime.datetime:
    """
    The start of the fixed-width bucket `at` falls into, by truncation --
    never rounding: floor division on the elapsed time since a fixed epoch,
    which is exact integer arithmetic on datetime.timedelta and never rounds
    a value up into the next bucket.
    """
    elapsed = at - _BUCKET_EPOCH
    return _BUCKET_EPOCH + (elapsed // resolution) * resolution


def _resolve_target_series(target_series, inputs):
    """
    Resolve a platform path against every input topic mapping's `source`,
    dropping its first path element the same way
    helpers/ts_wrapper.py's `_source_path` does when it turns a mapping
    source into the column name it asks timescale-wrapper for -- the same
    identity ODE and the wrapper already agree on, not a second rule invented
    here.

    `inputs` is the (InputTopic, frame) list __evaluate already built before
    the replay loop; no new read happens to answer this.

    Returns (topic_name, dest, error): exactly one of the pair or `error` is
    not None. `error` names why zero or more than one mapping matched --
    topic names are taken as unique among one operator's input topics, the
    same assumption dests_by_topic above already makes, so a match is
    reported per mapping rather than per topic.
    """
    from operator_lib.util.helpers.ts_wrapper import _source_path

    matches = []
    for topic, _frame in inputs:
        for mapping in topic.mappings:
            if mapping.source and _source_path(mapping.source) == target_series:
                matches.append((topic.name, mapping.dest))

    if not matches:
        return None, None, f"target series '{target_series}' matches no input topic"
    if len(matches) > 1:
        return None, None, (
            f"target series '{target_series}' matches {len(matches)} mappings, "
            f"expected exactly one")
    topic_name, dest = matches[0]
    return topic_name, dest, None


def _bucket_means(frame, dest: str, resolution: datetime.timedelta) -> typing.Dict[datetime.datetime, float]:
    """
    The mean of `dest` in `frame` -- already filtered to the target series'
    own topic -- grouped by the bucket its `time` falls into. A row whose
    value is missing or not numeric (see _plain_value) does not enter its
    bucket's mean at all, rather than counting as zero.
    """
    sums: typing.Dict[datetime.datetime, float] = {}
    counts: typing.Dict[datetime.datetime, int] = {}
    for _, row in frame.iterrows():
        value = _plain_value(row[dest])
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        bucket = _bucket_start(_as_utc(row["time"]), resolution)
        sums[bucket] = sums.get(bucket, 0.0) + value
        counts[bucket] = counts.get(bucket, 0) + 1
    return {bucket: sums[bucket] / counts[bucket] for bucket in sums}


def _compute_evaluation_metric(config, prediction_rows, merged, inputs):
    """
    The test-window metric, from what the replay already holds in memory --
    prediction_rows and merged -- and nothing else: no new read, no new
    artefact.

    Returns (status, name, value, n). n is the number of resolution buckets
    scored, not the number of predictions in them. status is "computed" once
    a metric was produced, and only then are name/value/n set; otherwise status names why
    not and the other three are None. Never raises on a data problem -- an
    unresolvable series, an unknown metric, zero matching predictions -- all
    end in a status reason instead; the caller still guards against a
    programming error escaping this function.
    """
    metric = config.evaluation_metric
    target_series = config.evaluation_target_series
    prediction_field = config.evaluation_prediction_field
    resolution = config.evaluation_resolution

    missing = [
        name for name, value in (
            ("evaluation_metric", metric),
            ("evaluation_target_series", target_series),
            ("evaluation_prediction_field", prediction_field),
            ("evaluation_resolution", resolution),
        ) if not value
    ]
    if missing:
        # Covers both the documented case -- an older ODE, or a launch
        # without a frozen scoring target, sets none of the four -- and a
        # partially filled config, which is just as unable to compute
        # anything.
        return f"{', '.join(missing)} not set", None, None, None

    if metric not in _EVALUATION_METRICS:
        return f"unknown metric '{metric}'", None, None, None

    import pandas as pd
    try:
        resolution_delta = pd.Timedelta(resolution).to_pytimedelta()
    except (ValueError, TypeError) as ex:
        return f"invalid resolution '{resolution}': {ex}", None, None, None
    if resolution_delta <= datetime.timedelta(0):
        return f"invalid resolution '{resolution}': not a positive duration", None, None, None

    topic_name, dest, error = _resolve_target_series(target_series, inputs)
    if error is not None:
        return error, None, None, None

    target_frame = merged[merged["topic"] == topic_name]
    bucket_means = _bucket_means(target_frame, dest, resolution_delta)

    # Each bucket counts once. The replay calls infer() for every message, so
    # an operator forecasting the next hour answers the same bucket once per
    # message -- thousands of times an hour at a one-second cadence. Pooling
    # those errors would weight every bucket by its message rate and report
    # the message count as n; averaging within a bucket first weights every
    # bucket equally, and n is the number of buckets scored.
    sums: typing.Dict[datetime.datetime, float] = {}
    counts: typing.Dict[datetime.datetime, int] = {}
    for row in prediction_rows:
        # A prediction is a replayed row with both a non-empty result and a
        # non-empty result_time; an empty result is a message that was
        # replayed but produced no answer, already counted elsewhere
        # (evaluation.results) and not a prediction for this metric.
        if not row["result"] or not row["result_time"]:
            continue
        try:
            result = json.loads(row["result"])
        except (TypeError, ValueError):
            continue
        if not isinstance(result, dict) or prediction_field not in result:
            continue
        predicted = result[prediction_field]
        if isinstance(predicted, bool) or not isinstance(predicted, (int, float)):
            continue
        try:
            result_time = clock.parse_time(row["result_time"])
        except ValueError:
            continue
        bucket = _bucket_start(result_time, resolution_delta)
        actual = bucket_means.get(bucket)
        if actual is None:
            # No actual for this bucket -- the tail of the window, where
            # result_time lands after test_end, is the expected case.
            continue
        error = predicted - actual
        sums[bucket] = sums.get(bucket, 0.0) + (abs(error) if metric == "mae" else error * error)
        counts[bucket] = counts.get(bucket, 0) + 1

    n = len(counts)
    if n == 0:
        return "no prediction has a matching actual value", None, None, None

    mean = sum(sums[bucket] / counts[bucket] for bucket in sums) / n
    value = mean if metric == "mae" else math.sqrt(mean)
    return "computed", metric, value, n

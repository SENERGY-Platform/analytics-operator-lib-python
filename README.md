# analytics-operator-lib-python

Library for SENERGY analytics operators written in Python. An operator subclasses `OperatorBase`, implements `run()` and is started with `OperatorLib`; `example/main.py` is a complete operator.

Install a tagged version:

```toml
"operator-lib @ git+https://github.com/SENERGY-Platform/analytics-operator-lib-python@v1.11.0"
```

Requires Python 3.10 or later.

MLflow comes as `mlflow-skinny`, the client: tracking, the model registry and pyfunc models. An environment that also runs the MLflow server or UI, or tracks into a local SQL database, installs `mlflow` itself.

## Declaring inputs, outputs and configuration

Optional. An operator may declare its ports and configuration with types; one that does not works as before.

```python
from operator_lib.util import Config, Inputs, Outputs, OperatorBase

class CustomConfig(Config):
    confidence_days: int = 7      # every field needs a type and a default

class MyInputs(Inputs):
    window_open: bool

class MyOutputs(Outputs):
    stopping_time: str

class Operator(OperatorBase):
    configType = CustomConfig
    inputs = MyInputs
    outputs = MyOutputs
```

- Types: `str`, `float`, `int`, `bool`.
- The types do not change parsing: config values arrive as strings and stay strings, as before.
- At runtime the library warns once per key when `run()` returns an output that `outputs` does not declare.

## Manifest

```bash
python -m operator_lib manifest main.py [--operator ClassName] [-o operator-manifest.json]
```

Writes `inputs`, `outputs` and `config_values`, each a list of `{"name", "type"}` in declaration order. Only the operator's own config fields are included, not the library's (`logger_level`, `ts_conn`, ...) unless the operator redeclares them. Without both `inputs` and `outputs` it writes nothing and exits 0; a config field without a type or default is an error. `main.py` is imported without running its `__main__` block.

The CI of an operator repository commits the manifest before tagging a version, and the platform registers the version from the manifest at that tag.

## History of inputs with several devices

An input assigned several devices (comma-separated in its `filterValue`) is still one dataset in `provide_historic_data`, `provide_historic_data_local` and `read_input_window`: the rows of every device, with an added column `device_id` naming the device. An input with one device has no such column.

## Reading history with `reader="sql"`

`provide_historic_data(..., reader="dataset")` is the default: one lazy Ray dataset per device. With `reader="sql"` every input that is read from Timescale over a database connection (a device input named `urn_infai_ses_service_*`, with `TS_CONN` or `ts_conn` set) is read in one Ray task over one connection, one query per device table. This is much faster for many small inputs, where the default spends its time on scheduling. The return value keeps its shape: a list in config order, one dataset or `ObjectRef` per input, with `device_id` for an input with several devices. Other inputs are read as before. `provide_historic_data_local` takes the same parameters and runs the queries in the calling process.

A table that fails with a connection error is read again on a new connection, 3 attempts 5 s apart. If it still fails, that input and the ones after it in config order raise when read; the ones before it are unaffected.

`changes_only=True` (only with `reader="sql"`) keeps, per table, the rows whose value differs from the previous or the next row in at least one mapped column. Periodic repeats of an unchanged value drop out, the first and the last row of the window stay, and so does every distinct value of rows that share a timestamp. The rows are ordered by time and then by the mapped columns.

`reader="sql"` with `require_full_duration=True` raises `ValueError`: waiting inside the single task would block every other input.

### Per-input settings: `overrides`

`provide_historic_data(..., reader="sql", overrides={"<input name>": SqlOverride(duration=..., bucket=...)})` changes how single inputs are read. The key is the name of the operator input (`mapping.dest`); `provide_historic_data_local` takes the same parameter. Without `reader="sql"`, or with a key that is no input, it raises `ValueError`; an override for an input that is not read over the database connection is ignored with a warning.

```python
from operator_lib.util.helpers import provide_historic_data, SqlOverride

provide_historic_data(
    timedelta(days=365), reader="sql", changes_only=True,
    overrides={"power": SqlOverride(duration=timedelta(days=90), bucket=timedelta(seconds=60))},
)
```

- `duration` replaces the window of that input; it ends at the same point as the others.
- `bucket` reads the input as one row per time bucket: `time_bucket(<bucket>, time)` as `time` and `max(<column>)` as the value. This needs TimescaleDB and numeric columns (`max` does not exist for booleans, and for text it would silently pick the largest string). `bucket` must be a whole number of seconds greater than zero. Empty buckets are not filled. `changes_only` then compares the bucket rows, not the raw rows.
- A topic whose mappings differ in their override is read with one query per group (mappings with the same override, and those without one) and the rows are put one under the other, sorted by time. The columns of the other groups are NaN in them, so integer columns come back as floats. For an input with several devices this happens per device, and `device_id` is set as before.

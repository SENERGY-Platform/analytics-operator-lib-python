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

An input assigned several devices (comma-separated in its `filterValue`) is still one dataset in `provide_historic_data`, `provide_historic_data_local` and `read_input_window` (one frame in `provide_historic_frames_local`): the rows of every device, with an added column `device_id` naming the device. An input with one device has no such column.

## Filtering a shared import by location

A shared import (all DWD stations in one topic, for example) delivers every place to every operator. An operator for one place declares an `ImportFilter`; the library then passes only that place's messages to `run()`, and reads only its rows from the history.

```python
from operator_lib.util import Config, ImportFilter, MLOperator

class CustomConfig(Config):
    station_id = ""
    lat = ""
    long = ""

class Operator(MLOperator):
    configType = CustomConfig
    import_filter = ImportFilter(
        path="value.meta.id",              # dotted path in the raw import message (live)
        column="station_id",               # the export column that holds it (history)
        value="station_id",                # config field holding the wanted value; may be empty
        near=("lat", "long"),              # config fields with a location, used when value is empty
        near_paths=("value.meta.lat", "value.meta.long"),
        near_columns=("lat", "lon"),       # the export columns of the two
    )
```

The export columns are named in the declaration because `import_exports` lists only the columns of the mapped inputs. The export itself (table, export id) still comes from the `import_exports` entry of the import.

The value is found once at start, before the first training:

1. The config field named by `value` is set: that value.
2. Otherwise, with the `near` fields set: the nearest place (great-circle distance) among the distinct `column`, `near_columns` rows of the export that have data in the last seven days, so a decommissioned place cannot win. The value and its distance are logged at info level. The same seven days apply over a database connection and over timescale-wrapper.
3. Neither: no filtering, one warning, and no check of the export.

An import has an `import_exports` entry only if an export carries every mapped path of the topic as a column; an import with list values (a forecast) never has one. Without an entry the filter still works: live as always, in the history through Kafka, and with `value` unset the nearest place is taken from the last `places_scan_messages` messages (default 20000, per partition, counted back from the end offset) of the input's topic: identifier and location are read from the messages at `path` and `near_paths`. If none of them carries the paths, the operator stops at start.

If an entry exists, the operator stops at start with a message naming the export and the column when the export's table lacks one of the declared columns (checked against the table over a database connection, against the answer of timescale-wrapper otherwise). A filter that was asked for never falls back to the unfiltered import. A `value` or `near` field that the config type does not declare is a `DeclarationError` at start. The comparison is on strings, so `02932` and `2932` differ. A message without the path is dropped.

An operator with several imports declares `import_filters` instead, each filter bound to inputs by name; the `near` fields may be shared:

```python
    import_filters = [
        ImportFilter(inputs=["outdoor_temperature"], path="value.meta.id", column="station_id", value="measure_station",
                     near=("lat", "long"), near_paths=("value.meta.lat", "value.meta.long"), near_columns=("lat", "lon")),
        ImportFilter(inputs=["forecast_temperature", "forecasted_for"], path="value.meta.id", column="station_id",
                     value="forecast_station",
                     near=("lat", "long"), near_paths=("value.meta.lat", "value.meta.long"), near_columns=("lat", "lon")),
    ]
```

`operator.resolved_import_filters()` returns, once `init()` has run, one entry per declared filter in declaration order: `value`, `found` (`"config"`, `"near"`, or `"none"` when nothing was set and the import is not filtered), `inputs`, `path`, and for `near` the `distance_km` and the station's `latitude` and `longitude`.

A filter applies to the import topics that one of its `inputs` is mapped to. `inputs` is required as soon as the operator reads more than one import: a filter without it is rejected at start rather than applied to every import. One import input can have only one filter. Device and operator inputs are never filtered. The history is filtered the same way in every reader that serves an import (database connection, timescale-wrapper, Kafka); `reader="sql"` does not read imports. A filtered history read runs in its own Ray tasks, which a Ray worker with an older library does not have; the unfiltered tasks are unchanged.

## Reading history without Ray: `provide_historic_frames_local`

`provide_historic_data_local` returns Ray datasets. Without a connection to a cluster, its first call starts a local Ray instance, which took about 2 GB of memory in our measurements and stays for the life of the process. An `MLOperator` runs `infer()` in exactly that state, after training has disconnected. While connected through Ray Client, as during the evaluation replay, the read fails.

`provide_historic_frames_local` takes the same parameters and returns the same history as one pandas `DataFrame` per input, in config order, read in the calling process without Ray. Devices and imports with an export are read over the database connection or timescale-wrapper as before.

An input whose history only Kafka has raises `HistoryNeedsRayError`: the output of another operator, an import without an entry in `import_exports`, or any input while `ALWAYS_PREFER_KAFKA` is set. Read it with `provide_historic_data_local`:

```python
from operator_lib.util.helpers import HistoryNeedsRayError, provide_historic_data_local, provide_historic_frames_local

try:
    frames = provide_historic_frames_local(timedelta(hours=8))
except HistoryNeedsRayError:
    frames = [ds.to_pandas() for ds in provide_historic_data_local(timedelta(hours=8))]
```

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

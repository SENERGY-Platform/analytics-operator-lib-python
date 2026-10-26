# analytics-operator-lib-python

Library for SENERGY analytics operators written in Python. An operator subclasses `OperatorBase`, implements `run()` and is started with `OperatorLib`; `example/main.py` is a complete operator.

Install a tagged version:

```toml
"operator-lib @ git+https://github.com/SENERGY-Platform/analytics-operator-lib-python@v1.10.1"
```

Requires Python 3.10 or later.

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

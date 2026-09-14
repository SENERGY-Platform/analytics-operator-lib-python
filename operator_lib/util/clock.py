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

"""
The bound every history read applies.

In a deployment this is the wall clock: `now()` is `datetime.now()`, and the
three history readers (timescale, timescale-wrapper, kafka) read up to it the
way they always have. Where the deployment config carries `training_end` --
an Operator Development Environment launch that set a data split on the
session -- `OperatorBase.init` starts the clock there instead, so that
`train()` never sees a value the split forbids. During the evaluation phase
that follows, `MLOperator.__evaluate` advances the clock to each replayed
message's own timestamp before calling `infer()`, so a bounded read inside
`infer()` sees exactly what would have been available at that moment and
nothing after it.

Operator code -- which is what the assistant in the Operator Development
Environment writes -- calls `provide_historic_data(duration)` and never sees
`training_end` or the clock at all; the bound is applied below it.

This module is driver state, not a value threaded through the readers'
distributed calls: it is set once in the driver process, by the deployment or
launch configuration, and Ray workers do not share a driver's module-level
state. So `data.py` resolves `clock.fixed()` in the driver and passes the
result as an explicit `end` argument to every reader call, local or
`.remote()`; the readers themselves never import this module.
"""

__all__ = ("now", "fixed", "set_fixed", "parse_time")

import datetime
import typing

_fixed: typing.Optional[datetime.datetime] = None


def now() -> datetime.datetime:
    """
    The fixed time if one is set, else the wall clock. Always aware UTC.
    """
    if _fixed is not None:
        return _fixed
    return datetime.datetime.now(datetime.timezone.utc)


def fixed() -> typing.Optional[datetime.datetime]:
    """
    The fixed time, or None when the clock is running on the wall clock.
    """
    return _fixed


def set_fixed(at: typing.Optional[datetime.datetime]) -> None:
    """
    Fix the clock at `at`, or release it back to the wall clock when `at` is
    None.
    """
    global _fixed
    _fixed = at


def parse_time(value: str) -> datetime.datetime:
    """
    Parse an ISO 8601 timestamp -- the format `Config.training_end` and
    `Config.test_end` carry -- into an aware UTC datetime.

    Accepts a trailing "Z" (translated to "+00:00" before parsing, since older
    Python versions of `fromisoformat` reject it even though this library
    targets one that does not), an explicit offset, or a naive value, which is
    treated as already being UTC. Raises ValueError naming the offending value
    on anything else.
    """
    text = value
    try:
        if isinstance(text, str) and text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.datetime.fromisoformat(text)
    except (ValueError, TypeError) as ex:
        raise ValueError(f"not a valid ISO 8601 timestamp: {value!r}") from ex
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)

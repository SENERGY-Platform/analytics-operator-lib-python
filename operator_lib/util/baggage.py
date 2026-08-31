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

__all__ = ("parse_baggage",)

from urllib.parse import unquote


def parse_baggage(header) -> dict:
    """
    Parse the OpenTelemetry baggage the flow engine hands over in BAGGAGE.

    The value is a W3C baggage header rather than JSON, because it is what the
    propagator on the other side produces and what an operator would have to send
    on to carry the context into a request of its own.

        baggage-string = list-member *( OWS "," OWS list-member )
        list-member    = key OWS "=" OWS value *( OWS ";" OWS property )

    The properties a member may carry are dropped: nothing in the platform sets
    them, and they are metadata about an entry rather than context to log.

    A malformed entry is skipped rather than raised on. The baggage exists so that
    logs can be correlated; an operator that refused to start over an unparseable
    annotation would trade a diagnostic aid for an outage.
    """
    if not header:
        return {}
    entries = dict()
    for member in header.split(","):
        member = member.split(";", 1)[0].strip()
        if not member or "=" not in member:
            continue
        key, value = member.split("=", 1)
        key = key.strip()
        if not key:
            continue
        entries[key] = unquote(value.strip())
    return entries

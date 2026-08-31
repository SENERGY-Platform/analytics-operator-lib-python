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

import unittest

from operator_lib.util.baggage import parse_baggage

# The exact headers the flow engine produces, taken from its own
# pkg/baggage.Header for these inputs. Pinned rather than described, because the
# environment variable is a contract between two languages and a drift on either
# side is otherwise only visible in production.
FLOW_ENGINE_HEADERS = [
    ("pipeline_id=3c1f9b42,smart_service_instance_id=8fbd0e8a,user_id=jonah",
     {"pipeline_id": "3c1f9b42", "smart_service_instance_id": "8fbd0e8a", "user_id": "jonah"}),
    ("username=jonah@bitnify.net",
     {"username": "jonah@bitnify.net"}),
    ("comma=a%2Cb,equals=a=b,semicolon=a%3Bb,space=a%20b",
     {"comma": "a,b", "equals": "a=b", "semicolon": "a;b", "space": "a b"}),
    ("backslash=a%5Cb,percent=50%25,quote=a%22b,umlaut=gr%C3%BCn",
     {"backslash": "a\\b", "percent": "50%", "quote": 'a"b', "umlaut": "grün"}),
    ("empty=",
     {"empty": ""}),
]


class TestParseBaggage(unittest.TestCase):
    def test_flow_engine_headers(self):
        for header, expected in FLOW_ENGINE_HEADERS:
            with self.subTest(header=header):
                self.assertEqual(expected, parse_baggage(header))

    def test_no_baggage(self):
        # A pipeline started by a caller that sent no context, or an operator run
        # outside the platform: neither is an error.
        self.assertEqual({}, parse_baggage(None))
        self.assertEqual({}, parse_baggage(""))

    def test_properties_are_dropped(self):
        # A member may carry properties after a semicolon. Nothing in the platform
        # sets them, and they describe the entry rather than being context to log.
        self.assertEqual({"key": "value"}, parse_baggage("key=value;prop=1;other"))

    def test_optional_whitespace_is_tolerated(self):
        # The specification allows whitespace around the list delimiter.
        self.assertEqual({"a": "1", "b": "2"}, parse_baggage("a=1 ,  b=2"))

    def test_malformed_entries_are_skipped(self):
        # Skipped rather than raised on: the baggage is a diagnostic aid, and an
        # operator that refused to start over it would trade one for an outage.
        self.assertEqual({"good": "1"}, parse_baggage("good=1,nonsense,=novalue,"))

    def test_last_entry_wins_on_a_duplicate_key(self):
        self.assertEqual({"a": "2"}, parse_baggage("a=1,a=2"))


if __name__ == "__main__":
    unittest.main()

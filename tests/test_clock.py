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

import datetime
import unittest

from operator_lib.util import clock


class TestClock(unittest.TestCase):
    def setUp(self):
        self.addCleanup(clock.set_fixed, None)

    def test_now_is_the_wall_clock_and_aware_utc_without_a_fixed_time(self):
        self.assertIsNone(clock.fixed())
        before = datetime.datetime.now(datetime.timezone.utc)
        now = clock.now()
        after = datetime.datetime.now(datetime.timezone.utc)
        self.assertIsNotNone(now.tzinfo)
        self.assertEqual(datetime.timezone.utc, now.tzinfo)
        self.assertLessEqual(before, now)
        self.assertLessEqual(now, after)

    def test_set_fixed_makes_now_return_it(self):
        at = datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc)
        clock.set_fixed(at)
        self.assertEqual(at, clock.now())
        self.assertEqual(at, clock.now())  # repeated call does not advance it

    def test_fixed_mirrors_set_fixed(self):
        self.assertIsNone(clock.fixed())
        at = datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc)
        clock.set_fixed(at)
        self.assertEqual(at, clock.fixed())
        clock.set_fixed(None)
        self.assertIsNone(clock.fixed())

    def test_parse_time_accepts_a_trailing_z(self):
        self.assertEqual(
            datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc),
            clock.parse_time("2026-06-01T00:00:00Z"),
        )

    def test_parse_time_accepts_an_explicit_utc_offset(self):
        self.assertEqual(
            datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc),
            clock.parse_time("2026-06-01T00:00:00+00:00"),
        )

    def test_parse_time_normalises_a_non_utc_offset(self):
        self.assertEqual(
            datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc),
            clock.parse_time("2026-06-01T02:00:00+02:00"),
        )

    def test_parse_time_keeps_fractional_seconds(self):
        parsed = clock.parse_time("2026-06-01T00:00:00.123456Z")
        self.assertEqual(123456, parsed.microsecond)
        self.assertEqual(datetime.timezone.utc, parsed.tzinfo)

    def test_parse_time_treats_a_naive_value_as_utc(self):
        self.assertEqual(
            datetime.datetime(2026, 6, 1, tzinfo=datetime.timezone.utc),
            clock.parse_time("2026-06-01T00:00:00"),
        )

    def test_parse_time_rejects_garbage(self):
        with self.assertRaises(ValueError) as ctx:
            clock.parse_time("not a timestamp")
        self.assertIn("not a timestamp", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

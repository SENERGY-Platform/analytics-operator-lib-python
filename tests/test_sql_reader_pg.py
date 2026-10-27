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

# The SQL of the SQL reader against a real PostgreSQL. Skipped without
# TEST_PG_DSN, e.g.
#   docker run -d --rm --name pg5036 -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:16-alpine
#   TEST_PG_DSN=postgresql://postgres:test@localhost:55432/postgres python -m unittest tests.test_sql_reader_pg
# The tables are created for the test and dropped after it.

import datetime
import os
import unittest
from datetime import timedelta

import pandas as pd
import psycopg2
import ray

from operator_lib.util.helpers import timescale
from operator_lib.util.model import InputTopic

PG_DSN = os.environ.get("TEST_PG_DSN")

E = datetime.datetime(2026, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
DURATION = timedelta(hours=6)
START = E - DURATION
SERVICE = "urn_infai_ses_service_1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"
DEVICES = {name: f"{i:08x}-0000-4000-8000-000000000000" for i, name in enumerate(["plain", "reversed", "nulls", "short"], start=1)}

_quote = getattr(timescale, "__quote_identifier")
_table_name = getattr(timescale, "__get_table_name")


def _topic(device):
    return InputTopic({
        "name": SERVICE,
        "filterType": "DeviceId",
        "filterValue": device,
        "mappings": [{"dest": "temp", "source": "value.temp"}, {"dest": "mode", "source": "value.mode"}],
    })


# (minute after START, temp, mode). Comments say what changes_only does with the row.
ROWS = [
    (-30, 99, "x"),   # before the window: never read
    (0, 1, "x"),      # first row of the window: stays
    (10, 1, "x"),     # periodic repeat: goes
    (20, 1, "x"),     # last of its run: stays
    (30, 2, "x"),     # first of the next run: stays
    (40, 2, "x"),     # goes
    (50, 2, "x"),     # stays, the next row differs in mode only
    (60, 2, "y"),     # stays
    (70, 2, "y"),     # goes
    (80, 2, "y"),     # stays, the burst follows
    (90, 3, "y"), (90, 4, "y"), (90, 3, "y"),  # same timestamp, contradicting values: all runs stay
    (100, 4, "y"),    # goes
    (110, 4, "y"),    # last row of the window: stays
    (360, 77, "z"),   # at the end bound: the window is half-open, never read
]
EXPECTED_CHANGES_ONLY = [
    (0, 1, "x"), (20, 1, "x"), (30, 2, "x"), (50, 2, "x"), (60, 2, "y"), (80, 2, "y"),
    (90, 3, "y"), (90, 3, "y"), (90, 4, "y"), (110, 4, "y"),
]


@unittest.skipUnless(PG_DSN, "TEST_PG_DSN is not set")
class TestSqlReaderAgainstPostgres(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=1, include_dashboard=False, ignore_reinit_error=True)
        cls.tables = {d: _quote(_table_name(DEVICES[d], SERVICE.replace("_", ":"))) for d in DEVICES}
        cls.conn = psycopg2.connect(PG_DSN)
        cls.conn.autocommit = True
        cur = cls.conn.cursor()
        for device, table in cls.tables.items():
            cur.execute(f'DROP TABLE IF EXISTS {table}')
            cur.execute(f'CREATE TABLE {table} (time timestamptz NOT NULL, "temp" integer, "mode" text)')
        cls._insert("plain", ROWS)
        # The same rows, written in the opposite physical order: the answer must not depend on it.
        cls._insert("reversed", list(reversed(ROWS)))
        cls._insert("nulls", [(0, None, "x"), (10, None, "x"), (20, None, "x")])
        cls._insert("short", [(60, 5, "x")])

    @classmethod
    def _insert(cls, device, rows):
        cur = cls.conn.cursor()
        for minute, temp, mode in rows:
            cur.execute(f'INSERT INTO {cls.tables[device]} (time, "temp", "mode") VALUES (%s, %s, %s)',
                        (START + timedelta(minutes=minute), temp, mode))

    @classmethod
    def tearDownClass(cls):
        cur = cls.conn.cursor()
        for table in cls.tables.values():
            cur.execute(f'DROP TABLE IF EXISTS {table}')
        cls.conn.close()
        ray.shutdown()

    def _read(self, device, changes_only):
        job = [[(None, _topic(DEVICES[device]))]]
        (ds,) = timescale.get_timescale_sql_datasets_local(PG_DSN, job, DURATION, E, changes_only)
        frame = ds.to_pandas()
        start = START.replace(tzinfo=None)
        return [(int((t - start).total_seconds() // 60), None if pd.isna(temp) else int(temp), mode)
                for t, temp, mode in zip(frame["time"], frame["temp"], frame["mode"])]

    def test_without_changes_only_every_row_of_the_window_comes_back(self):
        rows = self._read("plain", False)
        self.assertEqual(sorted(r for r in ROWS if 0 <= r[0] < 360 - 1 and r[1] not in (99, 77)), sorted(rows))
        self.assertEqual(14, len(rows))
        self.assertNotIn(99, [r[1] for r in rows])
        self.assertNotIn(77, [r[1] for r in rows])

    def test_periodic_repeats_go_and_the_edges_of_every_run_stay(self):
        self.assertEqual(EXPECTED_CHANGES_ONLY, self._read("plain", True))

    def test_a_burst_with_contradicting_values_stays_complete(self):
        burst = [r for r in self._read("plain", True) if r[0] == 90]
        self.assertEqual({3, 4}, {r[1] for r in burst})
        self.assertEqual(3, len(burst))

    def test_the_physical_order_of_the_rows_does_not_matter(self):
        self.assertEqual(self._read("plain", True), self._read("reversed", True))
        # Without changes_only the query keeps the old ORDER BY time: rows sharing a timestamp come in any order.
        self.assertEqual(sorted(self._read("plain", False)), sorted(self._read("reversed", False)))

    def test_the_first_and_the_last_row_stay_even_when_the_value_is_null(self):
        self.assertEqual([(0, None, "x"), (20, None, "x")], self._read("nulls", True))

    def test_a_single_row_stays(self):
        self.assertEqual([(60, 5, "x")], self._read("short", True))

    def test_the_window_clause_applies_to_the_neighbours_too(self):
        # The row at -30 differs from the one at 0 but is outside the window: it must
        # not make the first row look like a change, nor be returned.
        rows = self._read("plain", True)
        self.assertEqual((0, 1, "x"), rows[0])
        self.assertTrue(all(r[0] >= 0 for r in rows))

    def test_several_devices_are_tagged(self):
        job = [[(DEVICES["plain"], _topic(DEVICES["plain"])), (DEVICES["short"], _topic(DEVICES["short"]))]]
        (ds,) = timescale.get_timescale_sql_datasets_local(PG_DSN, job, DURATION, E, True)
        frame = ds.to_pandas()
        self.assertEqual({DEVICES["plain"]: 10, DEVICES["short"]: 1}, frame["device_id"].value_counts().to_dict())


if __name__ == "__main__":
    unittest.main()

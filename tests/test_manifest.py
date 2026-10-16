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

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest

from operator_lib import manifest

DECLARED = '''
    from operator_lib.util import Config, Inputs, Outputs, OperatorBase

    class In(Inputs):
        window_open: bool
        temperature: float

    class Out(Outputs):
        confidence: int
        reason: str

    class CustomConfig(Config):
        history_days: float = 60
        contact_sensor: bool = True

    class Operator(OperatorBase):
        configType = CustomConfig
        inputs = In
        outputs = Out

    if __name__ == "__main__":
        raise SystemExit("entry point must not run")
'''

EXPECTED = {
    "inputs": [{"name": "window_open", "type": "bool"}, {"name": "temperature", "type": "float"}],
    "outputs": [{"name": "confidence", "type": "int"}, {"name": "reason", "type": "string"}],
    "config_values": [{"name": "history_days", "type": "float"}, {"name": "contact_sensor", "type": "bool"}],
}


class TestManifest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name

    def run_main(self, source, *args):
        path = os.path.join(self.dir, "main.py")
        with open(path, "w") as f:
            f.write(textwrap.dedent(source))
        out = os.path.join(self.dir, "operator-manifest.json")
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = manifest.main(["manifest", path, "-o", out, *args])
        return code, stdout.getvalue(), stderr.getvalue(), out

    def test_a_declared_operator_writes_the_exact_manifest(self):
        code, _, _, out = self.run_main(DECLARED)
        self.assertEqual(0, code)
        with open(out) as f:
            text = f.read()
        self.assertEqual(json.dumps(EXPECTED, indent=2) + "\n", text)

    def test_regenerating_gives_a_byte_identical_file(self):
        _, _, _, out = self.run_main(DECLARED)
        with open(out, "rb") as f:
            first = f.read()
        self.run_main(DECLARED)
        with open(out, "rb") as f:
            self.assertEqual(first, f.read())

    def test_an_operator_without_declarations_writes_nothing_and_exits_zero(self):
        code, stdout, _, out = self.run_main('''
            from operator_lib.util import OperatorBase

            class Operator(OperatorBase):
                pass
        ''')
        self.assertEqual(0, code)
        self.assertIn("nothing written", stdout)
        self.assertFalse(os.path.exists(out))

    def test_declaring_only_the_outputs_writes_nothing(self):
        code, _, _, out = self.run_main('''
            from operator_lib.util import Outputs, OperatorBase

            class Out(Outputs):
                a: int

            class Operator(OperatorBase):
                outputs = Out
        ''')
        self.assertEqual(0, code)
        self.assertFalse(os.path.exists(out))

    def test_an_untyped_config_field_is_an_error_naming_it(self):
        code, _, stderr, out = self.run_main(DECLARED.replace("history_days: float = 60", "history_days = 60"))
        self.assertEqual(1, code)
        self.assertIn("history_days", stderr)
        self.assertFalse(os.path.exists(out))

    def test_two_operators_without_the_option_are_an_error_listing_them(self):
        two = DECLARED + '''
    class Other(Operator):
        pass
'''
        code, _, stderr, out = self.run_main(two)
        self.assertEqual(1, code)
        self.assertIn("Operator", stderr)
        self.assertIn("Other", stderr)
        self.assertFalse(os.path.exists(out))

    def test_the_option_picks_one_of_two_operators(self):
        two = DECLARED + '''
    class Other(Operator):
        pass
'''
        code, _, _, out = self.run_main(two, "--operator", "Other")
        self.assertEqual(0, code)
        self.assertTrue(os.path.exists(out))

    def test_an_unknown_operator_name_is_an_error(self):
        code, _, stderr, _ = self.run_main(DECLARED, "--operator", "Missing")
        self.assertEqual(1, code)
        self.assertIn("Missing", stderr)

    def test_the_module_entry_point_writes_the_manifest(self):
        path = os.path.join(self.dir, "main.py")
        with open(path, "w") as f:
            f.write(textwrap.dedent(DECLARED))
        out = os.path.join(self.dir, "operator-manifest.json")
        done = subprocess.run(
            [sys.executable, "-m", "operator_lib", "manifest", path, "-o", out],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=True, text=True,
        )
        self.assertEqual(0, done.returncode, done.stderr)
        with open(out) as f:
            self.assertEqual(EXPECTED, json.load(f))


if __name__ == "__main__":
    unittest.main()

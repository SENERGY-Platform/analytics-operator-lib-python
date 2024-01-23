"""
   Copyright 2024 InfAI (CC SES)

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
import util  # Dont do this in your operator!
util.setup() # Dont do this in your operator!


# define your operators config
from operator_lib.util import Config
class CustomConfig(Config):
    myconfig = "default"


# define your operator
import typing
from operator_lib.util import OperatorBase, Selector
class Operator(OperatorBase):
    # define your custom config type
    configType = CustomConfig

    # define your operators inputs and selectors
    selectors = [
        Selector({"name": "selector1", "args": ["value"]})
    ]

    # do any setup in this method and always call super!
    # your config is already populated here
    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        print(f"got configured with {self.config.myconfig}")

    # you need to implement this method
    # based on the selector you may call other methods
    def run(self, data: typing.Dict[str, typing.Any], selector: str):
        print(f"{selector} {data}")
        if data['value'] == 5:
            print('reached the end of sample data, everything seems to be working fine. Stop the operator with CTRL+C')

# launch your operator
from operator_lib.operator_lib import OperatorLib
if __name__ == "__main__":
    OperatorLib(Operator(), name="OperatorLibExample", git_info_file='example/git_commit')
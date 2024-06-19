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

import setuptools


def read_metadata(pkg_file):
    meta = dict()
    with open(pkg_file, 'r') as init_file:
        for line in init_file.readlines():
            if line.startswith('__'):
                line = line.replace("'", '')
                line = line.replace('\n', '')
                key, value = line.split(' = ')
                meta[key] = value
    return meta


metadata = read_metadata('operator_lib/__init__.py')


setuptools.setup(
    name=metadata.get('__title__'),
    version=metadata.get('__version__'),
    author=metadata.get('__author__'),
    description=metadata.get('__description__'),
    license=metadata.get('__license__'),
    url=metadata.get('__url__'),
    copyright=metadata.get('__copyright__'),
    install_requires=[
        'message-filter-lib @ git+https://github.com/SENERGY-Platform/message-filter-lib.git@0.8.2',
        'simple-env-var-manager @ git+https://github.com/y-du/simple-env-var-manager.git@2.3.0',
        'simple-struct @ git+https://github.com/y-du/simple-struct.git@0.2.0',
        'concurrency-watchdog @ git+https://github.com/SENERGY-Platform/concurrency-watchdog.git@0.1.1',
        'confluent_kafka<2',
        'kazoo<3',
        'prometheus-client<1',
        'model_trainer_client @ git+https://github.com/SENERGY-Platform/ml-trainer@v2.0.58#subdirectory=client',
        'mlflow==2.11.1'
    ],
    packages=setuptools.find_packages(),
    python_requires='>=3.5.3',
    classifiers=(
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'Operating System :: Unix',
        'Natural Language :: English',
    ),
)
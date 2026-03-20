# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from setuptools import setup, find_packages

setup(
    name="spark-history-cli",
    version="1.4.0",
    description="CLI for querying the Apache Spark History Server REST API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Apache Spark Contributors",
    url="https://github.com/yaooqinn/spark-history-cli",
    license="Apache-2.0",
    project_urls={
        "Source": "https://github.com/yaooqinn/spark-history-cli",
        "Issues": "https://github.com/yaooqinn/spark-history-cli/issues",
    },
    packages=find_packages(),
    package_data={
        "spark_history_cli": [
            "skills/*.md",
            "skills/spark-advisor/*.md",
            "skills/spark-advisor/references/*.md",
        ],
    },
    install_requires=[
        "click>=8.0.0",
        "requests>=2.28.0",
        "prompt-toolkit>=3.0.0",
    ],
    entry_points={
        "console_scripts": [
            "spark-history-cli=spark_history_cli.cli:main",
        ],
    },
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries",
    ],
)

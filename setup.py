from setuptools import setup, find_packages

setup(
    name="spark-history-cli",
    version="1.0.0",
    description="CLI for querying the Apache Spark History Server REST API",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Apache Spark Contributors",
    license="Apache-2.0",
    packages=find_packages(),
    package_data={
        "spark_history_cli": ["skills/*.md"],
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

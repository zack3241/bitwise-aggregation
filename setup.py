import os
from setuptools import setup

setup(
    name = "bitwiseaggregation",
    version = "1.0.0a1",
    author = "Zachary Lawson",
    author_email = "zmjlawson@gmail.com",
    description = ("A aggregation strategy for data_samples when aggregate datasets cannot be properly indexed."),
    license = "MIT",
    keywords = "spark aggregation",
    url = "https://github.com/zack3241/bitwise-aggregation",
    packages=['bitwiseaggregation'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Database",
        "License :: MIT License",
        'Programming Language :: Python :: 2.7',
    ],
)
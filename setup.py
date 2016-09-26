import os
from setuptools import setup

# Utility function to read the README file.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "bitwise-aggregation",
    version = "0.0.1",
    author = "Zachary Lawson",
    author_email = "zmjlawson@gmail.com",
    description = ("A aggregation strategy for data when aggregate datasets cannot be properly indexed."),
    license = "BSD",
    keywords = "spark aggregation",
    url = "http://packages.python.org/an_example_pypi_project",
    packages=['bitwise-aggregation', 'tests'],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Database",
        "License :: MIT License",
    ],
)
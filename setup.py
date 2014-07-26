#!/usr/bin/env python

from setuptools import setup


VERSION = (0, 0, 1, "")


setup(
    name="databacon",
    description="a graph storage API built on datahog",
    packages=["databacon"],
    version='.'.join(filter(None, map(str, VERSION))),
    install_requires=['datahog', 'mummy'],
)"")

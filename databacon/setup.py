#!/usr/bin/env python

from setuptools import setup


VERSION = (0, 0, 1, "")


setup(
    name="databacon",
    description="a graph storage API built on datahog",
    packages=["databacon"],
    version='.'.join([_f for _f in map(str, VERSION) if _f]),
    install_requires=['datahog', 'mummy'],
)

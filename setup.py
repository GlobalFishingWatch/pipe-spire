#!/usr/bin/env python
from setuptools import find_packages
from setuptools import setup

setup(
    name='pipe-spire',
    version='1.0.0',
    packages=find_packages(exclude=['test*.*', 'tests'])
)


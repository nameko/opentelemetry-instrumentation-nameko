#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name='nameko-opentelemetry',
    version='1.0.0',
    description='Nameko extension producing opentelemetry data',
    author='Nameko Authors',
    url='https://github.com/nameko/nameko-opentelemetry',
    packages=find_packages(exclude=['test', 'test.*']),
    install_requires=[
        "nameko>=2.8.5",
    ],
    extras_require={
        'dev': [
            "coverage",
            "pytest",
        ]
    },
    dependency_links=[],
    zip_safe=True,
    license='Apache License, Version 2.0'
)

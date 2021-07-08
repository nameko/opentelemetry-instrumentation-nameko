#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import find_packages, setup


setup(
    name="opentelemetry-instrumentation-nameko",
    description="Nameko extension producing opentelemetry data",
    author="Nameko Authors",
    url="https://github.com/nameko/opentelemetry-instrumentation-nameko",
    packages=find_packages(exclude=["test", "test.*"]),
    install_requires=[
        "nameko==3.0.0rc9",
        "opentelemetry-api",
        "opentelemetry-instrumentation",
        "opentelemetry-instrumentation-wsgi",
    ],
    extras_require={
        "dev": [
            "coverage",
            "pytest",
            "opentelemetry-sdk",
            "opentelemetry-instrumentation-requests",
        ]
    },
    dependency_links=[],
    zip_safe=True,
    license="Apache License, Version 2.0",
)

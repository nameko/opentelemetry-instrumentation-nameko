#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from setuptools import find_packages, setup


BASE_DIR = os.path.dirname(__file__)
PACKAGE_INFO = {}

VERSION_FILENAME = os.path.join(BASE_DIR, "nameko_opentelemetry", "version.py")
with open(VERSION_FILENAME) as f:
    exec(f.read(), PACKAGE_INFO)

PACKAGE_FILENAME = os.path.join(BASE_DIR, "nameko_opentelemetry", "package.py")
with open(PACKAGE_FILENAME) as f:
    exec(f.read(), PACKAGE_INFO)


setup(
    name="opentelemetry-instrumentation-nameko",
    description="Nameko extension producing opentelemetry data",
    python_requires=">=3.7",
    version=PACKAGE_INFO["__version__"],
    author="Nameko Authors",
    url="https://github.com/nameko/opentelemetry-instrumentation-nameko",
    packages=find_packages(exclude=["test", "test.*"]),
    install_requires=[
        "nameko>=3.0.0rc9",
        "opentelemetry-api",
        "opentelemetry-instrumentation",
        "opentelemetry-instrumentation-wsgi",
        "importlib-metadata<=4.13.0",  # Temporary pin,
        # remove when https://github.com/celery/kombu/pull/1601
        # is in the most recent version of kombu
    ],
    extras_require={
        "dev": list(PACKAGE_INFO["_instruments"])
        + [
            "coverage",
            "pytest",
            "opentelemetry-sdk",
            "opentelemetry-instrumentation-requests",
        ],
        "instruments": PACKAGE_INFO["_instruments"],
    },
    dependency_links=[],
    zip_safe=True,
    license="Apache License, Version 2.0",
)

#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import find_packages, setup


setup(
    name="nameko-opentelemetry",
    version="1.0.0",
    description="Nameko extension producing opentelemetry data",
    author="Nameko Authors",
    url="https://github.com/nameko/nameko-opentelemetry",
    packages=find_packages(exclude=["test", "test.*"]),
    install_requires=["nameko>=3", "opentelemetry-api"],
    extras_require={"dev": ["coverage", "pytest", "opentelemetry-sdk"]},
    dependency_links=[],
    zip_safe=True,
    license="Apache License, Version 2.0",
)

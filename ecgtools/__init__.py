#!/usr/bin/env python
# flake8: noqa
"""Top-level module for ecgtools ."""
from importlib.metadata import PackageNotFoundError, version

from .builder import Builder, RootDirectory, glob_to_regex

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    # package is not installed
    __version__ = '0.0.0'

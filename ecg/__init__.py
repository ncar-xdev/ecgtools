#!/usr/bin/env python
# flake8: noqa
"""Top-level module for ecg ."""
from pkg_resources import DistributionNotFound, get_distribution

from .core import Builder, parse_files_attributes

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    _version__ = '0.0.0'

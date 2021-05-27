#!/usr/bin/env python
# flake8: noqa
"""Top-level module for ecgtools ."""
from pkg_resources import DistributionNotFound, get_distribution

from .builder import Builder

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    __version__ = '0.0.0'

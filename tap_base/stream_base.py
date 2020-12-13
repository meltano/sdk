"""Shared parent class for TapBase, TargetBase, and TransformBase."""

import abc
import logging
import sys

from typing import Any, Optional

import backoff

from tap_base.exceptions import TapStreamConnectionFailure
from tap_base.helpers import classproperty


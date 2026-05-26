"""Detector implementations.

Each detector lives in its own module and is registered via the
``promanomaly.detectors`` entry-point group in ``pyproject.toml``.
"""

from .base import Detector, DetectorMeta, ParamSpec
from .registry import (
    DetectorRegistrationError,
    UnknownDetectorError,
    available,
    get,
    list_meta,
    meta,
)

__all__ = [
    "Detector",
    "DetectorMeta",
    "DetectorRegistrationError",
    "ParamSpec",
    "UnknownDetectorError",
    "available",
    "get",
    "list_meta",
    "meta",
]

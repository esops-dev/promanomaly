"""promanomaly-prophet — Prophet-residual anomaly detector.

Registered via the ``promanomaly.detectors`` entry-point group in
``pyproject.toml`` so the host promanomaly detector picks it up the
same way it picks up built-in detectors.
"""

from .detector import ProphetResidual

__all__ = ["ProphetResidual"]

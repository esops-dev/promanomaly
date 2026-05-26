"""Detector Protocol and shared dataclasses.

Each detector is a stateless callable that takes a rolling window of recent
samples for one series and returns a single-row DataFrame with the score
and baseline for the latest point. Stateless-across-restarts is a
project-wide invariant — detectors must rebuild any temporaries they need
from the input window on every call.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import pandas as pd

# Column contract for the DataFrame returned by Detector.fit_score().
SCORE_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "score",
    "baseline",
    "is_outside",
)
OPTIONAL_SCORE_COLUMNS: tuple[str, ...] = (
    "baseline_lower",
    "baseline_upper",
    "change_point",
)


@dataclass(frozen=True)
class DetectorMeta:
    """Static metadata about a detector implementation.

    Mirrors the protocol fields so the registry can introspect a
    detector class without instantiating it. Used by ``detectors list``
    and (once exposed) the ``/debug/detectors`` HTTP endpoint.
    """

    name: str
    description: str
    defaults: dict[str, Any]


@runtime_checkable
class Detector(Protocol):
    """Detector protocol.

    Implementations must be cheap to instantiate — the runner builds a new
    instance per series per run. Heavy detectors (BOCPD etc.) should
    nevertheless avoid persistent state since restarts wipe everything by
    design.
    """

    name: str
    description: str
    defaults: dict[str, Any]

    def fit_score(
        self,
        df: pd.DataFrame,
        freq: str,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        """Score the latest point in ``df`` against a baseline computed on the
        rolling window.

        Parameters
        ----------
        df:
            Rolling window of recent samples with columns ``timestamp`` and
            ``y``, sorted by ``timestamp`` ascending. Caller guarantees a
            non-empty frame.
        freq:
            Best-effort sample frequency as a pandas freq string (e.g.
            ``"15s"``). May be ``""`` if the runner could not infer it.
        params:
            Per-detector parameters merged from defaults and the YAML
            config. Detectors should pull only keys they understand and
            ignore unknown ones (forward-compat).

        Returns
        -------
        pd.DataFrame
            A single-row frame containing at minimum the columns in
            :data:`SCORE_COLUMNS`. Detectors that compute a confidence
            band may additionally populate
            :data:`OPTIONAL_SCORE_COLUMNS`.
        """
        ...


__all__ = [
    "OPTIONAL_SCORE_COLUMNS",
    "SCORE_COLUMNS",
    "Detector",
    "DetectorMeta",
]

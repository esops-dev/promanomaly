"""Detector Protocol and shared dataclasses.

Each detector is a stateless callable that takes a rolling window of recent
samples for one series and returns a single-row DataFrame with the score
and baseline for the latest point. Stateless-across-restarts is a
project-wide invariant — detectors must rebuild any temporaries they need
from the input window on every call.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
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
class ParamSpec:
    """One parameter of a detector, surfaced by ``/debug/detectors``.

    Detectors that accept tuning parameters declare them as a tuple of
    ``ParamSpec`` on the class so operators discovering a custom-built
    image can see types, defaults, and one-line docs without reading
    source. ``type_`` is a short string (``int``, ``float``,
    ``duration``, ``str``, ...) rather than a Python type object so the
    JSON serialisation is stable.
    """

    name: str
    type_: str
    default: Any
    doc: str

    def as_dict(self) -> dict[str, Any]:
        # ``type_`` -> ``type`` in the wire format. Avoids the
        # awkward trailing underscore while keeping ``type`` available
        # as a Python attribute without shadowing the builtin.
        out = asdict(self)
        out["type"] = out.pop("type_")
        return out


@dataclass(frozen=True)
class DetectorMeta:
    """Static metadata about a detector implementation.

    Mirrors the protocol fields so the registry can introspect a
    detector class without instantiating it. Used by ``detectors list``
    and the ``/debug/detectors`` HTTP endpoint.

    ``defaults`` stays as a plain ``{name: default}`` dict for
    backwards compatibility with operators who scripted against the
    earlier output; ``params`` is the canonical, richer shape (per-param
    type and doc).
    """

    name: str
    description: str
    defaults: dict[str, Any]
    params: tuple[ParamSpec, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "defaults": dict(self.defaults),
            "params": [p.as_dict() for p in self.params],
        }


@runtime_checkable
class Detector(Protocol):
    """Detector protocol.

    Implementations must be cheap to instantiate — the runner builds a new
    instance per series per run. Heavy detectors (BOCPD etc.) should
    nevertheless avoid persistent state since restarts wipe everything by
    design.

    ``param_spec`` is optional but strongly recommended: declared
    parameters surface in ``/debug/detectors`` and ``detectors list``,
    which are the primary discoverability surface for custom-built
    images. Detectors with no tuning knobs may leave it empty (the
    default ``()``).
    """

    name: str
    description: str
    defaults: dict[str, Any]
    # Default to empty tuple so existing detectors that don't declare
    # parameters keep working without modification.
    param_spec: tuple[ParamSpec, ...] = ()

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
    "ParamSpec",
]

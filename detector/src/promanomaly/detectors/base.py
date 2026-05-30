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
    "anomaly_type",
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
    # Optional numeric bounds. When set, ParamSpec.validate rejects
    # values outside ``[minimum, maximum]`` for ``int``/``float``
    # types and the parsed-seconds value for ``duration``. ``None`` on
    # either side leaves that side unbounded; many detectors only need
    # one side (e.g. ``min_cohort_size`` wants ``>= 1`` with no upper
    # bound). Strings and bools are unbounded — the type check is the
    # only validation that makes sense for them.
    minimum: float | None = None
    maximum: float | None = None

    def as_dict(self) -> dict[str, Any]:
        # ``type_`` -> ``type`` in the wire format. Avoids the
        # awkward trailing underscore while keeping ``type`` available
        # as a Python attribute without shadowing the builtin.
        out = asdict(self)
        out["type"] = out.pop("type_")
        # Drop the bounds keys when both are ``None`` so existing
        # consumers of ``/debug/detectors`` don't see a schema diff
        # for unchanged detectors.
        if out.get("minimum") is None and out.get("maximum") is None:
            out.pop("minimum", None)
            out.pop("maximum", None)
        return out

    def validate(self, value: Any) -> None:
        """Raise ``ValueError`` if ``value`` is not a usable instance of ``type_``.

        Used at config-load to catch typos like ``Hampel.t0: "three"``,
        ``MAD.window: ["1h"]``, or out-of-range ``min_cohort_size: 0``
        before they explode at scoring time. Plain coercion (e.g. int
        -> float) is permitted because pyyaml / json liberally round-
        trip numeric scalars.
        """
        if value is None:
            # Forward-compat: a detector may treat None as "use default"; we
            # don't second-guess that here.
            return
        if self.type_ == "bool":
            if not isinstance(value, bool):
                raise ValueError(f"param {self.name!r}: expected bool, got {type(value).__name__}")
        elif self.type_ == "int":
            # ``bool`` is a subclass of int in Python; reject it explicitly so
            # ``param: true`` doesn't sneak in as ``1``.
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"param {self.name!r}: expected int, got {type(value).__name__}")
            self._check_bounds(float(value))
        elif self.type_ == "float":
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise ValueError(
                    f"param {self.name!r}: expected number, got {type(value).__name__}"
                )
            self._check_bounds(float(value))
        elif self.type_ == "duration":
            # Numbers are accepted as second-counts (operator convenience);
            # strings are parsed eagerly so an invalid duration is caught here
            # rather than at scoring time.
            if isinstance(value, bool):
                raise ValueError(f"param {self.name!r}: expected duration string, got bool")
            if isinstance(value, int | float):
                self._check_bounds(float(value))
                return
            if not isinstance(value, str):
                raise ValueError(
                    f"param {self.name!r}: expected duration string, got {type(value).__name__}"
                )
            # Local import: parse_duration lives in config.py which imports
            # ParamSpec, so a top-level import would be circular.
            from ..config import parse_duration

            try:
                parsed_seconds = parse_duration(value)
            except ValueError as exc:
                raise ValueError(f"param {self.name!r}: {exc}") from exc
            self._check_bounds(parsed_seconds)
        elif self.type_ == "str" and not isinstance(value, str):
            raise ValueError(f"param {self.name!r}: expected str, got {type(value).__name__}")
        # Unknown ``type_`` strings are permissive on purpose so custom
        # detectors can introduce new kinds without forcing this validator
        # to grow.

    def _check_bounds(self, numeric: float) -> None:
        if self.minimum is not None and numeric < self.minimum:
            raise ValueError(
                f"param {self.name!r}: value {numeric} is below minimum {self.minimum}"
            )
        if self.maximum is not None and numeric > self.maximum:
            raise ValueError(
                f"param {self.name!r}: value {numeric} is above maximum {self.maximum}"
            )


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

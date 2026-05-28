"""Detector plan construction shared by runner and backtest.

A ``DetectorPlan`` captures everything needed to invoke one detector
against one series for one run: the class, the resolved (defaults +
user-overrides + group ``window``/``step``) parameters, and the
instance tag for output-metric labels.

Centralised here so the live runner and the backtest harness build
plans identically. A drift here would silently shift backtest scoring
away from production behaviour.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any

from .config import DefaultsConfig, QueryConfig
from .detectors import Detector, get


@dataclass
class DetectorPlan:
    """Resolved invocation plan for one detector under one query.

    The runner uses this internally as ``_DetectorPlan`` (historical
    alias kept for in-module references); backtest consumes a list of
    plans the same way the runner does.
    """

    name: str
    instance: str | None
    cls: type[Detector]
    params: dict[str, Any]


def merge_params(
    cls: type[Detector],
    user_params: dict[str, Any] | None,
    defaults: DefaultsConfig,
) -> dict[str, Any]:
    """Resolve a detector's effective params for one invocation.

    Layering: class-level defaults < user overrides from config < the
    group-level ``window`` and ``step`` defaults (set only when the
    user has not already overridden them).
    """
    merged: dict[str, Any] = {}
    merged.update(getattr(cls, "defaults", {}) or {})
    merged.update(user_params or {})
    # Surface group-level defaults so detectors can read window/step
    # without re-plumbing them through the runner.
    merged.setdefault("window", defaults.window)
    merged.setdefault("step", defaults.step)
    return merged


def build_detector_plans(
    query: QueryConfig,
    defaults: DefaultsConfig,
) -> list[DetectorPlan]:
    """Resolve every detector entry in ``query`` into a :class:`DetectorPlan`.

    Raises :class:`UnknownDetectorError` if any ``entry.name`` cannot
    be resolved from the registry. The caller decides whether to
    surface that as a config validation error or a runtime failure —
    the runner aborts the query; the backtest aborts the whole command.
    """
    plans: list[DetectorPlan] = []
    for entry in query.detectors:
        cls = get(entry.name)
        params = merge_params(cls, entry.params, defaults)
        plans.append(
            DetectorPlan(
                name=entry.name,
                instance=entry.instance,
                cls=cls,
                params=params,
            )
        )
    return plans


def effective_threshold(params: dict[str, Any], fallback: float) -> float:
    """Resolve the alerting threshold for one detector run.

    A detector that handles its own threshold semantics (BOCPD's
    posterior probability, future change-point detectors with non-sigma
    scoring) sets ``threshold`` in its ``defaults`` / ``param_spec``;
    the value flows through ``params`` and overrides the group-level
    ``alert_thresholds.score``. The live runner, the backtest, and
    ``/debug/inspect`` all resolve through this helper so they agree
    on the cutoff — diverging here was the bug that made BOCPD's
    auto-select F1 collapse to zero.
    """
    raw = params.get("threshold")
    if raw is None:
        return fallback
    try:
        return float(raw)
    except (TypeError, ValueError):
        return fallback


# Cache of ``cls -> (accepts_var_kwargs, frozenset[parameter_names])``.
# ``inspect.signature`` walks the function object on every call and is
# the dominant per-series CPU cost at 10k+ series x N detectors per
# refresh. The detector class identity is the right cache key — params
# differ across invocations but the constructor's accepted-parameter
# set is fixed per class.
_SIGNATURE_CACHE: dict[type, tuple[bool, frozenset[str]] | None] = {}


def _signature_for(cls: type) -> tuple[bool, frozenset[str]] | None:
    cached = _SIGNATURE_CACHE.get(cls)
    if cached is not None or cls in _SIGNATURE_CACHE:
        return cached
    try:
        sig = inspect.signature(cls)
    except (TypeError, ValueError):
        _SIGNATURE_CACHE[cls] = None
        return None
    accepts_var = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
    names = frozenset(sig.parameters)
    resolved = (accepts_var, names)
    _SIGNATURE_CACHE[cls] = resolved
    return resolved


def build_detector(cls: type[Detector], params: dict[str, Any]) -> Detector:
    """Instantiate ``cls`` and forward any ``params`` the constructor accepts.

    Detectors that take no constructor arguments (the historical
    contract: read params off the ``params`` dict in ``fit_score``)
    continue to work because we only filter for names ``__init__``
    actually accepts. Detectors that declare matching keyword args
    receive them as kwargs — the v0.2 "params forwarded as kwargs to
    the detector's constructor" surface. The full params dict is also
    handed to ``fit_score`` so both styles compose freely.
    """
    sig = _signature_for(cls)
    if sig is None:
        return cls()
    accepts_var, names = sig
    if accepts_var:
        return cls(**params)
    matched = {k: v for k, v in params.items() if k in names}
    return cls(**matched) if matched else cls()


__all__ = [
    "DetectorPlan",
    "build_detector",
    "build_detector_plans",
    "effective_threshold",
    "merge_params",
]

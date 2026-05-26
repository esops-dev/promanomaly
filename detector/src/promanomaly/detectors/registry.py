"""Detector registry.

Built-ins and third-party detectors are both discovered through the
``promanomaly.detectors`` entry point group, so the registration path is
identical for everyone. The registry is lazy and cached: first call to
:func:`available` triggers discovery; later calls return the same dict.
"""

from __future__ import annotations

from importlib import metadata

from .base import Detector, DetectorMeta


class DetectorRegistrationError(RuntimeError):
    """Two packages claim the same detector name."""


class UnknownDetectorError(KeyError):
    """Config references a detector name we can't resolve."""

    def __init__(self, name: str, available: list[str]) -> None:
        super().__init__(name)
        self.detector_name = name
        self.available = available

    def __str__(self) -> str:
        return f"unknown detector {self.detector_name!r}; available: {sorted(self.available)}"


_cache: dict[str, type[Detector]] | None = None


def _discover() -> dict[str, type[Detector]]:
    discovered: dict[str, type[Detector]] = {}
    sources: dict[str, str] = {}
    for ep in metadata.entry_points(group="promanomaly.detectors"):
        cls = ep.load()
        # Detector is a Protocol; runtime_checkable lets us validate.
        if not (hasattr(cls, "name") and hasattr(cls, "fit_score")):
            raise DetectorRegistrationError(
                f"entry point {ep.name!r} -> {ep.value!r} does not implement Detector"
            )
        name = getattr(cls, "name", ep.name)
        if name in discovered:
            raise DetectorRegistrationError(
                f"detector name collision on {name!r}: {sources[name]!r} vs {ep.value!r}"
            )
        discovered[name] = cls
        sources[name] = ep.value
    return discovered


def available(*, refresh: bool = False) -> dict[str, type[Detector]]:
    """Return the discovered detector classes keyed by name."""
    global _cache
    if _cache is None or refresh:
        _cache = _discover()
    return dict(_cache)


def get(name: str) -> type[Detector]:
    """Look up a detector class by name. Raises :class:`UnknownDetectorError`."""
    found = available()
    if name not in found:
        raise UnknownDetectorError(name, list(found))
    return found[name]


def meta(name: str) -> DetectorMeta:
    """Return registry metadata for ``name`` without instantiating it."""
    cls = get(name)
    return DetectorMeta(
        name=cls.name,
        description=getattr(cls, "description", ""),
        defaults=dict(getattr(cls, "defaults", {}) or {}),
    )


def list_meta() -> list[DetectorMeta]:
    """All detectors, sorted by name, for ``detectors list`` etc."""
    return [meta(name) for name in sorted(available())]


def register(cls: type[Detector]) -> None:
    """Register a detector at runtime (test helper).

    Production registration goes through entry points; this is exposed so
    tests don't need to round-trip through ``pyproject.toml`` to exercise
    detector wiring.
    """
    available()  # populate cache
    assert _cache is not None
    name = cls.name
    if name in _cache and _cache[name] is not cls:
        raise DetectorRegistrationError(
            f"detector {name!r} already registered to a different class"
        )
    _cache[name] = cls


def unregister(name: str) -> None:
    """Drop a detector from the registry (test helper)."""
    if _cache is not None and name in _cache:
        del _cache[name]


def reset() -> None:
    """Drop the cached registry (test helper). Next call rediscovers."""
    global _cache
    _cache = None


__all__: list[str] = [
    "DetectorRegistrationError",
    "UnknownDetectorError",
    "available",
    "get",
    "list_meta",
    "meta",
    "register",
    "reset",
    "unregister",
]

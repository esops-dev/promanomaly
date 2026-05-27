"""Drift guards on the registered detector set.

A detector ships two parallel descriptions of its tuning knobs: the
``defaults`` dict (consumed at runtime by the runner when merging
config-supplied ``params``) and the ``param_spec`` tuple (surfaced on
``/debug/detectors`` and the ``detectors list`` CLI). They MUST agree
on both names and default values — otherwise the operator-visible
documentation and the runtime behaviour diverge silently.

This file pins that contract for every registered detector. Adding a
new detector with mismatched ``defaults`` / ``param_spec`` will fail
here at PR time rather than at a surprised operator's first
``detectors list``.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
import pytest

from promanomaly.detectors import (
    DetectorRegistrationError,
    UnknownDetectorError,
    available,
    get,
    list_meta,
)
from promanomaly.detectors import registry as registry_module


def test_every_detector_defaults_match_param_spec_defaults() -> None:
    for meta in list_meta():
        spec_defaults = {p.name: p.default for p in meta.params}
        # Every key in ``defaults`` MUST appear in ``param_spec`` with
        # the same default. The reverse is also true — extra entries
        # in either side mean the surfaces have drifted.
        assert meta.defaults == spec_defaults, (
            f"detector {meta.name!r}: defaults vs param_spec drift "
            f"(defaults={meta.defaults!r}, param_spec={spec_defaults!r})"
        )


def test_every_detector_has_non_empty_description() -> None:
    """Description feeds /debug/detectors and the CLI — empty strings
    would make the surfaces useless for operators trying to identify
    what an unfamiliar entry does."""
    for meta in list_meta():
        assert meta.description.strip(), f"detector {meta.name!r} ships an empty description"


def test_every_param_has_non_empty_doc() -> None:
    """The whole point of ParamSpec.doc is operator discovery; an
    empty doc string makes the spec strictly less useful than the
    legacy defaults-only output."""
    for meta in list_meta():
        for param in meta.params:
            assert param.doc.strip(), (
                f"detector {meta.name!r} parameter {param.name!r} has empty doc"
            )
            assert param.type_.strip(), (
                f"detector {meta.name!r} parameter {param.name!r} has empty type"
            )


# ---------------------------------------------------------------------------
# Entry-point discovery (v0.4)
#
# Built-in detectors are themselves registered through entry points, but
# the path that matters operationally is third-party packages: anyone
# can drop a wheel into the runtime image, declare a
# ``promanomaly.detectors`` entry point, and have the detector show up
# without forking the core repo. These tests pin the discovery contract.
# ---------------------------------------------------------------------------


class _StubPlugin:
    """Minimal Detector implementation a hypothetical third-party wheel might ship."""

    name = "_StubPlugin"
    description = "stub detector used in tests of the entry-point discovery path"
    defaults: ClassVar[dict[str, Any]] = {}
    param_spec: ClassVar[tuple[Any, ...]] = ()

    def fit_score(self, df: pd.DataFrame, freq: str, params: dict[str, Any]) -> pd.DataFrame:
        del freq, params
        return pd.DataFrame(
            [
                {
                    "timestamp": float(df["timestamp"].iloc[-1]),
                    "score": 0.0,
                    "baseline": float(df["y"].iloc[-1]),
                    "is_outside": False,
                }
            ]
        )


def test_register_third_party_detector_via_registry_helper() -> None:
    """Equivalent to a wheel declaring an entry point for the registry to discover."""
    try:
        registry_module.register(_StubPlugin)
        assert get("_StubPlugin") is _StubPlugin
        assert "_StubPlugin" in available()
    finally:
        registry_module.unregister("_StubPlugin")


def test_duplicate_registration_raises_collision_error() -> None:
    """Two packages claiming the same detector name must fail loudly —
    a silent overwrite would let an unrelated wheel hijack a built-in
    detector with no operator-visible signal."""

    class _Other:
        name = "_StubPlugin"
        description = "a different implementation under the same name"
        defaults: ClassVar[dict[str, Any]] = {}
        param_spec: ClassVar[tuple[Any, ...]] = ()

        def fit_score(
            self, df: pd.DataFrame, freq: str, params: dict[str, Any]
        ) -> pd.DataFrame:  # pragma: no cover - never called
            raise NotImplementedError

    try:
        registry_module.register(_StubPlugin)
        with pytest.raises(DetectorRegistrationError):
            registry_module.register(_Other)
    finally:
        registry_module.unregister("_StubPlugin")


def test_unknown_detector_error_surfaces_available_list() -> None:
    """Per the v0.4 story the registry must surface the available names
    inside ``UnknownDetectorError`` so operators get a usable hint
    instead of bare ``KeyError`` traces."""
    with pytest.raises(UnknownDetectorError) as excinfo:
        get("NotADetectorAtAll")
    msg = str(excinfo.value)
    assert "NotADetectorAtAll" in msg
    assert "MAD" in msg  # available list reaches the message

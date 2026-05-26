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

from promanomaly.detectors import list_meta


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

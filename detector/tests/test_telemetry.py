"""Tests for the optional OpenTelemetry tracing module.

The detector ships with telemetry **off by default**. These tests pin
two contracts:

1. The disabled path is allocation-free — calling ``span`` returns a
   no-op context manager without importing the SDK.
2. The config -> telemetry construction is idempotent and falls back
   safely when ``telemetry.otlp.endpoint`` is left unset.
"""

from __future__ import annotations

from typing import Any

from promanomaly.config import OTLPConfig, TelemetryConfig
from promanomaly.telemetry import Telemetry


def test_disabled_telemetry_is_a_singleton() -> None:
    a = Telemetry.disabled()
    b = Telemetry.disabled()
    assert a is b
    assert not a.enabled


def test_disabled_span_is_a_noop() -> None:
    telemetry = Telemetry.disabled()
    with telemetry.span("promanomaly.group_run", group="g1") as span:
        # Disabled spans yield ``None`` so callers can branch on
        # ``if span is not None`` when they need to enrich attributes.
        assert span is None


def test_from_config_returns_disabled_when_endpoint_missing() -> None:
    cfg = TelemetryConfig()
    telemetry = Telemetry.from_config(cfg)
    assert telemetry is Telemetry.disabled()


def test_from_config_returns_disabled_when_otlp_endpoint_blank() -> None:
    cfg = TelemetryConfig(otlp=OTLPConfig(endpoint=None))
    telemetry = Telemetry.from_config(cfg)
    assert not telemetry.enabled


def test_span_records_attributes_when_enabled() -> None:
    # Build a Telemetry instance directly with a fake tracer to verify
    # the attribute-stamping path without spinning up the OTel SDK
    # (which would pull in protobuf and gRPC just to satisfy the test).
    recorded: list[tuple[str, dict[str, Any]]] = []

    class _FakeSpan:
        def __init__(self) -> None:
            self.attrs: dict[str, Any] = {}

        def set_attribute(self, key: str, value: Any) -> None:
            self.attrs[key] = value

        def __enter__(self) -> _FakeSpan:
            return self

        def __exit__(self, *_: object) -> None:
            recorded.append(("end", self.attrs))

    class _FakeTracer:
        def start_as_current_span(self, name: str) -> _FakeSpan:
            recorded.append((name, {}))
            return _FakeSpan()

    telemetry = Telemetry(tracer=_FakeTracer())
    assert telemetry.enabled
    with telemetry.span("promanomaly.detect", detector="MAD", window=900) as span:
        assert span is not None

    assert recorded[0] == ("promanomaly.detect", {})
    # Attributes are prefixed with promanomaly. (OTel convention) unless
    # the caller already provided a dotted key.
    final_attrs = recorded[-1][1]
    assert final_attrs == {
        "promanomaly.detector": "MAD",
        "promanomaly.window": 900,
    }


def test_shutdown_drains_provider_when_enabled() -> None:
    # BatchSpanProcessor queues spans on a background thread; without an
    # explicit provider.shutdown() the last batch is dropped on rolling
    # restarts. Pin that Telemetry.shutdown() forwards to the provider.
    shutdowns: list[bool] = []

    class _Provider:
        def shutdown(self) -> None:
            shutdowns.append(True)

    telemetry = Telemetry(tracer=object(), provider=_Provider())
    telemetry.shutdown()
    assert shutdowns == [True]


def test_shutdown_is_safe_on_disabled() -> None:
    # The disabled singleton has no provider; shutdown() must not raise
    # so callers can always call it unconditionally during teardown.
    Telemetry.disabled().shutdown()


def test_shutdown_swallows_provider_errors() -> None:
    # A misbehaving provider must not break the application shutdown
    # sequence — telemetry is observability, not control flow.
    class _BadProvider:
        def shutdown(self) -> None:
            raise RuntimeError("boom")

    telemetry = Telemetry(tracer=object(), provider=_BadProvider())
    telemetry.shutdown()  # must not raise


def test_none_value_attributes_are_skipped() -> None:
    # Pass-through of ``None`` would surface as the literal string in
    # exporters that disallow None — easier to filter at the source.
    recorded: list[tuple[str, dict[str, Any]]] = []

    class _Span:
        def __init__(self) -> None:
            self.attrs: dict[str, Any] = {}

        def set_attribute(self, key: str, value: Any) -> None:
            self.attrs[key] = value

        def __enter__(self) -> _Span:
            return self

        def __exit__(self, *_: object) -> None:
            recorded.append(("end", self.attrs))

    class _Tracer:
        def start_as_current_span(self, name: str) -> _Span:
            return _Span()

    telemetry = Telemetry(tracer=_Tracer())
    with telemetry.span("promanomaly.query", group="g1", query=None):
        pass
    assert recorded[-1][1] == {"promanomaly.group": "g1"}

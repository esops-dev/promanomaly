"""Optional OpenTelemetry tracing for the detector pipeline.

Off by default. Operators opt in via ``telemetry.otlp.endpoint`` in the
config; the SDK ships as the ``[otel]`` optional extra and is only
imported when the feature is actually enabled. When disabled the
:meth:`Telemetry.span` context manager is a zero-allocation no-op so
callers can wrap critical sections unconditionally.

Three spans are emitted today, matching the roadmap surface:

- ``promanomaly.group_run`` — one detection pass for a group.
- ``promanomaly.query`` — fetching + scoring one (possibly rendered)
  query variant inside a group run.
- ``promanomaly.detect`` — one detector's compute against one series.

Attributes follow OTel naming convention: lowercase, dotted, prefixed
with ``promanomaly.`` so they don't collide with downstream conventions.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover - import only for type hints
    from .config import TelemetryConfig

# Single canonical no-op telemetry sentinel — built lazily by
# :meth:`Telemetry.disabled`. Cached so every call site shares one
# instance and ``with telemetry.span(...)`` is allocation-free.
_DISABLED: Telemetry | None = None


class Telemetry:
    """Wraps the optional OpenTelemetry tracer.

    Two construction paths:

    - :meth:`Telemetry.disabled` — no-op, zero overhead, always safe.
    - :meth:`Telemetry.from_config` — initialises the OTLP gRPC exporter
      using the operator's ``telemetry.otlp`` settings; raises a clear
      runtime error when the ``[otel]`` extra is not installed.
    """

    def __init__(self, tracer: Any | None, provider: Any | None = None) -> None:
        # ``tracer`` is an opentelemetry ``Tracer`` when enabled, ``None``
        # in the disabled / fallback path. Typed loosely to avoid leaking
        # the optional SDK type into the rest of the codebase.
        #
        # ``provider`` is the owning ``TracerProvider`` (when enabled), held
        # so ``shutdown()`` can flush the batch processor and drain
        # pending spans before the process exits. Without this, a clean
        # SIGTERM would drop the last batch on the floor.
        self._tracer = tracer
        self._provider = provider

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    @classmethod
    def disabled(cls) -> Telemetry:
        """Return the shared zero-overhead instance."""
        global _DISABLED
        if _DISABLED is None:
            _DISABLED = cls(tracer=None)
        return _DISABLED

    @classmethod
    def from_config(cls, cfg: TelemetryConfig | None) -> Telemetry:
        """Build a Telemetry instance from a validated :class:`TelemetryConfig`.

        Returns the disabled singleton when ``cfg`` is ``None`` or
        ``cfg.otlp`` is unset. Initialising the SDK when not requested
        would pull in protobuf/gRPC unnecessarily and surface in
        operator-facing logs as "we're paying for tracing nobody asked
        for".
        """
        if cfg is None or cfg.otlp is None or not cfg.otlp.endpoint:
            return cls.disabled()
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
        except ImportError as exc:  # pragma: no cover - exercised via integration
            raise RuntimeError(
                "telemetry.otlp configured but the OpenTelemetry SDK is not "
                "installed; install promanomaly with the [otel] extra: "
                "pip install promanomaly[otel]"
            ) from exc

        # Each Telemetry instance owns its own TracerProvider so a reload
        # that changes the endpoint can rebuild without touching global
        # state on the running process.
        resource = Resource.create(
            {
                "service.name": cfg.otlp.service_name,
                "service.namespace": "promanomaly",
            }
        )
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(
            endpoint=cfg.otlp.endpoint,
            insecure=cfg.otlp.insecure,
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))
        # We do NOT set the global tracer provider — keeping the
        # provider scoped to this Telemetry instance avoids interfering
        # with any host process that has already configured tracing.
        tracer = provider.get_tracer("promanomaly")
        return cls(tracer=tracer, provider=provider)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @property
    def enabled(self) -> bool:
        return self._tracer is not None

    def shutdown(self) -> None:
        """Flush buffered spans and shut down the tracer provider.

        ``BatchSpanProcessor`` queues spans on a background thread and
        flushes in batches. Without calling ``shutdown()`` on process
        exit the last batch would be dropped on the floor — visible to
        operators as missing tail spans whenever the detector restarts
        or is rolled. Safe to call on the disabled singleton (no-op).
        """
        if self._provider is None:
            return
        # Shutdown failures are observability-side and must not block the
        # rest of the application shutdown sequence — tracing is
        # observability, not control flow.
        try:  # pragma: no cover - defensive
            self._provider.shutdown()
        except Exception:  # pragma: no cover - defensive
            logger.debug("telemetry_provider_shutdown_failed", exc_info=True)

    @contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[Any]:
        """Open a span ``name`` with optional attributes.

        Zero-allocation when disabled — the context manager yields
        ``None`` immediately and does no other work. Callers may
        unconditionally wrap blocks with ``with telemetry.span(...):``
        regardless of whether tracing is on.

        Attribute keys are normalised to OTel convention by prefixing
        with ``promanomaly.`` when they're not already dotted, so
        ``span("...", group=...)`` produces ``promanomaly.group=...``.
        """
        if self._tracer is None:
            yield None
            return
        with self._tracer.start_as_current_span(name) as span:
            for key, value in attributes.items():
                if value is None:
                    continue
                attr_key = key if "." in key else f"promanomaly.{key}"
                try:
                    span.set_attribute(attr_key, value)
                except Exception:  # pragma: no cover - defensive
                    # Bad attribute types (e.g. nested dicts) must not
                    # take the detection run down. Tracing is observability,
                    # not control flow.
                    span.set_attribute(attr_key, str(value))
            yield span


__all__ = ["Telemetry"]

# Telemetry — distributed tracing

promanomaly emits optional OpenTelemetry spans around the query → detect → export pipeline. Telemetry is **off by default**: enabling it requires installing the `[otel]` extra and pointing the detector at an OTLP-gRPC collector.

Three spans are emitted when enabled:

| Span | Wraps |
| --- | --- |
| `promanomaly.group_run` | One full detection pass for a group. |
| `promanomaly.query` | Fetch + score for one (possibly rendered) query variant inside a group. |
| `promanomaly.detect` | One detector's compute against one series. |

Attributes use the `promanomaly.*` namespace (`promanomaly.group`, `promanomaly.query`, `promanomaly.detector`, `promanomaly.window`). The window attribute is the rolling-window size in seconds — useful when correlating slow runs against stratified detectors that opted into a longer fetch.

## Enabling

1. Install the SDK alongside the base package:

   ```bash
   pip install 'promanomaly[otel]'
   ```

   The SDK pulls in protobuf and gRPC, which is why it lives behind an extra rather than shipping in the base install.

2. Add a `telemetry.otlp` block to your config:

   ```yaml
   telemetry:
     otlp:
       endpoint: "otel-collector.observability.svc:4317"
       insecure: true             # default; flip to false if the collector terminates TLS
       service_name: "promanomaly" # default; override to distinguish multiple deployments
   ```

3. Restart the detector. The collector should start receiving spans within one `refresh_interval`.

   Via the Helm chart, set the same fields under `telemetry.otlp.*` in your `values.yaml`; the chart only renders a `telemetry:` block into the config when `endpoint` is non-empty, so default installs stay clean. You still need to build (or pull) a detector image with the `[otel]` extra installed.

## Zero overhead when disabled

The default config carries no `telemetry:` block at all. In that path `Telemetry.disabled()` returns a singleton whose `span(...)` context manager yields immediately — no SDK import, no allocations. The runner's `with self._telemetry.span(...)` wrappers are safe to keep in place unconditionally.

## Operational behaviour

- **Reload semantics**: telemetry settings are read at boot. A `/-/reload` does not currently rebuild the tracer; if you change `endpoint`, restart the detector. (Same caveat applies to promforecast.)
- **Failure isolation**: an unreachable collector causes the batched exporter to log warnings; detection itself is unaffected. Tracing is observability, not control flow.
- **HA mode**: every replica configures its own tracer. The `service.name` resource attribute is shared, so the leader and follower roles are distinguished via the OTel SDK's default `service.instance.id` if the operator wires that in via env.

## What's traced and what isn't

Traced: the per-group detection pipeline (`group_run` → `query` → `detect`).

Not traced (by design):
- The exporter's `/metrics` render — synchronous, predictable, and already covered by `anomaly_run_duration_seconds`.
- Background tasks (leader election renewals, follower-snapshot polling) — these would dominate the trace volume without adding actionable signal.
- The synthetic-injection calibration cycle — runs at most once per `auto_select_interval` per series; per-call duration is captured in `anomaly_detect_duration_seconds` already.

If you find yourself wanting one of those traced for a specific debugging session, raise an issue — we'd rather add narrowly-scoped spans than blanket the runtime.

## Configuration reference

| Field | Default | Notes |
| --- | --- | --- |
| `telemetry.otlp.endpoint` | _(unset)_ | gRPC endpoint of the OTel collector. Tracing is off until this is set. |
| `telemetry.otlp.insecure` | `true` | Plaintext gRPC. Set to `false` when the collector terminates TLS. |
| `telemetry.otlp.service_name` | `promanomaly` | Reported as the `service.name` resource attribute. |

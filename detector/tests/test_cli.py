"""CLI smoke tests via Click's CliRunner."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from click.testing import CliRunner

from promanomaly import cli as cli_module
from promanomaly.cli import cli
from promanomaly.config import CURRENT_API_VERSION
from promanomaly.source import QueryResult, QuerySeries, SourceQueryError
from tests.conftest import StubResponse, StubSource
from tests.fixtures import clean_baseline


def _write_config(tmp_path: Path, **extras: Any) -> Path:
    cfg: dict[str, Any] = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g",
                "queries": [{"id": "m", "promql": "up", "detectors": [{"name": "MAD"}]}],
            }
        ],
    }
    cfg.update(extras)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def test_validate_ok(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    result = CliRunner().invoke(cli, ["validate", "--config", str(path)])
    assert result.exit_code == 0
    assert "OK:" in result.output
    assert CURRENT_API_VERSION in result.output


def test_validate_bad_yaml_exits_nonzero(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("apiVersion: promanomaly.io/v999\nfoo: bar\n")
    result = CliRunner().invoke(cli, ["validate", "--config", str(bad)])
    assert result.exit_code != 0
    assert "INVALID:" in result.output


def test_validate_missing_file_exits_nonzero(tmp_path: Path) -> None:
    result = CliRunner().invoke(cli, ["validate", "--config", str(tmp_path / "nope.yaml")])
    assert result.exit_code != 0


def test_detectors_list_local_returns_built_ins() -> None:
    result = CliRunner().invoke(cli, ["detectors", "list"])
    assert result.exit_code == 0
    rows = json.loads(result.output)
    names = {row["name"] for row in rows}
    assert {"MAD", "Hampel"}.issubset(names)
    hampel = next(r for r in rows if r["name"] == "Hampel")
    # ``defaults`` stays around for compatibility with scripts that
    # parsed the v0.2.0 output…
    assert hampel["defaults"] == {"t0": 3, "recursive_trim": False, "trim_threshold": 3.0}
    # …and ``params`` is the canonical richer shape: name + type + doc
    # for each tunable parameter.
    params_by_name = {p["name"]: p for p in hampel["params"]}
    assert set(params_by_name) == {"t0", "recursive_trim", "trim_threshold"}
    assert params_by_name["t0"]["type"] == "int"
    assert params_by_name["t0"]["default"] == 3
    assert params_by_name["t0"]["doc"]
    assert params_by_name["recursive_trim"]["type"] == "bool"
    assert params_by_name["recursive_trim"]["default"] is False
    assert params_by_name["trim_threshold"]["type"] == "float"
    assert params_by_name["trim_threshold"]["default"] == 3.0
    # MAD picked up the same recursive_trim + trim_threshold knobs in
    # v0.4 — same opt-in baseline-contamination mitigation as Hampel.
    mad = next(r for r in rows if r["name"] == "MAD")
    mad_params = {p["name"] for p in mad["params"]}
    assert {"recursive_trim", "trim_threshold"}.issubset(mad_params)


def test_detectors_list_target_proxies_remote_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``detectors list --target <url>`` should print whatever
    /debug/detectors returns, so the CLI works against custom-built
    images without baking the local registry into the output."""
    import httpx

    body = json.dumps(
        [{"name": "MAD", "description": "robust", "defaults": {}}],
        sort_keys=True,
    )

    class FakeResponse:
        text = body
        status_code = 200

        def raise_for_status(self) -> None:
            return None

    def fake_get(url: str, timeout: float | None = None) -> FakeResponse:
        del timeout
        assert url.endswith("/debug/detectors")
        return FakeResponse()

    monkeypatch.setattr(httpx, "get", fake_get)
    result = CliRunner().invoke(cli, ["detectors", "list", "--target", "http://example/"])
    assert result.exit_code == 0
    assert "MAD" in result.output
    # The CLI passes the response body through verbatim — preserves the
    # remote's formatting so future schema fields appear without a CLI
    # bump.
    assert body in result.output


def test_detectors_list_target_reports_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx

    def boom(url: str, timeout: float | None = None) -> Any:
        del url, timeout
        raise httpx.ConnectError("refused")

    monkeypatch.setattr(httpx, "get", boom)
    result = CliRunner().invoke(cli, ["detectors", "list", "--target", "http://example/"])
    assert result.exit_code != 0
    assert "could not reach" in result.output


def test_no_subcommand_requires_config() -> None:
    result = CliRunner().invoke(cli, [])
    assert result.exit_code != 0
    assert "--config is required" in result.output


# ---------------------------------------------------------------------------
# validate --probe
# ---------------------------------------------------------------------------


def _stub_source_with(
    monkeypatch: pytest.MonkeyPatch,
    responder: Any,
) -> StubSource:
    """Swap the CLI's PromQLSource for a deterministic stub.

    The CLI builds a fresh PromQLSource per probe call so a class-level
    patch is enough — no fixture state to thread.
    """
    stub = StubSource()
    stub.respond(responder)
    monkeypatch.setattr(cli_module, "PromQLSource", lambda *_a, **_kw: stub)
    return stub


def _ok_response(promql: str) -> StubResponse:
    del promql
    return StubResponse(series=[QuerySeries(labels={"instance": "h"}, samples=clean_baseline(60))])


def test_validate_probe_reports_series_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write_config(tmp_path)
    _stub_source_with(monkeypatch, _ok_response)

    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe"])
    assert result.exit_code == 0, result.output
    # First line is a per-query JSON dict; last line is a summary.
    lines = [line for line in result.output.strip().splitlines() if line.strip()]
    first = json.loads(lines[0])
    summary = json.loads(lines[-1])["summary"]
    assert first["status"] == "ok"
    assert first["series"] == 1
    assert summary["total_series"] == 1
    assert summary["errored"] == 0


def test_validate_probe_strict_exits_nonzero_on_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write_config(tmp_path)
    _stub_source_with(monkeypatch, lambda _: StubResponse(series=[]))

    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe", "--strict"])
    assert result.exit_code != 0
    # The summary still surfaces — strict failure shouldn't suppress the
    # diagnostic, only flip the exit code.
    summary = json.loads(result.output.strip().splitlines()[-1])["summary"]
    assert summary["empty_unexpected"] == 1


def test_validate_probe_does_not_count_expect_true_as_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """expect:true queries are *supposed* to be empty sometimes — the
    probe must not flag them as unexpected empties under --strict."""
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "h",
                        "promql": "up",
                        "expect": True,
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg))
    _stub_source_with(monkeypatch, lambda _: StubResponse(series=[]))

    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe", "--strict"])
    assert result.exit_code == 0, result.output


def test_validate_probe_reports_query_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _write_config(tmp_path)
    _stub_source_with(
        monkeypatch,
        lambda _: StubResponse(error=SourceQueryError("server_error", "HTTP 500")),
    )

    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe", "--strict"])
    assert result.exit_code != 0
    lines = [line for line in result.output.strip().splitlines() if line.strip()]
    first = json.loads(lines[0])
    assert first["status"] == "error"
    assert first["reason"] == "server_error"


def test_validate_probe_reports_datasource_unreachable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the source client can't even start (DNS / cert / refused),
    the CLI should print a clean error line + summary and exit 1,
    not dump a traceback."""

    class _BrokenSource:
        def __init__(self, *_a: Any, **_kw: Any) -> None:
            return None

        async def start(self) -> None:
            raise RuntimeError("DNS failure: no such host")

        async def close(self) -> None:
            return None

    monkeypatch.setattr(cli_module, "PromQLSource", _BrokenSource)
    path = _write_config(tmp_path)
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe"])
    assert result.exit_code == 1
    lines = [line for line in result.output.strip().splitlines() if line.strip()]
    first = json.loads(lines[0])
    assert first["status"] == "datasource_unreachable"
    assert "DNS failure" in first["detail"]
    summary = json.loads(lines[-1])["summary"]
    assert summary["datasource_unreachable"] is True


def test_validate_strict_without_probe_is_user_error(tmp_path: Path) -> None:
    path = _write_config(tmp_path)
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--strict"])
    assert result.exit_code != 0
    assert "--probe" in result.output


def test_unused_query_result_import_kept() -> None:
    # Importing QueryResult here makes the dependency on the source module
    # explicit so a refactor that moves the type doesn't silently break
    # CLI imports we exercise via _stub_source_with.
    assert QueryResult is not None


# ---------------------------------------------------------------------------
# validate --probe with discover: blocks (v0.4)
# ---------------------------------------------------------------------------


def _write_discovery_config(tmp_path: Path) -> Path:
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "cpu_{{ instance }}",
                        "promql": 'rate(node_cpu_seconds_total{instance="{{ instance }}"}[1m])',
                        "discover": [{"variable": "instance", "probe": "up", "label": "instance"}],
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    path = tmp_path / "discovery.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def test_validate_probe_simulates_discovery_expansion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``validate --probe`` against a discover: query reports which
    variable values were resolved, how many variants the Cartesian
    expansion yielded, and (per variant) how many series the rendered
    PromQL would pull from the TSDB. This is the v0.4 "catch the wildcard
    foot-gun at PR time" gate."""
    path = _write_discovery_config(tmp_path)

    probe_text = "up"

    def responder(promql: str) -> StubResponse:
        if promql == probe_text:
            return StubResponse(
                series=[
                    QuerySeries(labels={"instance": "node_a"}, samples=clean_baseline(5)),
                    QuerySeries(labels={"instance": "node_b"}, samples=clean_baseline(5)),
                ]
            )
        return StubResponse(
            series=[QuerySeries(labels={"instance": "x"}, samples=clean_baseline(5))]
        )

    _stub_source_with(monkeypatch, responder)

    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe"])
    assert result.exit_code == 0, result.output
    lines = [line for line in result.output.strip().splitlines() if line.strip()]
    payloads = [json.loads(line) for line in lines]
    # One ``discovered`` line listing the variable's values.
    discovered = next(p for p in payloads if p.get("status") == "discovered")
    assert sorted(discovered["values"]) == ["node_a", "node_b"]
    assert discovered["count"] == 2
    # One ``expansion`` line announcing the fan-out size.
    expansion = next(p for p in payloads if p.get("status") == "expansion")
    assert expansion["variants"] == 2
    # Two ``ok`` rows (one per rendered variant).
    ok_rows = [p for p in payloads if p.get("status") == "ok"]
    assert len(ok_rows) == 2
    summary = payloads[-1]["summary"]
    assert summary["discovery_expansions"] == 2
    assert summary["total_series"] == 2


def test_validate_probe_surfaces_unsafe_discovered_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``validate --probe`` should surface dropped unsafe values so an
    operator inspecting the JSON output can fix the discovery axis
    rather than wonder why their probe returned three values but only
    two variants got rendered."""
    path = _write_discovery_config(tmp_path)

    def responder(promql: str) -> StubResponse:
        if promql == "up":
            return StubResponse(
                series=[
                    QuerySeries(labels={"instance": "node_a"}, samples=clean_baseline(5)),
                    QuerySeries(labels={"instance": 'evil"label'}, samples=clean_baseline(5)),
                ]
            )
        return StubResponse(
            series=[QuerySeries(labels={"instance": "x"}, samples=clean_baseline(5))]
        )

    _stub_source_with(monkeypatch, responder)

    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe"])
    assert result.exit_code == 0, result.output
    payloads = [json.loads(line) for line in result.output.strip().splitlines() if line.strip()]
    discovered = next(p for p in payloads if p.get("status") == "discovered")
    assert discovered["values"] == ["node_a"]
    assert discovered["dropped_unsafe"] == ['evil"label']


def test_validate_probe_strict_fails_when_expansion_exceeds_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wildcard probe that fans out beyond ``safety.max_total_series``
    must abort under ``--strict`` so the most common discovery foot-gun
    is caught at PR time rather than at runtime."""
    cfg = {
        "apiVersion": CURRENT_API_VERSION,
        "datasource": {"url": "http://stub"},
        "safety": {"max_total_series": 1},
        "groups": [
            {
                "name": "g",
                "queries": [
                    {
                        "id": "cpu_{{ instance }}",
                        "promql": 'up{instance="{{ instance }}"}',
                        "discover": [{"variable": "instance", "probe": "up", "label": "instance"}],
                        "detectors": [{"name": "MAD"}],
                    }
                ],
            }
        ],
    }
    path = tmp_path / "discovery_cap.yaml"
    path.write_text(yaml.safe_dump(cfg))

    def responder(promql: str) -> StubResponse:
        if promql == "up":
            return StubResponse(
                series=[
                    QuerySeries(labels={"instance": "node_a"}, samples=clean_baseline(5)),
                    QuerySeries(labels={"instance": "node_b"}, samples=clean_baseline(5)),
                ]
            )
        return StubResponse(
            series=[QuerySeries(labels={"instance": "x"}, samples=clean_baseline(5))]
        )

    _stub_source_with(monkeypatch, responder)
    result = CliRunner().invoke(cli, ["validate", "--config", str(path), "--probe", "--strict"])
    assert result.exit_code != 0
    # The summary still surfaces — strict failure exposes the
    # diagnostic alongside the non-zero exit.
    assert "cardinality_exceeded" in result.output

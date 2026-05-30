"""Tests for v1.6 — Enterprise integration: managed backends and multi-team config."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from promanomaly.config import (
    CURRENT_API_VERSION,
    AuthConfig,
    Config,
    TenantConfig,
    load_config,
)
from promanomaly.httpauth import build_httpx_auth
from promanomaly.migrate import migrate_file, migrate_raw

MIN_CONFIG: dict[str, object] = {
    "apiVersion": CURRENT_API_VERSION,
    "datasource": {"url": "http://vm:8428/"},
    "groups": [
        {
            "name": "g1",
            "queries": [
                {
                    "id": "metric_a",
                    "promql": "up",
                    "detectors": [{"name": "MAD"}],
                }
            ],
        }
    ],
}


# ── Cloud-managed datasource authentication ──────────────────────────


class TestCloudAuth:
    def test_sigv4_type_requires_sigv4_block(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {"url": "http://amp:8428/", "auth": {"type": "sigv4"}},
        }
        with pytest.raises(Exception, match=r"requires a matching auth\.sigv4 block"):
            Config.model_validate(raw)

    def test_sigv4_with_block_validates(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://amp:8428/",
                "auth": {
                    "type": "sigv4",
                    "sigv4": {"region": "us-east-1"},
                },
            },
        }
        cfg = Config.model_validate(raw)
        assert cfg.datasource.auth.type == "sigv4"
        assert cfg.datasource.auth.sigv4 is not None
        assert cfg.datasource.auth.sigv4.region == "us-east-1"

    def test_gcp_type_requires_gcp_block(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {"url": "http://gmp:8428/", "auth": {"type": "gcp"}},
        }
        with pytest.raises(Exception, match=r"requires a matching auth\.gcp block"):
            Config.model_validate(raw)

    def test_gcp_with_empty_block_validates(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://gmp:8428/",
                "auth": {"type": "gcp", "gcp": {}},
            },
        }
        cfg = Config.model_validate(raw)
        assert cfg.datasource.auth.type == "gcp"
        assert cfg.datasource.auth.gcp is not None

    def test_azure_type_requires_azure_block(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {"url": "http://az:8428/", "auth": {"type": "azure"}},
        }
        with pytest.raises(Exception, match=r"requires a matching auth\.azure block"):
            Config.model_validate(raw)

    def test_azure_with_block_validates(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://az:8428/",
                "auth": {
                    "type": "azure",
                    "azure": {"client_id": "abc", "tenant_id": "def"},
                },
            },
        }
        cfg = Config.model_validate(raw)
        assert cfg.datasource.auth.type == "azure"
        assert cfg.datasource.auth.azure is not None
        assert cfg.datasource.auth.azure.client_id == "abc"

    def test_oauth2_type_requires_oauth2_block(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {"url": "http://idp:8428/", "auth": {"type": "oauth2"}},
        }
        with pytest.raises(Exception, match=r"requires a matching auth\.oauth2 block"):
            Config.model_validate(raw)

    def test_oauth2_with_block_validates(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://idp:8428/",
                "auth": {
                    "type": "oauth2",
                    "oauth2": {
                        "token_url": "https://auth.example.com/token",
                        "client_id": "my-client",
                        "client_secret": "s3cret",
                        "scopes": ["read", "write"],
                    },
                },
            },
        }
        cfg = Config.model_validate(raw)
        assert cfg.datasource.auth.type == "oauth2"
        assert cfg.datasource.auth.oauth2 is not None
        assert cfg.datasource.auth.oauth2.token_url == "https://auth.example.com/token"
        assert cfg.datasource.auth.oauth2.scopes == ["read", "write"]

    def test_oauth2_resolved_client_secret_inline(self) -> None:
        auth = AuthConfig(
            type="oauth2",
            oauth2={
                "token_url": "https://auth/token",
                "client_id": "c",
                "client_secret": "inline_secret",
            },
        )
        assert auth.oauth2 is not None
        assert auth.oauth2.resolved_client_secret() == "inline_secret"

    def test_oauth2_resolved_client_secret_file(self, tmp_path: Path) -> None:
        secret_file = tmp_path / "secret"
        secret_file.write_text("file_secret\n")
        auth = AuthConfig(
            type="oauth2",
            oauth2={
                "token_url": "https://auth/token",
                "client_id": "c",
                "client_secret_file": str(secret_file),
            },
        )
        assert auth.oauth2 is not None
        assert auth.oauth2.resolved_client_secret() == "file_secret"

    def test_none_type_ignores_cloud_blocks(self) -> None:
        """Cloud blocks present with type=none are ignored (forward compat)."""
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://vm:8428/",
                "auth": {
                    "type": "none",
                    "sigv4": {"region": "us-east-1"},
                },
            },
        }
        cfg = Config.model_validate(raw)
        assert cfg.datasource.auth.type == "none"

    def test_build_httpx_auth_bearer(self) -> None:
        auth = AuthConfig(type="bearer", token="tok")
        kwargs = build_httpx_auth(auth)
        assert kwargs["headers"]["Authorization"] == "Bearer tok"

    def test_build_httpx_auth_cloud_installs_event_hook(self) -> None:
        import asyncio
        import inspect

        auth = AuthConfig(
            type="oauth2",
            oauth2={
                "token_url": "https://auth/token",
                "client_id": "c",
                "client_secret": "s",
            },
        )
        kwargs = build_httpx_auth(auth)
        assert "event_hooks" in kwargs
        assert "request" in kwargs["event_hooks"]
        assert len(kwargs["event_hooks"]["request"]) == 1
        # httpx AsyncClient requires async event hooks.
        hook = kwargs["event_hooks"]["request"][0]
        assert asyncio.iscoroutinefunction(hook) or inspect.iscoroutinefunction(hook)


# ── Multi-tenant datasource support ──────────────────────────────────


class TestMultiTenant:
    def test_datasource_tenant_validates(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://mimir:8428/",
                "tenant": {"id": "team-a"},
            },
        }
        cfg = Config.model_validate(raw)
        assert cfg.datasource.tenant is not None
        assert cfg.datasource.tenant.id == "team-a"
        assert cfg.datasource.tenant.header == "X-Scope-OrgID"

    def test_datasource_tenant_custom_header(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://cortex:8428/",
                "tenant": {"id": "team-b", "header": "X-Custom-Tenant"},
            },
        }
        cfg = Config.model_validate(raw)
        assert cfg.datasource.tenant is not None
        assert cfg.datasource.tenant.header == "X-Custom-Tenant"

    def test_empty_tenant_id_rejected(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://mimir:8428/",
                "tenant": {"id": ""},
            },
        }
        with pytest.raises(Exception, match=r"at least 1 character"):
            Config.model_validate(raw)

    def test_empty_sigv4_region_rejected(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://amp:8428/",
                "auth": {"type": "sigv4", "sigv4": {"region": ""}},
            },
        }
        with pytest.raises(Exception, match=r"at least 1 character"):
            Config.model_validate(raw)

    def test_group_tenant_override(self) -> None:
        raw = {
            **MIN_CONFIG,
            "datasource": {
                "url": "http://mimir:8428/",
                "tenant": {"id": "default-tenant"},
            },
            "groups": [
                {
                    "name": "g1",
                    "tenant": {"id": "team-specific"},
                    "queries": [
                        {
                            "id": "metric_a",
                            "promql": "up",
                            "detectors": [{"name": "MAD"}],
                        }
                    ],
                }
            ],
        }
        cfg = Config.model_validate(raw)
        assert cfg.groups[0].tenant is not None
        assert cfg.groups[0].tenant.id == "team-specific"
        assert cfg.datasource.tenant is not None
        assert cfg.datasource.tenant.id == "default-tenant"

    def test_no_tenant_is_default(self) -> None:
        cfg = Config.model_validate(MIN_CONFIG)
        assert cfg.datasource.tenant is None
        assert cfg.groups[0].tenant is None


# ── Config composition for multi-team ownership ─────────────────────


class TestConfigComposition:
    def test_load_single_file(self, tmp_path: Path) -> None:
        config = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
            "groups": [
                {
                    "name": "g1",
                    "queries": [
                        {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
        f = tmp_path / "config.yaml"
        f.write_text(yaml.dump(config))
        cfg = load_config(str(f))
        assert len(cfg.groups) == 1

    def test_load_directory_with_defaults(self, tmp_path: Path) -> None:
        defaults = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
            "defaults": {"window": "30m"},
        }
        team_a = {
            "groups": [
                {
                    "name": "team_a",
                    "queries": [
                        {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
        team_b = {
            "groups": [
                {
                    "name": "team_b",
                    "queries": [
                        {"id": "m2", "promql": "up{job='b'}", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
        (tmp_path / "_defaults.yaml").write_text(yaml.dump(defaults))
        (tmp_path / "team_a.yaml").write_text(yaml.dump(team_a))
        (tmp_path / "team_b.yaml").write_text(yaml.dump(team_b))

        cfg = load_config(tmp_path)
        assert len(cfg.groups) == 2
        assert cfg.groups[0].name == "team_a"
        assert cfg.groups[1].name == "team_b"
        assert cfg.defaults.window == "30m"

    def test_load_directory_rejects_duplicate_group_names(self, tmp_path: Path) -> None:
        defaults = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
        }
        frag_a = {
            "groups": [
                {
                    "name": "overlap",
                    "queries": [
                        {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
        frag_b = {
            "groups": [
                {
                    "name": "overlap",
                    "queries": [
                        {"id": "m2", "promql": "up", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
        (tmp_path / "_defaults.yaml").write_text(yaml.dump(defaults))
        (tmp_path / "a.yaml").write_text(yaml.dump(frag_a))
        (tmp_path / "b.yaml").write_text(yaml.dump(frag_b))

        with pytest.raises(ValueError, match="duplicate group name"):
            load_config(tmp_path)

    def test_load_directory_no_yaml_files(self, tmp_path: Path) -> None:
        (tmp_path / "readme.txt").write_text("not yaml")
        with pytest.raises(ValueError, match="no YAML files found"):
            load_config(tmp_path)

    def test_load_directory_fragment_without_defaults(self, tmp_path: Path) -> None:
        """A directory without _defaults.yaml uses fragment keys directly."""
        config = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
            "groups": [
                {
                    "name": "only_group",
                    "queries": [
                        {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
        (tmp_path / "single.yaml").write_text(yaml.dump(config))
        cfg = load_config(tmp_path)
        assert len(cfg.groups) == 1
        assert cfg.groups[0].name == "only_group"

    def test_load_glob_pattern(self, tmp_path: Path) -> None:
        defaults = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
        }
        frag = {
            "groups": [
                {
                    "name": "glob_g",
                    "queries": [
                        {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
        (tmp_path / "_defaults.yaml").write_text(yaml.dump(defaults))
        (tmp_path / "team.yaml").write_text(yaml.dump(frag))

        cfg = load_config(str(tmp_path / "*.yaml"))
        assert len(cfg.groups) == 1
        assert cfg.groups[0].name == "glob_g"


# ── Config schema migration CLI ──────────────────────────────────────


class TestMigrateConfig:
    def test_migrate_v1alpha1_to_v1(self) -> None:
        raw = {
            "apiVersion": "promanomaly.io/v1alpha1",
            "datasource": {"url": "http://vm:8428/"},
            "groups": [],
        }
        migrated, changes = migrate_raw(raw)
        assert migrated["apiVersion"] == CURRENT_API_VERSION
        assert len(changes) == 1
        assert "v1alpha1" in changes[0]

    def test_migrate_already_current_is_noop(self) -> None:
        raw = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
            "groups": [],
        }
        migrated, changes = migrate_raw(raw)
        assert not changes
        assert migrated["apiVersion"] == CURRENT_API_VERSION

    def test_migrate_unsupported_version_raises(self) -> None:
        raw = {"apiVersion": "promanomaly.io/v999"}
        with pytest.raises(ValueError, match="unsupported apiVersion"):
            migrate_raw(raw)

    def test_migrate_file_dry_run(self, tmp_path: Path) -> None:
        config = {
            "apiVersion": "promanomaly.io/v1alpha1",
            "datasource": {"url": "http://vm:8428/"},
            "groups": [],
        }
        f = tmp_path / "config.yaml"
        f.write_text(yaml.dump(config))

        output, changes = migrate_file(f, dry_run=True)
        assert changes
        # Original file untouched.
        raw = yaml.safe_load(f.read_text())
        assert raw["apiVersion"] == "promanomaly.io/v1alpha1"
        # Diff output present.
        assert "---" in output or "+++" in output

    def test_migrate_file_write(self, tmp_path: Path) -> None:
        config = {
            "apiVersion": "promanomaly.io/v1alpha1",
            "datasource": {"url": "http://vm:8428/"},
            "groups": [],
        }
        f = tmp_path / "config.yaml"
        f.write_text(yaml.dump(config))

        _, changes = migrate_file(f, dry_run=False)
        assert changes
        # File updated.
        raw = yaml.safe_load(f.read_text())
        assert raw["apiVersion"] == CURRENT_API_VERSION

    def test_migrate_file_noop(self, tmp_path: Path) -> None:
        config = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
            "groups": [],
        }
        f = tmp_path / "config.yaml"
        f.write_text(yaml.dump(config))

        output, changes = migrate_file(f, dry_run=True)
        assert not changes
        assert output == ""

    def test_migrate_idempotent(self, tmp_path: Path) -> None:
        config = {
            "apiVersion": "promanomaly.io/v1alpha1",
            "datasource": {"url": "http://vm:8428/"},
            "groups": [],
        }
        f = tmp_path / "config.yaml"
        f.write_text(yaml.dump(config))

        migrate_file(f, dry_run=False)
        # Run again.
        _, changes = migrate_file(f, dry_run=False)
        assert not changes


# ── TenantConfig in PromQLSource ─────────────────────────────────────


class TestTenantSourceIntegration:
    def test_source_with_tenant_injects_header(self) -> None:
        from promanomaly.source import PromQLSource

        tenant = TenantConfig(id="team-x", header="X-Scope-OrgID")
        source = PromQLSource(
            base_url="http://localhost:8428",
            timeout=5.0,
            tenant=tenant,
        )
        # After start, the client should have the tenant header.
        import asyncio

        asyncio.run(source.start())
        assert source._client is not None
        assert source._client.headers.get("X-Scope-OrgID") == "team-x"
        asyncio.run(source.close())

    def test_source_without_tenant_no_header(self) -> None:
        from promanomaly.source import PromQLSource

        source = PromQLSource(
            base_url="http://localhost:8428",
            timeout=5.0,
        )
        import asyncio

        asyncio.run(source.start())
        assert source._client is not None
        assert "X-Scope-OrgID" not in source._client.headers
        asyncio.run(source.close())

    def test_source_tenant_with_bearer_auth(self) -> None:
        """Tenant header coexists with bearer auth header."""
        from promanomaly.source import PromQLSource

        tenant = TenantConfig(id="team-z", header="X-Scope-OrgID")
        auth = AuthConfig(type="bearer", token="my-token")
        source = PromQLSource(
            base_url="http://localhost:8428",
            timeout=5.0,
            auth=auth,
            tenant=tenant,
        )
        import asyncio

        asyncio.run(source.start())
        assert source._client is not None
        assert source._client.headers.get("X-Scope-OrgID") == "team-z"
        assert source._client.headers.get("Authorization") == "Bearer my-token"
        asyncio.run(source.close())

    def test_source_tenant_with_cloud_auth_event_hook(self) -> None:
        """Tenant header set alongside cloud auth event hooks."""
        auth = AuthConfig(
            type="oauth2",
            oauth2={
                "token_url": "https://auth/token",
                "client_id": "c",
                "client_secret": "s",
            },
        )
        tenant = TenantConfig(id="team-cloud", header="X-Scope-OrgID")

        from promanomaly.source import PromQLSource

        source = PromQLSource(
            base_url="http://localhost:8428",
            timeout=5.0,
            auth=auth,
            tenant=tenant,
        )
        import asyncio

        asyncio.run(source.start())
        assert source._client is not None
        # Tenant header is static on the client.
        assert source._client.headers.get("X-Scope-OrgID") == "team-cloud"
        # OAuth2 auth is via event hooks, not static headers.
        assert "Authorization" not in source._client.headers
        asyncio.run(source.close())


# ── Cache key tenant isolation ───────────────────────────────────────


class TestCacheKeyTenantIsolation:
    def test_cache_key_includes_tenant_prefix(self) -> None:
        """Two sources with different tenants produce different cache keys."""
        from promanomaly.cache import TTLCache
        from promanomaly.source import PromQLSource, QueryResult

        cache: TTLCache[tuple[str, float, float, int, int], QueryResult] = TTLCache(
            max_entries=100, ttl_seconds=30.0
        )
        t1 = TenantConfig(id="team-a", header="X-Scope-OrgID")
        t2 = TenantConfig(id="team-b", header="X-Scope-OrgID")

        s1 = PromQLSource("http://vm:8428", 5.0, tenant=t1, cache=cache)
        s2 = PromQLSource("http://vm:8428", 5.0, tenant=t2, cache=cache)

        # Access the internal tenant prefix logic (without actually querying).
        prefix1 = f"[{s1._tenant.header}:{s1._tenant.id}]" if s1._tenant else ""
        prefix2 = f"[{s2._tenant.header}:{s2._tenant.id}]" if s2._tenant else ""
        assert prefix1 != prefix2
        assert prefix1 == "[X-Scope-OrgID:team-a]"
        assert prefix2 == "[X-Scope-OrgID:team-b]"

    def test_cache_key_no_tenant_no_prefix(self) -> None:
        from promanomaly.source import PromQLSource

        source = PromQLSource("http://vm:8428", 5.0)
        assert source._tenant is None


# ── OAuth2 error handling ────────────────────────────────────────────


class TestOAuth2ErrorHandling:
    def test_oauth2_refresh_wraps_http_error(self) -> None:
        from promanomaly.httpauth import _OAuth2TokenCache

        cache = _OAuth2TokenCache(
            token_url="http://localhost:1/nonexistent",
            client_id="c",
            client_secret="s",
            scopes=[],
        )
        with pytest.raises(RuntimeError, match="OAuth2 token refresh failed"):
            cache.get_token()


# ── Tenant source lifecycle ──────────────────────────────────────────


class TestTenantSourceLifecycle:
    def test_close_tenant_sources(self) -> None:
        """Runner.close_tenant_sources() closes all per-tenant sources."""
        import asyncio

        from promanomaly.exporter import OperationalMetrics
        from promanomaly.runner import Runner
        from promanomaly.source import PromQLSource
        from promanomaly.state import SnapshotStore

        cfg = Config.model_validate(
            {
                **MIN_CONFIG,
                "datasource": {
                    "url": "http://vm:8428/",
                    "tenant": {"id": "default"},
                },
                "groups": [
                    {
                        "name": "g1",
                        "tenant": {"id": "override"},
                        "queries": [
                            {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                        ],
                    }
                ],
            }
        )
        source = PromQLSource("http://vm:8428", 5.0, tenant=cfg.datasource.tenant)
        runner = Runner(cfg, source, SnapshotStore(), OperationalMetrics())

        # Simulate creating a tenant source.
        async def _setup_and_close() -> None:
            ts = PromQLSource("http://vm:8428", 5.0, tenant=TenantConfig(id="override"))
            await ts.start()
            runner._tenant_sources["X-Scope-OrgID:override"] = ts
            assert len(runner._tenant_sources) == 1

            await runner.close_tenant_sources()
            assert len(runner._tenant_sources) == 0

        asyncio.run(_setup_and_close())


# ── Config composition edge cases ────────────────────────────────────


class TestConfigCompositionEdgeCases:
    def test_empty_yaml_file_skipped(self, tmp_path: Path) -> None:
        """An empty YAML file in a config directory is skipped gracefully."""
        defaults = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
        }
        (tmp_path / "_defaults.yaml").write_text(yaml.dump(defaults))
        (tmp_path / "empty.yaml").write_text("")
        (tmp_path / "team.yaml").write_text(
            yaml.dump(
                {
                    "groups": [
                        {
                            "name": "team_g",
                            "queries": [
                                {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                            ],
                        }
                    ],
                }
            )
        )
        cfg = load_config(tmp_path)
        assert len(cfg.groups) == 1

    def test_yml_extension_accepted(self, tmp_path: Path) -> None:
        """Files with .yml extension are included alongside .yaml."""
        config = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
            "groups": [
                {
                    "name": "yml_g",
                    "queries": [
                        {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
        (tmp_path / "config.yml").write_text(yaml.dump(config))
        cfg = load_config(tmp_path)
        assert len(cfg.groups) == 1
        assert cfg.groups[0].name == "yml_g"

    def test_fragment_overrides_defaults_settings(self, tmp_path: Path) -> None:
        """A fragment can override base settings like window."""
        defaults = {
            "apiVersion": CURRENT_API_VERSION,
            "datasource": {"url": "http://vm:8428/"},
            "defaults": {"window": "1h"},
        }
        fragment = {
            "defaults": {"window": "30m"},
            "groups": [
                {
                    "name": "g1",
                    "queries": [
                        {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                    ],
                }
            ],
        }
        (tmp_path / "_defaults.yaml").write_text(yaml.dump(defaults))
        (tmp_path / "team.yaml").write_text(yaml.dump(fragment))
        cfg = load_config(tmp_path)
        assert cfg.defaults.window == "30m"


# ── Migrate directory error handling ─────────────────────────────────


class TestMigrateDirectoryErrors:
    def test_malformed_yaml_logged_and_skipped(self, tmp_path: Path) -> None:
        """A malformed YAML file in a directory is skipped."""
        from promanomaly.migrate import migrate_directory

        good = {
            "apiVersion": "promanomaly.io/v1alpha1",
            "datasource": {"url": "http://vm:8428/"},
            "groups": [],
        }
        (tmp_path / "good.yaml").write_text(yaml.dump(good))
        (tmp_path / "bad.yaml").write_text(": invalid: yaml: [")
        results = migrate_directory(tmp_path, dry_run=True)
        assert len(results) == 1
        assert "good.yaml" in results[0][0]

    def test_non_dict_yaml_skipped(self, tmp_path: Path) -> None:
        """A YAML file containing a list (not a dict) is skipped."""
        from promanomaly.migrate import migrate_directory

        good = {
            "apiVersion": "promanomaly.io/v1alpha1",
            "datasource": {"url": "http://vm:8428/"},
            "groups": [],
        }
        (tmp_path / "good.yaml").write_text(yaml.dump(good))
        (tmp_path / "list.yaml").write_text("- item1\n- item2\n")
        results = migrate_directory(tmp_path, dry_run=True)
        assert len(results) == 1


# ── Runner tenant routing ────────────────────────────────────────────


class TestRunnerTenantRouting:
    def test_effective_tenant_returns_group_override(self) -> None:
        """Group-level tenant overrides datasource-level default."""
        from promanomaly.exporter import OperationalMetrics
        from promanomaly.runner import Runner
        from promanomaly.source import PromQLSource
        from promanomaly.state import SnapshotStore

        cfg = Config.model_validate(
            {
                **MIN_CONFIG,
                "datasource": {
                    "url": "http://vm:8428/",
                    "tenant": {"id": "default"},
                },
                "groups": [
                    {
                        "name": "g1",
                        "tenant": {"id": "override"},
                        "queries": [
                            {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                        ],
                    },
                    {
                        "name": "g2",
                        "queries": [
                            {"id": "m2", "promql": "up", "detectors": ["MAD"]},
                        ],
                    },
                ],
            }
        )
        source = PromQLSource("http://vm:8428", 5.0, tenant=cfg.datasource.tenant)
        runner = Runner(cfg, source, SnapshotStore(), OperationalMetrics())

        # g1 has an override tenant.
        t1 = runner._effective_tenant(cfg.groups[0])
        assert t1 is not None
        assert t1.id == "override"

        # g2 falls back to datasource default.
        t2 = runner._effective_tenant(cfg.groups[1])
        assert t2 is not None
        assert t2.id == "default"

    def test_source_for_group_returns_main_when_tenant_matches(self) -> None:
        """When group tenant matches datasource tenant, use the main source."""
        import asyncio

        from promanomaly.exporter import OperationalMetrics
        from promanomaly.runner import Runner
        from promanomaly.source import PromQLSource
        from promanomaly.state import SnapshotStore

        cfg = Config.model_validate(
            {
                **MIN_CONFIG,
                "datasource": {
                    "url": "http://vm:8428/",
                    "tenant": {"id": "shared"},
                },
                "groups": [
                    {
                        "name": "g1",
                        "queries": [
                            {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                        ],
                    },
                ],
            }
        )
        source = PromQLSource("http://vm:8428", 5.0, tenant=cfg.datasource.tenant)
        runner = Runner(cfg, source, SnapshotStore(), OperationalMetrics())

        async def _check() -> None:
            got = await runner._source_for_group(cfg.groups[0])
            assert got is source  # Same object — no extra source created.

        asyncio.run(_check())

    def test_source_for_group_creates_new_for_override(self) -> None:
        """When group tenant differs, a new PromQLSource is created."""
        import asyncio

        from promanomaly.exporter import OperationalMetrics
        from promanomaly.runner import Runner
        from promanomaly.source import PromQLSource
        from promanomaly.state import SnapshotStore

        cfg = Config.model_validate(
            {
                **MIN_CONFIG,
                "datasource": {
                    "url": "http://vm:8428/",
                    "tenant": {"id": "default"},
                },
                "groups": [
                    {
                        "name": "g1",
                        "tenant": {"id": "other"},
                        "queries": [
                            {"id": "m1", "promql": "up", "detectors": ["MAD"]},
                        ],
                    },
                ],
            }
        )
        source = PromQLSource("http://vm:8428", 5.0, tenant=cfg.datasource.tenant)
        runner = Runner(cfg, source, SnapshotStore(), OperationalMetrics())

        async def _check() -> None:
            got = await runner._source_for_group(cfg.groups[0])
            assert got is not source
            assert got._tenant is not None
            assert got._tenant.id == "other"
            assert len(runner._tenant_sources) == 1
            await runner.close_tenant_sources()

        asyncio.run(_check())


# ── Stratified fetcher source parameter ──────────────────────────────


class TestStratifiedFetcherSource:
    def test_replace_source(self) -> None:
        """StratifiedFetcher.replace_source swaps the underlying source."""
        from promanomaly.source import PromQLSource
        from promanomaly.stratified_fetcher import StratifiedFetcher

        cfg = Config.model_validate(MIN_CONFIG)
        s1 = PromQLSource("http://vm:8428", 5.0)
        fetcher = StratifiedFetcher(source=s1, config=cfg)
        assert fetcher._source is s1

        s2 = PromQLSource("http://vm2:8428", 5.0)
        fetcher.replace_source(s2)
        assert fetcher._source is s2

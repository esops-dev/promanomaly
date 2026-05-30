"""Adapter process entrypoint: load config, run uvicorn (TLS in-cluster)."""

from __future__ import annotations

from pathlib import Path

import uvicorn

from ..logging import configure_logging, get_logger
from ..main import _parse_listen
from .app import build_adapter_app
from .config import load_adapter_config

logger = get_logger(__name__)


def serve_adapter(config_path: str | Path) -> None:
    """Boot the metrics adapter. Used by the ``promanomaly adapter`` CLI."""
    config_path = Path(config_path)
    config = load_adapter_config(config_path)
    configure_logging("INFO")
    logger.info(
        "adapter_boot",
        config_path=str(config_path),
        metrics=config.metrics,
        tls=config.tls.enabled,
    )
    app = build_adapter_app(config)
    host, port = _parse_listen(config.listen)
    # The aggregation layer only speaks HTTPS to an APIService backend, so
    # in-cluster the Helm chart always supplies a cert pair; the plain-HTTP
    # fallback exists only for local testing.
    cert_file: str | None = None
    key_file: str | None = None
    if config.tls.enabled:
        cert_file = config.tls.cert_file
        key_file = config.tls.key_file
    else:
        logger.warning("adapter_tls_disabled", note="serving plain HTTP; in-cluster requires TLS")
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=host,
            port=port,
            log_config=None,
            access_log=False,
            lifespan="on",
            ssl_certfile=cert_file,
            ssl_keyfile=key_file,
        )
    )
    server.run()


__all__ = ["serve_adapter"]

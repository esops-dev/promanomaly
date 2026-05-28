"""Kubernetes Lease-based leader election for HA mode.

Only one replica runs the scheduler at any moment; followers serve the
leader's last-published snapshot from the shared Redis cache. The Lease
API (``coordination.k8s.io/v1``) is the standard primitive used by every
controller-runtime-based controller in Kubernetes — same backing store,
same renewal semantics — so the failure modes are well understood by
cluster operators.

Failover behaviour (mirrored in the project guide):

- On Lease acquisition, the new leader **immediately** triggers a full
  detection run for every group, rather than waiting for the next
  scheduler tick. This minimises the gap in fresh snapshots after the
  handover.
- The previous leader observes Lease loss in its renewal loop and
  stops its scheduler within a brief cooldown
  (``≤ refresh_interval / 2``) before any further snapshot writes,
  preventing split-brain updates to the shared cache.
- In-flight runs on the demoted leader are cancelled rather than
  allowed to complete; the new leader's immediate run reproduces the
  same scores from the same source.

The implementation intentionally avoids depending on
``kubernetes.leaderelection`` (which doesn't ship a public, stable
high-level helper at the time of writing) and instead drives the Lease
directly. This keeps the renewal loop small enough to reason about and
matches what every other Go controller in the ecosystem does anyway.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import structlog

from .config import HighAvailabilityConfig

logger = structlog.get_logger(__name__)


class LeaseError(RuntimeError):
    """Raised when the Lease API call fails for non-conflict reasons."""


@dataclass
class LeaseSnapshot:
    """Current view of the Lease, returned by the client wrapper."""

    holder_identity: str | None
    acquire_time: float | None
    renew_time: float | None
    lease_duration_seconds: int
    resource_version: str | None = None
    raw: Any = field(default=None, repr=False)


class LeaseClient:
    """Thin wrapper around the Kubernetes coordination.v1 Lease API.

    Synchronous on purpose — the underlying ``kubernetes`` client is
    blocking, so we run each call inside :meth:`asyncio.to_thread` from
    the asyncio side. Keeping the blocking client confined to this
    module limits the impact of the dependency.
    """

    def __init__(
        self,
        *,
        namespace: str,
        name: str,
        lease_duration_seconds: int,
        client: Any | None = None,
    ) -> None:
        self._namespace = namespace
        self._name = name
        self._duration = lease_duration_seconds
        if client is None:
            try:
                from kubernetes import client as kube_client
                from kubernetes import config as kube_config
            except ImportError as exc:
                raise RuntimeError(
                    "highAvailability.enabled=true requires the kubernetes "
                    "client; install with 'pip install promanomaly[ha]'"
                ) from exc

            try:
                kube_config.load_incluster_config()
            except Exception:
                # Fall back to local kubeconfig for development.
                kube_config.load_kube_config()
            client = kube_client.CoordinationV1Api()
        self._api = client

    def get(self) -> LeaseSnapshot | None:
        try:
            obj = self._api.read_namespaced_lease(self._name, self._namespace)
        except Exception as exc:
            # 404 is the legitimate "Lease not created yet" branch; the
            # ApiException class has a ``status`` attribute we sniff on.
            status = getattr(exc, "status", None)
            if status == 404:
                return None
            raise LeaseError(f"read Lease failed: {exc}") from exc
        return _snapshot_from(obj)

    def create(self, identity: str) -> LeaseSnapshot:
        from kubernetes import client as kube_client

        now = _iso_now()
        body = kube_client.V1Lease(
            metadata=kube_client.V1ObjectMeta(name=self._name, namespace=self._namespace),
            spec=kube_client.V1LeaseSpec(
                holder_identity=identity,
                lease_duration_seconds=self._duration,
                acquire_time=now,
                renew_time=now,
            ),
        )
        try:
            obj = self._api.create_namespaced_lease(self._namespace, body)
        except Exception as exc:
            raise LeaseError(f"create Lease failed: {exc}") from exc
        return _snapshot_from(obj)

    def update(self, snapshot: LeaseSnapshot, identity: str, *, renew_only: bool) -> LeaseSnapshot:
        """Renew or take over the Lease.

        ``renew_only=True`` keeps the existing ``acquire_time``; the
        caller uses that path when it already holds the Lease. Otherwise
        a fresh ``acquire_time`` is written — the takeover case.
        """
        now = _iso_now()
        if snapshot.raw is None:
            raise LeaseError("update requires the original Lease body")
        body = snapshot.raw
        body.spec.holder_identity = identity
        body.spec.renew_time = now
        if not renew_only:
            body.spec.acquire_time = now
        try:
            obj = self._api.replace_namespaced_lease(self._name, self._namespace, body)
        except Exception as exc:
            raise LeaseError(f"replace Lease failed: {exc}") from exc
        return _snapshot_from(obj)

    def release(self, snapshot: LeaseSnapshot) -> None:
        """Surrender the Lease so a follower can take over immediately.

        Implementation: rewrite ``renew_time`` to one full
        ``lease_duration`` ago and blank ``holder_identity``. The next
        follower poll sees an expired Lease with no holder and grabs
        it on the spot, without waiting the natural ``lease_duration``
        expiry window. Stays within the existing
        ``get/update/patch`` RBAC verbs — no extra permissions needed.
        """
        import datetime as _dt

        if snapshot.raw is None:
            raise LeaseError("release requires the original Lease body")
        body = snapshot.raw
        body.spec.holder_identity = ""
        # Subtract twice the lease duration so we are unambiguously
        # past the expiry threshold regardless of clock skew.
        stale = _dt.datetime.now(_dt.UTC) - _dt.timedelta(seconds=self._duration * 2)
        body.spec.renew_time = stale
        try:
            self._api.replace_namespaced_lease(self._name, self._namespace, body)
        except Exception as exc:
            raise LeaseError(f"release Lease failed: {exc}") from exc


def _iso_now() -> str:
    # Kubernetes wants RFC3339 timestamps.
    import datetime as _dt

    return _dt.datetime.now(_dt.UTC).isoformat().replace("+00:00", "Z")


def _snapshot_from(obj: Any) -> LeaseSnapshot:
    spec = obj.spec
    return LeaseSnapshot(
        holder_identity=spec.holder_identity,
        acquire_time=_ts(spec.acquire_time),
        renew_time=_ts(spec.renew_time),
        lease_duration_seconds=spec.lease_duration_seconds or 15,
        resource_version=getattr(obj.metadata, "resource_version", None),
        raw=obj,
    )


def _ts(value: Any) -> float | None:
    if value is None:
        return None
    if hasattr(value, "timestamp"):
        try:
            return float(value.timestamp())
        except Exception:
            return None
    return None


def resolve_identity(configured: str | None) -> str:
    """Pick the Lease identity for this process.

    Order: configured value > POD_NAME env > HOSTNAME env > literal
    ``promanomaly``. ``POD_NAME`` is set automatically by Kubernetes
    Downward API in the chart's Deployment, so the resolved identity
    matches the pod name in ``kubectl get pods`` without any operator
    action.
    """
    if configured:
        return configured
    for env in ("POD_NAME", "HOSTNAME"):
        value = os.environ.get(env)
        if value:
            return value
    return "promanomaly"


class LeaderElector:
    """Lease-based leader election loop.

    Hands the caller two callbacks:

    - ``on_started_leading`` fires once per acquisition. The runner uses
      it to kick off an immediate full detection pass so the freshness
      gap during failover stays small.
    - ``on_stopped_leading`` fires once per loss. The runner uses it to
      cancel in-flight detector tasks and stop publishing to the shared
      cache.

    The elector itself owns no scheduler — it only flips the leader
    flag and invokes callbacks. The caller wires those into its own
    scheduling.
    """

    def __init__(
        self,
        *,
        ha: HighAvailabilityConfig,
        identity: str,
        client: LeaseClient,
        on_started_leading: Callable[[], Awaitable[None]],
        on_stopped_leading: Callable[[], Awaitable[None]],
    ) -> None:
        self._ha = ha
        self._identity = identity
        self._client = client
        self._on_started = on_started_leading
        self._on_stopped = on_stopped_leading
        self._is_leader = False
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._transitions = 0

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @property
    def transitions(self) -> int:
        return self._transitions

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        was_leader = self._is_leader
        self._stop.set()
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if was_leader:
            # Graceful release so the next replica takes over within
            # one poll interval instead of waiting the full
            # ``lease_duration`` for natural expiry. Best-effort —
            # if the release call fails (Redis blip, RBAC change,
            # whatever) followers fall back to the normal expiry path.
            try:
                await asyncio.wait_for(self._release_lease(), timeout=5.0)
            except (TimeoutError, Exception) as exc:
                logger.warning("graceful_lease_release_failed", error=str(exc))

    async def _release_lease(self) -> None:
        snapshot = await asyncio.to_thread(self._client.get)
        if snapshot is None or snapshot.holder_identity != self._identity:
            return
        await asyncio.to_thread(self._client.release, snapshot)
        logger.info("lease_released", identity=self._identity)

    async def _run(self) -> None:
        retry = self._ha.retry_period_seconds
        while not self._stop.is_set():
            try:
                acquired, snapshot = await asyncio.to_thread(self._try_acquire)
            except LeaseError as exc:
                logger.warning("leader_election_api_error", error=str(exc))
                await self._wait(retry)
                continue
            if acquired:
                if not self._is_leader:
                    self._is_leader = True
                    self._transitions += 1
                    logger.info("leader_acquired", identity=self._identity)
                    try:
                        await self._on_started()
                    except Exception as exc:
                        logger.exception("on_started_leading_failed", error=str(exc))
                await self._wait(self._renew_interval())
                continue
            if self._is_leader:
                self._is_leader = False
                logger.info(
                    "leader_lost",
                    identity=self._identity,
                    holder=snapshot.holder_identity if snapshot else None,
                )
                try:
                    await self._on_stopped()
                except Exception as exc:
                    logger.exception("on_stopped_leading_failed", error=str(exc))
            await self._wait(retry)

    def _renew_interval(self) -> float:
        # Re-attempt renewal at roughly one-third of the lease duration
        # — same heuristic as controller-runtime's helper.
        return max(1.0, self._ha.lease_duration_seconds / 3.0)

    async def _wait(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            return

    def _try_acquire(self) -> tuple[bool, LeaseSnapshot | None]:
        snapshot = self._client.get()
        now = time.time()
        if snapshot is None:
            try:
                self._client.create(self._identity)
                return True, None
            except LeaseError as exc:
                # 409 conflict means somebody beat us to creation; fall
                # through and re-read on the next tick.
                logger.info("lease_create_conflict", error=str(exc))
                return False, None
        if snapshot.holder_identity == self._identity:
            # We already hold it; renew. Renew failure (e.g. resource
            # version conflict) demotes us so the elector observes the
            # transition normally.
            try:
                self._client.update(snapshot, self._identity, renew_only=True)
                return True, snapshot
            except LeaseError as exc:
                logger.warning("lease_renew_failed", error=str(exc))
                return False, snapshot
        # Held by another identity. Take over only if the lease has
        # expired (renew_time + lease_duration < now). Otherwise wait.
        expiry = (snapshot.renew_time or 0.0) + snapshot.lease_duration_seconds
        if now < expiry:
            return False, snapshot
        try:
            self._client.update(snapshot, self._identity, renew_only=False)
            return True, snapshot
        except LeaseError as exc:
            logger.info("lease_takeover_conflict", error=str(exc))
            return False, snapshot


__all__ = [
    "LeaderElector",
    "LeaseClient",
    "LeaseError",
    "LeaseSnapshot",
    "resolve_identity",
]

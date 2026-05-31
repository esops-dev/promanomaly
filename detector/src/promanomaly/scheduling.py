"""Priority-aware admission control for group scheduling.

By default the scheduler runs every group on its own cadence with no
admission control — the historical behaviour, preserved exactly when
``safety.max_concurrent_groups`` is unset. When the operator sets that
cap, this gate bounds how many group runs execute at once and protects
the most important groups under TSDB backpressure:

* **Priority ordering.** When the gate is saturated, pending runs wait
  on a priority-ordered queue; a freed slot wakes the highest-``priority``
  waiter first, so a slow source delays low-priority signals before the
  ones the operator cares most about.
* **Load shedding.** A run that cannot acquire a slot within its own
  refresh interval is *skipped*, not queued unboundedly — the next tick
  will try again with fresh data. The skip is surfaced on
  ``anomaly_group_skipped_total{group, reason="backpressure"}`` and never
  hidden: shedding delays a score, it never suppresses a detected
  anomaly (the last good snapshot stays in place per serve-stale).

This is strictly a *scheduling* concern, not a *silencing* one. Overrun
(a group's previous run still in flight when the next tick fires) is
surfaced separately by the APScheduler ``max_instances`` listener in
:mod:`promanomaly.main` as ``reason="overrun"``.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
from dataclasses import dataclass, field


@dataclass(order=True)
class _Waiter:
    # Heap ordering: higher priority first (negated), then FIFO by
    # insertion sequence so equal-priority groups keep arrival order.
    sort_key: tuple[int, int]
    future: asyncio.Future[None] = field(compare=False)


class LoadShedder:
    """Bounded, priority-ordered admission gate for group runs.

    Thread-confined to the event loop (all methods are called from
    scheduler tasks on the same loop), so the running count and waiter
    heap need no locking. ``max_concurrent=None`` makes every method a
    pass-through with zero overhead and no behavioural change.
    """

    def __init__(self, max_concurrent: int | None) -> None:
        self._max_concurrent = max_concurrent
        self._running = 0
        self._waiters: list[_Waiter] = []
        self._seq = itertools.count()

    @property
    def max_concurrent(self) -> int | None:
        return self._max_concurrent

    def set_max_concurrent(self, value: int | None) -> None:
        """Update the cap on reload, preserving the live running count.

        Lowering the cap doesn't preempt in-flight runs; it just admits
        fewer new ones until the count drains below the new bound.
        Raising it wakes any waiters that now fit. Removing it (``None``)
        is "admit everyone" — every queued waiter is released immediately
        rather than left to time out and shed against a cap that no
        longer exists.
        """
        self._max_concurrent = value
        if value is None:
            # Uncapped: drain the whole queue. Each woken waiter takes a
            # slot in the running count so the eventual release() (which
            # decrements directly in the uncapped branch) stays balanced.
            while self._wake_one():
                self._running += 1
        else:
            self._drain_waiters()

    @property
    def running(self) -> int:
        return self._running

    async def acquire(self, *, priority: int, deadline_seconds: float) -> bool:
        """Acquire a concurrency slot, or return ``False`` if shed.

        Returns ``True`` immediately when uncapped or a slot is free.
        When saturated, waits up to ``deadline_seconds`` for a slot,
        woken in priority order; returns ``False`` (shed) if the deadline
        passes first. The caller MUST call :meth:`release` iff this
        returned ``True``.
        """
        if self._max_concurrent is None:
            self._running += 1
            return True
        if self._running < self._max_concurrent:
            self._running += 1
            return True
        # Saturated: queue this run, highest priority served first.
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        waiter = _Waiter(sort_key=(-priority, next(self._seq)), future=future)
        heapq.heappush(self._waiters, waiter)
        try:
            await asyncio.wait_for(future, timeout=max(0.0, deadline_seconds))
        except TimeoutError:
            # Shed: no slot freed within the deadline. The waiter may have
            # been popped-and-resolved by a concurrent release in the race
            # window; if so, hand the slot straight back.
            self._abandon_waiter(waiter, future)
            return False
        except asyncio.CancelledError:
            # Leadership loss / shutdown cancelled us — release any slot
            # that raced in and let the cancellation propagate so the task
            # unwinds cleanly rather than being treated as a shed.
            self._abandon_waiter(waiter, future)
            raise
        # Woken by release(): the slot was already accounted to us there.
        return True

    def _abandon_waiter(self, waiter: _Waiter, future: asyncio.Future[None]) -> None:
        """Give back a slot won in the timeout/cancel race, else drop the waiter."""
        if future.done() and not future.cancelled():
            self.release()
        else:
            self._discard_waiter(waiter)

    def release(self) -> None:
        """Release a held slot and wake the next-highest-priority waiter."""
        if self._max_concurrent is None:
            self._running = max(0, self._running - 1)
            return
        # Hand the slot directly to the best waiter (count stays put) so
        # there is no admission gap a lower-priority newcomer could slip
        # through. Only decrement when nobody is waiting.
        woke = self._wake_one()
        if not woke:
            self._running = max(0, self._running - 1)

    def _wake_one(self) -> bool:
        while self._waiters:
            waiter = heapq.heappop(self._waiters)
            if waiter.future.done():
                continue  # already timed out / cancelled
            waiter.future.set_result(None)
            return True
        return False

    def _drain_waiters(self) -> None:
        # Admit as many queued waiters as the (possibly raised) cap allows.
        while (
            self._max_concurrent is not None
            and self._running < self._max_concurrent
            and self._wake_one_into_slot()
        ):
            pass

    def _wake_one_into_slot(self) -> bool:
        if self._wake_one():
            self._running += 1
            return True
        return False

    def _discard_waiter(self, waiter: _Waiter) -> None:
        waiter.future.cancel()
        try:
            self._waiters.remove(waiter)
            heapq.heapify(self._waiters)
        except ValueError:
            pass


__all__ = ["LoadShedder"]

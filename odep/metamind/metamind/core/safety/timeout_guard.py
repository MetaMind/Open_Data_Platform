"""Query timeout guard — enforces wall-clock timeouts for query execution."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import platform
import threading
from typing import AsyncIterator, Iterator

logger = logging.getLogger(__name__)

_IS_UNIX = platform.system() != "Windows"


class TimeoutGuard:
    """Enforces wall-clock timeouts for query execution.

    Uses signal.alarm() on Unix for true interrupt-based timeout.
    Falls back to threading.Timer on Windows.
    """

    def __init__(self, seconds: int) -> None:
        self._seconds = seconds
        self._timer: threading.Timer | None = None

    @contextlib.contextmanager
    def enforce(self) -> Iterator[None]:
        """Sync version: use signal.alarm() on Unix, threading.Timer on Windows."""
        if self._seconds <= 0:
            yield
            return
        if _IS_UNIX:
            yield from self._enforce_signal()
        else:
            yield from self._enforce_timer()

    def _enforce_signal(self) -> Iterator[None]:
        import signal

        def _handler(signum: int, frame: object) -> None:
            raise TimeoutError(f"Query exceeded {self._seconds}s wall-clock timeout")

        old_handler = signal.signal(signal.SIGALRM, _handler)
        signal.alarm(self._seconds)
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

    def _enforce_timer(self) -> Iterator[None]:
        timed_out = threading.Event()

        def _timeout() -> None:
            timed_out.set()

        self._timer = threading.Timer(self._seconds, _timeout)
        self._timer.daemon = True
        self._timer.start()
        try:
            yield
            if timed_out.is_set():
                raise TimeoutError(f"Query exceeded {self._seconds}s timeout")
        finally:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

    @contextlib.asynccontextmanager
    async def enforce_async(self) -> AsyncIterator[None]:
        """Async version: use asyncio task cancellation internally."""
        if self._seconds <= 0:
            yield
            return
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()

        def _timeout_callback() -> None:
            if task is not None and not task.done():
                task.cancel()

        handle = loop.call_later(self._seconds, _timeout_callback)
        try:
            yield
        except asyncio.CancelledError:
            raise TimeoutError(
                f"Query exceeded {self._seconds}s wall-clock timeout"
            ) from None
        finally:
            handle.cancel()

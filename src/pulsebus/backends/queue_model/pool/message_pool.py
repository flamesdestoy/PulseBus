"""Thread‑safe singleton object pool for reusable message instances."""
import threading
import queue
from typing import Optional

from ..message.message import MessageTemplate

class MessagePool:
    """Fixed‑size, thread‑safe pool for :class:`MessageTemplate` objects.

    Parameters
    ----------
    template:
        Prototype used to *clone* new message instances when the internal
        queue is empty and the maximum pool size has not yet been reached.
    max_size:
        Upper bound on the total number of message objects managed by the
        pool. Default is ``100``.
    blocking:
        If *True* (default) ``acquire`` will block when the pool is empty
        *and* the maximum size has been reached, waiting until another
        thread releases an instance. If *False* an empty pool raises
        :class:`queue.Empty` immediately.
    timeout:
        Maximum seconds to wait when ``blocking`` is *True*. ``None`` means
        wait forever.
    """

    def __init__(self,
                 template: MessageTemplate,
                 max_size: int = 100,
                 blocking: bool = True,
                 timeout: Optional[float] = None):
        self._template = template
        self._max_size = max_size
        self._blocking = blocking
        self._timeout = timeout

        self._pool: queue.LifoQueue[MessageTemplate] = queue.LifoQueue(maxsize=max_size)
        self._created = 0  # number of ever‑created objects
        self._create_lock = threading.Lock()

    # ---------------------------------------------------------------------
    # Public API -----------------------------------------------------------
    # ---------------------------------------------------------------------
    def acquire(self) -> MessageTemplate:
        """Get a message instance, cloning lazily if needed."""
        try:
            # Fast‑path: take from pool without blocking
            return self._pool.get_nowait()
        except queue.Empty:
            # Need to clone lazily if capacity allows
            with self._create_lock:
                if self._created < self._max_size:
                    self._created += 1
                    return self._template.clone()
            # Capacity exhausted — either block or raise
            if self._blocking:
                return self._pool.get(timeout=self._timeout)
            raise queue.Empty("MessagePool exhausted and blocking is False")

    def release(self, msg: MessageTemplate) -> None:
        """Return a message to the pool after resetting it."""
        msg.reset()
        try:
            self._pool.put_nowait(msg)
        except queue.Full:
            # Should not happen in normal flow; drop silently.
            pass

    # ---------------------------------------------------------------------
    # Diagnostics ----------------------------------------------------------
    # ---------------------------------------------------------------------
    def stats(self) -> dict[str, int]:
        """Return current usage statistics."""
        available = self._pool.qsize()
        return {
            "available": available,
            "in_use": self._created - available,
            "capacity": self._max_size,
        }

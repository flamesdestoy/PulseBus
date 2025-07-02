import threading
import queue
import time
from collections import deque

from .base import BaseQueue

class LockFreeQueue(BaseQueue):
    """High-performance queue with condition-based notifications."""
    def __init__(self, maxsize: int = 0):
        self._queue = deque()
        self._maxsize = maxsize
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._unfinished_tasks = 0

    def put(self, item, block=True, timeout=None):
        """Thread-safe put with backpressure handling."""
        with self._not_full:
            if self._maxsize > 0:
                if not block:
                    if len(self._queue) >= self._maxsize:
                        raise queue.Full
                elif timeout is None:
                    while len(self._queue) >= self._maxsize:
                        self._not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be non-negative")
                else:
                    endtime = time.monotonic() + timeout
                    while len(self._queue) >= self._maxsize:
                        remaining = endtime - time.monotonic()
                        if remaining <= 0.0:
                            raise queue.Full
                        self._not_full.wait(remaining)
            self._queue.append(item)
            self._unfinished_tasks += 1
            self._not_empty.notify()

    def get(self, block=True, timeout=None):
        """Thread-safe get with immediate wakeup on new data."""
        with self._not_empty:
            if not block:
                if not self._queue:
                    raise queue.Empty
            elif timeout is None:
                while not self._queue:
                    self._not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be non-negative")
            else:
                endtime = time.monotonic() + timeout
                while not self._queue:
                    remaining = endtime - time.monotonic()
                    if remaining <= 0.0:
                        raise queue.Empty
                    self._not_empty.wait(remaining)
            item = self._queue.popleft()
            self._not_full.notify()
            return item

    def task_done(self):
        """Mark a message as processed (for queue tracking)."""
        with self._lock:
            unfinished = self._unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    raise ValueError('task_done() called too many times')
            self._unfinished_tasks = unfinished

    def qsize(self):
        """Get current queue size (approximate)."""
        with self._lock:
            return len(self._queue)

    def empty(self):
        """Check if queue is empty."""
        with self._lock:
            return not self._queue

    def full(self):
        """Check if queue is full."""
        with self._lock:
            return 0 < self._maxsize <= len(self._queue)
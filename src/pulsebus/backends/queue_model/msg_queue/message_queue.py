# pulsebus/queue/message_queue.py

import threading
from queue import Queue, Full, Empty
from typing import Callable, Optional
from ..pool.message_pool import MessagePool
from ..message.interface import PoolableMessage

class MessageQueue:
    """
    Thread-safe queue for transferring pooled message objects between producers and consumers.
    Supports blocking and notification-based consumption with backpressure control.
    """

    def __init__(self, maxsize: int = 100):
        """
        Initialize the queue and prepare it for multithreaded operation.

        Args:
            maxsize (int): Maximum number of messages that can be enqueued.
        """
        self._queue = Queue(maxsize=maxsize)
        self._subscribers = []
        self._lock = threading.Lock()
        self._shutdown_event = threading.Event()

    def publish(self, message: PoolableMessage, block: bool = True, timeout: Optional[float] = None) -> None:
        """
        Add a message to the queue.

        Args:
            message (PoolableMessage): The message object to enqueue.
            block (bool): Whether to block if the queue is full.
            timeout (float | None): How long to wait if blocking.
        Raises:
            queue.Full: If the queue is full and blocking is False or times out.
        """
        self._queue.put(message, block=block, timeout=timeout)

    def consume(self, block: bool = True, timeout: Optional[float] = None) -> PoolableMessage:
        """
        Retrieve a message from the queue.

        Args:
            block (bool): Whether to wait for a message.
            timeout (float | None): How long to wait if blocking.
        Returns:
            PoolableMessage: The next available message.
        Raises:
            queue.Empty: If the queue is empty and blocking is False or times out.
        """
        return self._queue.get(block=block, timeout=timeout)

    def subscribe(self, pool: MessagePool, handler_fn: Callable[[PoolableMessage], None], daemon: bool = True) -> None:
        """
        Starts a consumer thread that calls `handler_fn` on each received message.

        Args:
            handler_fn (Callable[[PoolableMessage], None]): Callback to process each message.
            daemon (bool): Whether the consumer thread should be a daemon thread.
        """
        def _consumer_loop():
            while not self._shutdown_event.is_set():
                try:
                    message = self.consume(block=True, timeout=0.5)
                    handler_fn(message)
                    pool.release(message)
                except Empty:
                    continue

        thread = threading.Thread(target=_consumer_loop, daemon=daemon)
        with self._lock:
            self._subscribers.append(thread)
        thread.start()

    def shutdown(self):
        """
        Stops all active consumer threads gracefully.
        """
        self._shutdown_event.set()
        with self._lock:
            for thread in self._subscribers:
                if thread.is_alive():
                    thread.join()
            self._subscribers.clear()

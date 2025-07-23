import threading
import time
import logging
import queue

from .base import BaseConsumer
from ..queues import LockFreeQueue

class ConsumerController:
    """Manages consumer thread pool."""
    def __init__(self, consumer: BaseConsumer, queue: LockFreeQueue, parallelism: int = 1):
        self._consumer = consumer
        self._queue = queue
        self._parallelism = max(1, parallelism)
        self._threads = []
        self._running = False
        self._logger = logging.getLogger(f"Consumer.{consumer.__class__.__name__}")

    def start(self):
        """Start all consumer threads."""
        if self._running:
            return
            
        self._running = True
        self._consumer.on_start()
        
        for i in range(self._parallelism):
            thread = threading.Thread(
                target=self._run_loop,
                name=f"Consumer-{self._consumer.__class__.__name__}-{i}",
                daemon=True
            )
            thread.start()
            self._threads.append(thread)
        self._logger.info(f"Started {self._parallelism} consumers")

    def stop(self):
        """Stop all consumer threads gracefully."""
        if not self._running:
            return
            
        self._running = False
        self._consumer.on_stop()
        
        for thread in self._threads:
            thread.join(timeout=2.0)
            if thread.is_alive():
                self._logger.warning("Consumer thread did not stop gracefully")
        self._threads.clear()
        self._logger.info("All consumers stopped")

    def _run_loop(self):
        """Internal message consumption loop."""
        while self._running:
            try:
                message = self._queue.get(block=True, timeout=0.1)
                self._consumer.consume(message)
                self._queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self._logger.error(f"Consumer error: {e}", exc_info=True)
                time.sleep(0.1)  # Prevent tight error loops
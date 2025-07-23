import threading
import time
import logging

from .base import BaseProducer
from ..queues import LockFreeQueue

class ProducerController:
    """Manages producer thread lifecycle."""
    def __init__(self, producer: BaseProducer, queue: LockFreeQueue):
        self._producer = producer
        self._queue = queue
        self._thread = None
        self._running = False
        self._logger = logging.getLogger(f"Producer.{producer.__class__.__name__}")

    def start(self):
        """Start producer thread."""
        if self._running:
            return
            
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"Producer-{self._producer.__class__.__name__}",
            daemon=True
        )
        self._producer.on_start()
        self._thread.start()
        self._logger.info("Producer started")

    def stop(self):
        """Stop producer thread gracefully."""
        if not self._running:
            return
            
        self._running = False
        self._producer.on_stop()
        if self._thread:
            self._thread.join(timeout=2.0)
            if self._thread.is_alive():
                self._logger.warning("Producer thread did not stop gracefully")
        self._logger.info("Producer stopped")

    def _run_loop(self):
        """Internal message production loop."""
        while self._running:
            try:
                message = self._producer.produce()
                if message is None:  # Signal to stop
                    self._running = False
                    break
                self._queue.put(message)
            except Exception as e:
                self._logger.error(f"Producer error: {e}", exc_info=True)
                time.sleep(0.1)  # Prevent tight error loops

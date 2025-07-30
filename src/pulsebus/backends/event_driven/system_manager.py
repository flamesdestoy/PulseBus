import logging
import time
from threading import Thread, Event
from .queues import LockFreeQueue
from .producers import BaseProducer, ProducerController
from .consumers import BaseConsumer, ConsumerController

class EventSystem:
    """Orchestrates producers and consumers."""
    def __init__(self, queue_size: int = 100, idle_timeout: float = 5):
        self._queue = LockFreeQueue(maxsize=queue_size)
        self._producers = {}
        self._consumers = {}
        self._auto_shutdown = False
        self._idle_timeout = idle_timeout  # Seconds to wait before shutdown
        self._shutdown_flag = Event()
        self._logger = logging.getLogger("EventSystem")

    def register_producer(self, producer: BaseProducer, name: str = None) -> str:
        """Register a producer."""
        name = name or producer.__class__.__name__
        if name in self._producers:
            raise ValueError(f"Producer '{name}' already exists")
        self._producers[name] = ProducerController(producer, self._queue)
        return name

    def register_consumer(self, consumer: BaseConsumer, name: str = None, parallelism: int = 1) -> str:
        """Register a consumer."""
        name = name or consumer.__class__.__name__
        if name in self._consumers:
            raise ValueError(f"Consumer '{name}' already exists")
        self._consumers[name] = ConsumerController(consumer, self._queue, parallelism)
        return name

    def start_all(self):
        """Start all registered producers and consumers."""
        for name, producer in self._producers.items():
            producer.start()
            self._logger.info(f"Started producer: {name}")
            
        for name, consumer in self._consumers.items():
            consumer.start()
            self._logger.info(f"Started consumer: {name}")

    def stop_all(self):
        """Stops all components gracefully."""
        if self._auto_shutdown:
            self._shutdown_flag.set()
        
        for name, consumer in self._consumers.items():
            consumer.stop()
            self._logger.info(f"Stopped consumer: {name}")
            
        for name, producer in self._producers.items():
            producer.stop()
            self._logger.info(f"Stopped producer: {name}")

    def enable_auto_shutdown(self, idle_timeout: float = 30.0):
        """Enable automatic shutdown when idle (opt-in)."""
        self._auto_shutdown = True
        self._idle_timeout = idle_timeout
        Thread(
            target=self._monitor_shutdown,
            daemon=True,
            name="ShutdownMonitor"
        ).start()
        self._logger.info(f"Started the monitoring thread")

    def _monitor_shutdown(self):
        """Start monitor thread only if auto-shutdown enabled."""
        last_active = time.time()
        while not self._shutdown_flag.is_set():
            # Check shutdown conditions
            if (self._queue.empty() and 
                all(not c.has_active_workers() for c in self._consumers.values()) and
                (time.time() - last_active) >= self._idle_timeout):
                self._logger.info("Auto-shutdown: System idle")
                self.stop_all()
                break
            
            # Reset timer on new activity
            if not self._queue.empty():
                last_active = time.time()
            time.sleep(1)  # Polling interval

    def __enter__(self):
        """Context manager support."""
        self.start_all()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure cleanup on exit."""
        self.stop_all()

import logging
from queues import LockFreeQueue
from producers import BaseProducer, ProducerController
from consumers import BaseConsumer, ConsumerController

class EventSystem:
    """Orchestrates producers and consumers."""
    def __init__(self, queue_size: int = 100):
        self._queue = LockFreeQueue(maxsize=queue_size)
        self._producers = {}
        self._consumers = {}
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
        """Stop all components gracefully."""
        for name, consumer in self._consumers.items():
            consumer.stop()
            self._logger.info(f"Stopped consumer: {name}")
            
        for name, producer in self._producers.items():
            producer.stop()
            self._logger.info(f"Stopped producer: {name}")

    def __enter__(self):
        """Context manager support."""
        self.start_all()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure cleanup on exit."""
        self.stop_all()

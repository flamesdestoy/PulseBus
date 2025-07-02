from abc import ABC, abstractmethod
from typing import Optional, Any

class BaseProducer(ABC):
    """Define your message producer here."""
    @abstractmethod
    def on_start(self):
        """Called when producer starts."""
        pass

    @abstractmethod
    def on_stop(self):
        """Called when producer stops."""
        pass

    @abstractmethod
    def produce(self):
        """Yield messages here (return `None` to stop)."""
        pass

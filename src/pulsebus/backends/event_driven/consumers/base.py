from abc import ABC, abstractmethod
from typing import Optional, Any

class BaseConsumer(ABC):
    """Define your message consumer here."""
    @abstractmethod
    def on_start(self):
        """Called when consumer starts."""
        pass

    @abstractmethod
    def on_stop(self):
        """Called when consumer stops."""
        pass

    @abstractmethod
    def consume(self, message):
        """Process messages here."""
        pass
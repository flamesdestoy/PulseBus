from abc import ABC, abstractmethod
from typing import Optional, Any

class BaseQueue(ABC):
    @abstractmethod
    def put(self, item: Any, block: bool = True, timeout: Optional[float] = None):
        pass
    
    @abstractmethod
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        pass

class BaseProducer(ABC):
    @abstractmethod
    def start(self):
        pass
        
    @abstractmethod
    def stop(self):
        pass
        
    @abstractmethod
    def produce(self) -> Optional[Any]:
        pass

class BaseConsumer(ABC):
    @abstractmethod
    def start(self):
        pass
        
    @abstractmethod
    def stop(self):
        pass
        
    @abstractmethod
    def consume(self, message: Any):
        pass
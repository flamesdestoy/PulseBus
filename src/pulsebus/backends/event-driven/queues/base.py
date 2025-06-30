from abc import ABC, abstractmethod
from typing import Optional, Any

class BaseQueue(ABC):
    @abstractmethod
    def put(self, item: Any, block: bool = True, timeout: Optional[float] = None):
        pass
    
    @abstractmethod
    def get(self, block: bool = True, timeout: Optional[float] = None) -> Any:
        pass

    @abstractmethod
    def task_done(self) -> None:
        pass

    @abstractmethod
    def qsize(self) -> int:
        pass

    @abstractmethod
    def empty(self) -> bool:
        pass

    @abstractmethod
    def full(self) -> bool:
        pass

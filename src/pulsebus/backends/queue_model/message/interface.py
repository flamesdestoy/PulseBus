from abc import ABC, abstractmethod

class PoolableMessage(ABC):
    """ Interface for message objects, enforcing cloning and resetting capabilities """
    
    @abstractmethod
    def clone(self):
        pass

    @abstractmethod
    def reset(self):
        pass

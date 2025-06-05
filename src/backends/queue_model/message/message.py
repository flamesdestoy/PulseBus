import copy
from .interface import PoolableMessage

class MessageTemplate(PoolableMessage):
    """
    Represents a structured message instance with dynamic fields.
    Supports cloning to allow reuse of message structure.
    Implements IMessage interface.
    """

    def __init__(self):
        self._properties = {}

    def set_property(self, key, value):
        self._properties[key] = value

    def get_property(self, key):
        return self._properties.get(key)

    def clone(self):
        """
        Returns a deep copy of the message template.
        This enables reusing the message structure for new data instances.
        """
        return copy.deepcopy(self)

    def reset(self):
        """
        Resets the message by clearing all properties.
        Useful for reusing an object from the pool.
        """
        self._properties.clear()

    def to_dict(self):
        return dict(self._properties)
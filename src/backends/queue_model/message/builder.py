from .message import MessageTemplate

class MessageBuilder:
    """
    Builder class for step-by-step construction of a MessageTemplate.
    Enables users to define message structure and default values.
    """

    def __init__(self):
        self._template = MessageTemplate()

    def add_field(self, key, value=None):
        self._template.set_property(key, value)
        return self

    def build(self):
        """
        Finalizes the message structure and returns a clone of the template.
        The original remains unchanged for future use.
        """
        return self._template.clone()
# tests/test_message.py
import sys
import os

# Add the parent directory (which contains pulsebus/) to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import pytest

from src.backends.queue_model.message.builder import MessageBuilder
from src.backends.queue_model.message.message import MessageTemplate
from src.backends.queue_model.message.interface import PoolableMessage   # rename later if you accept the new name

def make_template():
    builder = MessageBuilder()
    return (
        builder
        .add_field("task_id", None)
        .add_field("progress", 0.0)
        .add_field("status", "idle")
        .build()
    )

def test_builder_and_clone():
    template = make_template()
    assert isinstance(template, MessageTemplate)

    clone1 = template.clone()
    clone2 = template.clone()

    # Each clone starts with the same defaults
    assert clone1.get_property("progress") == 0.0
    assert clone2.get_property("status")   == "idle"

    # Clones are independent
    clone1.set_property("progress", 50.0)
    assert clone2.get_property("progress") == 0.0

def test_reset():
    template = make_template()
    msg = template.clone()
    msg.set_property("status", "downloading")
    msg.reset()
    # After reset the property should be cleared (None) or back to default—adjust if you change logic
    assert msg.get_property("status") in (None, "idle")

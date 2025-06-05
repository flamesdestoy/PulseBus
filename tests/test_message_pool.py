import pytest
import sys
import os

# Add the parent directory (which contains pulsebus/) to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.backends.queue_model.message.builder import MessageBuilder
from src.backends.queue_model.pool.message_pool import MessagePool

@pytest.fixture(scope="module")
def message_pool():
    template = MessageBuilder().add_field("id", None).build()
    return MessagePool(template, max_size=2)

def test_acquire_and_release(message_pool):
    msg = message_pool.acquire()
    msg.set_property("id", 123)
    message_pool.release(msg)
    reused = message_pool.acquire()
    assert reused.get_property("id") is None

def test_clone_when_pool_empty(message_pool):
    m1 = message_pool.acquire()
    m2 = message_pool.acquire()
    assert m1 is not m2
    message_pool.release(m1)
    message_pool.release(m2)

def test_pool_reuse_object(message_pool):
    msg = message_pool.acquire()
    message_pool.release(msg)
    reused = message_pool.acquire()
    assert reused is msg

def test_max_capacity_throws_when_exceeded(message_pool):
    m1 = message_pool.acquire()
    m2 = message_pool.acquire()
    with pytest.raises(RuntimeError):
        message_pool.acquire(block=False)
    message_pool.release(m1)
    message_pool.release(m2)

import time
import threading
from pulsebus import MessageBuilder, MessagePool, MessageQueue, MessageTemplate

# This system simulates a download progress tracking where maximizing message throughput and continuous data transfer along with an expecting consumer take place. 

# Initialize workspace by 
#  - Creating a template message (for transferred data modeling)
#  - Creating a message pool (to have a fixed capacity storage for message management enabling high memory optimization)
#  - Initializing an intercommunication channel "MessageQueue" (for reliable sorted message transfer)

# ----------------------------- Program Cycle -------------------------------
# A producer thread fills up message templates continuously and pushes them to queue
# Consumer waits to be notified to retrieve any pushed message in the queue, Takes it and does whatever it wants with it
# This process continues in a cycle iteration until the producers stops

# ✅ 1. MessageTemplate (Blueprint)
# Describes the reusable fields each message should have
progress_template = (
    MessageBuilder()
        .add_field("task_id", None)
        .add_field("filename", None)
        .add_field("progress", 0.0)
        .add_field("speed", None)
        .build()
)

# ✅ 2. MessagePool
# A thread-safe object pool to reuse message objects instead of re-creating them.
# Good for performance and memory, especially under high load.
pool = MessagePool(template=progress_template, max_size=10)

# ✅ 3. MessageQueue
# A lightweight event bus that broadcasts messages to all registered handlers.
queue = MessageQueue()

# ✅ 4. Producer Thread
# Your logic that generates updates and pushes them into the queue.
# Always acquire a message from the pool, set properties, then publish.
def producer(task_id: str):
    for i in range(0, 101, 20):
        msg = pool.acquire()
        msg.set_property("task_id", task_id)
        msg.set_property("filename", f"{task_id}_file.dat")
        msg.set_property("progress", i)
        msg.set_property("speed", f"{5 + i}MB/s")
        queue.publish(msg)
        time.sleep(0.2)

# ✅ 5. Message Handler (Consumer)
# Subscribed to the queue. Automatically called when a new message is published.
# Must return the message back to the pool after processing.
def handle_message(msg: MessageTemplate):
    print(f"→ {msg.get_property('task_id')}: "
          f"{msg.get_property('filename')} at {msg.get_property('progress')}% "
          f"({msg.get_property('speed')})")
    pool.release(msg)

def main():
    # ✅ 6. Queue Lifecycle
    # You can subscribe multiple handlers. Shutdown stops all internal threads cleanly
    queue.subscribe(handle_message)
    threads = [
        threading.Thread(target=producer, args=("P1",)),
        threading.Thread(target=producer, args=("P2",))
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    time.sleep(0.5)
    queue.shutdown()

if __name__ == "__main__":
    main()

import argparse
import threading
import time
from pulsebus import MessageBuilder, MessagePool, MessageQueue, MessageTemplate

def producer_loop(pool, queue, label):
    for i in range(3):
        msg = pool.acquire()
        msg.set_property("label", label)
        msg.set_property("count", i)
        queue.publish(msg)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--producers", type=int, default=2)
    args = parser.parse_args()

    template = MessageBuilder().add_field("label", None).add_field("count", 0).build()
    pool = MessagePool(template=template, max_size=10)
    queue = MessageQueue()

    def handler(msg: MessageTemplate):
        print(f"CLI→ {msg.get_property('label')} - {msg.get_property('count')}")
        pool.release(msg)

    queue.subscribe(handler)
    threads = []
    for i in range(args.producers):
        t = threading.Thread(target=producer_loop, args=(pool, queue, f"P{i}"))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    time.sleep(0.5)
    queue.shutdown()

if __name__ == "__main__":
    main()

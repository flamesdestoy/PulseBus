# examples/flask_demo.py

from flask import Flask, jsonify
import threading
import time
from pulsebus import MessageBuilder, MessagePool, MessageQueue, MessageTemplate

app = Flask(__name__)

progress_template = MessageBuilder().add_field("status", None).build()
pool = MessagePool(template=progress_template, max_size=5)
queue = MessageQueue()

@app.route('/enqueue/<task>')
def enqueue(task):
    msg = pool.acquire()
    msg.set_property("status", f"Task {task} started")
    queue.publish(msg)
    return jsonify(success=True)

@app.route('/shutdown')
def shutdown():
    queue.shutdown()
    return jsonify(shutdown=True)

def consumer():
    def handler(m: MessageTemplate):
        print("[Flask-Consumer] ", m.get_property("status"))
        pool.release(m)
    queue.subscribe(handler)

if __name__ == "__main__":
    threading.Thread(target=consumer, daemon=True).start()
    app.run(debug=True)

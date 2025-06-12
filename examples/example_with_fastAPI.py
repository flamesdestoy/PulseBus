# examples/fastapi_demo.py

from fastapi import FastAPI, BackgroundTasks
import uvicorn
from pulsebus import MessageBuilder, MessagePool, MessageQueue, MessageTemplate

app = FastAPI()
progress_template = MessageBuilder().add_field("msg", None).build()
pool = MessagePool(template=progress_template, max_size=5)
queue = MessageQueue()

def background_consumer():
    def handler(m: MessageTemplate):
        print("[FastAPI-Consumer] ", m.get_property("msg"))
        pool.release(m)
    queue.subscribe(handler)

@app.on_event("startup")
def on_startup():
    queue.subscribe(lambda m: queue)  # wake subscriber system
    background_consumer()

@app.get("/send/{text}")
def send(text: str):
    msg = pool.acquire()
    msg.set_property("msg", text)
    queue.publish(msg)
    return {"sent": text}

@app.get("/stop")
def stop():
    queue.shutdown()
    return {"stopped": True}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)

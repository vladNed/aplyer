import logging
import time
from aplyer import AplyerApp

logging.basicConfig(level=logging.DEBUG)

app = AplyerApp("test")


@app.task(queue="alerts")
async def hello(name: str | None = None):
    time.sleep(10)
    print(f"Hello, {name or 'World'} !!")


@app.task(queue="numbers")
async def add(x: int, y: int):
    return x + y

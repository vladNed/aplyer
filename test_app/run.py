import asyncio
from .app import main


async def mainer():
    await main.hello.send("vlad1")
    await main.hello.send("vlad2")
    await main.add.send(12, 23)


asyncio.run(mainer())

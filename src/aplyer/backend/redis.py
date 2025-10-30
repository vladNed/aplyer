import logging
from typing import Any, AsyncGenerator, Awaitable

import redis.asyncio as redis
from redis import exceptions as redis_exc

from aplyer import exceptions
from aplyer.tasks import message

from . import base

logger = logging.getLogger(__name__)


class RedisPubSubBackend(base.IBackend):
    """Backend where messages are living inside redis."""

    def __init__(self, redis_url: str):
        self.client = redis.Redis.from_url(redis_url)
        self.pubsub = self.client.pubsub()

    async def enqueue(self, msg: message.Task) -> None:
        await self.client.publish(msg.queue, msg.serialize())

    async def consume(self, queues: set[str]) -> AsyncGenerator[Any, Any]:
        self.queues = queues or {"default"}
        await self.pubsub.subscribe(*self.queues)

        async for msg in self.pubsub.listen():
            if msg["type"] == "message":
                yield msg["data"]

    async def aclose(self):
        logger.debug("Closing redis backend connection...")
        await self.client.aclose()
        logger.debug("Connection redis backend closed.")


class RedisListQueueBackend(base.IBackend):
    """List Queue backend has message persistency.

    Messages that are received while no listener is waiting are not discarded,
    but kep until a new listener consumes the messages.
    """

    def __init__(self, redis_url: str):
        self.client = redis.Redis.from_url(redis_url)

    async def enqueue(self, msg: message.Task) -> None:
        task = self.client.lpush(f"{msg.queue}", msg.serialize())
        if isinstance(task, Awaitable):
            await task

    async def consume(self, queues: set[str]) -> AsyncGenerator[Any, Any]:
        """Starts the queue consumer.

        :param queues: A list of queue names [ "default", "my_queue" ]
        :raises exceptions.BackendConnectionError: If the backend is not reachable
        :returns: An async generator
        """
        logger.info("Listening for messages from %s", queues)

        while True:
            try:
                result = self.client.brpop(list(queues), timeout=0)
                if isinstance(result, Awaitable):
                    _, data = await result
                else:
                    _, data = result

                yield data
            except TypeError:
                logger.warning("Could not unpack received message")
            except redis_exc.ConnectionError as ex:
                logger.error("Could not connect to backend", exc_info=ex)
                raise exceptions.BackendConnectionError("Backend is not reachable")

    async def aclose(self):
        logger.debug("Closing redis backend connection...")
        await self.client.aclose()
        logger.debug("Connection redis backend closed.")

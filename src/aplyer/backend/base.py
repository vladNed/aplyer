from abc import ABC, abstractmethod
from typing import Any, AsyncIterator

from aplyer.tasks import message


class IBackend(ABC):
    """Abstract backend interface for processing task messages."""

    @abstractmethod
    async def enqueue(self, msg: message.Task) -> None:
        """Adds a message to the queue"""
        ...

    @abstractmethod
    def consume(self, queues: set[str]) -> AsyncIterator[Any]:
        """Consumes messages from the queue as an interator"""
        ...

    @abstractmethod
    async def aclose(self) -> None:
        """Closes the backend client connection"""
        ...

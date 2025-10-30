import asyncio
import functools
import logging
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

from aplyer import backend, exceptions, tasks

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R", covariant=True)


class TaskWithSend(Protocol[P, R]):
    """Protocol for a coroutine function that has a `.send` method attached."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]: ...

    send: Callable[P, Coroutine[Any, Any, None]]


@dataclass
class AplyerApp:
    name: str
    queues: set[str] = field(default_factory=set)
    registry: dict[str, Callable[..., Awaitable[Any]]] = field(default_factory=dict)

    def task(self, queue: str = "default"):
        """Wrapper over a function to register it as a task to be ran by the
        application within the worker code.

        :param queue: Define on what queue this will run. Defaults to `default`
        queue
        """

        self.queues.add(queue)

        def task_wrapper(func: Callable[P, Awaitable[R]]):

            if not asyncio.iscoroutinefunction(func):
                raise exceptions.TaskNotAwaitable(
                    f"{func.__module__}.{func.__name__} is not awaitable"
                )

            task_full_name = f"{func.__module__}.{func.__name__}"
            self.registry[task_full_name] = func

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                result = await func(*args, **kwargs)
                return result

            async def send(*args, **kwargs):
                """Sends a message task enqued to the task queue and will be
                processed by the workers.
                """

                new_task = tasks.message.Task(
                    name=task_full_name,
                    queue=queue,
                    args=args,
                    kwargs=kwargs,
                )
                logger.info(
                    "Sending task=%s id=%s to queue=%s",
                    task_full_name,
                    new_task.id,
                    queue,
                )

                await backend.DefaultBackend.enqueue(new_task)

            wrapper_with_send = cast(TaskWithSend[P, R], wrapper)
            wrapper_with_send.send = send

            return wrapper_with_send

        return task_wrapper

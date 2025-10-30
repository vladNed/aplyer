import asyncio
import dataclasses
import logging
from concurrent.futures import ThreadPoolExecutor

from aplyer import app, backend, exceptions, tasks

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class AplyerWorker:
    worker_id: int

    # Tasks
    app: app.AplyerApp
    backend: backend.IBackend
    queues: list[str]

    # Async Worker
    async_workers: int
    _active_tasks: set[asyncio.Task] = dataclasses.field(default_factory=set)
    _thread_pool: ThreadPoolExecutor = dataclasses.field(
        default_factory=ThreadPoolExecutor
    )

    def __post_init__(self):
        object.__setattr__(self, "_thread_pool", ThreadPoolExecutor(self.async_workers))

    async def run(self):
        """Runs the aplyer worker.

        All the async workers are generated based on the task load read from
        the list queue.

        :raises exceptions.BackendConnectionError: If the worker cannot read
        from the queue due to the backend being unreachable
        """
        listen_queues = [queue for queue in self.app.queues if queue in self.queues]
        loop = asyncio.get_event_loop()
        async for msg in self.backend.consume(set(listen_queues)):
            try:
                new_task = tasks.message.Task.deserilize(msg)
            except Exception as ex:
                logger.error("Could not parse new task.", exc_info=ex)
                continue

            logger.debug("Received new task %s", new_task)
            loop.run_in_executor(self._thread_pool, self._task_runner, new_task)

    def _task_runner(self, task: tasks.message.Task):
        asyncio.run(self._process_task(task))

    async def _process_task(
        self,
        task: tasks.message.Task,
    ) -> None:
        """Processes a received task message.

        This is also responsible for retrying a task if it fails and the task
        was configured for.

        Also saves the result in a backend that is a persistent storage such as
        Redis.

        :param task: A processed task message

        :returns: Nothing
        """
        func = self.app.registry.get(task.name)
        if func is None:
            raise exceptions.TaskNotRegistered(f"Task {task.name} is not registered")

        logger.info(
            "Running id=%s name=%s args=%s kwargs=%s",
            task.id,
            task.name,
            task.args,
            task.kwargs,
        )
        try:
            result = await func(*task.args, **task.kwargs)
        except Exception as ex:
            logger.error("Task id=%s failed.", task.id, exc_info=ex)
            return
        else:
            logger.info(
                "Task id=%s finished successfully.\nResult: %s",
                task.id,
                result,
            )
        finally:
            current_task = asyncio.current_task()
            if current_task:
                self._active_tasks.discard(current_task)

    async def close(self):
        logger.info(
            "W=%s Closing tasks %d", self.worker_id, len(self._thread_pool._threads)
        )
        self._thread_pool.shutdown(cancel_futures=True, wait=False)
        await self.backend.aclose()

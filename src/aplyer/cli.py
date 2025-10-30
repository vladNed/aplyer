import asyncio
import importlib
import logging
import multiprocessing
import os
import random
import signal
import sys
import types
from multiprocessing.synchronize import Event as EventClass
from pathlib import Path
from typing import Annotated, List

import typer

from aplyer import AplyerApp, backend, worker

# Ensure project root is on sys.path for dynamic imports
sys.path.append(str(Path(__file__).parents[2].resolve()))


LOGGING_FORMAT = (
    "[%(asctime)s] [PID %(process)d] [%(name)s] [%(levelname)s] %(message)s"
)
HANDLED_SIGNALS: set[signal.Signals] = {signal.SIGINT, signal.SIGTERM}

logging.basicConfig(level=logging.DEBUG, format=LOGGING_FORMAT)
app = typer.Typer(
    name="Aplyer CLI",
    help="Aplyer CLI: Runner for distributed task manager",
)

logger = logging.getLogger(__name__)


def _get_aplyer_app_instance(app_path: str) -> AplyerApp:
    app_module, _, app_name = app_path.rpartition(":")
    if not app_module or not app_name:
        raise typer.BadParameter("Expected path format: module:attribute")

    module = importlib.import_module(app_module)
    app_instance = getattr(module, app_name)
    if not isinstance(app_instance, AplyerApp):
        raise typer.BadParameter("Not an AplyerApp instance given to run")

    return app_instance


async def run_worker(
    app_worker: worker.AplyerWorker,
    event: EventClass,
):
    try:
        await app_worker.run()
    except asyncio.CancelledError:
        logger.debug("Worker cancelled, shutting down...")
        await app_worker.close()
        raise
    finally:
        if not event.is_set():
            event.set()
        sys.exit(1)


def worker_boot(
    app_instance: AplyerApp,
    worker_id: int,
    queues: list[str],
    async_workers: int,
    event: EventClass,
):
    """Boots up a worker"""
    random.seed()
    logger.info(f"Booting worker id={worker_id} async.")

    app_worker = worker.AplyerWorker(
        worker_id,
        app_instance,
        backend.DefaultBackend,
        queues or ["default"],
        async_workers=async_workers,
    )

    worker_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(worker_loop)
    task = worker_loop.create_task(run_worker(app_worker, event))

    def handle_sigint():
        logger.info("Worker id=%s interrupted by SIGINT, shutting down...", worker_id)
        for task in asyncio.all_tasks(worker_loop):
            task.cancel()

        logger.debug("Worker async tasks cancelled.")

    def handle_sigterm():
        logger.info(
            "Worker id=%s interrupted by SIGTERM, softly shutting down...", worker_id
        )
        task.cancel()

    worker_loop.add_signal_handler(signal.SIGINT, handle_sigint)
    worker_loop.add_signal_handler(signal.SIGTERM, handle_sigterm)
    try:
        worker_loop.run_until_complete(task)
    except KeyboardInterrupt:
        logger.info("Worker id=%s interrupted by SIGINT", worker_id)
        sys.exit(1)


@app.command()
def run(
    app: Annotated[str, typer.Argument(help="e.g module.main:instance")],
    queues: Annotated[List[str], typer.Option(..., "--queues", "-q")] = ["default"],
    workers: Annotated[int, typer.Option(..., "--workers", "-w")] = 4,
    async_workers: Annotated[int, typer.Option(..., "--async-workers", "-aw")] = 100,
) -> None:
    app_instance = _get_aplyer_app_instance(app)
    worker_processes = []
    worker_events = []
    for worker_id in range(workers):
        proc_event = multiprocessing.Event()
        proc = multiprocessing.Process(
            target=worker_boot,
            args=(
                app_instance,
                worker_id,
                queues,
                async_workers,
                proc_event,
            ),
            daemon=False,
        )
        proc.start()
        worker_processes.append(proc)
        worker_events.append(proc_event)

    def stop_all_workers(signum: signal.Signals) -> None:
        """Stops a process by sending a signum"""
        for proc in worker_processes:
            if not proc.is_alive():
                continue

            try:
                os.kill(proc.pid, signum)
            except OSError:
                if proc.exitcode is None:
                    logger.warning("Failed to send %r to PID %s", signum.name, proc.pid)

        for event in worker_events:
            event.set()

    def global_signal_handler(signum: int, frame: types.FrameType | None) -> None:
        signum = signal.Signals(signum)

        logger.info("Sending signal %s to workers...", signum.name)
        stop_all_workers(signum)
        sys.exit(15)

    for sig in HANDLED_SIGNALS:
        signal.signal(sig, global_signal_handler)

    # Wait for all workers to terminate. In an event where one
    # of the workers has stopped working it is generally better
    # to terminate all of them
    while not any(event.is_set() for event in worker_events):
        for proc in worker_processes:
            proc.join(timeout=1)
            if proc.exitcode is None:
                continue

            logger.info(
                "Worker PID %r stopped working (code %r). Shutting all down...",
                proc.pid,
                proc.exitcode,
            )
            match proc.exitcode:
                case 0:
                    break
                case 15:
                    stop_all_workers(signum=signal.SIGTERM)
                case _:
                    stop_all_workers(signum=signal.SIGINT)
            break

    # Still, wait and make sure all processes have finished
    for proc in worker_processes:
        proc.join()

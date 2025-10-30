# Aplyer

Aplyer is a small, lightweight distributed task runner that uses Redis as a backend.  
It's designed for simple async task registration and dispatching with a minimal API.

## Features
- Register async tasks with [`AplyerApp`](src/aplyer/app.py) and call `.send(...)` to enqueue tasks.
- Message serialization with MessagePack using the [`Task`](src/aplyer/tasks/message.py) model.
- Redis-backed queue implementations (pub/sub and list queue) — see [`RedisListQueueBackend`](src/aplyer/backend/redis.py) and the configured [`DefaultBackend`](src/aplyer/backend/__init__.py).
- Worker runtime implemented in [`AplyerWorker`](src/aplyer/worker.py).
- CLI to run workers in multiple processes: see [`app`](src/aplyer/cli.py).

## Quickstart

### 1. Install
```sh
pip install -e .
```
(see project configuration in [pyproject.toml](pyproject.toml)).

### 2. Start Redis (example via Docker)
```sh
docker compose up -d
# or
docker-compose up -d
```
(see [docker-compose.yaml](docker-compose.yaml))

### 3. Define tasks
- See example app: [test_app/app/main.py](test_app/app/main.py)
- Register tasks using `AplyerApp.task()` — implementation: [`AplyerApp`](src/aplyer/app.py)

### 4. Send tasks
- From your code call the generated `.send(...)` coroutine on the registered task (example in [test_app/run.py](test_app/run.py)).

### 5. Run workers
- Use the bundled CLI to boot worker processes:
```sh
aplyer run module.path:app_instance --queues alerts --workers 2 --async-workers 50
```
(see CLI implementation: [`app`](src/aplyer/cli.py))

## Testing
- Unit tests exist for message serialization: [tests/test_messages.py](tests/test_messages.py)
- Run tests with:
```sh
pytest
```

## Reference
- Task model and serialization: [`Task`](src/aplyer/tasks/message.py)
- App/task registration and `.send`: [`AplyerApp`](src/aplyer/app.py)
- Worker runtime: [`AplyerWorker`](src/aplyer/worker.py)
- Backend factory / default: [`DefaultBackend`](src/aplyer/backend/__init__.py)
- Redis backends: [`RedisListQueueBackend`](src/aplyer/backend/redis.py)

## Contributing
- Open issues or PRs with focused changes.
- Keep changes small, add tests for new behavior.

## License
- MIT (see `pyproject.toml`)

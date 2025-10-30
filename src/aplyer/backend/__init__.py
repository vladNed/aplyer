from enum import Enum

from .base import IBackend
from .redis import RedisListQueueBackend


class BackendType(Enum):
    REDIS = "redis"


class BackendFactory:

    @classmethod
    def create(cls, backend_type: BackendType) -> IBackend:
        match backend_type:
            case BackendType.REDIS:
                # TODO: Add this to settings
                return RedisListQueueBackend(
                    redis_url="redis://localhost:6379",
                )
            case _:
                raise RuntimeError("Unkown backend type")


DefaultBackend = BackendFactory.create(BackendType.REDIS)

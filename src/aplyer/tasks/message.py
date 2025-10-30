import logging
from typing import Any, Self, cast
from uuid import UUID, uuid4

import msgpack
from pydantic import BaseModel, Field, field_serializer

from aplyer import exceptions

logger = logging.getLogger(__name__)


class Schema(BaseModel):
    """Interface for serializing and deserializing dataclasses"""

    def serialize(self) -> bytes:
        data = self.model_dump(serialize_as_any=True)
        try:
            payload = cast(bytes, msgpack.packb(data, use_bin_type=True))
        except TypeError as err:
            logger.error("Invalid type used on task data")
            raise exceptions.InvalidTaskData(err)

        return payload

    @classmethod
    def deserilize(cls, data: bytes) -> Self:
        raw_data: dict = msgpack.unpackb(data, raw=False)
        return cls.model_validate(raw_data)


class Task(Schema):
    name: str
    queue: str
    id: UUID = Field(default_factory=uuid4)
    args: tuple
    kwargs: dict[str, Any]

    @field_serializer("id")
    def serialize_id(self, v: UUID) -> str:
        return str(v)

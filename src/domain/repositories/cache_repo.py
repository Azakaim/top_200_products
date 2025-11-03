from abc import ABC, abstractmethod
from typing import Any


class CacheRepository(ABC):
    @abstractmethod
    async def set(self, key: str, value: Any, ex: int | None = None) -> None:
        pass

    @abstractmethod
    async def get(self, key: str):
        pass

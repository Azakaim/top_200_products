from typing import Any, Optional

import redis.asyncio as aioredis
from pydantic import BaseModel, Field

from src.domain.repositories.cache_repo import CacheRepository


class Cache(BaseModel, CacheRepository):
    host: Optional[str] = Field(default="localhost")
    port: Optional[int] = Field(default=6379)
    decode_resp: Optional[bool] = Field(default=True)
    db: Optional[int] = Field(default=0) # логическая секция редис до 15 штук как листы в гугл таблице
    _cli = None

    def model_post_init(self, __context) -> None:
        self._cli = aioredis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            decode_responses=self.decode_resp,
        )

    async def set(self, key: str, value: Any,nx: bool | None = None, ex: int | None = None):
        return await self._cli.set(name=key, value=value,nx=nx, ex=ex)

    async def get(self, key: str) -> Any | None:
        try:
            value = await self._cli.get(name=key)
            return value
        except (aioredis.ResponseError, TypeError):
            return None


cache = Cache()

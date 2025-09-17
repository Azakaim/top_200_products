import asyncio
import json
from collections.abc import Callable
from typing import Any, Optional, Awaitable

import redis.asyncio as aioredis
from pydantic import BaseModel, Field

from src.domain.repositories.cache_repo import CacheRepository
from src.mappers.transformation_functions import get_type_func


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

async def call_cache(func: Callable[...,Awaitable[Any]],
                     cache_cli: Cache = Cache | None,
                     acc_id: str | None = "",
                     key: str | None = "",
                     ttl: int | None = 5,
                     *args,
                     **kwargs) -> Any:
        # включаем id кабинета в ключ кеша
        cached = await cache_cli.get(key)
        if cached:
            # проверяем пайдентик ли это модель, если да помогаем вернуть функции ожидаемый объект
            return_type = await get_type_func(func)
            if return_type and issubclass(return_type, BaseModel):
                return return_type.model_validate(cached)
            # тут возврат строки
            return cached
        # для одного процесса кто первый возьмет лок-кей
        locked_key = f"{acc_id}:{key}" + ':lock'
        got_lock_key = cache_cli.set(locked_key, '1', nx=True, ex=ttl)
        if got_lock_key:
            result = await func(*args, **kwargs)
            await cache_cli.set(f"{acc_id}:{key}", json.dumps(result),ex=ttl)
            await cache_cli.delete(locked_key)
            return result
        else:
            while True:
                memory = await cache_cli.get(f"{acc_id}:{key}")
                if memory is not None:
                    return json.loads(memory)
                await asyncio.sleep(0.05)


cache = Cache()

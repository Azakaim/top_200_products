import asyncio

import httpx
from pydantic import BaseModel, PrivateAttr

from src.utils.limiter import RateLimiter


class BaseRateLimitedHttpClient(BaseModel):
    concurrency: int = 45  # количество параллельных запросов
    default_rps: int = 45  # дефолтный лимит
    base_url: str

    _sem: asyncio.Semaphore = PrivateAttr(default=None)  # семафор для ограничения параллельных запросов
    _limiters: dict[str, RateLimiter] = PrivateAttr(default_factory=dict)  # словарь лимитеров для каждого эндпоинта
    _client: httpx.AsyncClient = PrivateAttr(default=None)
    _timeout: float = PrivateAttr(default=None)  # таймаут для запросов
    _default_limiter: RateLimiter = PrivateAttr(default=None)  # лимитер по умолчанию для всех эндпоинтов

    def model_post_init(self, __context):
        self._sem = asyncio.Semaphore(self.concurrency)
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self._timeout,
            limits=httpx.Limits(max_keepalive_connections=100, max_connections=100),
            headers={},
        )
        self._default_limiter = RateLimiter(self.default_rps, 1.0)

    async def _limiter_for(self, endpoint: str) -> RateLimiter:
        # ищем точное совпадение или префикс
        return self._limiters.get(endpoint, self._default_limiter)

    async def aclose(self):
        await self._client.aclose()

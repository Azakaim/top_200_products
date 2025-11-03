import asyncio
from typing import Optional, Any

import httpx
from pydantic import BaseModel, PrivateAttr
from tenacity import AsyncRetrying, wait_exponential_jitter, stop_after_attempt, retry_if_exception_type

from src.schemas.ozon_schemas import APIError
from src.utils.limiter import RateLimiter, parse_retry_after_seconds


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
            limits=httpx.Limits(max_keepalive_connections=100, max_connections=100)
        )
        self._default_limiter = RateLimiter(self.default_rps, 1.0)

    async def _limiter_for(self, endpoint: str) -> RateLimiter:
        # ищем точное совпадение или префикс
        return self._limiters.get(endpoint, self._default_limiter)

    async def aclose(self):
        await self._client.aclose()

    async def request(self, method: str, endpoint: str, *, json: Optional[dict] = None, headers: Optional[dict]=None) \
            -> Any:
        limiter = await self._limiter_for(endpoint) # получаем лимитер для данного эндпоинта #TODO: убрать, если не нужно
        if limiter:
            await limiter.acquire()
        # если лимитер не задан, используем дефолтный
        else:
            await self._default_limiter.acquire()
        await self._sem.acquire()
        try:
            async for attempt in AsyncRetrying(
                wait=wait_exponential_jitter(initial=0.5, max=8.0),
                stop=stop_after_attempt(3),
                retry=retry_if_exception_type((httpx.TransportError, APIError)),
                reraise=True,
            ):
                with attempt:
                    resp = await self._client.request(method, endpoint, json=json, headers=headers)
                    # 2xx — ок
                    if 200 <= resp.status_code < 300:
                        return resp.json()
                    # 429 — подчиняемся Retry-After и бросаем ретраибл
                    if resp.status_code == 429:
                        delay = parse_retry_after_seconds(resp.headers, default=30.5)
                        await asyncio.sleep(delay)
                        raise APIError(resp.status_code, endpoint, resp.text)
                    if resp.status_code == 400:
                        # 400 — ошибка авторизации, не ретраим
                        return  APIError(resp.status_code, endpoint, resp.text)
                    if resp.status_code == 401:
                        # 401 — ошибка авторизации, не ретраим
                        return APIError(resp.status_code, endpoint, resp.text)
                    # 5xx — ретраим с экспонентой
                    if 500 <= resp.status_code < 600 or (resp.status_code == 400):
                        raise APIError(resp.status_code, endpoint, resp.text)
                    # 4xx (кроме 429) — логическая ошибка, не ретраим
                    raise APIError(resp.status_code, endpoint, resp.text)
        finally:
            self._sem.release()

import asyncio

import httpx
from pydantic import BaseModel, PrivateAttr

from src.utils.limiter import RateLimiter


class BaseRateLimitedHttpClient(BaseModel):
    """
    BaseRateLimitedHttpClient:
        Args:
            - base_url - требуемые аргументы
            - prod_uid_url - требуемые аргументы
            - stocks_url - требуемые аргументы
        """
    concurrency: int = 45  # количество параллельных запросов
    default_rps: int = 45  # дефолтный лимит
    base_url: str
    prod_uid_url: str
    stocks_url: str

    _sem: asyncio.Semaphore = PrivateAttr(default=None)  # семафор для ограничения параллельных запросов
    _limiters: dict[str, RateLimiter] = PrivateAttr({})  # словарь лимитеров для каждого эндпоинта
    _client: httpx.AsyncClient = PrivateAttr(None)
    _timeout: float = PrivateAttr()  # таймаут для запросов
    _default_limiter: RateLimiter = PrivateAttr(None)  # лимитер по умолчанию для всех эндпоинтов

    def model_post_init(self, __context):
        self._sem = asyncio.Semaphore(self.concurrency)
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self._timeout,
            limits=httpx.Limits(max_keepalive_connections=100, max_connections=100),
            headers={},
        )
        self._default_limiter = RateLimiter(self.default_rps, 1.0)

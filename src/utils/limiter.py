import asyncio
import time
from collections import deque
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, wait_exponential_jitter, stop_after_attempt


# --------- утилиты ---------

def parse_retry_after_seconds(headers: httpx.Headers, default: float = 1.0) -> float:
    ra = headers.get("Retry-After")
    if not ra:
        return default
    try:
        # вариант "число секунд"
        return float(ra)
    except ValueError:
        # вариант "дата"
        try:
            dt = parsedate_to_datetime(ra)
            return max(0.0, (dt - parsedate_to_datetime(time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()))).total_seconds())
        except Exception:
            return default


class RateLimiter:
    """Простой лимитер 'rate per period' cо скользящим окном для asyncio."""
    def __init__(self, rate: int, period: float = 1.0):
        self.rate = rate
        self.period = period
        self._hits: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        # ждем, пока не освободится слот в лимитере
        async with self._lock:
            now = time.monotonic() # текущее время в секундах с начала эпохи не зависит от времени локального на машине
            # выкидываем устаревшие события
            while self._hits and now - self._hits[0] >= self.period:
                self._hits.popleft() # удаляем первый элемент из очереди, если он старше периода
            # если еще есть свободные слоты, то просто добавляем новый хит
            if len(self._hits) < self.rate:
                self._hits.append(now) # фиксируем новый хит
                return
            # нужно подождать до освобождения первого слота
            sleep_for = self.period - (now - self._hits[0])
            # после сна — фиксируем новый хит
            self._hits.append(time.monotonic())
        await asyncio.sleep(max(0.0, sleep_for)) # ждем, пока не освободится слот

# --------- доменные сущности ---------


# --------- сам клиент ---------

# class OzonAsyncClient:
#     BASE_URL = "https://api-seller.ozon.ru"
#
#     def __init__(
#         self,
#         account: Account,
#         *,
#         concurrency: int = 8,
#         default_rps: int = 8,      # дефолтный лимит на аккаунт
#         per_endpoint_rps: Optional[Dict[str, int]] = None,  # например: {"/v2/product/info": 5}
#         timeout: float = 30.0,
#     ):
#         self.account = account
#         self._sem = asyncio.Semaphore(concurrency)
#         self._timeout = timeout
#         self._client = httpx.AsyncClient(
#             base_url=self.BASE_URL, timeout=timeout,
#             limits=httpx.Limits(max_keepalive_connections=100, max_connections=100),
#             headers={
#                 "Client-Id": account.client_id,
#                 "Api-Key": account.api_key,
#                 "Content-Type": "application/json",
#             },
#         )
#         self._limiters: Dict[str, RateLimiter] = {}
#         self._default_limiter = RateLimiter(default_rps, 1.0)
#         if per_endpoint_rps:
#             for ep, rps in per_endpoint_rps.items():
#                 self._limiters[ep] = RateLimiter(rps, 1.0)
#
#     def _limiter_for(self, endpoint: str) -> RateLimiter:
#         # ищем точное совпадение или префикс
#         return self._limiters.get(endpoint, self._default_limiter)
#     # --- примеры метод-обёрток ---
#
#
#
#     async def list_fbo(self, since_iso: str, to_iso: str, *, limit: int = 1000):
#         endpoint = "/v2/posting/fbo/list"
#         offset = 0
#         while True:
#             body = {"dir": "asc", "filter": {"since": since_iso, "to": to_iso, "status": ""}, "limit": limit, "offset": offset}
#             data = await self.request("POST", endpoint, json=body)
#             postings = data.get("result", []) or []
#             if not postings:
#                 break
#             yield postings
#             if len(postings) < limit:
#                 break
#             offset += len(postings)
#
#     async def aclose(self):
#         await self._client.aclose()

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

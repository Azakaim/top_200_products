import asyncio
from typing import Dict, Optional, Any, ClassVar, Callable, Awaitable
from wsgiref import headers

from more_itertools import chunked
from pydantic import BaseModel, PrivateAttr, ValidationError

from src.clients.ozon.schemas import (OzonAPIError, PostingRequestSchema, StatusDelivery, SkusRequestShema,
                                      FilterProducts, FilterPosting, AnalyticsRequestSchema, AnalyticsResponseSchema,
                                      Datum)
from src.mappers.transformation_functions import parse_articles, parse_remainders
from src.utils.limiter import RateLimiter, parse_retry_after_seconds

from tenacity import AsyncRetrying, retry_if_exception_type, wait_exponential_jitter, stop_after_attempt
import httpx


class OzonClient(BaseModel):
    concurrency: int = 45 # количество параллельных запросов
    default_rps: int = 45 # дефолтный лимит на аккаунт
    base_url: str
    fbs_reports_url: str
    fbo_reports_url: str
    remain_url: str
    products_url: str
    products_whole_info_url: str
    analytics_url: str

    _sem: asyncio.Semaphore = PrivateAttr(default=None) # семафор для ограничения параллельных запросов
    _limiters: Dict[str, RateLimiter] = PrivateAttr({}) # словарь лимитеров для каждого эндпоинта
    _client: httpx.AsyncClient = PrivateAttr(None)
    _per_endpoint_rps: Optional[Dict[str, int]] = PrivateAttr(default_factory=dict) # например: {"/v2/product/info": 5}
    _timeout: float = PrivateAttr(30.0) # таймаут для запросов к Ozon API
    _default_limiter: RateLimiter = PrivateAttr(None) # лимитер по умолчанию для всех эндпоинтов

    STATUS_DELIVERY: ClassVar = [
        StatusDelivery.AWAITING_REGISTRATION.value,
        StatusDelivery.ACCEPTANCE_IN_PROGRESS.value,
        StatusDelivery.AWAITING_APPROVE.value,
        StatusDelivery.AWAITING_PACKAGING.value,
        StatusDelivery.AWAITING_DELIVER.value,
        StatusDelivery.ARBITRATION.value,
        StatusDelivery.CLIENT_ARBITRATION.value,
        StatusDelivery.DELIVERING.value,
        StatusDelivery.DRIVER_PICKUP.value,
        StatusDelivery.DELIVERED.value,
        StatusDelivery.NOT_ACCEPTED.value,
    ]

    def model_post_init(self, __context):
        self._sem = asyncio.Semaphore(self.concurrency)
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self._timeout,
            limits=httpx.Limits(max_keepalive_connections=100, max_connections=100),
            headers={},
        )
        self._default_limiter = RateLimiter(self.default_rps, 1.0)
        self._per_endpoint_rps[self.analytics_url] = 1
        # инициализируем лимитеры для каждого эндпоинта #TODO: убрать, если не нужен лимиттер для каждого эндпоинта
        if self._per_endpoint_rps:
            for ep, rps in self._per_endpoint_rps.items():
                self._limiters[ep] = RateLimiter(rps, 60.0)

    async def _limiter_for(self, endpoint: str) -> RateLimiter:
        # ищем точное совпадение или префикс
        return self._limiters.get(endpoint, self._default_limiter)

    async def request(self, method: str, endpoint: str, *, json: Optional[dict] = None, headers: Optional[dict]=None) -> Any:
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
                stop=stop_after_attempt(6),
                retry=retry_if_exception_type((httpx.TransportError, OzonAPIError)),
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
                        raise OzonAPIError(resp.status_code, endpoint, resp.text)
                    if resp.status_code == 401:
                        # 401 — ошибка авторизации, не ретраим
                        not_auth = OzonAPIError(resp.status_code, endpoint, resp.text)
                        return { "error": "Unauthorized", "details": str(not_auth) }
                    # 5xx — ретраим с экспонентой
                    if 500 <= resp.status_code < 600 or (resp.status_code == 400):
                        raise OzonAPIError(resp.status_code, endpoint, resp.text)
                    # 4xx (кроме 429) — логическая ошибка, не ретраим
                    raise OzonAPIError(resp.status_code, endpoint, resp.text)
        finally:
            self._sem.release()

    async def __build_remain_payload(self, skus: list) -> dict:
        return { "skus": skus }

    async def __build_sku_payload(self, skus: list) -> dict:
        return { "offer_id": skus, "product_id": [], "sku": [] }

    async def __manage_batches(self,
                               endpoint: str,
                               batches: list,
                               batch_size: int,
                               headers: dict,
                               payload_builder: Callable[[list],Awaitable[dict]]) -> list:
        bodies = []
        for batch in chunked(batches, batch_size):
            # создаем необходимое тело запроса
            payload = await payload_builder(batch)
            resp = await self.request("POST", endpoint, json=payload, headers=headers)
            if resp:
                bodies.extend(resp["items"])
        return bodies

    async def get_skus(self, *, headers: Optional[dict]=None) -> list:
        articles = await self.__get_articles(headers=headers)
        skus = await self.__manage_batches(self.products_whole_info_url,
                                           articles,
                                           1000,
                                           headers,
                                           self.__build_sku_payload)
        return skus

    async def __get_articles(self, *, headers: Optional[dict]=None)-> list:
        articles = []
        last_id = ""
        while len(articles) % 1000 == 0:
            _filter = FilterProducts()
            _data = SkusRequestShema(filter=_filter,
                                    last_id=last_id,
                                    limit=1000)
            json = _data.model_dump()
            resp = await self.request("Post",
                                      self.products_url,
                                      json=json,
                                      headers=headers)
            acc_articles, last_id, total = await parse_articles(resp)
            articles.extend(acc_articles)
            if total < 1000 or len(articles) == total:
                break
        return articles

    async def fetch_remainders(self, skus: list[str], headers: Optional[dict]=None):
        bodies = await self.__manage_batches( self.remain_url,
                                              skus,
                                              100,
                                              headers,
                                              payload_builder= self.__build_remain_payload)
        return await parse_remainders(bodies)

    async def receive_analytics_data(self, analyt_body: AnalyticsRequestSchema, headers: Optional[dict]=None) \
            -> list[Datum]:
        analytics_data = []
        while True:
            try:
                resp = await self.request("POST", self.analytics_url, json=analyt_body.to_dict(), headers=headers)
                parsed_resp = AnalyticsResponseSchema(**resp) if resp else None
            except ValidationError as e:
                break
                raise Exception(e)
            if parsed_resp:
                if parsed_resp.result.data:
                    count = len(parsed_resp.result.data)
                    if count < 1000:
                        analytics_data.extend(parsed_resp.result.data)
                        break
                    else :
                        analyt_body.offset += 1000
                        analytics_data.extend(parsed_resp.result.data)
            else:
                break
        return analytics_data

    async def generate_reports(self, delivery_way: str,
                               since: str,
                               to: str,
                               *,
                               limit: int = 1000,
                               headers: Optional[dict]=None):
        """
        Получает отчет FBS с Ozon API.

        :param delivery_way:
        :param since: Start date in ISO 8601 format (e.g., "2025-10-01T00:00:00Z").
        :param to: End date in ISO 8601 format (e.g., "2025-10-01T00:00:00Z").
        :param limit: Number of records to fetch.
        :param headers: dict
        :return: JSON response from the Ozon API.
        """
        if delivery_way == "FBS":
            url = self.fbs_reports_url
        else:
            url = self.fbo_reports_url
        offset = 0
        while True:
            status_delivery = self.STATUS_DELIVERY
            filter_req = FilterPosting(status_alias=status_delivery, since=since, to=to)
            body_req = PostingRequestSchema(dir="asc", filter=filter_req, limit=limit, offset=offset)
            # Выполняем запрос к Ozon API
            data = await self.request("POST", url,
                                      json=body_req.model_dump(by_alias=True, exclude_none=True),
                                      headers=headers)
            result = data.get("result", {})

            if delivery_way == "FBS":
                postings = result.get("postings", []) or []
            else:
                postings = result

            if not postings:
                break
            yield postings
            # для FBO
            if isinstance(result, list):
                if len(result) < limit:
                    break
            else:
                if not result.get("has_next"):
                    break
            offset += len(postings)

    async def aclose(self):
        await self._client.aclose()

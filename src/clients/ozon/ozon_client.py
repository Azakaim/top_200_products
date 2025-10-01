import asyncio
import logging
from typing import Dict, Optional, Any, ClassVar, Callable, Awaitable

from more_itertools import chunked
from pydantic import PrivateAttr, ValidationError

from src.schemas.ozon_schemas import (APIError, PostingRequestSchema, StatusDelivery,
                                      FilterPosting, AnalyticsRequestSchema, AnalyticsResponseSchema,
                                      Datum, ArticlesResponseShema)
from src.schemas.ozon_schemas import FilterProducts, SkusRequestShema
from src.utils.http_base_client import BaseRateLimitedHttpClient
from src.utils.limiter import RateLimiter, parse_retry_after_seconds

from tenacity import AsyncRetrying, retry_if_exception_type, wait_exponential_jitter, stop_after_attempt
import httpx

log = logging.getLogger("ozon client")

class OzonClient(BaseRateLimitedHttpClient):
    base_url: str
    fbs_reports_url: str
    fbo_reports_url: str
    remain_url: str
    products_url: str
    products_whole_info_url: str
    analytics_url: str

    _per_endpoint_rps: Optional[Dict[str, int]] = PrivateAttr(default_factory=dict) # например: {"/v2/product/info": 5}

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
        super().model_post_init(__context)
        self._per_endpoint_rps[self.analytics_url] = 1
        # инициализируем лимитеры для каждого эндпоинта #TODO: убрать, если не нужен лимиттер для каждого эндпоинта
        if self._per_endpoint_rps:
            for ep, rps in self._per_endpoint_rps.items():
                self._limiters[ep] = RateLimiter(rps, 60.0)

    async def __parse_articles(self,articles_data: dict) -> tuple:
        """
        :param articles_data: dict
        :return: tuple: articles, last_id, total
        """
        try:
            account_articles = ArticlesResponseShema(**articles_data)
        except (ValueError, OverflowError, TypeError) as e:
            raise Exception(e)
        return ([p.offer_id for p in account_articles.result.items],
                account_articles.result.last_id,
                account_articles.result.total)

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
            acc_articles, last_id, total = await self.__parse_articles(resp)
            articles.extend(acc_articles)
            if total < 1000 or len(articles) == total:
                break
        return articles

    async def get_skus(self, *, headers: Optional[dict]=None) -> list:
        articles = await self.__get_articles(headers=headers)
        skus = await self.__manage_batches(self.products_whole_info_url,
                                           articles,
                                           1000,
                                           headers,
                                           self.__build_sku_payload)
        return skus

    async def fetch_remainders(self, skus: list[str], headers: Optional[dict]=None):
        bodies = await self.__manage_batches( self.remain_url,
                                              skus,
                                              100,
                                              headers,
                                              payload_builder= self.__build_remain_payload)
        return bodies

    async def receive_analytics_data(self, analyt_body: AnalyticsRequestSchema, headers: Optional[dict]=None) \
            -> list[Datum]:
        analytics_data = []
        while True:
            try:
                resp = await self.request("POST", self.analytics_url, json=analyt_body.to_dict(), headers=headers)
                parsed_resp = AnalyticsResponseSchema(**resp) if resp else None
            except (ValidationError, TypeError) as e:
                break
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
            filter_req = FilterPosting(since=since, to=to)
            body_req = PostingRequestSchema(dir="ASC", filter=filter_req, limit=limit, offset=offset)
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

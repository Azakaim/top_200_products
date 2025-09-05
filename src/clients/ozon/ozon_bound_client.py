from typing import Optional

from src.clients.ozon.ozon_client import OzonClient
from src.clients.ozon.schemas import AnalyticsRequestSchema, Datum


class OzonCliBound:
    def __init__(self, base: OzonClient,
                 headers: dict[str,str]):
        self._base = base
        self._headers = headers

    async def request(self, method: str, endpoint: str, *, json: Optional[dict]=None):
        return await self._base.request(method, endpoint, json=json, headers=self._headers)

    async def fetch_remainders(self, skus: list[str], headers: Optional[dict]=None):
        return await self._base.fetch_remainders(skus, headers=self._headers or headers)

    async def generate_reports(self, delivery_way: str,
                               since: str,
                               to: str, *,
                               limit: int = 1000,
                               headers: Optional[dict]=None):
        async for chunk in self._base.generate_reports(delivery_way,
                                                       since,
                                                       to,
                                                       limit=limit,
                                                       headers=self._headers or headers):
            yield chunk

    async def get_skus(self, *, headers: Optional[dict]=None)-> list:
        return await self._base.get_skus(headers=self._headers or headers)

    async def receive_analytics_data(self, analyt_body: AnalyticsRequestSchema, headers: Optional[dict] = None) \
            -> list[Datum]:
        return await self._base.receive_analytics_data(analyt_body,self._headers or headers)

    async def aclose(self):
        await self._base.aclose()

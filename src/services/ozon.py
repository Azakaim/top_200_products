import asyncio
from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from src.clients.ozon.ozon_bound_client import OzonCliBound
from src.clients.ozon.schemas import AnalyticsRequestSchema, AnalyticsMetrics, Sort
from src.mappers import parse_postings
from src.mappers.transformation_functions import parse_skus


class OzonService(BaseModel):
    cli: Optional[OzonCliBound] = None

    model_config = {
        "arbitrary_types_allowed": True #По умолчанию, если засунуть в модель поле с пользовательским классом
                                        # (типа OzonCliBound), Pydantic выдаст ошибку — он не знает,
                                        # как валидировать этот тип.
    }

    async def __collect_reports(self, reports: list, gen):
        async for r in gen:
            postings = await parse_postings(r)
            reports.extend(postings)

    async def collect_skus(self):
        skus_data = await self.cli.get_skus()
        skus = await parse_skus(skus_data)
        return skus if skus else []

    async def fetch_postings(self, account_name: str, account_id: str, date_since: str, date_to: str) -> dict:
        """
        Fetch postings from Ozon API based on delivery way and date range.

        :param date_to:
        :param date_since:
        :param account_id:
        :param account_name:
        :return: List of postings.
        """
        postings = {}
        print(f"Обработка аккаунта: {account_name} (ID: {account_id})")
        acc_name_method_fbs = f"{account_name}_FBS"
        acc_name_method_fbo = f"{account_name}_FBO"
        postings[acc_name_method_fbs] = []
        postings[acc_name_method_fbo] = []

        # Получаем отчеты
        tasks = [
            # Получаем отчеты FBS
            self.__collect_reports(reports=postings[acc_name_method_fbs],
                        gen=self.cli.generate_reports(delivery_way="FBS",
                                                       since=date_since,
                                                       to=date_to)),
            # Получаем отчеты FBO
            self.__collect_reports(reports=postings[acc_name_method_fbo],
                        gen=self.cli.generate_reports(delivery_way="FBO",
                                                       since=date_since,
                                                       to=date_to))
        ]
        await asyncio.gather(*tasks)
        return postings

    async def collect_analytics_data(self,month_name: str, date_since: datetime, date_to: datetime):
        """
        Метод можно использовать не более 1 р в минуту
        """
        metrics = [AnalyticsMetrics.REVENUE, AnalyticsMetrics.ORDERED_UNITS, AnalyticsMetrics.SESSION_VIEW_PDP]
        dimension = ["sku", "month"]
        sort = Sort(key=AnalyticsMetrics.REVENUE,order="DESC")
        body = AnalyticsRequestSchema(date_from=date_since,
                                      date_to=date_to,
                                      metrics=metrics,
                                      dimension=dimension,
                                      sort=[sort]
        )
        data = await self.cli.receive_analytics_data(body)
        return month_name, data

    async def get_remainders(self, skus: list) -> list:
        sorted_skus = list(set(skus))
        remainders = await self.cli.fetch_remainders(sorted_skus)
        if remainders:
            return remainders
        return []

import asyncio
import logging
from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from src.clients.ozon.ozon_bound_client import OzonCliBound
from src.schemas.ozon_schemas import AnalyticsRequestSchema, AnalyticsMetrics, Sort, Remainder
from src.dto.dto import PostingsProductsCollection, PostingsDataByDeliveryModel, MonthlyStats
from src.infrastructure.cache import call_cache, cache
from src.mappers import parse_postings
from src.mappers.transformation_functions import parse_skus


log = logging.getLogger("ozon")

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

    async def fetch_postings(self, account_name: str, date_since: str, date_to: str) \
            -> PostingsProductsCollection:
        """
        Fetch postings from Ozon API based on delivery way and date range.

        :param date_to:
        :param date_since:
        :param account_name:
        :return: List of postings.
        """

        acc_name_fbs = f"{account_name}_FBS"
        acc_name_fbo = f"{account_name}_FBO"
        product_collection = PostingsProductsCollection()
        product_collection.postings_fbs = PostingsDataByDeliveryModel(
            model=acc_name_fbs
        )
        product_collection.postings_fbo = PostingsDataByDeliveryModel(
            model=acc_name_fbo
        )

        # Получаем отчеты
        tasks = [
            # Получаем отчеты FBS
            self.__collect_reports(reports=product_collection.postings_fbs.items,
                                   gen=self.cli.generate_reports(delivery_way="FBS",
                                                                 since=date_since,
                                                                 to=date_to)),
            # Получаем отчеты FBO
            self.__collect_reports(reports=product_collection.postings_fbo.items,
                        gen=self.cli.generate_reports(delivery_way="FBO",
                                                       since=date_since,
                                                       to=date_to))
        ]
        await asyncio.gather(*tasks)
        return product_collection

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
        return MonthlyStats(month=month_name,
                            datum=data)

    async def get_remainders(self, skus: list) -> list:
        sorted_skus = list(set(skus))
        remainders = await self.cli.fetch_remainders(sorted_skus)
        if remainders:
            return [Remainder(**r) for r in remainders]
        return []

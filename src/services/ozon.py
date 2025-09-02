import asyncio
from typing import Optional

from pydantic import BaseModel, Field

from src.clients.ozon.ozon_client import OzonCliBound


class OzonService(BaseModel):
    cli: Optional[OzonCliBound] = None

    model_config = {
        "arbitrary_types_allowed": True
    }

    async def __collect_reports(self, reports: list, gen):
        async for r in gen:
            postings = await self.__parse_posting(r)
            reports.extend(postings)

    async def __collect_skus(self):
        ...

    async def __parse_posting(self, postings: list[dict]) -> list:
        """
        Преобразует данные о доставке в нужный формат.

        :param postings: Список данных о доставке.
        :return: Список преобразованных данных.
        """
        parsed_postings = []

        for posting in postings:
            status = posting.get("status")
            products = posting.get("products", []) or []
            if products:
                # Преобразуем каждый продукт доставки в нужный формат
                chunks = [{str(prod.get("sku")): [prod.get("name"), prod.get("price"), status, str(prod.get("quantity"))]} for prod in products]
                parsed_postings.extend(chunks) # добавляем преобразованные продукты в общий список
        return parsed_postings

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
                        gen=self._cli.generate_reports(delivery_way="FBS",
                                                       since=date_since,
                                                       to=date_to)),
            # Получаем отчеты FBO
            self.__collect_reports(reports=postings[acc_name_method_fbo],
                        gen=self._cli.generate_reports(delivery_way="FBO",
                                                       since=date_since,
                                                       to=date_to))
        ]
        await asyncio.gather(*tasks)
        return postings

    async def get_remainders(self, postings: list) -> list:
        skus = []
        for s in postings:
            skus.append(next((k for k, v in s.items()), None))
        sorted_skus = list(set(skus))
        remainders = await self._cli.fetch_remainders(sorted_skus)
        if remainders:
            return remainders
        return []

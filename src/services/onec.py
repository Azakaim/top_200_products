import asyncio
import base64
import logging
from itertools import chain
from typing import Optional

from pydantic import BaseModel, Field

from src.clients.onec.onec_cli import OneCClient
from src.mappers.transformation_functions import parse_obj_by_type_base_cls
from src.schemas.onec_schemas import OneCArticlesResponse, OneCProductByUidResponse, OneCProductInfo, \
    OneCProductsResults

log = logging.getLogger("OneC-service")

class OneCService(BaseModel):
    cli: Optional[OneCClient] = Field(default=None)

    model_config = {
        "arbitrary_types_allowed": True
    }

    def model_post_init(self, __context) -> None:
        self.cli.headers['Authorization'] += self.__convert_userpass_base64()

    async def run_pipeline(self) -> OneCProductsResults | None:
        try:
            resp_to_stock = await self.cli.fetch_stock_prods()
            resp_articles: OneCArticlesResponse = await parse_obj_by_type_base_cls(resp_to_stock, OneCArticlesResponse)
            uids = [u.uid for u in resp_articles.data]
            if resp_articles.done:
                tasks_prods_by_uid = []
                for i in range(0, len(uids), 100):
                    batch = uids[i:i + 100]
                    tasks = [self.cli.fetch_prod_by_uid(u) for u in batch]
                    tasks_prods_by_uid.extend(tasks)
                products = await asyncio.gather(*tasks_prods_by_uid)
                result = [
                    OneCProductByUidResponse(**art)
                    for art in products # потому что одна задача потому и [0] объект
                ]
                return OneCProductsResults(onec_responses=result)
        except Exception as e:
            log.info(e)
        finally:
            await self.cli.aclose()
        return None

    def __convert_userpass_base64(self):
         token= base64.b64encode(self.cli.userpass.encode()).decode()
         return f" {token}"

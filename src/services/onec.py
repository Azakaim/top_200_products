import base64
from typing import Any, Optional

from pydantic import BaseModel, PrivateAttr

from src.clients.onec.onec_cli import OneCClient


class OneCService(BaseModel):
    cli: Optional[OneCClient] = None

    model_config = {"allo"}

    def model_post_init(self, __context) -> None:
        self.cli.headers['Authorization'] += self.__convert_userpass_base64()

    async def run_pipeline(self):
        resp = await self.cli.fetch_stock_prods()
        print(resp)
        v= ""

    def __convert_userpass_base64(self):
         token= base64.b64decode(self.cli.userpass.encode()).decode()
         return f" {token}"

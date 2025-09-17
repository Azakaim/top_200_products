from src.utils.http_base_client import BaseRateLimitedHttpClient


class OneCClient(BaseRateLimitedHttpClient):
    """
    OneCClient:
        Args:
            - base_url
            - prod_uid_url
            - stocks_url
    """
    prod_uid_url: str
    stocks_url: str
    headers: dict[str, str]
    userpass: str

    async def fetch_prod_by_uid(self, uid: str) -> dict:
        resp = await self.request("POST", self.prod_uid_url,json={ "uid": uid }, headers=self.headers)
        return resp

    async def fetch_stock_prods(self):
        resp = await self.request("POST", self.stocks_url, json={}, headers=self.headers)
        return resp
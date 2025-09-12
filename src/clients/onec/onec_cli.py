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

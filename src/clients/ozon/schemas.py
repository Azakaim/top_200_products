from typing import List

from pydantic import BaseModel, Field

from settings import proj_settings

from pydantic import BaseModel, field_validator


class Seller(BaseModel):
    """
    Ozon_cli API settings.
    """
    api_key: str = Field(..., description="API key for Ozon API")
    name: str = Field(..., description="Name of the seller in Ozon")
    client_id: str = Field(..., description="Client ID for Ozon API")

def _extract_sellers() -> list[Seller]:
    """
    Extracts sellers from the environment variables.
    """
    client_ids = proj_settings.OZON_CLIENT_IDS.split(',')
    api_keys = proj_settings.OZON_API_KEYS.split(',')
    names = proj_settings.OZON_NAME_LK.split(',')

    if len(client_ids) != len(api_keys) != len(names):
        raise ValueError("Client IDs, API keys, and names must have the same length.")

    return [
        Seller(api_key=api_keys[i], name=names[i], client_id=client_ids[i])
        for i in range(len(client_ids)) if client_ids[i] and api_keys[i] and names[i]
    ]


class OzonSettings(BaseModel):
    sellers: list[Seller] = Field( default=_extract_sellers(), description="List of sellers for Ozon API")
    base_url: str = proj_settings.OZON_BASE_URL
    timeout: int = 10

    # TODO: разобрать филд валидатор и почему он не работает для поля sellers
    # @field_validator('sellers', mode='before')
    # @classmethod
    # def extract_sellers(cls, values: Any) :
    #     client_ids = proj_settings.OZON_CLIENT_IDS.split(',')
    #     api_keys = proj_settings.OZON_API_KEYS.split(',')
    #     names = proj_settings.OZON_NAME_LK.split(',')
    #
    #     if not(len(client_ids) == len(api_keys) == len(names)):
    #         raise ValueError("Client IDs, API keys, and names must have the same length.")
    #
    #     return [
    #         Seller(api_key=api_keys[i], name=names[i], client_id=client_ids[i])
    #         for i in range(len(client_ids)) if client_ids[i] and api_keys[i] and names[i]
    #     ]

class LastChangedStatusDate(BaseModel):
    date_from: str = Field(default="", alias="from", description="Start date for the last changed status, in ISO 8601 format")
    to: str = Field(default="", description="End date for the last changed status, in ISO 8601 format")

    model_config = {
        "populate_by_name": True
    }

class Filter(BaseModel):
    delivery_method_id: List[str]
    is_quantum: bool
    last_changed_status_date: LastChangedStatusDate = Field(
        default_factory=lambda: LastChangedStatusDate,
        description="Date range for the last changed status, in ISO 8601 format"
    )
    order_id: int = Field(default=0, description="Order ID to filter by")
    provider_id: List[str] = Field(default_factory=list, description="List of provider IDs to filter by")
    since: str # ISO 8601 format, e.g. "2025-10-01T00:00:00Z" -- год-месяц-деньTчасы:минуты:секундыZ
    status: str = Field(default="all", description="Status of the posting, e.g. 'all', 'delivered', 'in_progress'")
    # ISO 8601 format, e.g. "2025-
    to: str # ISO 8601 format, e.g. "2025-10-01T00:00:00Z" -- год-месяц-деньTчасы:минуты:секундыZ
    warehouse_id: List[str] = Field(default_factory=list, description="List of warehouse IDs to filter by")


class With(BaseModel):
        analytics_data: bool = Field(default=False, description="Whether to include analytics data or not")
        barcodes: bool = Field(default=False, description="Whether to include barcodes or not")
        financial_data: bool = Field( default=True, description="Whether to include financial data or not")
        translit: bool = Field(default=False, description="Whether to include transliterated data or not")


class PostingRequestSchema(BaseModel):
    dir: str = Field(default="asc", description="Direction of sorting results, either 'asc' or 'desc'")
    filter: Filter = Field(
        default_factory=lambda: Filter(
            delivery_method_id=[],
            is_quantum=False,
            last_changed_status_date=LastChangedStatusDate(),
            order_id=0,
            provider_id=[],
            since="",
            status="",
            to="",
            warehouse_id=[]
        ),
        description="Filter criteria for the posting request"
    )# default_factory а не default, чтобы избежать мутабельности, если оставить default,
    # то будет использоваться один и тот же объект для всех экземпляров PostingRequest
    limit: int = Field(default=1000, description="Maximum number of records to return")
    offset: int = Field(default=0, description="Offset from which to start the search")
    add_with: With = Field(default=None, alias="with", description="Additional fields to include in the response")

    model_config = {
        "populate_by_name": True
    }
